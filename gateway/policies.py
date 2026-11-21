"""Single-event-loop admission and fixed-window failure policies.

The Redis limiter coordinates RPM only. Concurrency and breaker state are local
to one process; running multiple gateway workers is deliberately unsupported.
"""
from __future__ import annotations

import asyncio
import hashlib
import math
from collections import deque
from typing import Literal


def _now() -> float:
    return asyncio.get_running_loop().time()


def _positive(value: float, name: str) -> None:
    if not math.isfinite(value) or value <= 0:
        raise ValueError(f"{name} must be finite and positive")


class RateBackendError(RuntimeError):
    """Configured rate backend is unavailable. Never fall back implicitly."""


class MemoryRateLimiter:
    """First-admission anchored windows; only for explicit single-process use.

    Callers must use a finite set of configured keys, never arbitrary client IDs.
    Each admitted attempt consumes one count even if later cancelled.
    """

    def __init__(self, limit: int, window_seconds: float = 60):
        _positive(limit, "limit")
        _positive(window_seconds, "window_seconds")
        self.limit = limit
        self.window_seconds = window_seconds
        self._windows: dict[str, tuple[float, int]] = {}

    async def acquire(self, key: str) -> float | None:
        now = _now()
        expires, count = self._windows.get(key, (now + self.window_seconds, 0))
        if now >= expires:
            expires, count = now + self.window_seconds, 0
        if count >= self.limit:
            return max(0.001, expires - now)
        self._windows[key] = (expires, count + 1)
        return None

    async def close(self) -> None:
        self._windows.clear()


# EVAL is atomic. Rejected calls do not extend TTL or consume extra counts.
# Redis's own TTL clock governs the shared window; no client clock is involved.
_RATE_LUA = """
local current = tonumber(redis.call('GET', KEYS[1]) or '0')
if current >= tonumber(ARGV[1]) then
    local ttl = redis.call('PTTL', KEYS[1])
    if ttl < 0 then
        redis.call('PEXPIRE', KEYS[1], ARGV[2])
        ttl = tonumber(ARGV[2])
    end
    return math.max(ttl, 1)
end
local value = redis.call('INCR', KEYS[1])
if value == 1 or redis.call('PTTL', KEYS[1]) < 0 then
    redis.call('PEXPIRE', KEYS[1], ARGV[2])
end
return 0
"""


class RedisRateLimiter:
    def __init__(self, redis_url: str, limit: int, window_seconds: float = 60,
                 prefix: str = "llmgw"):
        _positive(limit, "limit")
        _positive(window_seconds, "window_seconds")
        from redis.asyncio import Redis

        self.limit = limit
        self.window_ms = max(1, math.ceil(window_seconds * 1000))
        self.prefix = prefix
        self._redis = Redis.from_url(redis_url, socket_connect_timeout=1,
                                     socket_timeout=1, decode_responses=False)

    async def acquire(self, key: str) -> float | None:
        # Never place a raw caller identifier or credential into Redis key names.
        redis_key = f"{self.prefix}:rpm:{hashlib.sha256(key.encode()).hexdigest()}"
        try:
            result = int(await self._redis.eval(_RATE_LUA, 1, redis_key,
                                                 self.limit, self.window_ms))
            if result < 0:
                raise ValueError("invalid rate limiter result")
            return None if result == 0 else result / 1000
        except asyncio.CancelledError:
            raise
        except Exception:
            # Redis errors may contain URLs/passwords. Export only a stable code.
            raise RateBackendError("rate_limit_backend_unavailable") from None

    async def close(self) -> None:
        await self._redis.aclose()


class QueueFull(RuntimeError):
    pass


class QueueTimeout(RuntimeError):
    pass


class Lease:
    def __init__(self, gate: ConcurrencyGate):
        self._gate = gate
        self._released = False

    def _release_now(self) -> None:
        if not self._released:
            self._released = True
            self._gate._release_now()

    async def release(self) -> None:
        # Intentionally no suspension: cancellation cannot split the handover.
        self._release_now()


class ConcurrencyGate:
    """FIFO bounded waiting with cancellation-safe slot handover."""

    def __init__(self, limit: int, queue_capacity: int):
        _positive(limit, "limit")
        if queue_capacity < 0:
            raise ValueError("queue_capacity must not be negative")
        self.limit = limit
        self.queue_capacity = queue_capacity
        self._active = 0
        self._waiters: deque[asyncio.Future[Lease]] = deque()

    async def acquire(self, deadline: float) -> Lease:
        if _now() >= deadline:
            raise QueueTimeout("queue_deadline_exceeded")
        if self._active < self.limit and not self._waiters:
            self._active += 1
            return Lease(self)
        if len(self._waiters) >= self.queue_capacity:
            raise QueueFull("provider_queue_full")
        waiter = asyncio.get_running_loop().create_future()
        self._waiters.append(waiter)
        try:
            # Shield makes timeout/cancellation cleanup own the handover race:
            # if a lease was already assigned we return that slot ourselves.
            lease = await asyncio.wait_for(asyncio.shield(waiter), deadline - _now())
            if _now() >= deadline:
                lease._release_now()
                raise QueueTimeout("queue_deadline_exceeded")
            return lease
        except (asyncio.CancelledError, TimeoutError) as exc:
            if waiter.done() and not waiter.cancelled():
                waiter.result()._release_now()
            else:
                waiter.cancel()
                try:
                    self._waiters.remove(waiter)
                except ValueError:
                    pass
            # Preserve cancellation, but expose a distinct admission timeout.
            if isinstance(exc, asyncio.CancelledError):
                raise
            raise QueueTimeout("queue_deadline_exceeded") from None

    def _release_now(self) -> None:
        self._active -= 1
        assert self._active >= 0, "unbalanced concurrency lease"
        while self._waiters:
            waiter = self._waiters.popleft()
            if waiter.done():
                continue
            self._active += 1
            waiter.set_result(Lease(self))
            break

    def snapshot(self) -> dict:
        return {"active": self._active, "waiting": len(self._waiters)}


class _Permit:
    __slots__ = ("generation", "probe")

    def __init__(self, generation: int, probe: bool):
        self.generation = generation
        self.probe = probe


class CircuitBreaker:
    """Completion-time fixed-window error ratio, one half-open probe.

    Caller classifies upstream outcomes. Parameter/auth errors and client
    cancellation are neutral; they do not change the error-rate denominator.
    """

    def __init__(self, min_samples: int, failure_ratio: float,
                 window_seconds: float, cooldown: float):
        _positive(min_samples, "min_samples")
        _positive(failure_ratio, "failure_ratio")
        if failure_ratio > 1:
            raise ValueError("failure_ratio must be at most one")
        _positive(window_seconds, "window_seconds")
        _positive(cooldown, "cooldown")
        self.min_samples = min_samples
        self.failure_ratio = failure_ratio
        self.window_seconds = window_seconds
        self.cooldown = cooldown
        self._state = "closed"
        self._window_started: float | None = None
        self._samples = self._failures = 0
        self._open_until = 0.0
        self._generation = 0
        self._probe_inflight = False
        self._permits: set[_Permit] = set()

    def _refresh(self) -> float:
        now = _now()
        if self._window_started is None:
            self._window_started = now
        if self._state == "closed" and now >= self._window_started + self.window_seconds:
            # Advance along fixed boundaries, not a rolling/sliding window.
            elapsed = int((now - self._window_started) // self.window_seconds)
            self._window_started += elapsed * self.window_seconds
            self._samples = self._failures = 0
        if self._state == "open" and now >= self._open_until:
            self._state = "half_open"
        return now

    def allow(self) -> _Permit | None:
        self._refresh()
        if self._state == "open" or (self._state == "half_open" and self._probe_inflight):
            return None
        probe = self._state == "half_open"
        if probe:
            self._probe_inflight = True
        permit = _Permit(self._generation, probe)
        self._permits.add(permit)
        return permit

    def is_current(self, token: _Permit | None) -> bool:
        """Recheck a permit after waiting, before starting an upstream attempt.

        Queued work can outlive a closed->open transition. Such stale permits
        must be recorded neutral and their slots released, not sent upstream.
        """
        self._refresh()
        return bool(token is not None and token in self._permits
                    and token.generation == self._generation
                    and (self._state == "closed" and not token.probe
                         or self._state == "half_open" and token.probe))

    def _open(self, now: float) -> None:
        self._state = "open"
        self._open_until = now + self.cooldown
        self._generation += 1
        self._probe_inflight = False

    def record(self, token: _Permit | None,
               outcome: Literal["success", "failure", "neutral"]) -> None:
        if outcome not in {"success", "failure", "neutral"}:
            raise ValueError("unknown breaker outcome")
        if token is None or token not in self._permits:
            return
        self._permits.remove(token)
        now = self._refresh()
        # A late completion from before opening must not close a newer circuit.
        if token.generation != self._generation:
            return
        if token.probe:
            self._probe_inflight = False
            if outcome == "failure":
                self._open(now)
            elif outcome == "success":
                self._state = "closed"
                self._generation += 1
                self._samples = self._failures = 0
                self._window_started = now
            return
        if self._state != "closed" or outcome == "neutral":
            return
        self._samples += 1
        self._failures += outcome == "failure"
        if self._samples >= self.min_samples and self._failures / self._samples >= self.failure_ratio:
            self._open(now)

    def snapshot(self) -> dict:
        now = self._refresh()
        return {"state": self._state, "samples": self._samples,
                "failures": self._failures, "probe_inflight": self._probe_inflight,
                "inflight": len(self._permits),
                "retry_after": max(0.0, self._open_until - now) if self._state == "open" else 0.0}
