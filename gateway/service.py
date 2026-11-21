import asyncio
import json
import logging
from dataclasses import dataclass

import httpx

from .config import Settings
from .metrics import Metrics
from .policies import (CircuitBreaker, ConcurrencyGate, MemoryRateLimiter, RedisRateLimiter,
                       QueueFull, QueueTimeout, RateBackendError)
from .providers import Adapter, ProviderError, event_has_content

log = logging.getLogger("gateway")
if not log.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    log.addHandler(handler)
log.setLevel(logging.INFO)
log.propagate = False


@dataclass
class Attempt:
    provider: object
    lease: object
    token: object
    stream: object = None
    closed: bool = False
    started: float = 0
    request_id: str = ""
    reported: bool = False


class Service:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.routing = settings.load_routing()
        self.providers = {p.name: p for p in self.routing.providers}
        self.metrics = Metrics()
        self.client = httpx.AsyncClient(trust_env=False, follow_redirects=False,
                                       limits=httpx.Limits(max_connections=sum(p.concurrency for p in self.providers.values()),
                                                          max_keepalive_connections=32))
        self.adapter = Adapter(self.client)
        self.gates = {p.name: ConcurrencyGate(p.concurrency, p.queue_capacity) for p in self.providers.values()}
        self.breakers = {p.name: CircuitBreaker(settings.breaker_min_samples, settings.breaker_failure_ratio,
                        settings.breaker_window_seconds, settings.breaker_cooldown_seconds) for p in self.providers.values()}
        self.rates = {p.name: (MemoryRateLimiter(p.rpm) if settings.rate_backend == "memory" else
                     RedisRateLimiter(settings.redis_url.get_secret_value(), p.rpm, prefix=settings.redis_prefix))
                      for p in self.providers.values()}

    async def close(self):
        await self.client.aclose()
        for rate in self.rates.values():
            await rate.close()

    def state(self):
        result = {}
        for name in self.providers:
            gate, breaker = self.gates[name].snapshot(), self.breakers[name].snapshot()
            self.metrics.active.labels(name).set(gate["active"])
            self.metrics.waiting.labels(name).set(gate["waiting"])
            self.metrics.circuit.labels(name).set({"closed": 0, "open": 1, "half_open": 2, "half-open": 2}[breaker["state"]])
            result[name] = {"gate": gate, "breaker": breaker}
        return result

    async def cleanup(self, attempt, outcome):
        if not attempt or attempt.closed:
            return
        attempt.closed = True
        try:
            if attempt.stream:
                await attempt.stream.aclose()
        finally:
            self.breakers[attempt.provider.name].record(attempt.token, outcome)
            await attempt.lease.release()

    async def protected_cleanup(self, attempt, outcome):
        # ASGI disconnect cancels the body task; cleanup must survive its cancel scope.
        import anyio
        with anyio.CancelScope(shield=True):
            await self.cleanup(attempt, outcome)

    def finish(self, outcome, started, request_id):
        elapsed = asyncio.get_running_loop().time() - started
        self.metrics.outcomes.labels(outcome).inc()
        self.metrics.duration.labels(outcome).observe(elapsed)
        log.info("request_id=%s outcome=%s elapsed=%.4f", request_id, outcome, elapsed)

    def finish_stream(self, attempt, outcome):
        if not attempt.reported:
            attempt.reported = True
            self.finish(outcome, attempt.started, attempt.request_id)

    async def execute(self, body, request_id):
        loop = asyncio.get_running_loop()
        started = loop.time()
        deadline = started + self.settings.deadline_seconds
        self.metrics.requests.inc()
        handed_off = False
        final_outcome = "error"
        current = None
        try:
            route = self.routing.routes.get(body.model)
            if route is None:
                raise ProviderError("unknown_model", 404)
            candidates = [self.providers[n] for n in route if body.capabilities() <= self.providers[n].capabilities]
            if not candidates:
                raise ProviderError("unsupported_capability", 400)
            last = ProviderError("no_available_provider", 503)
            actual_attempts = 0
            not_before = {}
            async with asyncio.timeout_at(deadline):
                for index in range(self.settings.max_attempts):
                    provider = candidates[index % len(candidates)]
                    name = provider.name
                    wait = max(0, not_before.get(name, 0) - loop.time())
                    if wait:
                        if loop.time() + wait >= deadline:
                            continue
                        await asyncio.sleep(wait)
                    token = self.breakers[name].allow()
                    if token is None:
                        last = ProviderError("circuit_open", 503)
                        continue
                    current = None
                    queue_started = loop.time()
                    try:
                        lease = await self.gates[name].acquire(min(deadline, loop.time() + self.settings.queue_timeout_seconds))
                    except (QueueFull, QueueTimeout) as exc:
                        self.breakers[name].record(token, "neutral")
                        last = ProviderError("queue_full" if isinstance(exc, QueueFull) else "queue_timeout", 503)
                        continue
                    except BaseException:
                        self.breakers[name].record(token, "neutral")
                        raise
                    finally:
                        self.metrics.queue.labels(name).observe(loop.time() - queue_started)
                    current = Attempt(provider, lease, token, started=started, request_id=request_id)
                    try:
                        if not self.breakers[name].is_current(token):
                            last = ProviderError("circuit_open", 503)
                            continue
                        try:
                            retry_after = await self.rates[name].acquire(name)
                        except RateBackendError:
                            raise ProviderError("rate_backend_unavailable", 503) from None
                        if retry_after is not None:
                            raise ProviderError("rate_limited", 429, retryable=True, retry_after=retry_after)
                        # Redis admission yields; another in-flight failure may revoke
                        # this permit while that atomic RPM operation is pending.
                        if not self.breakers[name].is_current(token):
                            last = ProviderError("circuit_open", 503)
                            continue
                        self.metrics.attempts.labels(name).inc()
                        if actual_attempts:
                            self.metrics.retries.labels(name).inc()
                        actual_attempts += 1
                        log.info("request_id=%s provider=%s attempt=%d", request_id, name, actual_attempts)
                        payload = body.model_dump(exclude_none=True)
                        if body.stream:
                            current.stream = await self.adapter.open_stream(provider, payload, deadline, request_id)
                            handed_off = True
                            return self.stream_body(current, started, deadline, request_id), current
                        result = await self.adapter.complete(provider, payload, deadline, request_id)
                        if result["choices"][0]["message"].get("content"):
                            self.metrics.first_content.observe(loop.time() - started)
                        await self.protected_cleanup(current, "success")
                        final_outcome = "success"
                        return result, None
                    except ProviderError as exc:
                        await self.protected_cleanup(current, "failure" if exc.fault else "neutral")
                        last = exc
                        if exc.retry_after:
                            not_before[name] = loop.time() + exc.retry_after
                        if not exc.retryable:
                            raise
                        # Retry-After is respected for a retry to the same provider. Switching to
                        # a different provider has an independent rate window and need not wait.
                        next_same = candidates[(index + 1) % len(candidates)].name == name
                        delay = max(self.settings.retry_delay_seconds, (exc.retry_after or 0) if next_same else 0)
                        if index + 1 < self.settings.max_attempts:
                            if loop.time() + delay >= deadline:
                                raise last
                            await asyncio.sleep(delay)
                    except asyncio.CancelledError:
                        await self.protected_cleanup(current, "failure" if loop.time() >= deadline else "neutral")
                        raise
                    finally:
                        if not handed_off:
                            await self.protected_cleanup(current, "neutral")
                raise last
        except TimeoutError:
            final_outcome = "timeout"
            raise ProviderError("deadline_exceeded", 504) from None
        except ProviderError as exc:
            if exc.status == 504:
                final_outcome = "timeout"
            raise
        except asyncio.CancelledError:
            final_outcome = "cancelled"
            raise
        finally:
            if not handed_off:
                await self.protected_cleanup(current, "neutral")
                self.finish(final_outcome, started, request_id)

    async def stream_body(self, attempt, started, deadline, request_id):
        outcome, breaker_outcome, content_seen = "error", "neutral", False
        try:
            async with asyncio.timeout_at(deadline):
                async for data in attempt.stream:
                    if not content_seen and event_has_content(data):
                        self.metrics.first_content.observe(asyncio.get_running_loop().time() - started)
                        content_seen = True
                    if data == b"data: [DONE]\n\n":
                        outcome, breaker_outcome = "success", "success"
                    yield data
            outcome, breaker_outcome = "success", "success"
        except asyncio.CancelledError:
            if outcome != "success":
                outcome = "cancelled"
            raise
        except GeneratorExit:
            if outcome != "success":
                outcome = "cancelled"
            raise
        except (ProviderError, TimeoutError) as exc:
            timeout = isinstance(exc, TimeoutError) or getattr(exc, "status", None) == 504
            outcome = "timeout" if timeout else "error"
            breaker_outcome = "failure"
            # Once downstream headers/body are sent, never retry or splice another model.
            payload = {"error": {"code": "deadline_exceeded" if timeout else "upstream_stream_error", "request_id": request_id}}
            yield ("event: error\ndata: " + json.dumps(payload) + "\n\n").encode()
        finally:
            await self.protected_cleanup(attempt, breaker_outcome)
            self.finish_stream(attempt, outcome)
