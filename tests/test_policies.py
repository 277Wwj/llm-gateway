import asyncio
import os
import uuid

import pytest

from gateway import policies
from gateway.policies import (
    CircuitBreaker, ConcurrencyGate, MemoryRateLimiter, QueueFull, QueueTimeout,
    RateBackendError, RedisRateLimiter,
)


@pytest.mark.asyncio
async def test_memory_fixed_window_does_not_extend_on_rejection(monkeypatch):
    clock = [10.0]
    monkeypatch.setattr(policies, "_now", lambda: clock[0])
    limiter = MemoryRateLimiter(2, 60)
    assert await limiter.acquire("logical") is None
    assert await limiter.acquire("logical") is None
    assert await limiter.acquire("logical") == 60
    clock[0] = 69
    assert await limiter.acquire("logical") == 1
    assert await limiter.acquire("another-configured-key") is None
    clock[0] = 70
    assert await limiter.acquire("logical") is None
    await limiter.close()


@pytest.mark.asyncio
async def test_memory_concurrent_requests_share_limit():
    limiter = MemoryRateLimiter(7)
    results = await asyncio.gather(*(limiter.acquire("configured-client") for _ in range(100)))
    assert results.count(None) == 7
    assert sum(x is not None for x in results) == 93


@pytest.mark.asyncio
async def test_gate_capacity_fifo_and_idempotent_release():
    gate = ConcurrencyGate(1, 2)
    deadline = asyncio.get_running_loop().time() + 10
    first = await gate.acquire(deadline)
    second = asyncio.create_task(gate.acquire(deadline))
    third = asyncio.create_task(gate.acquire(deadline))
    await asyncio.sleep(0)
    assert gate.snapshot() == {"active": 1, "waiting": 2}
    with pytest.raises(QueueFull):
        await gate.acquire(deadline)
    await first.release()
    await first.release()
    second_lease = await second
    assert not third.done()
    assert gate.snapshot() == {"active": 1, "waiting": 1}
    await second_lease.release()
    third_lease = await third
    await third_lease.release()
    assert gate.snapshot() == {"active": 0, "waiting": 0}


@pytest.mark.asyncio
async def test_gate_waiting_cancellation_frees_queue_capacity():
    gate = ConcurrencyGate(1, 1)
    deadline = asyncio.get_running_loop().time() + 10
    lease = await gate.acquire(deadline)
    waiting = asyncio.create_task(gate.acquire(deadline))
    await asyncio.sleep(0)
    waiting.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiting
    assert gate.snapshot() == {"active": 1, "waiting": 0}
    replacement = asyncio.create_task(gate.acquire(deadline))
    await asyncio.sleep(0)
    await lease.release()
    await (await replacement).release()
    assert gate.snapshot() == {"active": 0, "waiting": 0}


@pytest.mark.asyncio
async def test_gate_cancel_during_slot_handover_returns_reserved_slot():
    gate = ConcurrencyGate(1, 1)
    deadline = asyncio.get_running_loop().time() + 10
    lease = await gate.acquire(deadline)
    waiting = asyncio.create_task(gate.acquire(deadline))
    await asyncio.sleep(0)
    await lease.release()  # Assign a lease before the waiting coroutine resumes.
    waiting.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiting
    assert gate.snapshot() == {"active": 0, "waiting": 0}
    await (await gate.acquire(deadline)).release()


@pytest.mark.asyncio
async def test_gate_timeout_cleans_waiter():
    gate = ConcurrencyGate(1, 1)
    now = asyncio.get_running_loop().time()
    lease = await gate.acquire(now + 5)
    with pytest.raises(QueueTimeout):
        await gate.acquire(now + 0.03)
    assert gate.snapshot() == {"active": 1, "waiting": 0}
    with pytest.raises(QueueTimeout):
        await gate.acquire(now - 1)
    await lease.release()
    assert gate.snapshot() == {"active": 0, "waiting": 0}


@pytest.mark.asyncio
async def test_gate_no_queue_mode_rejects_immediately():
    gate = ConcurrencyGate(1, 0)
    deadline = asyncio.get_running_loop().time() + 5
    lease = await gate.acquire(deadline)
    with pytest.raises(QueueFull):
        await gate.acquire(deadline)
    await lease.release()


@pytest.mark.asyncio
async def test_many_cancelled_and_completed_waiters_leave_no_slots():
    gate = ConcurrencyGate(3, 100)
    deadline = asyncio.get_running_loop().time() + 10
    owners = [await gate.acquire(deadline) for _ in range(3)]
    tasks = [asyncio.create_task(gate.acquire(deadline)) for _ in range(60)]
    await asyncio.sleep(0)
    for task in tasks[::2]:
        task.cancel()
    for owner in owners:
        await owner.release()
    for task in tasks:
        try:
            lease = await task
        except asyncio.CancelledError:
            continue
        await lease.release()
    assert gate.snapshot() == {"active": 0, "waiting": 0}


def breaker_clock(monkeypatch):
    clock = [100.0]
    monkeypatch.setattr(policies, "_now", lambda: clock[0])
    breaker = CircuitBreaker(min_samples=4, failure_ratio=0.5,
                             window_seconds=20, cooldown=5)
    return breaker, clock


def test_breaker_minimum_ratio_and_neutral_not_counted(monkeypatch):
    breaker, _ = breaker_clock(monkeypatch)
    for _ in range(100):
        breaker.record(breaker.allow(), "neutral")
    assert breaker.snapshot()["samples"] == 0
    for outcome in ["failure", "success", "failure"]:
        breaker.record(breaker.allow(), outcome)
    assert breaker.snapshot()["state"] == "closed"
    breaker.record(breaker.allow(), "success")
    assert breaker.snapshot()["state"] == "open"
    assert breaker.snapshot()["samples"] == 4
    assert breaker.allow() is None


def test_breaker_fixed_window_resets_counts(monkeypatch):
    breaker, clock = breaker_clock(monkeypatch)
    for _ in range(3):
        breaker.record(breaker.allow(), "failure")
    clock[0] = 121
    breaker.record(breaker.allow(), "success")
    assert breaker.snapshot()["samples"] == 1
    assert breaker.snapshot()["failures"] == 0
    clock[0] = 139.9
    assert breaker.snapshot()["samples"] == 1
    clock[0] = 140
    assert breaker.snapshot()["samples"] == 0


def test_breaker_single_probe_cancel_releases_and_success_recovers(monkeypatch):
    breaker, clock = breaker_clock(monkeypatch)
    for _ in range(4):
        breaker.record(breaker.allow(), "failure")
    clock[0] += 5
    probe = breaker.allow()
    assert probe is not None
    assert all(breaker.allow() is None for _ in range(100))
    assert breaker.snapshot()["probe_inflight"]
    breaker.record(probe, "neutral")
    assert not breaker.snapshot()["probe_inflight"]
    replacement = breaker.allow()
    assert replacement is not None
    breaker.record(replacement, "success")
    assert breaker.snapshot()["state"] == "closed"
    assert breaker.snapshot()["inflight"] == 0
    assert breaker.snapshot()["samples"] == 0


def test_breaker_probe_failure_restarts_cooldown(monkeypatch):
    breaker, clock = breaker_clock(monkeypatch)
    for _ in range(4):
        breaker.record(breaker.allow(), "failure")
    clock[0] += 5
    breaker.record(breaker.allow(), "failure")
    assert breaker.snapshot()["state"] == "open"
    assert breaker.snapshot()["retry_after"] == 5


def test_breaker_late_and_duplicate_completions_do_not_corrupt_new_state(monkeypatch):
    breaker, clock = breaker_clock(monkeypatch)
    stale = breaker.allow()
    for _ in range(4):
        breaker.record(breaker.allow(), "failure")
    breaker.record(stale, "success")
    breaker.record(stale, "success")
    assert breaker.snapshot()["state"] == "open"
    clock[0] += 5
    probe = breaker.allow()
    breaker.record(probe, "success")
    breaker.record(probe, "failure")
    assert breaker.snapshot()["state"] == "closed"
    assert breaker.snapshot()["inflight"] == 0


def test_breaker_queued_permits_are_revoked_when_circuit_opens(monkeypatch):
    breaker, clock = breaker_clock(monkeypatch)
    queued = [breaker.allow() for _ in range(20)]
    assert all(breaker.is_current(token) for token in queued)
    for _ in range(4):
        breaker.record(breaker.allow(), "failure")
    assert all(not breaker.is_current(token) for token in queued)
    clock[0] += 5
    probe = breaker.allow()
    assert breaker.is_current(probe)
    assert all(not breaker.is_current(token) for token in queued)
    breaker.record(probe, "success")
    assert not breaker.is_current(probe)
    assert all(not breaker.is_current(token) for token in queued)
    for token in queued:
        breaker.record(token, "neutral")
    assert breaker.snapshot()["inflight"] == 0
    assert breaker.snapshot()["samples"] == 0


@pytest.mark.asyncio
async def test_redis_failure_is_explicit_and_sanitized():
    limiter = RedisRateLimiter("redis://:secret-must-not-leak@127.0.0.1:1/0", 2)
    try:
        with pytest.raises(RateBackendError) as caught:
            await limiter.acquire("client")
        assert str(caught.value) == "rate_limit_backend_unavailable"
    finally:
        await limiter.close()


@pytest.mark.asyncio
@pytest.mark.skipif(not os.getenv("LLMGW_TEST_REDIS_URL"),
                    reason="requires explicitly configured real Redis; not a fake validation")
async def test_real_redis_lua_atomic_across_clients_and_ttl_recovery():
    url = os.environ["LLMGW_TEST_REDIS_URL"]
    prefix = "llmgw-test-" + uuid.uuid4().hex
    # Separate pools represent two gateway clients sharing a Redis limit.
    left = RedisRateLimiter(url, 8, window_seconds=1, prefix=prefix)
    right = RedisRateLimiter(url, 8, window_seconds=1, prefix=prefix)
    try:
        results = await asyncio.gather(*[
            (left if i % 2 else right).acquire("configured-client") for i in range(64)
        ])
        assert results.count(None) == 8
        assert all(0 < value <= 1 for value in results if value is not None)
        await asyncio.sleep(1.05)
        assert await right.acquire("configured-client") is None
    finally:
        await left.close()
        await right.close()
