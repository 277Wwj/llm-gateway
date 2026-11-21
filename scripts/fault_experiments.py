"""Reproducible fault injection over real HTTP; writes per-scenario raw evidence.

python scripts/fault_experiments.py --output artifacts/faults.json
All subprocesses are temporary and all model outputs are explicit mocks.
"""
from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path
import sys
import time
from datetime import datetime, timezone

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scripts.local_stack import AUTH, local_stack


def body(**kwargs):
    return {"model": "demo-chat", "messages": [{"role": "user", "content": "scripted fault experiment"}], **kwargs}


def record(stack, name, request_body, **expected):
    begin = time.perf_counter()
    response = stack.client.post("/v1/chat/completions", json=request_body)
    elapsed = time.perf_counter() - begin
    stats, state = stack.wait_idle()
    row = {"scenario": name, "status": response.status_code, "elapsed_seconds": elapsed,
           "request_id": response.headers.get("x-request-id"), "mock_stats": stats, "gateway_state": state,
           "response": response.text, "expected": expected}
    checks = [response.status_code == expected.get("status", 200)]
    if "attempts" in expected:
        checks.append(stats["requests"] == expected["attempts"])
    if "done" in expected:
        checks.append(("[DONE]" in response.text) == expected["done"])
    if "max_seconds" in expected:
        checks.append(elapsed < expected["max_seconds"])
    row["passed"] = all(checks)
    return row


async def cancellation(stack):
    stack.control(defaults={"chunks": ["first", "second", "third"], "chunk_delay": .2})
    async with httpx.AsyncClient(base_url=stack.url, headers=AUTH, trust_env=False) as client:
        async with client.stream("POST", "/v1/chat/completions", json=body(stream=True)) as response:
            async for line in response.aiter_lines():
                if "first" in line:
                    break
    stats, state = stack.wait_idle()
    return {"scenario": "client_disconnect_after_first_content", "status": response.status_code,
            "mock_stats": stats, "gateway_state": state, "passed": stats["active"] == 0 and stats["cancelled"] == 1}


async def overload(stack):
    stack.control(defaults={"delay": .3})
    async with httpx.AsyncClient(base_url=stack.url, headers=AUTH, trust_env=False) as client:
        responses = await asyncio.gather(*(client.post("/v1/chat/completions", json=body(model="primary-only")) for _ in range(6)))
    stats, state = stack.wait_idle()
    statuses = sorted(response.status_code for response in responses)
    return {"scenario": "concurrency_1_queue_1_six_requests", "statuses": statuses,
            "mock_stats": stats, "gateway_state": state, "passed": statuses == [200, 200, 503, 503, 503, 503]}


def run(output):
    rows = []
    failure = None
    try:
        with local_stack() as stack:
            for label, scenario in [("upstream_500_before_output", {"status": 500}),
                                    ("upstream_429_before_output", {"status": 429, "retry_after": 1}),
                                    ("sse_error_before_first_event", {"pre_event_error": True}),
                                    ("sse_eof_before_first_event", {"pre_event_error": "eof"})]:
                stack.control(models={"mock-primary": [scenario]})
                rows.append(record(stack, label, body(stream=True), status=200, attempts=2, done=True))
            stack.control(defaults={"midstream_error": True, "chunks": ["first", "must-not-appear"]})
            rows.append(record(stack, "sse_eof_after_first_content_no_switch", body(stream=True), status=200, attempts=1, done=False))
            rows.append(asyncio.run(cancellation(stack)))
        with local_stack(provider_overrides={"primary": {"concurrency": 1, "queue_capacity": 1}},
                         env_overrides={"GATEWAY_MAX_ATTEMPTS": "1"}) as stack:
            rows.append(asyncio.run(overload(stack)))
        with local_stack(env_overrides={"GATEWAY_DEADLINE_SECONDS": ".25"}) as stack:
            stack.control(defaults={"first_event_delay": 1})
            rows.append(record(stack, "first_event_total_deadline", body(stream=True), status=504, attempts=1, max_seconds=.8))
        with local_stack(env_overrides={"GATEWAY_MAX_ATTEMPTS": "1", "GATEWAY_BREAKER_MIN_SAMPLES": "2",
                                       "GATEWAY_BREAKER_FAILURE_RATIO": "1", "GATEWAY_BREAKER_COOLDOWN_SECONDS": ".25"}) as stack:
            stack.control(defaults={"status": 500})
            rows.append(record(stack, "breaker_failure_1", body(model="primary-only"), status=502, attempts=1))
            rows.append(record(stack, "breaker_failure_2", body(model="primary-only"), status=502, attempts=2))
            rows.append(record(stack, "breaker_open_rejects_without_upstream", body(model="primary-only"), status=503, attempts=2))
            time.sleep(.3)
            stack.control()
            rows.append(record(stack, "breaker_half_open_probe_recovers", body(model="primary-only"), status=200, attempts=1))
    except Exception as exc:
        failure = {"type": type(exc).__name__, "message": str(exc)}
    result = {"mode": "explicit_mock_real_loopback_http", "real_llm": False,
              "recorded_at_utc": datetime.now(timezone.utc).isoformat(), "scenarios": rows,
              "harness_failure": failure, "passed": failure is None and all(row["passed"] for row in rows)}
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"passed": result["passed"], "scenarios": len(rows), "output": str(output), "harness_failure": failure}))
    return 0 if result["passed"] else 1


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=Path("artifacts/faults.json"))
    raise SystemExit(run(parser.parse_args().output))
