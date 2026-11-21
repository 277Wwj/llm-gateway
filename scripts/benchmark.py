"""Fixed-workload loopback benchmark; run against the explicit mock stack.

python scripts/benchmark.py --gateway-url http://127.0.0.1:9100 --mock-url http://127.0.0.1:9101
This reports observed whole-request latency and a paired workload difference,
not production performance or the isolated CPU cost of the gateway.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import math
import os
from pathlib import Path
import time
import sys

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


def percentile(values: list[float], fraction: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    return ordered[max(0, math.ceil(len(ordered) * fraction) - 1)]


async def workload(client, url, key, model, requests, concurrency):
    gate = asyncio.Semaphore(concurrency)
    rows = []
    begin = time.perf_counter()

    async def one(index):
        async with gate:
            start = time.perf_counter()
            try:
                response = await client.post(url.rstrip("/") + "/v1/chat/completions",
                                             headers={"Authorization": "Bearer " + key},
                                             json={"model": model, "messages": [{"role": "user", "content": "benchmark"}]})
                status = response.status_code
                success = status == 200 and bool(response.json().get("choices"))
                error = None
            except (httpx.HTTPError, ValueError) as exc:
                status, success, error = None, False, type(exc).__name__
            rows.append({"index": index, "elapsed_ms": (time.perf_counter() - start) * 1000,
                         "status": status, "success": success, "error_type": error})

    await asyncio.gather(*(one(index) for index in range(requests)))
    elapsed = time.perf_counter() - begin
    durations = [row["elapsed_ms"] for row in rows]
    successes = sum(row["success"] for row in rows)
    return {"requests": requests, "concurrency": concurrency, "successes": successes,
            "success_rate": successes / requests, "wall_seconds": elapsed,
            "completed_per_second": successes / elapsed,
            "p50_ms": percentile(durations, .5), "p95_ms": percentile(durations, .95),
            "raw": sorted(rows, key=lambda row: row["index"])}


async def run(args):
    async with httpx.AsyncClient(timeout=30, trust_env=False) as client:
        health = await client.get(args.mock_url.rstrip("/") + "/health")
        health.raise_for_status()
        if health.json().get("mode") != "mock":
            raise RuntimeError("benchmark requires explicit mock provider")
        control = await client.post(args.mock_url.rstrip("/") + "/control",
                                    json={"defaults": {"delay": args.delay_ms / 1000}})
        control.raise_for_status()
        direct = await workload(client, args.mock_url, "mock-upstream-key", args.upstream_model,
                                args.requests, args.concurrency)
        gateway = await workload(client, args.gateway_url, "demo-client-key" if args.start_local else os.getenv("GATEWAY_API_KEY", "demo-client-key"),
                                 args.model, args.requests, args.concurrency)
        stats = (await client.get(args.mock_url.rstrip("/") + "/stats")).json()
    result = {"description": "Explicit mock; loopback HTTP; non-stream text; direct phase then gateway phase",
              "simulated_upstream_delay_ms": args.delay_ms, "direct": direct, "gateway": gateway,
              "observed_p50_difference_ms": gateway["p50_ms"] - direct["p50_ms"],
              "observed_p95_difference_ms": gateway["p95_ms"] - direct["p95_ms"],
              "interpretation": "Difference includes scheduling, HTTP connection pools and queueing; not isolated gateway CPU overhead.",
              "mock_stats": stats}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({key: value for key, value in result.items() if key not in {"direct", "gateway", "mock_stats"}}, ensure_ascii=False, indent=2))
    for name in ("direct", "gateway"):
        print(name, json.dumps({key: value for key, value in result[name].items() if key != "raw"}))
    return 0 if direct["success_rate"] == gateway["success_rate"] == 1 else 1


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--gateway-url", default="http://127.0.0.1:9100")
    parser.add_argument("--mock-url", default="http://127.0.0.1:9101")
    parser.add_argument("--model", default="demo-chat")
    parser.add_argument("--upstream-model", default="mock-primary")
    parser.add_argument("--requests", type=int, default=40)
    parser.add_argument("--concurrency", type=int, default=4)
    parser.add_argument("--delay-ms", type=float, default=50)
    parser.add_argument("--output", type=Path, default=Path("artifacts/benchmark.json"))
    parser.add_argument("--start-local", action="store_true", help="Start and clean up a temporary explicit mock + gateway pair")
    args = parser.parse_args()
    if args.requests < 1 or args.concurrency < 1 or args.delay_ms < 0:
        parser.error("requests/concurrency must be positive and delay-ms non-negative")
    if args.start_local:
        from scripts.local_stack import local_stack
        with local_stack() as stack:
            args.gateway_url, args.mock_url = stack.url, stack.mock_url
            raise SystemExit(asyncio.run(run(args)))
    raise SystemExit(asyncio.run(run(args)))


if __name__ == "__main__":
    main()
