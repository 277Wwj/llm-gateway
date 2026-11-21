"""Real Redis/Lua HTTP acceptance. Requires a separately started Redis server.

No FLUSHDB, SHUTDOWN, or configuration mutation is issued against that server.
Each run uses a random key prefix. Windows run: python scripts/verify_redis.py.
"""
from __future__ import annotations

import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import json
from pathlib import Path
import sys
import uuid

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import httpx
import redis

from scripts.local_stack import AUTH, local_stack


def verify(redis_url: str, output: Path) -> dict:
    output.parent.mkdir(parents=True, exist_ok=True)
    redis_client = redis.Redis.from_url(redis_url, socket_timeout=2, socket_connect_timeout=2)
    try:
        version = redis_client.info("server")["redis_version"]
    finally:
        redis_client.close()
    prefix = "llmgw-http-" + uuid.uuid4().hex
    overrides = {"GATEWAY_RATE_BACKEND": "redis", "GATEWAY_REDIS_URL": redis_url,
                 "GATEWAY_REDIS_PREFIX": prefix, "GATEWAY_MAX_ATTEMPTS": "1",
                 "GATEWAY_DEADLINE_SECONDS": "4"}
    payload = {"model": "primary-only", "messages": [{"role": "user", "content": "Redis HTTP verification"}]}
    report = {"timestamp_utc": datetime.now(timezone.utc).isoformat(),
              "redis_version": version, "real_redis": True, "real_llm": False,
              "workload": {"http_requests": 16, "gateway_processes": 2,
                           "provider_rpm": 8, "route": "primary-only", "max_attempts": 1}}
    with local_stack(provider_overrides={"primary": {"rpm": 8}}, env_overrides=overrides,
                     artifact_dir=output.parent / "redis-stack-a") as left:
        with local_stack(provider_overrides={"primary": {"rpm": 8}}, env_overrides=overrides,
                         artifact_dir=output.parent / "redis-stack-b") as right:
            assert left.client.get("/health").json()["rate_backend"] == "redis"
            assert right.client.get("/health").json()["rate_backend"] == "redis"

            def request(i):
                # Independent TCP clients and two separate gateway processes.
                with httpx.Client(trust_env=False, headers=AUTH, timeout=6) as client:
                    response = client.post((left.url if i % 2 else right.url) + "/v1/chat/completions", json=payload)
                    assert response.headers.get("x-request-id")
                    if response.status_code == 429:
                        assert response.json()["error"]["code"] == "rate_limited"
                        assert float(response.headers["retry-after"]) > 0
                    return response.status_code

            with ThreadPoolExecutor(max_workers=16) as pool:
                results = list(pool.map(request, range(16)))
            counts = Counter(results)
            assert counts == {200: 8, 429: 8}, counts
            left_stats, left_state = left.wait_idle()
            right_stats, right_state = right.wait_idle()
            assert left_stats["requests"] + right_stats["requests"] == 8
            report["shared_rpm_http"] = {"statuses": dict(counts),
                                         "upstream_requests": left_stats["requests"] + right_stats["requests"],
                                         "left_state": left_state, "right_state": right_state}

    # This second stack explicitly selects Redis at a closed endpoint. A successful
    # mock response here would reveal an implicit memory fallback. Never stop an
    # externally supplied Redis server to create a fault.
    failure_overrides = dict(overrides, GATEWAY_REDIS_URL="redis://127.0.0.1:1/0",
                             GATEWAY_DEADLINE_SECONDS="3")
    with local_stack(env_overrides=failure_overrides,
                     artifact_dir=output.parent / "redis-unavailable-stack") as stack:
        response = stack.client.post("/v1/chat/completions", json=payload)
        assert response.status_code == 503, response.text
        assert response.json()["error"]["code"] == "rate_backend_unavailable"
        stats, state = stack.wait_idle()
        assert stats["requests"] == 0
        report["backend_failure"] = {"status": response.status_code,
                                     "error": response.json()["error"]["code"],
                                     "upstream_requests": stats["requests"], "state": state}
    report["passed"] = True
    output.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--redis-url", default="redis://127.0.0.1:16379/15")
    parser.add_argument("--output", type=Path, default=ROOT / "artifacts/redis-http.json")
    args = parser.parse_args()
    result = verify(args.redis_url, args.output)
    print(json.dumps({"passed": result["passed"], "redis_version": result["redis_version"],
                      "statuses": result["shared_rpm_http"]["statuses"],
                      "backend_failure_status": result["backend_failure"]["status"]}))
