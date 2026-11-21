"""Opt-in smoke checks against an already-running REAL-mode gateway (billable)."""
import argparse
import json
import os
from pathlib import Path
import time
import httpx


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="http://127.0.0.1:9100")
    parser.add_argument("--model", required=True, help="Use a single-provider alias to establish provenance")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    key = os.environ.get("GATEWAY_API_KEY")
    if not key or key == "demo-client-key":
        parser.error("export a private GATEWAY_API_KEY for the real gateway")
    with httpx.Client(base_url=args.url, headers={"Authorization": "Bearer " + key}, timeout=90, trust_env=False) as client:
        health = client.get("/health")
        health.raise_for_status()
        if health.json()["mode"] != "real":
            raise RuntimeError("Refusing to count mock as real provider verification")
        checks = [
            ("text", {"messages": [{"role": "user", "content": "Reply with OK."}]}),
            ("json", {"messages": [{"role": "user", "content": 'Return a JSON object with key ok and boolean true.'}], "response_format": {"type": "json_object"}}),
            ("tools", {"messages": [{"role": "user", "content": "Call lookup for city Beijing."}], "tool_choice": "required", "tools": [{"type": "function", "function": {"name": "lookup", "parameters": {"type": "object", "properties": {"city": {"type": "string"}}, "required": ["city"]}}}]}),
            ("sse", {"messages": [{"role": "user", "content": "Reply with OK."}], "stream": True}),
        ]
        rows = []
        for name, fields in checks:
            start = time.perf_counter()
            response = client.post("/v1/chat/completions", json={"model": args.model, "max_tokens": 100, **fields})
            passed = response.status_code == 200
            if passed:
                if name == "sse":
                    passed = "data: [DONE]" in response.text and "event: error" not in response.text
                else:
                    message = response.json()["choices"][0]["message"]
                    if name == "tools":
                        passed = bool(message.get("tool_calls"))
                    elif name == "json":
                        passed = isinstance(json.loads(message["content"]), dict)
                    else:
                        passed = bool(message.get("content"))
            rows.append({"case": name, "passed": passed, "status": response.status_code,
                         "elapsed_ms": round((time.perf_counter() - start) * 1000, 2), "request_id": response.headers.get("x-request-id")})
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps({"mode": "real", "alias": args.model, "checks": rows}, indent=2), encoding="utf-8")
    print(json.dumps(rows, indent=2))
    raise SystemExit(0 if all(row["passed"] for row in rows) else 1)


if __name__ == "__main__":
    main()
