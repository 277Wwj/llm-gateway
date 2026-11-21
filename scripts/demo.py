"""Normal text, JSON object, function call and SSE through a running gateway."""
import json
import os
import httpx

with httpx.Client(base_url="http://127.0.0.1:9100", trust_env=False, timeout=30,
                  headers={"Authorization": "Bearer " + os.getenv("GATEWAY_API_KEY", "demo-client-key")}) as client:
    base = {"model": "demo-chat", "messages": [{"role": "user", "content": "Reply with a JSON object if JSON mode is requested."}]}
    for name, extra in [
        ("text", {}),
        ("json", {"response_format": {"type": "json_object"}}),
        ("tools", {"tools": [{"type": "function", "function": {"name": "lookup", "parameters": {"type": "object", "properties": {}}}}]}),
    ]:
        response = client.post("/v1/chat/completions", json={**base, **extra})
        response.raise_for_status()
        print(name, response.headers["x-request-id"], json.dumps(response.json()["choices"][0]["message"], ensure_ascii=False))
    with client.stream("POST", "/v1/chat/completions", json={**base, "stream": True}) as response:
        response.raise_for_status()
        print("SSE", response.headers["x-request-id"])
        for line in response.iter_lines():
            if line:
                print(line)
