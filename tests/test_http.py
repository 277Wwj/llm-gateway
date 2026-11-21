"""Acceptance through actual TCP/HTTP, including disconnect and recovery."""
import asyncio
import json
import time

import httpx
import pytest

from conftest import AUTH


def payload(**extra):
    return {"model": "demo-chat", "messages": [{"role": "user", "content": "hello"}], **extra}


TOOLS = [{"type": "function", "function": {"name": "lookup", "parameters": {"type": "object", "properties": {}}}}]


def post(stack, **extra):
    return stack.client.post("/v1/chat/completions", json=payload(**extra))


def test_plain_json_tools_auth_request_id_and_metrics(stack):
    response = post(stack)
    assert response.status_code == 200, response.text
    assert response.json()["choices"][0]["message"]["content"]
    assert response.headers["x-request-id"]
    tools = post(stack, tools=TOOLS)
    assert tools.status_code == 200, tools.text
    assert tools.json()["choices"][0]["message"]["tool_calls"][0]["function"]["name"] == "lookup"
    structured = post(stack, response_format={"type": "json_object"})
    assert structured.status_code == 200, structured.text
    assert json.loads(structured.json()["choices"][0]["message"]["content"])["status"] == "refused"
    unauth = stack.client.post("/v1/chat/completions", headers={"Authorization": "Bearer incorrect"}, json=payload())
    assert unauth.status_code == 401
    stats, _ = stack.wait_idle()
    assert stats["authenticated"] == stats["requests"] == 3
    assert stats["calls"][0]["request_id"] == response.headers["x-request-id"]
    metrics = stack.client.get("/metrics")
    assert metrics.status_code == 200
    assert "request" in metrics.text and "attempt" in metrics.text
    assert "demo-client-key" not in metrics.text and "hello" not in metrics.text


@pytest.mark.parametrize("extra", [{"stream": True, "tools": TOOLS}, {"stream": True, "response_format": {"type": "json_object"}}, {"n": 2}, {"model": "absent"}])
def test_unsupported_parameters_rejected_before_upstream(stack, extra):
    response = post(stack, **extra)
    assert response.status_code in {400, 404, 422}
    assert stack.mock.get("/stats").json()["requests"] == 0


def test_capability_route_skips_incompatible_provider(stack_factory):
    with stack_factory(provider_overrides={"primary": {"capabilities": ["text"]}}) as stack:
        response = post(stack, tools=TOOLS)
        assert response.status_code == 200, response.text
        assert stack.mock.get("/stats").json()["by_model"] == {"mock-secondary": 1}
        response = post(stack, model="primary-only", stream=True)
        assert response.status_code in {400, 422}


def test_sse_fragmented_utf8_crlf_completion(stack):
    stack.control(defaults={"chunks": ["你", "好 🌍"], "fragment_size": 1, "crlf": True})
    response = post(stack, stream=True)
    assert response.status_code == 200, response.text
    assert response.headers["content-type"].startswith("text/event-stream")
    events = [line[6:] for line in response.text.splitlines() if line.startswith("data: ")]
    assert events[-1] == "[DONE]"
    chunks = [json.loads(event) for event in events[:-1]]
    assert "".join(c["choices"][0]["delta"].get("content", "") for c in chunks) == "你好 🌍"
    stats, _ = stack.wait_idle()
    assert stats["completed"] == 1


@pytest.mark.parametrize("scenario", [{"status": 500}, {"status": 429, "retry_after": 0.01}, {"pre_event_error": True}, {"pre_event_error": "eof"}])
def test_retry_or_fallback_before_first_event(stack, scenario):
    stack.control(models={"mock-primary": [scenario]})
    response = post(stack, stream=True)
    assert response.status_code == 200, response.text
    assert "[DONE]" in response.text
    stats, _ = stack.wait_idle()
    assert stats["requests"] == 2


def test_nonstream_retry_fallback_and_nonretryable_auth(stack):
    stack.control(models={"mock-primary": [{"status": 500}]})
    response = post(stack)
    assert response.status_code == 200, response.text
    assert stack.mock.get("/stats").json()["requests"] == 2
    stack.control(models={"mock-primary": [{"status": 401}]})
    response = post(stack)
    assert response.status_code >= 400
    assert stack.mock.get("/stats").json()["requests"] == 1


def test_stream_failure_after_output_does_not_switch(stack):
    stack.control(models={"mock-primary": [{"midstream_error": True, "chunks": ["first", "forbidden"]}]})
    response = post(stack, stream=True)
    assert response.status_code == 200
    assert "first" in response.text and "forbidden" not in response.text
    assert "[DONE]" not in response.text
    stats, _ = stack.wait_idle()
    assert stats["requests"] == 1


def test_retry_after_respects_total_budget(stack_factory):
    with stack_factory(env_overrides={"GATEWAY_DEADLINE_SECONDS": ".25"}) as stack:
        stack.control(defaults={"status": 429, "retry_after": 30})
        before = time.monotonic()
        response = post(stack, model="primary-only")
        assert response.status_code in {429, 502, 503, 504}
        assert time.monotonic() - before < 1
        stats, _ = stack.wait_idle()
        assert stats["requests"] <= 3


def test_timeout_before_first_event_returns_http_error(stack_factory):
    with stack_factory(env_overrides={"GATEWAY_DEADLINE_SECONDS": ".2"}) as stack:
        stack.control(defaults={"first_event_delay": 2})
        response = post(stack, stream=True)
        assert response.status_code == 504, response.text
        stack.wait_idle()


@pytest.mark.asyncio
async def test_queue_capacity_and_disconnect_cleanup(stack_factory):
    with stack_factory(provider_overrides={"primary": {"concurrency": 1, "queue_capacity": 1}},
                       env_overrides={"GATEWAY_MAX_ATTEMPTS": "1"}) as stack:
        stack.control(defaults={"delay": 2})
        async with httpx.AsyncClient(base_url=stack.url, headers=AUTH, trust_env=False, timeout=5) as client:
            first = asyncio.create_task(client.post("/v1/chat/completions", json=payload(model="primary-only")))
            await asyncio.sleep(.15)
            queued = asyncio.create_task(client.post("/v1/chat/completions", json=payload(model="primary-only")))
            await asyncio.sleep(.15)
            rejected = await client.post("/v1/chat/completions", json=payload(model="primary-only"))
            assert rejected.status_code in {429, 503}, rejected.text
            queued.cancel()
            first.cancel()
            await asyncio.gather(first, queued, return_exceptions=True)
        stats, _ = stack.wait_idle()
        assert stats["requests"] == 1
        assert stats["cancelled"] >= 1
        stack.control()
        assert post(stack, model="primary-only").status_code == 200


@pytest.mark.asyncio
async def test_stream_client_disconnect_releases_upstream_and_slot(stack_factory):
    with stack_factory(provider_overrides={"primary": {"concurrency": 1, "queue_capacity": 0}}) as stack:
        stack.control(defaults={"chunks": ["first", "second", "third"], "chunk_delay": .15})
        async with httpx.AsyncClient(base_url=stack.url, headers=AUTH, trust_env=False) as client:
            async with client.stream("POST", "/v1/chat/completions", json=payload(model="primary-only", stream=True)) as response:
                assert response.status_code == 200
                async for line in response.aiter_lines():
                    if "first" in line:
                        break
        stats, _ = stack.wait_idle()
        assert stats["cancelled"] >= 1
        stack.control()
        assert post(stack, model="primary-only").status_code == 200


def test_fixed_window_breaker_opens_and_recovers(stack_factory):
    env = {"GATEWAY_MAX_ATTEMPTS": "1", "GATEWAY_BREAKER_MIN_SAMPLES": "2", "GATEWAY_BREAKER_FAILURE_RATIO": "1",
           "GATEWAY_BREAKER_COOLDOWN_SECONDS": ".2"}
    with stack_factory(env_overrides=env) as stack:
        stack.control(defaults={"status": 500})
        assert post(stack, model="primary-only").status_code >= 500
        assert post(stack, model="primary-only").status_code >= 500
        assert post(stack, model="primary-only").status_code == 503
        assert stack.mock.get("/stats").json()["requests"] == 2
        time.sleep(.25)
        stack.control()
        assert post(stack, model="primary-only").status_code == 200
        assert post(stack, model="primary-only").status_code == 200
        assert stack.mock.get("/stats").json()["requests"] == 2


def test_rpm_is_distinct_from_inflight_concurrency(stack_factory):
    with stack_factory(provider_overrides={"primary": {"rpm": 1, "concurrency": 4}}) as stack:
        assert post(stack, model="primary-only").status_code == 200
        response = post(stack, model="primary-only")
        assert response.status_code == 429, response.text
        stats, _ = stack.wait_idle()
        assert stats["requests"] == 1


def test_upstream_invalid_request_does_not_open_breaker(stack_factory):
    with stack_factory(env_overrides={"GATEWAY_BREAKER_MIN_SAMPLES": "1"}) as stack:
        stack.control(models={"mock-primary": [{"status": 400}]})
        assert post(stack, model="primary-only").status_code == 400
        assert post(stack, model="primary-only").status_code == 200
        assert stack.mock.get("/stats").json()["requests"] == 2


@pytest.mark.asyncio
async def test_half_open_allows_only_one_concurrent_probe(stack_factory):
    with stack_factory(env_overrides={"GATEWAY_MAX_ATTEMPTS": "1", "GATEWAY_BREAKER_MIN_SAMPLES": "1",
                                     "GATEWAY_BREAKER_COOLDOWN_SECONDS": ".15"}) as stack:
        stack.control(defaults={"status": 500})
        assert post(stack, model="primary-only").status_code == 502
        await asyncio.sleep(.2)
        stack.control(defaults={"delay": .25})
        async with httpx.AsyncClient(base_url=stack.url, headers=AUTH, trust_env=False, timeout=5) as client:
            responses = await asyncio.gather(*(client.post("/v1/chat/completions", json=payload(model="primary-only")) for _ in range(6)))
        assert sorted(response.status_code for response in responses) == [200, 503, 503, 503, 503, 503]
        assert stack.mock.get("/stats").json()["requests"] == 1


def test_retry_and_fallback_share_the_same_deadline(stack_factory):
    with stack_factory(env_overrides={"GATEWAY_DEADLINE_SECONDS": ".3"}) as stack:
        stack.control(models={"mock-primary": [{"delay": .15, "status": 500}]}, defaults={"delay": .2})
        before = time.monotonic()
        response = post(stack)
        assert response.status_code == 504, response.text
        assert time.monotonic() - before < .7
        stats, _ = stack.wait_idle()
        assert stats["requests"] == 2


def test_retry_after_survives_switch_away_and_back(stack):
    stack.control(models={"mock-primary": [{"status": 429, "retry_after": .3}],
                          "mock-secondary": [{"status": 500}]})
    response = post(stack)
    assert response.status_code == 200, response.text
    stats, _ = stack.wait_idle()
    assert stats["requests"] == 3
    primary = [call for call in stats["calls"] if call["model"] == "mock-primary"]
    assert len(primary) == 2
    assert primary[1]["monotonic_started"] - primary[0]["monotonic_started"] >= .29
    assert {call["request_id"] for call in stats["calls"]} == {response.headers["x-request-id"]}


def test_oversized_body_rejected_with_request_id_before_upstream(stack):
    response = stack.client.post("/v1/chat/completions", content=b"x" * (2 * 1024 * 1024),
                                 headers={"Content-Type": "application/json"})
    assert response.status_code == 413, response.text
    assert response.json()["error"]["request_id"] == response.headers["x-request-id"]
    assert stack.mock.get("/stats").json()["requests"] == 0


@pytest.mark.asyncio
async def test_queued_requests_recheck_breaker_after_first_failure(stack_factory):
    """Queued permits cannot outlive the closed circuit generation that issued them."""
    with stack_factory(provider_overrides={"primary": {"concurrency": 1, "queue_capacity": 4}},
                       env_overrides={"GATEWAY_MAX_ATTEMPTS": "1", "GATEWAY_BREAKER_MIN_SAMPLES": "1",
                                      "GATEWAY_BREAKER_COOLDOWN_SECONDS": "5"}) as stack:
        stack.control(models={"mock-primary": [{"status": 500, "delay": .3}]})
        async with httpx.AsyncClient(base_url=stack.url, headers=AUTH, trust_env=False, timeout=5) as client:
            tasks = [asyncio.create_task(client.post("/v1/chat/completions", json=payload(model="primary-only")))
                     for _ in range(4)]
            try:
                # Observe all three waiters while the first upstream call is still
                # running, so a passing test cannot merely reject later arrivals.
                queued = False
                deadline = asyncio.get_running_loop().time() + .25
                while asyncio.get_running_loop().time() < deadline:
                    state = (await client.get("/admin/state")).json()
                    gate = state["providers"]["primary"]["gate"]
                    if gate["active"] == 1 and gate["waiting"] == 3:
                        queued = True
                        break
                    await asyncio.sleep(.005)
                assert queued, state
                responses = await asyncio.gather(*tasks)
            finally:
                for task in tasks:
                    if not task.done():
                        task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
        assert sorted(response.status_code for response in responses) == [502, 503, 503, 503]
        stats, state = stack.wait_idle()
        assert stats["requests"] == 1
        assert state["providers"]["primary"]["gate"] == {"active": 0, "waiting": 0}
