"""Explicit local test double. No calls to a language model are made here.

Run only on loopback: python -m uvicorn gateway.mock_provider:app --host 127.0.0.1 --port 9101
The control endpoints are intentionally unauthenticated and must not be published.
"""
from __future__ import annotations

import asyncio
import json
import time
from collections import Counter, deque
from contextlib import suppress
from typing import Any

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel, ConfigDict, Field


class Scenario(BaseModel):
    model_config = ConfigDict(extra="forbid")
    status: int = Field(default=200, ge=200, le=599)
    retry_after: float | None = Field(default=None, ge=0, le=3600)
    delay: float = Field(default=0, ge=0, le=120)
    first_event_delay: float = Field(default=0, ge=0, le=120)
    chunk_delay: float = Field(default=0, ge=0, le=120)
    chunks: list[str] = Field(default_factory=lambda: ["mock ", "response"])
    fragment_size: int = Field(default=0, ge=0, le=65536)
    pre_event_error: bool | str = False
    midstream_error: bool = False
    crlf: bool = False


class Control(BaseModel):
    model_config = ConfigDict(extra="forbid")
    models: dict[str, list[Scenario]] = Field(default_factory=dict)
    defaults: Scenario = Field(default_factory=Scenario)
    reset: bool = True


app = FastAPI(title="LLM Gateway explicit mock provider")
plans: dict[str, deque[Scenario]] = {}
default = Scenario()
counts: Counter = Counter()
by_model: Counter = Counter()
calls: deque[dict] = deque(maxlen=10000)


@app.get("/health")
async def health():
    return {"status": "ok", "mode": "mock", "real_llm": False}


@app.post("/control")
async def control(body: Control):
    global default
    if counts["active"]:
        return JSONResponse(status_code=409, content={"error": "wait for active requests before reset"})
    if body.reset:
        counts.clear()
        by_model.clear()
        plans.clear()
        calls.clear()
    default = body.defaults
    plans.update({name: deque(scenarios) for name, scenarios in body.models.items()})
    return {"mode": "mock", "queued": {name: len(plan) for name, plan in plans.items()}}


@app.get("/stats")
async def stats():
    return {"mode": "mock", "active": counts["active"], "requests": counts["requests"],
            "completed": counts["completed"], "cancelled": counts["cancelled"],
            "failed": counts["failed"], "authenticated": counts["authenticated"],
            "by_model": dict(by_model), "calls": list(calls)}


def completion(body: dict[str, Any]) -> dict:
    content = "Explicit mock response; no real LLM was called."
    if body.get("response_format", {}).get("type") == "json_object":
        # AgentRAG sends its retrieved evidence in the last user message. Only copy
        # one supplied excerpt; this is protocol validation, not quality evaluation.
        evidence = []
        for message in body.get("messages", []):
            if message.get("role") == "user":
                with suppress(ValueError, TypeError, AttributeError):
                    evidence = json.loads(message.get("content", "")).get("evidence", [])
        if evidence:
            item = evidence[0]
            cid = item["chunk_id"]
            answer = {"status": "answered", "answer": f"显式 mock 链路验收，引用样例原文：[{cid}] {item['text']}",
                      "citation_ids": [cid]}
        else:
            answer = {"status": "refused", "answer": "显式 mock：未提供证据。", "citation_ids": []}
        content = json.dumps(answer, ensure_ascii=False)
    message: dict[str, Any] = {"role": "assistant", "content": content}
    finish = "stop"
    if body.get("tools") and body.get("tool_choice") != "none":
        tool = body["tools"][0]["function"]
        message = {"role": "assistant", "content": None, "tool_calls": [
            {"id": "call_mock_1", "type": "function", "function": {"name": tool["name"], "arguments": "{}"}}]}
        finish = "tool_calls"
    return {"id": "chatcmpl-mock", "object": "chat.completion", "created": 0,
            "model": body["model"], "choices": [{"index": 0, "message": message, "finish_reason": finish}],
            "usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2}}


async def disconnect(request: Request):
    while not await request.is_disconnected():
        await asyncio.sleep(0.01)


@app.post("/v1/chat/completions")
async def chat(request: Request):
    body = await request.json()
    model = body.get("model", "mock-primary")
    counts["requests"] += 1
    by_model[model] += 1
    calls.append({"model": model, "monotonic_started": time.monotonic(),
                  "request_id": request.headers.get("x-request-id")})
    # These are demo-only fixed credentials, distinct from the gateway client key.
    if request.headers.get("authorization") != "Bearer mock-upstream-key":
        return JSONResponse(status_code=401, content={"error": {"message": "mock authentication failed"}})
    counts["authenticated"] += 1
    scenario = plans[model].popleft() if plans.get(model) else default
    counts["active"] += 1
    transferred = False
    try:
        if scenario.delay:
            sleeper = asyncio.create_task(asyncio.sleep(scenario.delay))
            watcher = asyncio.create_task(disconnect(request))
            try:
                done, _ = await asyncio.wait([sleeper, watcher], return_when=asyncio.FIRST_COMPLETED)
                if watcher in done:
                    counts["cancelled"] += 1
                    return JSONResponse(status_code=499, content={"error": "client disconnected"})
            finally:
                sleeper.cancel()
                watcher.cancel()
                await asyncio.gather(sleeper, watcher, return_exceptions=True)
        if scenario.status != 200:
            counts["failed"] += 1
            headers = {} if scenario.retry_after is None else {"Retry-After": str(scenario.retry_after)}
            return JSONResponse(status_code=scenario.status, headers=headers,
                                content={"error": {"message": "injected mock status", "type": "mock_error"}})
        if not body.get("stream"):
            counts["completed"] += 1
            return completion(body)

        async def stream():
            finished = False
            try:
                await asyncio.sleep(scenario.first_event_delay)
                ending = "\r\n\r\n" if scenario.crlf else "\n\n"

                def event(delta: dict, finish_reason=None):
                    value = {"id": "chatcmpl-mock", "object": "chat.completion.chunk", "created": 0,
                             "model": model, "choices": [{"index": 0, "delta": delta, "finish_reason": finish_reason}]}
                    return ("data: " + json.dumps(value, ensure_ascii=False) + ending).encode()

                async def fragments(data):
                    size = scenario.fragment_size or len(data)
                    for position in range(0, len(data), size):
                        yield data[position:position + size]
                        await asyncio.sleep(0)

                if scenario.pre_event_error:
                    if scenario.pre_event_error != "eof":
                        yield b'data: {"error":{"message":"injected pre-event failure"}}\n\n'
                    return
                async for fragment in fragments(event({"role": "assistant"})):
                    yield fragment
                for index, chunk in enumerate(scenario.chunks):
                    await asyncio.sleep(scenario.chunk_delay)
                    async for fragment in fragments(event({"content": chunk})):
                        yield fragment
                    if index == 0 and scenario.midstream_error:
                        # A normal EOF lacking DONE is also an invalid upstream stream;
                        # avoid an expected traceback obscuring the acceptance logs.
                        return
                async for fragment in fragments(event({}, "stop")):
                    yield fragment
                async for fragment in fragments(("data: [DONE]" + ending).encode()):
                    yield fragment
                finished = True
                counts["completed"] += 1
            except asyncio.CancelledError:
                counts["cancelled"] += 1
                raise
            finally:
                if not finished:
                    counts["failed"] += 1
                counts["active"] -= 1

        transferred = True
        return StreamingResponse(stream(), media_type="text/event-stream")
    finally:
        if not transferred:
            counts["active"] -= 1
