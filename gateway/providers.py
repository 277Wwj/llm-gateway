"""Bounded OpenAI Chat Completions protocol adapter (also used by DeepSeek).

The adapter never owns retries. A returned PreparedStream owns its response until
DONE, failure, cancellation or explicit aclose; the caller must close in finally.
"""
from __future__ import annotations

import asyncio
import json
import math
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import AsyncIterator

import httpx
import anyio

MAX_EVENT_BYTES = 256 * 1024
MAX_RESPONSE_BYTES = 2 * 1024 * 1024


async def _close_response(response: httpx.Response) -> None:
    with anyio.CancelScope(shield=True):
        await response.aclose()


class ProviderError(Exception):
    def __init__(self, code: str, status: int = 502, *, retryable: bool = False,
                 fault: bool = False, retry_after: float | None = None):
        super().__init__(code)
        self.code, self.status = code, status
        self.retryable, self.fault, self.retry_after = retryable, fault, retry_after


def _protocol(code: str = "upstream_protocol_error") -> ProviderError:
    return ProviderError(code, retryable=True, fault=True)


def _translate(exc: BaseException) -> ProviderError:
    if isinstance(exc, (TimeoutError, httpx.TimeoutException)):
        return ProviderError("upstream_timeout", 504, retryable=True, fault=True)
    return ProviderError("upstream_connection_error", 502, retryable=True, fault=True)


def _retry_after(value: str | None) -> float | None:
    if value is None:
        return None
    try:
        seconds = float(value)
    except ValueError:
        try:
            stamp = parsedate_to_datetime(value)
            if stamp.tzinfo is None:
                stamp = stamp.replace(tzinfo=timezone.utc)
            seconds = (stamp - datetime.now(timezone.utc)).total_seconds()
        except (ValueError, TypeError, OverflowError):
            return None
    return max(0.0, seconds) if math.isfinite(seconds) else None


def _check_status(response: httpx.Response) -> None:
    if response.status_code == 200:
        return
    status = response.status_code
    retryable = status in {408, 429, 500, 502, 503, 504}
    # Other 5xx (e.g. unsupported endpoint 501) are not automatically replayed.
    # Upstream credentials belong to operators; do not imply caller auth failed.
    mapped = 429 if status == 429 else (400 if status in {400, 413, 422} else 502)
    raise ProviderError("upstream_http_" + str(status), mapped, retryable=retryable,
                        fault=retryable, retry_after=_retry_after(response.headers.get("retry-after")))


def _json(data: bytes | str):
    try:
        return json.loads(data)
    except (ValueError, UnicodeError, RecursionError):
        raise _protocol() from None


def _choice(value: dict) -> dict:
    if not isinstance(value, dict) or "error" in value:
        raise _protocol()
    choices = value.get("choices")
    if not isinstance(choices, list) or len(choices) != 1 or not isinstance(choices[0], dict):
        raise _protocol()
    choice = choices[0]
    if choice.get("index", 0) != 0:
        raise _protocol()
    if choice.get("finish_reason") in {"insufficient_system_resource", "aborted", "error"}:
        raise _protocol("upstream_generation_interrupted")
    return choice


def _validate_completion(value: dict, payload: dict) -> dict:
    choice = _choice(value)
    if choice.get("finish_reason") not in {"stop", "length", "content_filter", "tool_calls"}:
        raise _protocol("upstream_incomplete_completion")
    message = choice.get("message")
    if not isinstance(message, dict) or message.get("role") != "assistant":
        raise _protocol()
    calls = message.get("tool_calls")
    content = message.get("content")
    if calls:
        if not payload.get("tools") or not isinstance(calls, list):
            raise _protocol()
        for call in calls:
            if not isinstance(call, dict) or call.get("type") != "function" or not isinstance(call.get("id"), str):
                raise _protocol()
            function = call.get("function")
            if not isinstance(function, dict) or not isinstance(function.get("name"), str) or not isinstance(function.get("arguments"), str):
                raise _protocol()
    elif not isinstance(content, str):
        # A provider refusal is a valid outcome, not an infrastructure fault.
        if not isinstance(message.get("refusal"), str):
            raise _protocol()
    if payload.get("response_format", {}).get("type") == "json_object":
        if not isinstance(content, str) or not isinstance(_json(content), dict):
            raise _protocol("upstream_invalid_json_object")
    return value


async def _sse_data(response: httpx.Response) -> AsyncIterator[bytes]:
    """Parse UTF-8 SSE at byte boundaries; CRLF may itself span TCP reads.

    Every event, including comments/unknown fields, has a strict byte limit.
    A partial event at EOF is not dispatched (SSE requires a blank delimiter).
    """
    buffer = bytearray()
    data: list[bytes] = []
    event_size = 0
    async for chunk in response.aiter_bytes():
        buffer.extend(chunk)
        while True:
            lf, cr = buffer.find(b"\n"), buffer.find(b"\r")
            indexes = [x for x in (lf, cr) if x >= 0]
            if not indexes:
                break
            end = min(indexes)
            if buffer[end] == 13 and end + 1 == len(buffer):
                break
            width = 2 if buffer[end:end + 2] == b"\r\n" else 1
            line = bytes(buffer[:end])
            del buffer[:end + width]
            event_size += len(line) + width
            if event_size > MAX_EVENT_BYTES:
                raise _protocol("upstream_event_too_large")
            if not line:
                if data:
                    yield b"\n".join(data)
                data = []
                event_size = 0
            elif line.startswith(b"data:"):
                value = line[5:]
                data.append(value[1:] if value.startswith(b" ") else value)
            elif line == b"data":
                data.append(b"")
        if len(buffer) + event_size > MAX_EVENT_BYTES:
            raise _protocol("upstream_event_too_large")
    # A terminal bare CR can itself finish the blank event delimiter.
    if buffer == b"\r" and data:
        yield b"\n".join(data)


def event_has_content(event: bytes) -> bool:
    try:
        value = json.loads(event.removeprefix(b"data: ").strip())
        return bool(value["choices"][0]["delta"].get("content"))
    except (ValueError, KeyError, IndexError, TypeError):
        return False


class PreparedStream:
    def __init__(self, response: httpx.Response, deadline: float):
        self.response, self.deadline = response, deadline
        self._events = _sse_data(response).__aiter__()
        self._first: bytes | None = None
        self._closed = False
        self._saw_event = False
        self._saw_finish = False

    def __aiter__(self):
        return self

    async def prepare(self):
        self._first = await self._next()
        return self

    async def __anext__(self) -> bytes:
        if self._first is not None:
            result, self._first = self._first, None
            return result
        if self._closed:
            raise StopAsyncIteration
        return await self._next()

    async def _next(self) -> bytes:
        try:
            async with asyncio.timeout_at(self.deadline):
                raw = await anext(self._events)
            if raw == b"[DONE]":
                if not self._saw_event or not self._saw_finish:
                    raise _protocol("upstream_premature_done")
                await self.aclose()
                return b"data: [DONE]\n\n"
            value = _json(raw)
            # Optional OpenAI usage-only chunks are legal after a finish chunk.
            if isinstance(value, dict) and value.get("choices") == [] and isinstance(value.get("usage"), dict):
                if not self._saw_finish:
                    raise _protocol()
            else:
                choice = _choice(value)
                delta = choice.get("delta")
                if not isinstance(delta, dict):
                    raise _protocol()
                if delta.get("tool_calls") or delta.get("function_call"):
                    raise _protocol("upstream_stream_tools_unsupported")
                if delta.get("reasoning_content"):
                    raise _protocol("upstream_stream_reasoning_unsupported")
                if delta.get("content") is not None and not isinstance(delta["content"], str):
                    raise _protocol()
                if self._saw_finish:
                    raise _protocol("upstream_content_after_finish")
                if choice.get("finish_reason") is not None:
                    if choice["finish_reason"] not in {"stop", "length", "content_filter"}:
                        raise _protocol("upstream_invalid_finish_reason")
                    self._saw_finish = True
                self._saw_event = True
            return b"data: " + json.dumps(value, ensure_ascii=True, separators=(",", ":")).encode() + b"\n\n"
        except StopAsyncIteration:
            await self.aclose()
            raise _protocol("upstream_incomplete_stream") from None
        except (httpx.HTTPError, TimeoutError) as exc:
            await self.aclose()
            raise _translate(exc) from None
        except BaseException:
            await self.aclose()
            raise

    async def aclose(self):
        if not self._closed:
            self._closed = True
            self._first = None
            # Starlette's disconnect cancellation is level-triggered; cleanup
            # needs its own shield even after catching CancelledError.
            with anyio.CancelScope(shield=True):
                try:
                    await self.response.aclose()
                finally:
                    await self._events.aclose()


class Adapter:
    def __init__(self, client: httpx.AsyncClient):
        self.client = client

    async def _open(self, provider, payload: dict, deadline: float, request_id: str) -> httpx.Response:
        remaining = deadline - asyncio.get_running_loop().time()
        if remaining <= 0:
            raise ProviderError("upstream_timeout", 504, retryable=True, fault=True)
        body = {**getattr(provider, "extra_body", {}), **payload, "model": provider.model}
        request = self.client.build_request("POST", provider.base_url.rstrip("/") + "/chat/completions",
            headers={"Authorization": "Bearer " + provider.api_key, "X-Request-ID": request_id,
                     "Accept": "text/event-stream" if payload.get("stream") else "application/json"},
            json=body, timeout=httpx.Timeout(min(remaining, 30), connect=min(remaining, 5)))
        try:
            async with asyncio.timeout_at(deadline):
                response = await self.client.send(request, stream=True, follow_redirects=False)
        except (httpx.HTTPError, TimeoutError) as exc:
            raise _translate(exc) from None
        try:
            _check_status(response)
        except BaseException:
            await _close_response(response)
            raise
        return response

    async def complete(self, provider, payload: dict, deadline: float, request_id: str) -> dict:
        response = await self._open(provider, {**payload, "stream": False}, deadline, request_id)
        try:
            body = bytearray()
            async with asyncio.timeout_at(deadline):
                async for chunk in response.aiter_bytes():
                    body.extend(chunk)
                    if len(body) > MAX_RESPONSE_BYTES:
                        raise _protocol("upstream_response_too_large")
            return _validate_completion(_json(body), payload)
        except (httpx.HTTPError, TimeoutError) as exc:
            raise _translate(exc) from None
        finally:
            await _close_response(response)

    async def open_stream(self, provider, payload: dict, deadline: float, request_id: str) -> PreparedStream:
        if payload.get("tools"):
            raise ProviderError("stream_tools_unsupported", 400)
        response = await self._open(provider, {**payload, "stream": True}, deadline, request_id)
        stream = PreparedStream(response, deadline)
        try:
            if response.headers.get("content-type", "").split(";")[0].strip().lower() != "text/event-stream":
                raise _protocol("upstream_not_sse")
            return await stream.prepare()
        except BaseException:
            await stream.aclose()
            raise
