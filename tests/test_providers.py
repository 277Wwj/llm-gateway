import asyncio
import json
from types import SimpleNamespace

import httpx
import pytest
import anyio

from gateway.providers import Adapter, ProviderError, event_has_content


PROVIDER = SimpleNamespace(name="test", base_url="https://provider.example/v1",
                           api_key="upstream-only", model="actual-model",
                           extra_body={"thinking": {"type": "disabled"}})
PAYLOAD = {"model": "alias", "messages": [{"role": "user", "content": "hi"}]}


def deadline(seconds=2):
    return asyncio.get_running_loop().time() + seconds


def chunk(delta=None, finish=None):
    return {"id": "chunk", "object": "chat.completion.chunk", "choices": [
        {"index": 0, "delta": delta or {}, "finish_reason": finish}]}


def event(value):
    return ("data: " + json.dumps(value, ensure_ascii=False) + "\n\n").encode()


def completion(content="hello"):
    return {"id": "completion", "choices": [{"index": 0, "message": {
        "role": "assistant", "content": content}, "finish_reason": "stop"}]}


class TrackingStream(httpx.AsyncByteStream):
    def __init__(self, pieces, delay=0, error=None):
        self.pieces, self.delay, self.error = pieces, delay, error
        self.closed = False

    async def __aiter__(self):
        for piece in self.pieces:
            await asyncio.sleep(self.delay)
            yield piece
        if self.error:
            raise self.error

    async def aclose(self):
        self.closed = True


def client_for(stream, status=200, headers=None):
    async def handler(request):
        return httpx.Response(status, stream=stream, headers=headers or {"content-type": "text/event-stream"})
    return httpx.AsyncClient(transport=httpx.MockTransport(handler), trust_env=False)


@pytest.mark.asyncio
async def test_complete_rewrites_model_and_uses_only_operator_credentials():
    async def handler(request):
        assert request.url == "https://provider.example/v1/chat/completions"
        assert request.headers["authorization"] == "Bearer upstream-only"
        assert request.headers["x-request-id"] == "req-123"
        body = json.loads(request.content)
        assert body["model"] == "actual-model"
        assert body["thinking"] == {"type": "disabled"}
        return httpx.Response(200, json=completion())
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        result = await Adapter(client).complete(PROVIDER, PAYLOAD, deadline(), "req-123")
        assert result["choices"][0]["message"]["content"] == "hello"


@pytest.mark.asyncio
@pytest.mark.parametrize("status,retryable,fault,mapped", [
    (429, True, True, 429), (500, True, True, 502), (503, True, True, 502),
    (400, False, False, 400), (401, False, False, 502), (403, False, False, 502),
    (302, False, False, 502), (501, False, False, 502)])
async def test_status_classification_and_redaction(status, retryable, fault, mapped):
    source = TrackingStream([b'secret key and prompt'])
    async with client_for(source, status, {"retry-after": "3.5"}) as client:
        with pytest.raises(ProviderError) as caught:
            await Adapter(client).complete(PROVIDER, PAYLOAD, deadline(), "r")
        exc = caught.value
        assert (exc.retryable, exc.fault, exc.status, exc.retry_after) == (retryable, fault, mapped, 3.5)
        assert str(exc) == f"upstream_http_{status}"
        assert source.closed


@pytest.mark.asyncio
async def test_sse_byte_fragmentation_utf8_crlf_multiline_comments_and_done():
    first = json.dumps(chunk({"role": "assistant"}), ensure_ascii=False)
    # JSON split across data lines remains valid JSON after newline joining.
    first = first.replace(', "choices"', ',\r\ndata: "choices"')
    wire = (": keepalive\r\n\r\ndata: " + first + "\r\n\r\n").encode()
    wire += event(chunk({"content": "你好"})).replace(b"\n", b"\r\n")
    wire += event(chunk(finish="stop")) + b"data: [DONE]\n\n"
    source = TrackingStream([bytes([value]) for value in wire])
    async with client_for(source) as client:
        prepared = await Adapter(client).open_stream(PROVIDER, PAYLOAD, deadline(), "r")
        assert not source.closed
        output = [part async for part in prepared]
        assert len(output) == 4 and output[-1] == b"data: [DONE]\n\n"
        assert not event_has_content(output[0]) and event_has_content(output[1])
        assert json.loads(output[1].removeprefix(b"data: "))["choices"][0]["delta"]["content"] == "你好"
        assert source.closed


@pytest.mark.asyncio
@pytest.mark.parametrize("wire,code", [
    (b"", "upstream_incomplete_stream"),
    (b"data: [DONE]\n\n", "upstream_premature_done"),
    (b"data: {bad}\n\n", "upstream_protocol_error"),
    (b'data: {"error":{"message":"secret"}}\n\n', "upstream_protocol_error"),
    (b"data: " + b"x" * (256 * 1024), "upstream_event_too_large"),
    (event(chunk({"tool_calls": [{"index": 0}]})), "upstream_stream_tools_unsupported"),
])
async def test_pre_event_failure_closes_before_returning_stream(wire, code):
    source = TrackingStream([wire])
    async with client_for(source) as client:
        with pytest.raises(ProviderError, match=code):
            await Adapter(client).open_stream(PROVIDER, PAYLOAD, deadline(), "r")
        assert source.closed


@pytest.mark.asyncio
@pytest.mark.parametrize("suffix", [b"", b"data: [DONE]\n\n", b"data: partial"])
async def test_midstream_missing_finish_or_done_is_not_success(suffix):
    source = TrackingStream([event(chunk({"content": "hello"})), suffix])
    async with client_for(source) as client:
        prepared = await Adapter(client).open_stream(PROVIDER, PAYLOAD, deadline(), "r")
        assert event_has_content(await anext(prepared))
        with pytest.raises(ProviderError):
            await anext(prepared)
        assert source.closed


@pytest.mark.asyncio
async def test_timeout_during_first_event_and_cancellation_during_next_close():
    source = TrackingStream([event(chunk({"content": "a"}))], delay=1)
    async with client_for(source) as client:
        with pytest.raises(ProviderError) as exc:
            await Adapter(client).open_stream(PROVIDER, PAYLOAD, deadline(.02), "r")
        assert exc.value.status == 504 and source.closed
    source = TrackingStream([event(chunk({"content": "a"})), event(chunk({"content": "b"}))])
    async with client_for(source) as client:
        prepared = await Adapter(client).open_stream(PROVIDER, PAYLOAD, deadline(), "r")
        await anext(prepared)
        source.delay = 1
        task = asyncio.create_task(anext(prepared))
        await asyncio.sleep(.01)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert source.closed


@pytest.mark.asyncio
async def test_usage_chunk_and_explicit_close():
    source = TrackingStream([event(chunk(finish="stop")), event({"choices": [], "usage": {"total_tokens": 2}}), b"data: [DONE]\n\n"])
    async with client_for(source) as client:
        prepared = await Adapter(client).open_stream(PROVIDER, PAYLOAD, deadline(), "r")
        assert len([piece async for piece in prepared]) == 3
        await prepared.aclose()
        assert source.closed


@pytest.mark.asyncio
@pytest.mark.parametrize("content,valid", [(' {"answer":"ok"} ', True), ('[]', False), ('not json', False)])
async def test_json_object_response_validation(content, valid):
    source = TrackingStream([json.dumps(completion(content)).encode()])
    async with client_for(source) as client:
        adapter = Adapter(client)
        payload = {**PAYLOAD, "response_format": {"type": "json_object"}}
        if valid:
            await adapter.complete(PROVIDER, payload, deadline(), "r")
        else:
            with pytest.raises(ProviderError):
                await adapter.complete(PROVIDER, payload, deadline(), "r")
        assert source.closed


@pytest.mark.asyncio
async def test_nonstream_size_limit_and_tools():
    source = TrackingStream([b"x" * (2 * 1024 * 1024 + 1)])
    async with client_for(source) as client:
        with pytest.raises(ProviderError, match="too_large"):
            await Adapter(client).complete(PROVIDER, PAYLOAD, deadline(), "r")
        assert source.closed
    value = completion(None)
    value["choices"][0]["message"]["tool_calls"] = [{"id": "tool-1", "type": "function", "function": {"name": "lookup", "arguments": "{}"}}]
    source = TrackingStream([json.dumps(value).encode()])
    async with client_for(source) as client:
        result = await Adapter(client).complete(PROVIDER, {**PAYLOAD, "tools": [{"type": "function"}]}, deadline(), "r")
        assert result["choices"][0]["message"]["tool_calls"]


@pytest.mark.asyncio
async def test_transport_failure_and_content_type():
    async def handler(request):
        raise httpx.ConnectError("private hostname and credentials", request=request)
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(ProviderError, match="^upstream_connection_error$"):
            await Adapter(client).complete(PROVIDER, PAYLOAD, deadline(), "r")
    source = TrackingStream([b"not sse"])
    async with client_for(source, headers={"content-type": "application/json"}) as client:
        with pytest.raises(ProviderError, match="upstream_not_sse"):
            await Adapter(client).open_stream(PROVIDER, PAYLOAD, deadline(), "r")
        assert source.closed


@pytest.mark.asyncio
async def test_asgi_style_level_cancellation_shields_nonstream_close():
    class SlowClose(TrackingStream):
        async def aclose(self):
            await asyncio.sleep(.01)
            self.closed = True
    source = SlowClose([json.dumps(completion()).encode()], delay=1)
    async with client_for(source) as client:
        async with anyio.create_task_group() as group:
            group.start_soon(Adapter(client).complete, PROVIDER, PAYLOAD, deadline(), "r")
            await asyncio.sleep(.01)
            group.cancel_scope.cancel()
        assert source.closed


@pytest.mark.asyncio
async def test_bare_cr_events_and_no_read_after_explicit_close():
    source = TrackingStream([(event(chunk(finish="stop")) + b"data: [DONE]\n\n").replace(b"\n", b"\r")])
    async with client_for(source) as client:
        prepared = await Adapter(client).open_stream(PROVIDER, PAYLOAD, deadline(), "r")
        assert len([part async for part in prepared]) == 2
    source = TrackingStream([event(chunk({"content": "a"}))])
    async with client_for(source) as client:
        prepared = await Adapter(client).open_stream(PROVIDER, PAYLOAD, deadline(), "r")
        await prepared.aclose()
        with pytest.raises(StopAsyncIteration):
            await anext(prepared)
