import asyncio
import hmac
import logging
import math
from contextlib import asynccontextmanager
from uuid import uuid4

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, Response, StreamingResponse
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from .config import Settings
from .schemas import ChatRequest
from .service import Service
from .providers import ProviderError


class ManagedStream(StreamingResponse):
    def __init__(self, *args, service, attempt, **kwargs):
        super().__init__(*args, **kwargs)
        self.service, self.attempt = service, attempt

    async def __call__(self, scope, receive, send):
        try:
            await super().__call__(scope, receive, send)
        finally:
            import anyio
            with anyio.CancelScope(shield=True):
                try:
                    await self.body_iterator.aclose()
                finally:
                    await self.service.protected_cleanup(self.attempt, "neutral")
                    self.service.finish_stream(self.attempt, "cancelled")


class RequestContext:
    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            return await self.app(scope, receive, send)
        request_id = uuid4().hex
        scope.setdefault("state", {})["request_id"] = request_id
        async def wrapped_send(message):
            if message["type"] == "http.response.start":
                message.setdefault("headers", []).append((b"x-request-id", request_id.encode()))
            await send(message)
        if scope.get("path") == "/v1/chat/completions" and scope.get("method") == "POST":
            chunks, size = [], 0
            while True:
                message = await receive()
                if message["type"] == "http.disconnect":
                    return
                size += len(message.get("body", b""))
                if size > 1024 * 1024:
                    response = JSONResponse(status_code=413, content={"error": {"code": "request_too_large", "request_id": request_id}})
                    return await response(scope, receive, wrapped_send)
                chunks.append(message.get("body", b""))
                if not message.get("more_body", False):
                    break
            buffered = b"".join(chunks)
            delivered = False
            async def body_receive():
                nonlocal delivered
                if not delivered:
                    delivered = True
                    return {"type": "http.request", "body": buffered, "more_body": False}
                return await receive()
            return await self.app(scope, body_receive, wrapped_send)
        await self.app(scope, receive, wrapped_send)


async def disconnect_aware(request, coro):
    import anyio
    task = asyncio.create_task(coro)
    async def watch():
        while True:
            message = await request.receive()
            if message["type"] == "http.disconnect":
                return
    watcher = asyncio.create_task(watch())
    try:
        try:
            done, _ = await asyncio.wait({task, watcher}, return_when=asyncio.FIRST_COMPLETED)
            if watcher in done:
                raise ProviderError("client_disconnected", 499)
            result = await task
        finally:
            with anyio.CancelScope(shield=True):
                watcher.cancel()
                if not task.done():
                    task.cancel()
                await asyncio.gather(watcher, task, return_exceptions=True)
    except BaseException:
        with anyio.CancelScope(shield=True):
            await asyncio.gather(watcher, task, return_exceptions=True)
            if not task.cancelled() and task.exception() is None:
                body, attempt = task.result()
                if attempt:
                    await body.aclose()
                    service = request.app.state.service
                    await service.protected_cleanup(attempt, "neutral")
                    service.finish_stream(attempt, "cancelled")
        raise
    # No await between ownership transfer and returning the prepared stream.
    return result


def create_app(settings: Settings | None = None):
    @asynccontextmanager
    async def lifespan(app):
        # Startup errors are deliberately sanitized: validation inputs may contain secrets.
        try:
            app.state.service = Service(settings or Settings())
        except Exception:
            raise RuntimeError("Gateway startup configuration invalid; check mode, config and credential environment variables") from None
        try:
            yield
        finally:
            await app.state.service.close()
    app = FastAPI(title="LLM Gateway", version="0.1.0", lifespan=lifespan)
    app.add_middleware(RequestContext)

    def authorize(request):
        expected = "Bearer " + request.app.state.service.settings.api_key.get_secret_value()
        if not hmac.compare_digest(request.headers.get("authorization", "").encode(), expected.encode()):
            raise ProviderError("invalid_gateway_key", 401)

    @app.exception_handler(ProviderError)
    async def provider_error(request, exc):
        headers = {"Retry-After": str(max(1, math.ceil(exc.retry_after)))} if exc.retry_after else None
        return JSONResponse(status_code=exc.status, content={"error": {"code": exc.code, "message": exc.code,
                             "request_id": request.state.request_id}}, headers=headers)

    @app.exception_handler(RequestValidationError)
    async def validation_error(request, exc):
        return JSONResponse(status_code=422, content={"error": {"code": "unsupported_or_invalid_request",
                             "message": "See /docs for supported fields and combinations", "request_id": request.state.request_id}})

    @app.exception_handler(Exception)
    async def unexpected_error(request, exc):
        return JSONResponse(status_code=500, content={"error": {"code": "internal_error", "request_id": request.state.request_id}},
                            headers={"X-Request-ID": request.state.request_id})

    @app.get("/health")
    async def health(request: Request):
        s = request.app.state.service.settings
        return {"status": "ok", "mode": s.mode, "rate_backend": s.rate_backend, "supported_workers": 1}

    @app.get("/admin/state")
    async def state(request: Request):
        authorize(request)
        return {"providers": request.app.state.service.state()}

    @app.get("/metrics")
    async def metrics(request: Request):
        authorize(request)
        request.app.state.service.state()
        return Response(generate_latest(request.app.state.service.metrics.registry), media_type=CONTENT_TYPE_LATEST)

    @app.get("/v1/models")
    async def models(request: Request):
        authorize(request)
        s = request.app.state.service
        return {"object": "list", "data": [{"id": alias, "object": "model", "providers": [
            {"name": n, "capabilities": sorted(s.providers[n].capabilities)} for n in names
        ]} for alias, names in s.routing.routes.items()]}

    @app.post("/v1/chat/completions")
    async def chat(body: ChatRequest, request: Request):
        authorize(request)
        service = request.app.state.service
        result, attempt = await disconnect_aware(request, service.execute(body, request.state.request_id))
        if attempt:
            return ManagedStream(result, service=service, attempt=attempt, media_type="text/event-stream",
                                 headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})
        return JSONResponse(result)

    return app


logging.getLogger("httpx").setLevel(logging.WARNING)
app = create_app()
