"""HTTP: rewrite collection path segments and strip internal collection prefix from JSON bodies."""

from __future__ import annotations

import json
from urllib.parse import unquote

from starlette.datastructures import MutableHeaders
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import Response
from starlette.types import ASGIApp, Receive, Scope, Send

from temporal_graph.wiring.collection_ns import GRAPH_COLLECTION_PREFIX, strip_wire_from_json


class CollectionPathRewriteMiddleware:
    """Rewrite `GET /v1/collections/{id}` path segment to internal id before routing."""

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] == "http":
            path = scope.get("path") or ""
            parts = [p for p in path.split("/") if p != ""]
            if len(parts) == 3 and parts[0] == "v1" and parts[1] == "collections":
                seg = unquote(parts[2])
                if seg and not seg.startswith(GRAPH_COLLECTION_PREFIX):
                    parts[2] = GRAPH_COLLECTION_PREFIX + seg
                    scope = dict(scope)
                    trailing = path.endswith("/") and len(path) > 1
                    scope["path"] = "/" + "/".join(parts) + ("/" if trailing else "")
        await self.app(scope, receive, send)


class CollectionWireResponseMiddleware(BaseHTTPMiddleware):
    """Strip internal collection prefix from JSON response bodies (`collection_id` keys)."""

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        response = await call_next(request)
        ct = response.headers.get("content-type", "")
        if "application/json" not in ct or "text/event-stream" in ct:
            return response
        body = b""
        async for chunk in response.body_iterator:
            body += chunk
        try:
            payload = json.loads(body.decode("utf-8"))
            payload = strip_wire_from_json(payload)
            out = json.dumps(payload, default=str).encode("utf-8")
        except (json.JSONDecodeError, UnicodeDecodeError):
            out = body
        headers = MutableHeaders(response.headers)
        if "content-length" in headers:
            del headers["content-length"]
        return Response(
            content=out,
            status_code=response.status_code,
            headers=dict(headers),
            media_type=response.media_type,
            background=response.background,
        )
