"""Wire slug ↔ internal `tgo_graph_*` prefix at HTTP and validation boundaries."""

from __future__ import annotations

from fastapi import FastAPI
from fastapi.testclient import TestClient
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, PlainTextResponse
from starlette.routing import Route

from temporal_graph.middleware.collection_wire import (
    CollectionPathRewriteMiddleware,
    CollectionWireResponseMiddleware,
)
from temporal_graph.models.api import CollectionUpsertRequest, IngestPayload, RetrievalQuery
from temporal_graph.wiring.collection_ns import (
    GRAPH_COLLECTION_PREFIX,
    normalize_inbound_collection_id,
    strip_wire_from_json,
    wire_collection_id,
)


def test_normalize_and_wire_roundtrip() -> None:
    assert normalize_inbound_collection_id("pricing") == f"{GRAPH_COLLECTION_PREFIX}pricing"
    assert wire_collection_id(f"{GRAPH_COLLECTION_PREFIX}pricing") == "pricing"
    assert normalize_inbound_collection_id(f"{GRAPH_COLLECTION_PREFIX}pricing") == (
        f"{GRAPH_COLLECTION_PREFIX}pricing"
    )


def test_strip_wire_nested() -> None:
    payload = {
        "collection_id": "tgo_graph_x",
        "tool_trace": [{"collection_id": "tgo_graph_y", "n": 1}],
        "result_summary": {"collection_id": "tgo_graph_z"},
    }
    out = strip_wire_from_json(payload)
    assert out["collection_id"] == "x"
    assert out["tool_trace"][0]["collection_id"] == "y"
    assert out["result_summary"]["collection_id"] == "z"


def test_ingest_payload_internalizes_collection_id() -> None:
    raw = {
        "ontology_id": "company_data",
        "collection_id": "foo_bar",
        "chunks": [
            {
                "chunk_id": "c1",
                "content": "x",
                "type": "text",
                "doc_id": "d1",
                "page": 1,
                "bundle_id": "b1",
                "canonical_event": "Earnings",
                "canonical_subevent": "Results",
                "publish_date": "2025-01-01",
            }
        ],
    }
    p = IngestPayload.model_validate(raw)
    assert p.collection_id == f"{GRAPH_COLLECTION_PREFIX}foo_bar"


def test_retrieval_query_internalizes() -> None:
    q = RetrievalQuery.model_validate(
        {"question": "q", "collection_id": "abc"}
    )
    assert q.collection_id == f"{GRAPH_COLLECTION_PREFIX}abc"


def test_collection_upsert_internalizes() -> None:
    b = CollectionUpsertRequest.model_validate(
        {"name": "Test", "description": "", "collection_id": "my_coll"}
    )
    assert b.collection_id == f"{GRAPH_COLLECTION_PREFIX}my_coll"


def test_response_middleware_strips_json() -> None:
    app = FastAPI()

    @app.get("/x")
    def _x() -> dict[str, str]:
        return {"collection_id": f"{GRAPH_COLLECTION_PREFIX}wire_me"}

    app.add_middleware(CollectionWireResponseMiddleware)
    client = TestClient(app)
    assert client.get("/x").json() == {"collection_id": "wire_me"}


def test_path_rewrite_middleware() -> None:
    async def show_path(request: Request) -> PlainTextResponse:
        return PlainTextResponse(request.scope["path"])

    starlette_app = Starlette(
        routes=[Route("/v1/collections/{collection_id}", show_path, methods=["GET"])]
    )
    starlette_app.add_middleware(CollectionPathRewriteMiddleware)
    client = TestClient(starlette_app)
    r = client.get("/v1/collections/slug_a")
    assert r.text == f"/v1/collections/{GRAPH_COLLECTION_PREFIX}slug_a"


def test_path_rewrite_idempotent_when_prefixed() -> None:
    async def show_path(request: Request) -> PlainTextResponse:
        return PlainTextResponse(request.scope["path"])

    starlette_app = Starlette(
        routes=[Route("/v1/collections/{collection_id}", show_path, methods=["GET"])]
    )
    starlette_app.add_middleware(CollectionPathRewriteMiddleware)
    client = TestClient(starlette_app)
    internal = f"{GRAPH_COLLECTION_PREFIX}slug_a"
    r = client.get(f"/v1/collections/{internal}")
    assert r.text == f"/v1/collections/{internal}"


def test_json_response_middleware_skips_non_json() -> None:
    app = FastAPI()

    @app.get("/plain")
    def _plain() -> PlainTextResponse:
        return PlainTextResponse(f"{GRAPH_COLLECTION_PREFIX}raw")

    app.add_middleware(CollectionWireResponseMiddleware)
    client = TestClient(app)
    assert client.get("/plain").text == f"{GRAPH_COLLECTION_PREFIX}raw"


def test_starlette_json_bytes_roundtrip() -> None:
    """Ensure middleware handles Starlette JSONResponse body iteration."""

    async def ep(_: Request) -> JSONResponse:
        return JSONResponse({"collection_id": f"{GRAPH_COLLECTION_PREFIX}z"})

    starlette_app = Starlette(routes=[Route("/j", ep)])
    starlette_app.add_middleware(CollectionWireResponseMiddleware)
    client = TestClient(starlette_app)
    assert client.get("/j").json() == {"collection_id": "z"}
