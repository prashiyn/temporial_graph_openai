from __future__ import annotations

import json
from typing import Any

from fastapi import APIRouter, HTTPException, Request
from sse_starlette.sse import EventSourceResponse

from temporal_graph.models.api import IngestJobCreateResponse, IngestJobStatus, IngestPayload
from temporal_graph.neo4j.driver import get_driver
from temporal_graph.neo4j.repository import DocumentCollectionConflictError, GraphRepository
from temporal_graph.ontology.loader import list_ontology_ids
from temporal_graph.settings import get_settings
from temporal_graph.wiring.collection_ns import strip_wire_from_json

router = APIRouter(prefix="/v1", tags=["ingest"])


@router.post(
    "/ingest/jobs",
    response_model=IngestJobCreateResponse,
    responses={
        409: {
            "description": "Document already exists in a different collection",
            "content": {
                "application/json": {
                    "example": {
                        "detail": "Document (DOC123, 2025-01-01) already belongs to collection 'a', cannot ingest into 'b'"
                    }
                }
            },
        }
    },
)
async def create_ingest_job(payload: IngestPayload, request: Request) -> IngestJobCreateResponse:
    # Preflight conflict check for fast 409 at API boundary (before background job enqueue).
    settings = get_settings()
    repo = GraphRepository(get_driver(settings), settings)
    doc_id = payload.chunks[0].doc_id
    publish_date = payload.chunks[0].publish_date or ""
    try:
        await repo.assert_document_collection_compatible(doc_id, publish_date, payload.collection_id)
    except DocumentCollectionConflictError as e:
        raise HTTPException(status_code=409, detail=str(e)) from e

    mgr = request.app.state.job_manager
    rec = await mgr.create_job(payload)
    mgr.spawn(rec)
    base = str(request.base_url).rstrip("/")
    return IngestJobCreateResponse(
        job_id=rec.job_id,
        state=rec.state,
        poll_url=f"{base}/v1/ingest/jobs/{rec.job_id}",
        sse_url=f"{base}/v1/ingest/jobs/{rec.job_id}/stream",
    )


@router.get("/ingest/jobs/{job_id}", response_model=IngestJobStatus)
async def get_ingest_job(job_id: str, request: Request) -> IngestJobStatus:
    mgr = request.app.state.job_manager
    rec = await mgr.get(job_id)
    if not rec:
        raise HTTPException(status_code=404, detail="job not found")
    return mgr.status_model(rec)


async def _job_sse(job_id: str, request: Request) -> Any:
    mgr = request.app.state.job_manager
    rec = await mgr.get(job_id)
    if not rec:
        yield {"event": "error", "data": json.dumps({"detail": "job not found"})}
        return
    async for msg in mgr.iter_sse_events(job_id):
        msg = strip_wire_from_json(msg)
        payload = json.dumps(msg, default=str)
        yield {"event": msg.get("type", "message"), "data": payload}


@router.get("/ingest/jobs/{job_id}/stream")
async def ingest_job_stream(job_id: str, request: Request) -> EventSourceResponse:
    return EventSourceResponse(_job_sse(job_id, request))


@router.get("/ontologies")
async def ontologies() -> dict[str, list[str]]:
    s = get_settings()
    return {"ontology_ids": list_ontology_ids(s.ontologies_dir)}
