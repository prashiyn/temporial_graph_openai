from __future__ import annotations

import json
import logging
from typing import Any

from fastapi import APIRouter, HTTPException

from temporal_graph.doc_processing.client import DocProcessingClient
from temporal_graph.llm.router import LLMRouter
from temporal_graph.models.api import RetrievalQuery, RetrievalResponse
from temporal_graph.neo4j.driver import get_driver
from temporal_graph.neo4j.repository import GraphRepository
from temporal_graph.settings import get_settings
from temporal_graph.wiring.collection_ns import wire_collection_id

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/v1", tags=["retrieve"])


@router.post("/retrieve", response_model=RetrievalResponse)
async def retrieve(q: RetrievalQuery) -> RetrievalResponse:
    settings = get_settings()
    driver = get_driver(settings)
    repo = GraphRepository(driver, settings)
    tool_trace: list[dict[str, Any]] = []

    if q.doc_id and q.publish_date:
        ctx = await repo.fetch_subgraph_for_doc(q.collection_id, q.doc_id, q.publish_date)
        tool_trace.append({"tool": "fetch_subgraph_for_doc", "result_keys": list(ctx.keys())})
    else:
        ctx = {
            "note": f"No doc filter inside collection '{wire_collection_id(q.collection_id)}'."
        }

    doc_client = DocProcessingClient(settings)
    router_llm = LLMRouter(settings, doc_client)
    try:
        sys = (
            "You are a temporal knowledge-graph analyst. Answer using the JSON context of documents, "
            "chunks, and statement events. If information is missing, say so. Prefer facts tied to dates."
        )
        user = f"Question: {q.question}\n\nContext JSON:\n{json.dumps(ctx, default=str)[:120_000]}"
        answer = await router_llm.complete_text("retrieval_agent", sys, user)
        return RetrievalResponse(answer=answer, tool_trace=tool_trace)
    finally:
        await router_llm.aclose()
        await doc_client.aclose()


@router.post("/retrieve/agent")
async def retrieve_multi_hop(q: RetrievalQuery) -> RetrievalResponse:
    """Lightweight multi-hop: iterative neighborhood expansion + synthesis (v1)."""
    settings = get_settings()
    driver = get_driver(settings)
    repo = GraphRepository(driver, settings)
    doc_client = DocProcessingClient(settings)
    router_llm = LLMRouter(settings, doc_client)
    trace: list[dict[str, Any]] = []

    try:
        focus = q.question.split()[:8]
        sub = max(focus, key=len) if focus else "company"
        nb = await repo.fetch_entity_neighborhood(q.collection_id, sub, limit=30)
        trace.append({"tool": "fetch_entity_neighborhood", "collection_id": q.collection_id, "sub": sub, "rows": len(nb)})

        if q.doc_id and q.publish_date:
            doc_ctx = await repo.fetch_subgraph_for_doc(q.collection_id, q.doc_id, q.publish_date)
            trace.append({"tool": "fetch_subgraph_for_doc", "keys": list(doc_ctx.keys())})
        else:
            doc_ctx = {}

        sys = (
            "You perform multi-hop reasoning over graph excerpts. Connect entities via stated relationships. "
            "Cite which edges support each hop. If the graph excerpt is insufficient, state the gap."
        )
        user = (
            f"Question: {q.question}\n\n"
            f"Neighborhood sample:\n{json.dumps(nb, default=str)[:80_000]}\n\n"
            f"Document slice:\n{json.dumps(doc_ctx, default=str)[:80_000]}"
        )
        answer = await router_llm.complete_text("retrieval_agent", sys, user)
        return RetrievalResponse(answer=answer, tool_trace=trace)
    except Exception as e:
        logger.exception("retrieve/agent failed")
        raise HTTPException(status_code=500, detail=str(e)) from e
    finally:
        await router_llm.aclose()
        await doc_client.aclose()
