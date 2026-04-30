from __future__ import annotations

from fastapi import APIRouter, HTTPException

from temporal_graph.models.api import (
    CollectionDetailResponse,
    CollectionResponse,
    CollectionUpsertRequest,
)
from temporal_graph.neo4j.driver import get_driver
from temporal_graph.neo4j.repository import GraphRepository
from temporal_graph.settings import get_settings

router = APIRouter(prefix="/v1", tags=["collections"])


def _prop_str(props: dict[str, object], key: str) -> str | None:
    v = props.get(key)
    if v is None:
        return None
    return str(v)


@router.post("/collections", response_model=CollectionResponse)
async def upsert_collection(body: CollectionUpsertRequest) -> CollectionResponse:
    """Get or create a collection by id (and update name/description)."""
    settings = get_settings()
    driver = get_driver(settings)
    repo = GraphRepository(driver, settings)
    created, props = await repo.upsert_collection(
        body.collection_id,
        body.name.strip(),
        (body.description or "").strip(),
    )
    return CollectionResponse(
        collection_id=str(props.get("collection_id", body.collection_id)),
        name=str(props.get("name", body.name)),
        description=str(props.get("description", "")),
        created=created,
    )


@router.get("/collections/{collection_id}", response_model=CollectionDetailResponse)
async def get_collection(collection_id: str) -> CollectionDetailResponse:
    """Return collection metadata and document/chunk/event counts."""
    settings = get_settings()
    driver = get_driver(settings)
    repo = GraphRepository(driver, settings)
    row = await repo.fetch_collection_detail(collection_id.strip())
    if not row:
        raise HTTPException(status_code=404, detail="collection not found")
    col = row["collection"]
    return CollectionDetailResponse(
        collection_id=str(col.get("collection_id", collection_id)),
        name=str(col.get("name", collection_id)),
        description=str(col.get("description", "")),
        created_at=_prop_str(col, "created_at"),
        updated_at=_prop_str(col, "updated_at"),
        document_count=row["document_count"],
        chunk_count=row["chunk_count"],
        statement_event_count=row["statement_event_count"],
    )
