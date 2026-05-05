from __future__ import annotations

import re
from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field, field_validator, model_validator

from temporal_graph.wiring.collection_ns import normalize_inbound_collection_id

_COLLECTION_ID_PATTERN = r"^[a-z][a-z0-9_]{0,127}$"


class IngestChunkItem(BaseModel):
    chunk_id: str = Field(..., description="Stable chunk identifier")
    content: str = Field(..., description="Chunk text, table, or image description")
    type: str = Field(..., description="One of text, table, image")
    doc_id: str = Field(..., description="Document id")
    page: int | None = Field(None, description="Source page when available")
    bundle_id: str = Field(..., description="Semantic bundle id")
    section_title: str | None = Field(None, description="Nearest section heading")
    title_summary: str = Field("", description="LLM section summary")
    publish_date: str | None = Field(None, description="Document publish date if provided")
    prev_chunk: str | None = Field(None, description="Previous chunk id")
    next_chunk: str | None = Field(None, description="Next chunk id")
    canonical_event: str = Field(..., description="Ontology level-1 event")
    canonical_subevent: str = Field(..., description="Ontology level-2 subevent")
    normalized_subtype: str | None = Field(
        None, description="Level-3 subtype; derived server-side if omitted"
    )


class IngestPayload(BaseModel):
    """Batch ingest: all chunks must share the same doc_id + publish_date grouping key."""

    ontology_id: str = Field(..., description="Ontology file id (e.g. company_data)")
    collection_id: str = Field(
        ...,
        description="Graph partition id (required). Documents/chunks/events are ingested in this collection context",
    )
    chunks: list[IngestChunkItem] = Field(..., min_length=1)
    webhook_url: str | None = Field(
        None, description="POST JSON job status on completion (optional)"
    )
    idempotency_key: str | None = None

    @field_validator("collection_id")
    @classmethod
    def _collection_id_slug(cls, v: str) -> str:
        s = (v or "default").strip()
        if not s:
            s = "default"
        if not re.fullmatch(_COLLECTION_ID_PATTERN, s):
            raise ValueError(
                "collection_id must match ^[a-z][a-z0-9_]{0,127}$ (use POST /v1/collections to register names)"
            )
        return normalize_inbound_collection_id(s)

    @model_validator(mode="after")
    def _one_doc_and_date(self) -> IngestPayload:
        doc_ids = {c.doc_id for c in self.chunks}
        if len(doc_ids) != 1:
            raise ValueError("All chunks must have the same doc_id")
        dates: list[str] = []
        for c in self.chunks:
            if not c.publish_date or not str(c.publish_date).strip():
                raise ValueError("Every chunk must include publish_date (ISO) for document grouping")
            d = str(c.publish_date).strip()
            dates.append(d[:10] if len(d) >= 10 else d)
        if len(set(dates)) != 1:
            raise ValueError("All chunks must share the same publish_date for document grouping")
        return self


class JobState(str, Enum):
    pending = "pending"
    running = "running"
    completed = "completed"
    failed = "failed"


class IngestJobCreateResponse(BaseModel):
    job_id: str
    state: JobState = JobState.pending
    poll_url: str
    sse_url: str


class IngestJobStatus(BaseModel):
    job_id: str
    state: JobState
    created_at: datetime
    updated_at: datetime
    error: str | None = None
    result_summary: dict[str, Any] | None = None


class RetrievalQuery(BaseModel):
    question: str = Field(..., description="Natural language question for multi-hop agent")
    collection_id: str = Field(..., description="Required collection partition id")
    doc_id: str | None = Field(None, description="Restrict context to a document")
    publish_date: str | None = Field(None, description="ISO date string matching Document.publish_date")

    @field_validator("collection_id")
    @classmethod
    def _retrieval_collection_id_slug(cls, v: str) -> str:
        s = (v or "").strip()
        if not re.fullmatch(_COLLECTION_ID_PATTERN, s):
            raise ValueError("collection_id must match ^[a-z][a-z0-9_]{0,127}$")
        return normalize_inbound_collection_id(s)


class RetrievalResponse(BaseModel):
    answer: str
    tool_trace: list[dict[str, Any]] = Field(default_factory=list)


class CollectionUpsertRequest(BaseModel):
    """Get-or-create body: pass name plus optional collection_id (if omitted, id is derived from name)."""

    name: str = Field(..., min_length=1, description="Human-readable collection title")
    description: str = Field("", description="Optional operator notes")
    collection_id: str = Field(
        default="",
        description="Stable slug; leave empty to derive from name (slugify)",
    )

    @model_validator(mode="after")
    def _normalize_collection_id(self) -> CollectionUpsertRequest:
        from temporal_graph.utils.slug import slugify_collection_id

        raw = (self.collection_id or "").strip()
        cid = raw if raw else slugify_collection_id(self.name)
        if not re.fullmatch(_COLLECTION_ID_PATTERN, cid):
            raise ValueError(
                "collection_id must match ^[a-z][a-z0-9_]{0,127}$ — provide collection_id explicitly or adjust name"
            )
        return self.model_copy(update={"collection_id": normalize_inbound_collection_id(cid)})


class CollectionResponse(BaseModel):
    """Collection metadata after upsert."""

    collection_id: str
    name: str
    description: str
    created: bool = Field(
        False,
        description="True if this request created the node (best-effort; see implementation)",
    )


class CollectionDetailResponse(BaseModel):
    """Collection plus aggregate counts for documents attached under this partition."""

    collection_id: str
    name: str
    description: str
    created_at: str | None = None
    updated_at: str | None = None
    document_count: int = 0
    chunk_count: int = 0
    statement_event_count: int = 0
