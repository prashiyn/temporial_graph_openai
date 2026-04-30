from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field


class StatementEventPayload(BaseModel):
    id: str
    chunk_id: str
    doc_id: str
    collection_id: str
    publish_date: str
    statement: str
    statement_type: str
    temporal_type: str
    valid_at: datetime | None = None
    invalid_at: datetime | None = None
    expired_at: datetime | None = None
    invalidated_by: str | None = None
    created_at: datetime
    embedding: list[float] = Field(default_factory=list)
    ontology_id: str
    ontology_version: str
    canonical_event: str
    canonical_subevent: str
    normalized_subtype: str


class PipelineEntity(BaseModel):
    id: str
    name: str
    tg_type: str
    description: str = ""
    resolved_id: str | None = None
    financial: dict[str, Any] = Field(default_factory=dict)


class ExtractedTriplet(BaseModel):
    id: str
    event_id: str
    subject_name: str
    subject_id: str
    predicate: str
    object_name: str
    object_id: str
    value: str | None = None
