from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class ChatMessage(BaseModel):
    role: str
    content: str


class CompletionRequest(BaseModel):
    provider: str
    messages: list[dict[str, str]] = Field(..., min_length=1)
    model: str | None = None
    reasoning_effort: str | None = None
    response_format: dict[str, Any] | None = None


class CompletionResponse(BaseModel):
    content: str
    parsed: Any | None = None


class EmbeddingRequest(BaseModel):
    provider: str
    input: str | list[str]
    model: str | None = None
    encoding_format: str | None = None
    dimensions: int | None = None
    input_type: str | None = None
    user: str | None = None


class EmbeddingDataItem(BaseModel):
    object: str
    index: int
    embedding: list[float] | str


class EmbeddingResponse(BaseModel):
    object: str
    model: str
    data: list[EmbeddingDataItem]


class ModelsResponse(BaseModel):
    default_model: str
    fallback_model: str
    models: list[str]
