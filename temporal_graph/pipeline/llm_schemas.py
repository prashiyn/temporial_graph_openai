"""Pydantic shapes for doc-processing json_schema completions (notebook-aligned)."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field


TemporalType = Literal["ATEMPORAL", "STATIC", "DYNAMIC"]
StatementType = Literal["FACT", "OPINION", "PREDICTION"]


class RawStatement(BaseModel):
    statement: str
    temporal_type: TemporalType
    statement_type: StatementType


class RawStatementList(BaseModel):
    statements: list[RawStatement] = Field(default_factory=list)


class RawTemporalRange(BaseModel):
    valid_at: str | None = None
    invalid_at: str | None = None


class RawEntity(BaseModel):
    name: str
    type: str
    description: str = ""
    attributes: dict[str, Any] = Field(
        default_factory=dict,
        description=(
            "Optional structured fields aligned with financial_entity_schema.md, flattened snake_case "
            "e.g. company_ticker, company_sector, person_role, institution_type, sector_code, "
            "corp_event_direction, news_headline, price_point_close, impact_probability, etc. "
            "Omit keys you cannot ground in the statement."
        ),
    )


class RawTriplet(BaseModel):
    subject_name: str
    predicate: str
    object_name: str
    value: str | None = None


class RawExtraction(BaseModel):
    triplets: list[RawTriplet] = Field(default_factory=list)
    entities: list[RawEntity] = Field(default_factory=list)
