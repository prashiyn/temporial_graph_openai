"""Full financial graph entity properties (financial_entity_schema.md) as optional Pydantic fields."""

from __future__ import annotations

from datetime import date, datetime
from typing import Literal

from pydantic import BaseModel, Field

EntityKind = Literal[
    "Company",
    "Person",
    "Institution",
    "Sector",
    "CorpEvent",
    "News",
    "PricePoint",
    "Impact",
    "CausalHypothesis",
]


class CompanyFields(BaseModel):
    ticker: str | None = None
    aliases: list[str] | None = None
    sector: str | None = None
    industry: str | None = None
    country: str | None = None
    exchange: str | None = None
    market_cap: float | None = None
    market_cap_bucket: str | None = None
    listing_date: date | None = None
    is_active: bool | None = None
    volatility_bucket: str | None = None
    beta: float | None = None
    avg_volume: float | None = None
    sentiment_score: float | None = None
    risk_score: float | None = None


class PersonFields(BaseModel):
    role: str | None = None
    company: str | None = None
    start_date: date | None = None
    end_date: date | None = None
    is_active: bool | None = None


class InstitutionFields(BaseModel):
    institution_type: str | None = Field(None, description="maps schema 'type'")
    category: str | None = None
    country: str | None = None


class SectorFields(BaseModel):
    code: str | None = None
    macro_sensitivity_interest_rate: float | None = None
    macro_sensitivity_inflation: float | None = None


class CorpEventFields(BaseModel):
    canonical_event: str | None = None
    canonical_subevent: str | None = None
    normalized_subtype: str | None = None
    event_time: datetime | None = None
    confidence: float | None = None
    direction: str | None = None
    magnitude: str | None = None
    description: str | None = None
    source_type: str | None = None
    ontology_version: str | None = None
    recency_score: float | None = None
    event_weight: float | None = None
    event_cluster_id: str | None = None


class NewsFields(BaseModel):
    source: str | None = None
    author: str | None = None
    timestamp: datetime | None = None
    headline: str | None = None
    content: str | None = None
    sentiment: float | None = None
    sentiment_label: str | None = None
    url: str | None = None
    language: str | None = None
    chunk_count: int | None = None


class PricePointFields(BaseModel):
    timestamp: datetime | None = None
    open: float | None = None
    close: float | None = None
    price_return: float | None = Field(None, description="return from schema")
    volume: float | None = None
    volatility: float | None = None


class ImpactFields(BaseModel):
    short_term_return: float | None = None
    medium_term_return: float | None = None
    probability: float | None = None
    decay_lambda: float | None = None
    time_horizon: str | None = None
    model_source: str | None = None
    decayed_value: float | None = None
    impact_score: float | None = None


class CausalHypothesisFields(BaseModel):
    probability: float | None = None
    confidence: float | None = None
    reasoning: str | None = None
    model_source: str | None = None


class EntityNodePayload(BaseModel):
    """MERGE payload for (:Entity:<Kind>) — name + id required; rest optional."""

    id: str
    name: str
    kind: EntityKind
    description: str | None = None
    source: str | None = None
    version: str | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None

    company: CompanyFields | None = None
    person: PersonFields | None = None
    institution: InstitutionFields | None = None
    sector: SectorFields | None = None
    corp_event: CorpEventFields | None = None
    news: NewsFields | None = None
    price_point: PricePointFields | None = None
    impact: ImpactFields | None = None
    causal_hypothesis: CausalHypothesisFields | None = None

    # Relationship metadata (when relevant)
    rel_event_time: datetime | None = None
    rel_valid_from: datetime | None = None
    rel_valid_to: datetime | None = None
    rel_confidence: float | None = None
    rel_probability: float | None = None

    def flat_properties(self) -> dict:
        """Flatten nested kind-specific fields onto Neo4j property map (snake_case)."""
        base = self.model_dump(
            exclude={"company", "person", "institution", "sector", "corp_event", "news", "price_point", "impact", "causal_hypothesis"},
            exclude_none=True,
        )
        nested = {
            "company": self.company,
            "person": self.person,
            "institution": self.institution,
            "sector": self.sector,
            "corp_event": self.corp_event,
            "news": self.news,
            "price_point": self.price_point,
            "impact": self.impact,
            "causal_hypothesis": self.causal_hypothesis,
        }
        for key, block in nested.items():
            if block is None:
                continue
            for k, v in block.model_dump(exclude_none=True).items():
                if v is not None:
                    base[f"{key}_{k}"] = v
        base["tg_kind"] = self.kind
        return base
