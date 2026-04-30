from __future__ import annotations

from typing import Any

from temporal_graph.models.financial import EntityNodePayload
from temporal_graph.pipeline.entity_kinds import infer_kind


def build_flat_entity_props(
    entity_id: str,
    name: str,
    raw_type: str,
    description: str,
    attributes: dict[str, Any] | None,
) -> dict[str, Any]:
    kind = infer_kind(raw_type)
    payload = EntityNodePayload(
        id=entity_id,
        name=name.strip(),
        kind=kind,
        description=(description or "").strip() or None,
    )
    flat = payload.flat_properties()
    for k, v in _coerce_attributes(attributes or {}, kind).items():
        if v is not None and k != "id":
            flat[k] = v
    flat["tg_type"] = raw_type
    return flat


def _coerce_attributes(attributes: dict[str, Any], kind: str) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for key, val in attributes.items():
        if val is None:
            continue
        if isinstance(val, str) and not val.strip():
            continue
        k = key.strip()
        if "_" in k:
            out[k] = val
            continue
        prefix = {
            "Company": "company",
            "Person": "person",
            "Institution": "institution",
            "Sector": "sector",
            "CorpEvent": "corp_event",
            "News": "news",
            "PricePoint": "price_point",
            "Impact": "impact",
            "CausalHypothesis": "causal_hypothesis",
        }.get(kind, "company")
        out[f"{prefix}_{k}"] = val
    return out


def merge_entity_properties(existing: dict[str, Any], incoming: dict[str, Any]) -> dict[str, Any]:
    merged = dict(existing)
    for k, v in incoming.items():
        if k == "id":
            continue
        if v is None:
            continue
        if isinstance(v, str) and not v.strip():
            continue
        if isinstance(v, (list, dict)) and not v:
            continue
        merged[k] = v
    return merged
