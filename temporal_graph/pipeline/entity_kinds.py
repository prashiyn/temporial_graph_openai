from __future__ import annotations

import re
import uuid

from temporal_graph.models.financial import EntityKind


def infer_kind(raw_type: str) -> EntityKind:
    t = raw_type.lower()
    if any(x in t for x in ("company", "corp", "issuer", "firm")):
        return "Company"
    if "person" in t or "ceo" in t or "director" in t or "executive" in t:
        return "Person"
    if "institution" in t or "bank" in t or "fund" in t or "investor" in t:
        return "Institution"
    if "sector" in t or "industry" in t:
        return "Sector"
    if "news" in t or "article" in t:
        return "News"
    if "price" in t or "quote" in t or "market" in t:
        return "PricePoint"
    if "impact" in t:
        return "Impact"
    if "causal" in t or "hypothesis" in t:
        return "CausalHypothesis"
    if "event" in t or "filing" in t or "disclosure" in t:
        return "CorpEvent"
    return "Company"


def slug_uuid(seed: str) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"tg:{seed}"))


def normalize_predicate(pred: str, allowed: dict[str, str]) -> str:
    if not allowed:
        raise ValueError("predicate vocabulary is empty — check predicates/default.yml")
    p = pred.strip().upper().replace(" ", "_")
    if p in allowed:
        return p
    # strip non-alphanumeric underscores
    clean = re.sub(r"[^A-Z0-9_]", "", p)
    if clean in allowed:
        return clean
    for k in allowed:
        if k in p or p in k:
            return k
    return "IS_A" if "IS_A" in allowed else sorted(allowed.keys())[0]
