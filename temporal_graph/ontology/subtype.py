from __future__ import annotations

from temporal_graph.ontology.loader import OntologySpec


def derive_normalized_subtype(
    spec: OntologySpec,
    canonical_event: str,
    canonical_subevent: str,
    provided: str | None,
) -> str:
    if provided and provided.strip():
        return provided.strip()
    node = spec.event_tree.get(canonical_event) or {}
    sub = node.get("subevents")
    if isinstance(sub, dict) and canonical_subevent in sub:
        entry = sub[canonical_subevent]
        if isinstance(entry, dict):
            d = entry.get("default_normalized_subtype") or entry.get("normalized_subtype")
            if isinstance(d, str) and d.strip():
                return d.strip()
    default = node.get("default_normalized_subtype")
    if isinstance(default, str) and default.strip():
        return default.strip()
    return canonical_subevent
