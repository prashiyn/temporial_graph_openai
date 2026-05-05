"""External API uses bare collection slugs; Neo4j and pipeline use `tgo_graph_<slug>`."""

from __future__ import annotations

import re
from typing import Any

# Wire (HTTP / client): ^[a-z][a-z0-9_]{0,127}$
# Internal (Neo4j, jobs, pipeline): this prefix + wire slug
GRAPH_COLLECTION_PREFIX = "tgo_graph_"

_WIRE_SLUG = re.compile(r"^[a-z][a-z0-9_]{0,127}$")


def normalize_inbound_collection_id(v: str) -> str:
    """Accept wire slug or already-internal id; return canonical internal id."""
    s = (v or "").strip()
    if not s:
        raise ValueError("collection_id is required")
    if s.startswith(GRAPH_COLLECTION_PREFIX):
        rest = s[len(GRAPH_COLLECTION_PREFIX) :]
        if not _WIRE_SLUG.fullmatch(rest):
            raise ValueError(
                "collection_id must match ^[a-z][a-z0-9_]{0,127}$ after prefix "
                + GRAPH_COLLECTION_PREFIX
            )
        return s
    if not _WIRE_SLUG.fullmatch(s):
        raise ValueError("collection_id must match ^[a-z][a-z0-9_]{0,127}$")
    return GRAPH_COLLECTION_PREFIX + s


def wire_collection_id(internal: str) -> str:
    """Strip internal prefix for HTTP responses; pass through if already wire."""
    s = (internal or "").strip()
    if s.startswith(GRAPH_COLLECTION_PREFIX):
        return s[len(GRAPH_COLLECTION_PREFIX) :]
    return s


def strip_wire_from_json(obj: Any) -> Any:
    """Recursively strip internal collection prefix for keys named collection_id."""
    if isinstance(obj, dict):
        out: dict[str, Any] = {}
        for k, v in obj.items():
            if k == "collection_id" and isinstance(v, str):
                out[k] = wire_collection_id(v)
            else:
                out[k] = strip_wire_from_json(v)
        return out
    if isinstance(obj, list):
        return [strip_wire_from_json(x) for x in obj]
    return obj


def conflict_detail_wire(doc_id: str, publish_date_key: str, existing_internal: str, requested_internal: str) -> str:
    """Human-readable 409 message using wire collection ids."""
    return (
        f"Document ({doc_id}, {publish_date_key}) already belongs to collection "
        f"'{wire_collection_id(existing_internal)}', cannot ingest into "
        f"'{wire_collection_id(requested_internal)}'"
    )
