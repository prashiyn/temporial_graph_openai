"""Slug helpers for stable API identifiers."""

from __future__ import annotations

import re


def slugify_collection_id(name: str) -> str:
    """Derive a collection_id from a human-readable name (lowercase snake_case)."""
    s = name.lower().strip()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s or "collection"
