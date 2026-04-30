from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml
from jsonschema import Draft202012Validator
from jsonschema.exceptions import ValidationError as JsonSchemaValidationError
from pydantic import BaseModel, Field

from temporal_graph.settings import Settings, get_settings


class OntologySchemaError(ValueError):
    """Raised when ontology JSON fails JSON Schema validation."""


@lru_cache(maxsize=1)
def _ontology_json_schema_validator() -> Draft202012Validator:
    schema_path = Path(__file__).resolve().parent / "ontology.schema.json"
    with schema_path.open(encoding="utf-8") as f:
        schema = json.load(f)
    return Draft202012Validator(schema)


def validate_ontology_json(data: dict[str, Any]) -> None:
    """Validate raw ontology dict against ``ontology.schema.json`` (before Pydantic)."""
    validator = _ontology_json_schema_validator()
    try:
        validator.validate(data)
    except JsonSchemaValidationError as e:
        path = ".".join(str(p) for p in e.absolute_path) if e.absolute_path else "(root)"
        msg = e.message
        if path and path != "(root)":
            msg = f"At {path}: {e.message}"
        raise OntologySchemaError(msg) from e


class InvalidationConfig(BaseModel):
    """Per-ontology invalidation (enable via config only — no code changes for new ontologies)."""

    enabled: bool = True
    similarity_threshold: float = 0.5
    top_k: int = 10
    default_publish_date_threshold_hours: float = 12.0
    subevent_publish_date_threshold_hours: dict[str, float] = Field(
        default_factory=dict,
        description="Hours window: |publish_date_new - publish_date_existing| must be <= this to allow invalidation LLM for this canonical_subevent.",
    )


class OntologySpec(BaseModel):
    """Ontology definition JSON (multi-ontology safe).

    Required: id, event_tree with list subevents per canonical_event.
    Optional: invalidation, predicate_groups (override file-based groups).
    """

    schema_version: str = "1"
    id: str
    name: str = ""
    ontology_version: str = "1.0"
    description: str = ""
    event_tree: dict[str, dict[str, Any]] = Field(default_factory=dict)
    invalidation: InvalidationConfig = Field(default_factory=InvalidationConfig)
    predicate_groups: list[list[str]] | None = Field(
        None,
        description="If null, load from predicates/groups.yml",
    )

    def validate_event(self, canonical_event: str, canonical_subevent: str) -> None:
        if canonical_event not in self.event_tree:
            allowed = ", ".join(sorted(self.event_tree.keys()))
            raise ValueError(f"canonical_event '{canonical_event}' not in ontology '{self.id}'. Allowed: {allowed}")
        node = self.event_tree[canonical_event]
        sub = node.get("subevents") or []
        if isinstance(sub, dict):
            keys = set(sub.keys())
        else:
            keys = set(sub)
        if canonical_subevent not in keys:
            raise ValueError(
                f"canonical_subevent '{canonical_subevent}' not under '{canonical_event}' in ontology '{self.id}'"
            )

    def publish_date_threshold_hours(self, canonical_subevent: str, settings: Settings | None = None) -> float:
        s = settings or get_settings()
        env_default = float(s.default_invalidation_publish_date_threshold_hours)
        if canonical_subevent in self.invalidation.subevent_publish_date_threshold_hours:
            return float(self.invalidation.subevent_publish_date_threshold_hours[canonical_subevent])
        ont = float(self.invalidation.default_publish_date_threshold_hours)
        return ont if ont > 0 else env_default

    def resolved_predicate_groups(self, groups_yml: Path) -> list[list[str]]:
        if self.predicate_groups is not None:
            return self.predicate_groups
        if not groups_yml.is_file():
            return []
        with groups_yml.open(encoding="utf-8") as f:
            raw = yaml.safe_load(f)
        return list(raw.get("groups", []))


def load_ontology(ontologies_dir: Path, ontology_id: str) -> OntologySpec:
    path = ontologies_dir / f"{ontology_id}.json"
    if not path.is_file():
        raise FileNotFoundError(f"Ontology '{ontology_id}' not found at {path}")
    with path.open(encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, dict):
        raise OntologySchemaError(f"Ontology file must contain a JSON object, got {type(data).__name__}")
    validate_ontology_json(data)
    file_id = data.get("id")
    if file_id != ontology_id:
        raise OntologySchemaError(
            f'Filename stem {ontology_id!r} must match JSON "id" field {file_id!r} ({path})'
        )
    return OntologySpec.model_validate(data)


def list_ontology_ids(ontologies_dir: Path) -> list[str]:
    if not ontologies_dir.is_dir():
        return []
    return sorted(p.stem for p in ontologies_dir.glob("*.json"))
