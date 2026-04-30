from __future__ import annotations

import json
from pathlib import Path

import pytest

from temporal_graph.ontology.loader import (
    OntologySchemaError,
    load_ontology,
    validate_ontology_json,
)

REPO_ROOT = Path(__file__).resolve().parents[1]


def test_company_data_validates_and_loads() -> None:
    path = REPO_ROOT / "ontologies" / "company_data.json"
    data = json.loads(path.read_text(encoding="utf-8"))
    validate_ontology_json(data)
    spec = load_ontology(REPO_ROOT / "ontologies", "company_data")
    assert spec.id == "company_data"
    assert "EARNINGS_FINANCIALS" in spec.event_tree


def test_rejects_unknown_root_key() -> None:
    with pytest.raises(OntologySchemaError, match="Additional properties"):
        validate_ontology_json(
            {
                "id": "x",
                "event_tree": {"A": {"subevents": ["B"]}},
                "extra_field": 1,
            }
        )


def test_rejects_bad_id_pattern() -> None:
    with pytest.raises(OntologySchemaError):
        validate_ontology_json(
            {
                "id": "BadId",
                "event_tree": {"A": {"subevents": ["B"]}},
            }
        )


def test_rejects_filename_id_mismatch(tmp_path: Path) -> None:
    p = tmp_path / "my_onto.json"
    p.write_text(
        json.dumps(
            {
                "id": "other_id",
                "event_tree": {"CANON": {"subevents": ["SUB"]}},
            }
        ),
        encoding="utf-8",
    )
    with pytest.raises(OntologySchemaError, match="Filename stem"):
        load_ontology(tmp_path, "my_onto")


def test_dict_form_subevents() -> None:
    validate_ontology_json(
        {
            "id": "demo",
            "event_tree": {
                "EARNINGS": {
                    "subevents": {
                        "RESULTS": {"default_normalized_subtype": "RESULTS"},
                        "GUIDANCE": {},
                    },
                    "default_normalized_subtype": "RESULTS",
                }
            },
        }
    )
