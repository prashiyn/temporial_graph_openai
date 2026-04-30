"""CLI: validate an ontology JSON file against ontology.schema.json (no Neo4j required)."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from temporal_graph.ontology.loader import OntologySchemaError, validate_ontology_json


def main() -> int:
    """Console entry point (``tg-validate-ontology``); uses ``sys.argv``."""
    return _run(sys.argv[1:])


def _run(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description="Validate a temporal-graph ontology JSON file.")
    parser.add_argument(
        "path",
        type=Path,
        help="Path to ontology .json (e.g. ontologies/company_data.json)",
    )
    args = parser.parse_args(argv)
    path: Path = args.path
    if not path.is_file():
        print(f"Not a file: {path}", file=sys.stderr)
        return 2
    try:
        raw = path.read_text(encoding="utf-8")
        data = json.loads(raw)
    except json.JSONDecodeError as e:
        print(f"Invalid JSON: {e}", file=sys.stderr)
        return 2
    if not isinstance(data, dict):
        print("Ontology file must be a JSON object.", file=sys.stderr)
        return 2
    try:
        validate_ontology_json(data)
    except OntologySchemaError as e:
        print(str(e), file=sys.stderr)
        return 1
    print(f"OK: {path} (JSON Schema)")
    return 0


if __name__ == "__main__":
    raise SystemExit(_run(sys.argv[1:]))
