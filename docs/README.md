# Documentation

| Document | Purpose |
|----------|---------|
| [DEVELOPER.md](./DEVELOPER.md) | Architecture, layout, configuration, running the API, ingestion and external services |
| [ONTOLOGIES.md](./ONTOLOGIES.md) | Authoring ontology JSON, JSON Schema rules, invalidation and predicate groups |
| [openapi.md](./openapi.md) | Human/agent-oriented API reference (collections, ingest, retrieve, health) |
| [../openapi.json](../openapi.json) | Machine-readable OpenAPI 3.1 export (regenerate with `uv run python scripts/export_openapi.py`) |

Ontology files are validated at load time against `temporal_graph/ontology/ontology.schema.json`. From the repo root you can also run:

```bash
uv run tg-validate-ontology ontologies/company_data.json
```
# Moved

This service now lives in the [common_rag](https://github.com/prashiyn/common_rag) monorepo under `<service-path>/`.
