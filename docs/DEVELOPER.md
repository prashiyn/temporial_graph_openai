# Developer guide

**Other docs:** [Ontology authoring](./ONTOLOGIES.md) · [Documentation index](./README.md)

This repository implements a **temporal knowledge-graph RAG** service: documents are classified under a configurable **ontology**, statements and triplets are extracted via a **LLM service** LLM proxy, stored in **Neo4j**, and optionally processed through **embedding-based invalidation** with **publish-date proximity** rules. Background ingestion uses **asyncio** jobs with optional **Redis** for multi-worker deployments.

## Tooling

- Use **[uv](https://github.com/astral-sh/uv)** for environments and commands: `uv sync`, `uv run uvicorn`, `uv run pytest`, `uv run ruff check`.
- Dependencies and the application package are declared in **`pyproject.toml`**; the installable package is **`temporal_graph`**.

## Repository layout

| Path | Role |
|------|------|
| `temporal_graph/api/` | FastAPI app, ingest and retrieve routers |
| `temporal_graph/neo4j/` | Driver helpers, schema bootstrap, graph repository (MERGE patterns, invalidation queries) |
| `temporal_graph/pipeline/` | Ingestion (`TemporalIngestionPipeline`), invalidation, entity enrichment/resolution, LLM response schemas |
| `temporal_graph/ontology/` | Ontology loader, JSON Schema (`ontology.schema.json`), subtype derivation |
| `temporal_graph/jobs/` | In-memory or Redis-backed ingest job manager and worker loop |
| `temporal_graph/llm/` | Router to LLM service HTTP API (structured JSON completions) |
| `temporal_graph/doc_processing/` | HTTP client for the external LLM service service |
| `temporal_graph/models/` | Pydantic models for API, pipeline, and financial entity payloads |
| `temporal_graph/settings.py` | `pydantic-settings` from environment / `.env` |
| `ontologies/` | One JSON file per ontology (`{id}.json`); validated on load |
| `predicates/` | Default predicate list and **groups** for invalidation expansion |
| `llm_config.yml` | Role → provider routing for the LLM router |

## Configuration

Copy **`.env.sample`** to **`.env`** and set at least:

- **Neo4j**: `NEO4J_URI`, `NEO4J_USER`, `NEO4J_PASSWORD`, optional `NEO4J_DATABASE`
- **LLM service**: `LLM_SERVICE_BASE_URL` (and timeouts/retries as needed)
- **Paths**: `LLM_CONFIG_PATH`, `ONTOLOGIES_DIR`, `PREDICATES_PATH`, `PREDICATE_GROUPS_PATH` (repo-root-relative paths are resolved automatically)
- **Invalidation fallback**: `DEFAULT_INVALIDATION_PUBLISH_DATE_THRESHOLD_HOURS` (used when ontology default is `0` or omitted in logic—see ontology docs)
- **Jobs**: `JOB_BACKEND` (`memory` or `redis`), and if Redis: `REDIS_URL`, `REDIS_JOB_QUEUE_KEY`, `REDIS_JOB_KEY_PREFIX`, `INGEST_START_REDIS_WORKER`

Full reference: **`temporal_graph/settings.py`**.

## Running the API

```bash
uv sync
uv run uvicorn temporal_graph.api.main:app --host 0.0.0.0 --port 8080
```

- **`GET /health`** — liveness
- Ingest and ontology listing live under the ingest router (see **`temporal_graph/api/ingest_routes.py`**)
- Retrieval routes: **`temporal_graph/api/retrieve_routes.py`**

On startup, the app bootstraps Neo4j constraints/indexes where possible (**`temporal_graph/neo4j/bootstrap.py`**). If Neo4j is unreachable, the API may still start; ingestion will fail until the database is available.

## Ingestion flow (high level)

1. Client submits an **ingest payload** (`IngestPayload`) with required `collection_id`, `ontology_id`, document metadata, and text chunks with `canonical_event` / `canonical_subevent` (and optional `normalized_subtype`).
2. **`JobManager`** creates a job (memory task queue or Redis queue + hash state).
3. **`TemporalIngestionPipeline.ingest`** loads **`OntologySpec`** via **`load_ontology`**, validates chunk events against **`event_tree`**, derives **`normalized_subtype`**, calls the LLM router for temporal bounds and triplets, merges entities in Neo4j, writes statement events and `TG_REL` edges.
4. If ontology **`invalidation.enabled`**, **`run_batch_invalidation`** runs: candidate facts from shared entities/predicate groups, cosine similarity, **publish-date threshold** per subevent, then LLM true/false for invalidation.

## External services

- **Neo4j** — system of record for documents, chunks, statement events, entities, and triplet relationships.
- **LLM service service** — embeddings and structured LLM outputs; configured by URL in settings. The app does not call OpenAI directly for the main ingest path unless a role in `llm_config.yml` is set to a direct provider.

## Collections (graph partitions)

**Collections** isolate document processing contexts: each `(:Document)` is linked with `[:IN_COLLECTION]->(:Collection)` and stores `collection_id`. Ingest payloads require **`collection_id`**; retrieval queries also require it. Invalidation and retrieval are scoped to that collection. A `(doc_id, publish_date)` pair is exclusive to one collection; ingest into another collection is rejected. Prefer **`POST /v1/collections`** to register a friendly **name** / **description** first.

## Ontologies

Ontology JSON files are validated with **`temporal_graph/ontology/ontology.schema.json`** before Pydantic parsing. Authoring and field semantics are documented in **`docs/ONTOLOGIES.md`**.

## Testing and quality

```bash
uv run pytest
uv run ruff check temporal_graph tests
```

Add tests under **`tests/`**; `pytest-asyncio` is configured for async tests in **`pyproject.toml`**.

## Common issues

- **`OntologySchemaError`** — JSON does not match the ontology schema, or filename stem does not match `"id"`.
- **Redis worker** — With `JOB_BACKEND=redis`, either enable `INGEST_START_REDIS_WORKER` on API processes or run a dedicated worker that calls the same job execution path (see **`ingest_worker_loop`** in **`temporal_graph/jobs/manager.py`**).
