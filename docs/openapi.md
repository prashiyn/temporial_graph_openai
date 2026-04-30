# HTTP API reference (Temporal Graph RAG)

This document mirrors the service’s HTTP contract for **humans** and **agents** (e.g. Cursor). The canonical machine-readable spec is **[openapi.json](../openapi.json)** (OpenAPI 3.1). Regenerate it after changing routes:

```bash
uv run python scripts/export_openapi.py
```

The running app also exposes **`GET /openapi.json`** (FastAPI default) and interactive **`GET /docs`**.

---

## Conventions

| Item | Value |
|------|--------|
| Base path | All versioned APIs live under **`/v1`** except **`/health`**. |
| Content type | Request/response bodies are **`application/json`** unless noted (SSE uses `text/event-stream`). |
| Errors | Validation issues → **422** with `detail` array. Missing resources → **404** JSON `{"detail": "..."}`. |

---

## Data model concepts (for agents)

1. **Collection** — Logical partition for **documents** and everything hung off them (`Chunk`, `StatementEvent` via `Document` / `Chunk`). Neo4j: `(:Collection {collection_id, name, description, created_at, updated_at})`. Documents: `(:Document)-[:IN_COLLECTION]->(:Collection)` and property `collection_id` on the document.
2. **Ontology** — JSON vocabulary under `ontologies/{id}.json`; ingest sends **`ontology_id`**.
3. **Ingest job** — Async pipeline: LLM extraction → Neo4j merges → optional invalidation. Poll **`GET /v1/ingest/jobs/{job_id}`** or stream **SSE** on **`/v1/ingest/jobs/{job_id}/stream`**.

---

## Endpoints

### `GET /health`

- **Purpose:** Liveness probe.
- **Response:** `{"status": "ok"}`

---

### `POST /v1/collections`

- **Tag:** `collections`
- **Purpose:** **Get-or-create** a collection. Upserts **`name`** and **`description`** on the `Collection` node. If **`collection_id`** is omitted or empty, it is **derived** from **`name`** (lowercase snake_case slug).
- **Body (`CollectionUpsertRequest`):**

| Field | Type | Required | Notes |
|-------|------|----------|--------|
| `name` | string | yes | Display name |
| `description` | string | no | Default `""` |
| `collection_id` | string | no | Slug `^[a-z][a-z0-9_]{0,127}$`; omit to slugify `name` |

- **Response (`CollectionResponse`):** `collection_id`, `name`, `description`, `created` (best-effort: `true` if the collection node did not exist before this transaction’s optional match).

---

### `GET /v1/collections/{collection_id}`

- **Tag:** `collections`
- **Purpose:** **Details** for one collection: metadata plus **counts** of documents, chunks, and statement events reachable from documents in that collection.
- **Path param:** `collection_id` — same slug as ingest / upsert.
- **Response (`CollectionDetailResponse`):** `collection_id`, `name`, `description`, `created_at`, `updated_at` (ISO strings when set), `document_count`, `chunk_count`, `statement_event_count`.
- **Errors:** **404** if no `Collection` with that id.

---

### `POST /v1/ingest/jobs`

- **Tag:** `ingest`
- **Purpose:** Queue a background **ingest** job in a required collection context.
- **Body (`IngestPayload`):** See `openapi.json` → `IngestPayload`. Notable fields:
  - **`ontology_id`** — ontology file stem.
  - **`collection_id`** — required partition slug; must match slug rules.
  - **`chunks`** — non-empty array; all chunks must share one **`doc_id`** and one **`publish_date`** (temporal graph grouping).
- **Response (`IngestJobCreateResponse`):** `job_id`, `state`, `poll_url`, `sse_url`.

---

### `GET /v1/ingest/jobs/{job_id}`

- **Tag:** `ingest`
- **Purpose:** Poll job status and optional **`result_summary`** / **`error`**.
- **Response:** `IngestJobStatus`

---

### `GET /v1/ingest/jobs/{job_id}/stream`

- **Tag:** `ingest`
- **Purpose:** **Server-Sent Events** stream of job progress messages.
- **Response:** `text/event-stream` (schema in OpenAPI is generic).

---

### `GET /v1/ontologies`

- **Tag:** `ingest`
- **Purpose:** List available ontology file ids from `ONTOLOGIES_DIR`.
- **Response:** `{ "ontology_ids": ["...", ...] }`

---

### `POST /v1/retrieve`

- **Tag:** `retrieve`
- **Purpose:** Single-pass RAG answer scoped to one collection, with optional document filter.
- **Body (`RetrievalQuery`):** `question` + `collection_id` (both required); optional `doc_id` + `publish_date` to restrict context.
- **Response (`RetrievalResponse`):** `answer`, `tool_trace`

---

### `POST /v1/retrieve/agent`

- **Tag:** `retrieve`
- **Purpose:** Lightweight multi-hop scoped to one collection: entity neighborhood + optional document slice, then LLM synthesis.
- **Body / response:** Same shapes as `/v1/retrieve`.

---

## Schema index (components)

Open **`openapi.json`** → `components` → `schemas` for full JSON Schema. Commonly used:

- `IngestPayload`, `IngestChunkItem`, `IngestJobCreateResponse`, `IngestJobStatus`
- `CollectionUpsertRequest`, `CollectionResponse`, `CollectionDetailResponse`
- `RetrievalQuery`, `RetrievalResponse`
- `HTTPValidationError` (422)

---

## Agent checklist

1. For a new partition: **`POST /v1/collections`** with `name` (and optional `collection_id`), then ingest and retrieve using that **`collection_id`**.
2. Reuse **`openapi.json`** for codegen or tool definitions; use this file for narrative and domain rules (collections vs ontology).
3. After adding routes or models, re-run **`scripts/export_openapi.py`** and commit **`openapi.json`** if you track the snapshot in git.
