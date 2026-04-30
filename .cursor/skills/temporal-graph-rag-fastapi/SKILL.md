---
name: temporal-graph-rag-fastapi
description: Implements and maintains temporal knowledge-graph RAG with FastAPI and agent tool-calling (OpenAI-style tools, multi-hop graph retrieval, validity windows). Aligns event extraction with canonical_events.md. Use when adding or changing FastAPI routes, graph ingestion, retrieval pipelines, temporal agents, or uv/pyproject workflows in this repository.
---

# Temporal graph RAG (FastAPI + uv)

## When to use this skill

- Designing or editing **FastAPI** endpoints for query, ingest, or admin.
- Wiring **LLM agents** with **tools** that read/write a **temporal** graph.
- Ensuring **time-aware** retrieval (point-in-time or interval queries).
- Keeping **financial event** extraction consistent with **`canonical_events.md`**.

## Toolchain

- Use **uv** only: `uv sync`, `uv add`, `uv run …`. Prefer `uv run uvicorn …` for the API.
- Dependencies live in **`pyproject.toml`**; avoid ad-hoc `requirements.txt` unless requested.

## Architecture pointers

1. **HTTP layer**: Pydantic v2 models at the API boundary; map to internal graph/record types inside services.
2. **Shared resources**: DB and model clients in **lifespan** context or injectable dependencies, not hidden globals.
3. **Agents**: Define tools with explicit JSON/Pydantic shapes; implement **executors** that run graph queries and return **validated** payloads.
4. **Temporal RAG**: Prefer multi-step tools (entity resolution → subgraph fetch → time filter → optional rerank) over one opaque “answer” tool when debugging and safety matter.
5. **Ontology**: For event-like facts, restrict outputs to the vocabulary in **`canonical_events.md`**; include **`ontology_version`** on stored artifacts when the schema evolves.

## Checklist for new features

- [ ] Request/response models validated; errors return correct HTTP codes.
- [ ] Times are UTC-normalized; validity fields are documented for consumers.
- [ ] Tool schemas match executor output; failures are structured, not silent.
- [ ] New event types extend **`canonical_events.md`** and versioning, not ad-hoc strings.
- [ ] Commands/docs use **uv**, not raw pip.

## Deep context in-repo

- Runtime behavior: FastAPI ingest routes, **`temporal_graph/pipeline`**, and Neo4j graph repository
- Event hierarchy and JSON shape: **`canonical_events.md`**
