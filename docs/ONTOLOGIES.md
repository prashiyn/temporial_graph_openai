# Defining and enabling ontologies

Ontologies drive **which canonical events and subevents** are valid for ingest, how **normalized subtypes** default, how **invalidation** behaves (similarity, top-k, **publish-date windows** per subevent), and optionally which **predicate groups** expand candidate triplets for invalidation.

## File location and naming

1. Add a JSON file: **`ontologies/{id}.json`** (directory from `ONTOLOGIES_DIR`, default repo `ontologies/`).
2. Set the top-level **`"id"`** field to the same string as the filename stem (without `.json`).  
   Example: `ontologies/company_data.json` → `"id": "company_data"`.  
   **`load_ontology`** rejects a mismatch so operators cannot accidentally load the wrong definition.
3. Clients pass **`ontology_id`** in ingest payloads equal to that **`id`**.

## Validation

Every ontology file is validated in two steps:

1. **JSON Schema** — [`temporal_graph/ontology/ontology.schema.json`](../temporal_graph/ontology/ontology.schema.json) (Draft 2020-12), applied by **`validate_ontology_json`** inside **`load_ontology`**. Failures raise **`OntologySchemaError`** with a JSON Pointer–style path when possible.
2. **Pydantic** — **`OntologySpec`** parses the same object for runtime types and defaults.

To validate a file from the shell (JSON Schema only; does not check filename vs `id`):

```bash
uv run tg-validate-ontology ontologies/my_ontology.json
```

To validate in Python:

```python
import json
from pathlib import Path
from temporal_graph.ontology import validate_ontology_json

data = json.loads(Path("ontologies/my_ontology.json").read_text())
validate_ontology_json(data)
```

**Note:** `load_ontology` also requires the file stem to match `"id"`; use the CLI above for a quick schema check, or call `load_ontology(Path("ontologies"), "my_ontology")` for the full checks.

IDEs can associate `$schema` with the local file: either open `ontology.schema.json` and use editor “JSON Schema” association, or add to your ontology file:

```json
"$schema": "../temporal_graph/ontology/ontology.schema.json"
```

(optional; the loader does not require `$schema` in instance files)

## Top-level fields

| Field | Required | Description |
|-------|----------|-------------|
| **`id`** | Yes | Slug `[a-z][a-z0-9_]*`; must match filename stem. |
| **`event_tree`** | Yes | Map of **canonical_event** → node (see below). |
| **`schema_version`** | No | Format version of this JSON document (default `"1"`). Not the same as **`ontology_version`**. |
| **`name`** | No | Human-readable title (prompts / metadata). |
| **`ontology_version`** | No | Version string stored on graph artifacts (default `"1.0"`). Bump when vocabulary or rules change. |
| **`description`** | No | Free text for operators or documentation. |
| **`invalidation`** | No | Object controlling invalidation (defaults applied if omitted). |
| **`predicate_groups`** | No | `null` = load from **`predicates/groups.yml`**; or a non-empty array of non-empty string arrays (predicate names). |

Unknown top-level keys are **rejected** by the JSON Schema.

## `event_tree` structure

Keys are **canonical_event** identifiers: **UPPER_SNAKE** (`^[A-Z][A-Z0-9_]*$`). Align names with **`canonical_events.md`** in this repo when possible.

Each value is an object:

- **`subevents`** (required): either  
  - **Array** of unique **canonical_subevent** strings (same naming pattern), or  
  - **Object** mapping each subevent name to optional metadata (see below).
- **`default_normalized_subtype`** (optional): used when ingest does not send `normalized_subtype`; see **`derive_normalized_subtype`** in code.

### Subevents as a list

```json
"EARNINGS_FINANCIALS": {
  "subevents": ["RESULTS", "EARNINGS", "GUIDANCE"],
  "default_normalized_subtype": "RESULTS"
}
```

### Subevents as a map (per-subevent defaults)

Useful when different subevents need different **`default_normalized_subtype`** values:

```json
"EARNINGS_FINANCIALS": {
  "subevents": {
    "RESULTS": { "default_normalized_subtype": "RESULTS" },
    "GUIDANCE": { "normalized_subtype": "GUIDANCE" }
  },
  "default_normalized_subtype": "RESULTS"
}
```

Empty objects `{}` are valid entries; the event-level **`default_normalized_subtype`** may still apply.

## Invalidation block (`invalidation`)

All keys are optional; defaults match **`InvalidationConfig`** in code.

| Field | Meaning |
|-------|--------|
| **`enabled`** | If `false`, batch invalidation is skipped for this ontology. |
| **`similarity_threshold`** | Cosine similarity in **[0, 1]**; candidates below this are dropped before the LLM step. |
| **`top_k`** | Maximum neighbors considered per extracted fact (after filtering). |
| **`default_publish_date_threshold_hours`** | Maximum absolute difference in **document publish dates** (hours) between new and existing material for invalidation to be considered. If set to **`0`**, the loader falls back to **`DEFAULT_INVALIDATION_PUBLISH_DATE_THRESHOLD_HOURS`** from the environment. |
| **`subevent_publish_date_threshold_hours`** | Map **canonical_subevent** → hours, overriding the default for that subevent only. |

This design allows **new ontologies** to opt in or tune invalidation **without code changes**—only JSON (and ingest using the new `ontology_id`).

## Predicate groups

- **`predicate_groups": null`** — use **`PREDICATE_GROUPS_PATH`** (default `predicates/groups.yml`).
- **`predicate_groups": [["A","B"], ["C"]]`** — override for this ontology only; used when expanding related triplets for invalidation.

Predicates must still be consistent with your **`predicates/default.yml`** (or whatever **`PREDICATES_PATH`** points to) for extraction, or extraction may emit predicates not in the allowlist.

## Enabling a new ontology

1. Add **`ontologies/{your_id}.json`** satisfying the schema; set **`"id": "your_id"`**.
2. Ensure **`canonical_events.md`** (or your internal spec) documents the same vocabulary for LLM authors.
3. Call ingest with **`ontology_id": "your_id"`**.
4. List available IDs via the API that wraps **`list_ontology_ids`** (see ingest routes).

No application code change is required unless you introduce **new behavioral hooks** beyond what JSON already expresses.

## Reference implementation

See **`ontologies/company_data.json`** for a full example with list-style **`subevents`** and **`invalidation`**.
