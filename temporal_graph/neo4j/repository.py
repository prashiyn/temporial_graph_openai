from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any

from neo4j import AsyncDriver

from temporal_graph.models.pipeline import ExtractedTriplet, PipelineEntity, StatementEventPayload
from temporal_graph.pipeline.entity_enrichment import merge_entity_properties
from temporal_graph.settings import Settings, get_settings
from temporal_graph.wiring.collection_ns import conflict_detail_wire

logger = logging.getLogger(__name__)


class DocumentCollectionConflictError(ValueError):
    """Raised when an existing (doc_id, publish_date) belongs to a different collection."""


def _iso(dt: datetime | None) -> str | None:
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(microsecond=0).isoformat() + "Z"
    return dt.astimezone().replace(microsecond=0).isoformat()


class GraphRepository:
    def __init__(self, driver: AsyncDriver, settings: Settings | None = None) -> None:
        self._driver = driver
        self._settings = settings or get_settings()

    def _db(self) -> str | None:
        return self._settings.neo4j_database

    async def merge_document(
        self,
        doc_id: str,
        publish_date: str,
        collection_id: str,
        extra: dict[str, Any] | None = None,
    ) -> None:
        pd = publish_date[:10] if len(publish_date) >= 10 else publish_date
        ex = dict(extra or {})
        ex["collection_id"] = collection_id
        ts = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
        q_check = """
        MATCH (d:Document {doc_id: $doc_id, publish_date: $pd})-[:IN_COLLECTION]->(col:Collection)
        RETURN col.collection_id AS cid
        LIMIT 1
        """
        q = """
        MERGE (d:Document {doc_id: $doc_id, publish_date: $pd})
        SET d += $extra
        WITH d
        MERGE (col:Collection {collection_id: $cid})
        ON CREATE SET col.name = $cid, col.description = '', col.created_at = $ts, col.updated_at = $ts
        ON MATCH SET col.updated_at = $ts
        MERGE (d)-[:IN_COLLECTION]->(col)
        """
        async with self._driver.session(database=self._db()) as session:
            check = await session.run(q_check, {"doc_id": doc_id, "pd": pd})
            row = await check.single()
            if row and str(row["cid"]) != collection_id:
                raise DocumentCollectionConflictError(
                    conflict_detail_wire(
                        doc_id, pd, str(row["cid"]), collection_id
                    )
                )
            await session.run(
                q,
                {"doc_id": doc_id, "pd": pd, "extra": ex, "cid": collection_id, "ts": ts},
            )

    async def assert_document_collection_compatible(
        self,
        doc_id: str,
        publish_date: str,
        collection_id: str,
    ) -> None:
        """Preflight check for API-level 409 before enqueueing ingest jobs."""
        pd = publish_date[:10] if len(publish_date) >= 10 else publish_date
        q = """
        MATCH (d:Document {doc_id: $doc_id, publish_date: $pd})-[:IN_COLLECTION]->(col:Collection)
        RETURN col.collection_id AS cid
        LIMIT 1
        """
        async with self._driver.session(database=self._db()) as session:
            r = await session.run(q, {"doc_id": doc_id, "pd": pd})
            row = await r.single()
            if row and str(row["cid"]) != collection_id:
                raise DocumentCollectionConflictError(
                    conflict_detail_wire(
                        doc_id, pd, str(row["cid"]), collection_id
                    )
                )

    async def upsert_collection(
        self,
        collection_id: str,
        name: str,
        description: str = "",
    ) -> tuple[bool, dict[str, Any]]:
        """MERGE Collection; return (created, properties(col))."""
        ts = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
        q = """
        OPTIONAL MATCH (pre:Collection {collection_id: $cid})
        WITH pre IS NULL AS created
        MERGE (col:Collection {collection_id: $cid})
        ON CREATE SET col.created_at = $ts
        SET col.name = $name,
            col.description = $desc,
            col.updated_at = $ts
        RETURN created, properties(col) AS props
        """
        async with self._driver.session(database=self._db()) as session:
            result = await session.run(
                q,
                {"cid": collection_id, "name": name, "desc": description or "", "ts": ts},
            )
            row = await result.single()
            if not row:
                return False, {}
            props = dict(row["props"])
            return bool(row["created"]), props

    async def fetch_collection_detail(self, collection_id: str) -> dict[str, Any] | None:
        """Return collection properties and counts; None if collection missing."""
        q_col = """
        MATCH (col:Collection {collection_id: $cid})
        RETURN properties(col) AS col
        """
        q_docs = """
        MATCH (:Collection {collection_id: $cid})<-[:IN_COLLECTION]-(d:Document)
        RETURN count(DISTINCT d) AS c
        """
        q_chunks = """
        MATCH (:Collection {collection_id: $cid})<-[:IN_COLLECTION]-(d:Document)-[:HAS_CHUNK]->(c:Chunk)
        RETURN count(DISTINCT c) AS c
        """
        q_events = """
        MATCH (:Collection {collection_id: $cid})<-[:IN_COLLECTION]-(d:Document)-[:HAS_STATEMENT_EVENT]->(e:StatementEvent)
        RETURN count(DISTINCT e) AS c
        """
        async with self._driver.session(database=self._db()) as session:
            r1 = await session.run(q_col, {"cid": collection_id})
            row1 = await r1.single()
            if not row1 or not row1["col"]:
                return None
            col = dict(row1["col"])
            r2 = await session.run(q_docs, {"cid": collection_id})
            row2 = await r2.single()
            doc_count = int(row2["c"]) if row2 else 0
            r3 = await session.run(q_chunks, {"cid": collection_id})
            row3 = await r3.single()
            chunk_count = int(row3["c"]) if row3 else 0
            r4 = await session.run(q_events, {"cid": collection_id})
            row4 = await r4.single()
            event_count = int(row4["c"]) if row4 else 0
        return {
            "collection": col,
            "document_count": doc_count,
            "chunk_count": chunk_count,
            "statement_event_count": event_count,
        }

    async def merge_chunk(
        self,
        doc_id: str,
        publish_date: str,
        chunk: dict[str, Any],
        ontology_id: str,
        ontology_version: str,
        canonical_event: str,
        canonical_subevent: str,
        normalized_subtype: str,
    ) -> None:
        pd = publish_date[:10] if len(publish_date) >= 10 else publish_date
        q = """
        MATCH (d:Document {doc_id: $doc_id, publish_date: $pd})
        MERGE (c:Chunk {chunk_id: $chunk_id})
        SET c.content = $content,
            c.type = $ctype,
            c.page = $page,
            c.bundle_id = $bundle_id,
            c.section_title = $section_title,
            c.title_summary = $title_summary,
            c.prev_chunk = $prev_chunk,
            c.next_chunk = $next_chunk,
            c.ontology_id = $ontology_id,
            c.ontology_version = $ontology_version,
            c.canonical_event = $canonical_event,
            c.canonical_subevent = $canonical_subevent,
            c.normalized_subtype = $normalized_subtype,
            c.metadata_json = $metadata_json
        MERGE (d)-[:HAS_CHUNK]->(c)
        """
        meta = {k: v for k, v in chunk.items() if k not in {"chunk_id", "content", "type"}}
        async with self._driver.session(database=self._db()) as session:
            await session.run(
                q,
                {
                    "doc_id": doc_id,
                    "pd": pd,
                    "chunk_id": chunk["chunk_id"],
                    "content": chunk["content"],
                    "ctype": chunk["type"],
                    "page": chunk.get("page"),
                    "bundle_id": chunk.get("bundle_id"),
                    "section_title": chunk.get("section_title"),
                    "title_summary": chunk.get("title_summary") or "",
                    "prev_chunk": chunk.get("prev_chunk"),
                    "next_chunk": chunk.get("next_chunk"),
                    "ontology_id": ontology_id,
                    "ontology_version": ontology_version,
                    "canonical_event": canonical_event,
                    "canonical_subevent": canonical_subevent,
                    "normalized_subtype": normalized_subtype,
                    "metadata_json": json.dumps(meta, default=str),
                },
            )

    async def merge_statement_event(self, ev: StatementEventPayload) -> None:
        pd = ev.publish_date[:10] if len(ev.publish_date) >= 10 else ev.publish_date
        q = """
        MATCH (c:Chunk {chunk_id: $chunk_id})
        MERGE (e:StatementEvent {id: $id})
        SET e.doc_id = $doc_id,
            e.collection_id = $collection_id,
            e.publish_date = $pd,
            e.statement = $statement,
            e.statement_type = $statement_type,
            e.temporal_type = $temporal_type,
            e.valid_at = $valid_at,
            e.invalid_at = $invalid_at,
            e.expired_at = $expired_at,
            e.invalidated_by = $invalidated_by,
            e.created_at = $created_at,
            e.embedding = $embedding,
            e.ontology_id = $ontology_id,
            e.ontology_version = $ontology_version,
            e.canonical_event = $canonical_event,
            e.canonical_subevent = $canonical_subevent,
            e.normalized_subtype = $normalized_subtype
        MERGE (c)-[:HAS_STATEMENT_EVENT]->(e)
        WITH e
        MATCH (d:Document {doc_id: $doc_id, publish_date: $pd})
        MERGE (d)-[:HAS_STATEMENT_EVENT]->(e)
        """
        async with self._driver.session(database=self._db()) as session:
            await session.run(
                q,
                {
                    "chunk_id": ev.chunk_id,
                    "id": ev.id,
                    "doc_id": ev.doc_id,
                    "collection_id": ev.collection_id,
                    "pd": pd,
                    "statement": ev.statement,
                    "statement_type": ev.statement_type,
                    "temporal_type": ev.temporal_type,
                    "valid_at": _iso(ev.valid_at),
                    "invalid_at": _iso(ev.invalid_at),
                    "expired_at": _iso(ev.expired_at),
                    "invalidated_by": ev.invalidated_by,
                    "created_at": _iso(ev.created_at) or "",
                    "embedding": ev.embedding,
                    "ontology_id": ev.ontology_id,
                    "ontology_version": ev.ontology_version,
                    "canonical_event": ev.canonical_event,
                    "canonical_subevent": ev.canonical_subevent,
                    "normalized_subtype": ev.normalized_subtype,
                },
            )

    async def merge_entity_node(self, flat_props: dict[str, Any], labels: list[str]) -> None:
        """Create / replace-style merge (legacy). Prefer merge_entity_node_merged for incremental enrichment."""
        label_expr = ":" + ":".join(labels)
        q = f"""
        MERGE (n{label_expr} {{id: $id}})
        SET n += $props
        """
        pid = flat_props["id"]
        async with self._driver.session(database=self._db()) as session:
            await session.run(q, {"id": pid, "props": {k: v for k, v in flat_props.items() if k != "id"}})

    async def merge_entity_node_merged(self, flat_props: dict[str, Any], labels: list[str]) -> None:
        """MERGE Entity by id; combine existing Neo4j properties with new evidence (non-empty wins)."""
        nid = str(flat_props["id"])
        props_in = {k: v for k, v in flat_props.items() if k != "id"}
        async with self._driver.session(database=self._db()) as session:
            r = await session.run("MATCH (n:Entity {id: $id}) RETURN properties(n) AS p", {"id": nid})
            row = await r.single()
            old = dict(row["p"]) if row else {}
            merged = merge_entity_properties(old, props_in)
            r2 = await session.run("MATCH (n:Entity {id: $id}) RETURN n", {"id": nid})
            exists = await r2.single()
            payload = {k: v for k, v in merged.items() if k != "id"}
            if exists:
                await session.run(
                    "MATCH (n:Entity {id: $id}) SET n += $props",
                    {"id": nid, "props": payload},
                )
            else:
                label_expr = ":" + ":".join(labels)
                await session.run(
                    f"CREATE (n{label_expr} {{id: $id}}) SET n += $props",
                    {"id": nid, "props": payload},
                )

    async def has_statement_events(self, collection_id: str) -> bool:
        q = """
        MATCH (:Collection {collection_id: $cid})<-[:IN_COLLECTION]-(:Document)-[:HAS_STATEMENT_EVENT]->(e:StatementEvent)
        RETURN count(e) AS c LIMIT 1
        """
        async with self._driver.session(database=self._db()) as session:
            r = await session.run(q, {"cid": collection_id})
            row = await r.single()
            return bool(row and int(row["c"]) > 0)

    async def fetch_related_triplet_events_for_invalidation(
        self,
        collection_id: str,
        entity_ids: list[str],
        predicates: list[str],
    ) -> tuple[list[ExtractedTriplet], list[dict[str, Any]]]:
        """Related TG_REL rows joined to FACT StatementEvents (notebook batch_fetch_related_triplet_events)."""
        if not entity_ids or not predicates:
            return [], []
        q = """
        MATCH (es:Entity)-[r:TG_REL]->(eo:Entity)
        WHERE (es.id IN $eids OR eo.id IN $eids) AND r.predicate IN $preds
        MATCH (ev:StatementEvent {id: r.event_id})
        MATCH (d:Document)-[:HAS_STATEMENT_EVENT]->(ev)-[:MENTIONS_ENTITY]->(:Entity)
        MATCH (d)-[:IN_COLLECTION]->(:Collection {collection_id: $cid})
        WHERE ev.statement_type = 'FACT'
        RETURN DISTINCT r.triplet_id AS tid, r.event_id AS eid, r.predicate AS pred, r.value AS val,
               es.id AS sid, es.name AS sname, eo.id AS oid, eo.name AS oname, properties(ev) AS evp
        """
        triplets: list[ExtractedTriplet] = []
        events_by_id: dict[str, dict[str, Any]] = {}
        async with self._driver.session(database=self._db()) as session:
            result = await session.run(q, {"cid": collection_id, "eids": entity_ids, "preds": predicates})
            async for rec in result:
                tid = str(rec["tid"])
                eid = str(rec["eid"])
                triplets.append(
                    ExtractedTriplet(
                        id=tid,
                        event_id=eid,
                        subject_name=str(rec["sname"] or ""),
                        subject_id=str(rec["sid"]),
                        predicate=str(rec["pred"]),
                        object_name=str(rec["oname"] or ""),
                        object_id=str(rec["oid"]),
                        value=rec["val"],
                    )
                )
                if eid not in events_by_id:
                    events_by_id[eid] = dict(rec["evp"])
        return triplets, list(events_by_id.values())

    async def update_statement_event_invalidation(
        self,
        event_id: str,
        *,
        invalid_at: str | None,
        expired_at: str | None,
        invalidated_by: str | None,
    ) -> None:
        q = """
        MATCH (e:StatementEvent {id: $id})
        SET e.invalid_at = $invalid_at,
            e.expired_at = $expired_at,
            e.invalidated_by = $invalidated_by
        """
        async with self._driver.session(database=self._db()) as session:
            await session.run(
                q,
                {"id": event_id, "invalid_at": invalid_at, "expired_at": expired_at, "invalidated_by": invalidated_by},
            )

    async def merge_triplet_edge(self, t: ExtractedTriplet, valid_at: str | None, invalid_at: str | None) -> None:
        q = """
        MATCH (s:Entity {id: $sid})
        MATCH (o:Entity {id: $oid})
        MERGE (s)-[r:TG_REL {triplet_id: $tid}]->(o)
        SET r.predicate = $predicate,
            r.event_id = $event_id,
            r.value = $value,
            r.valid_at = $valid_at,
            r.invalid_at = $invalid_at
        """
        async with self._driver.session(database=self._db()) as session:
            await session.run(
                q,
                {
                    "sid": t.subject_id,
                    "oid": t.object_id,
                    "tid": t.id,
                    "predicate": t.predicate,
                    "event_id": t.event_id,
                    "value": t.value,
                    "valid_at": valid_at,
                    "invalid_at": invalid_at,
                },
            )

    async def link_entity_to_statement(self, entity_id: str, event_id: str) -> None:
        q = """
        MATCH (n:Entity {id: $eid})
        MATCH (ev:StatementEvent {id: $vid})
        MERGE (ev)-[:MENTIONS_ENTITY]->(n)
        """
        async with self._driver.session(database=self._db()) as session:
            await session.run(q, {"eid": entity_id, "vid": event_id})

    async def list_entities_for_resolution(self, collection_id: str) -> list[PipelineEntity]:
        q = """
        MATCH (:Collection {collection_id: $cid})<-[:IN_COLLECTION]-(:Document)-[:HAS_STATEMENT_EVENT]->(:StatementEvent)-[:MENTIONS_ENTITY]->(n:Entity)
        RETURN n.id AS id, n.name AS name, coalesce(n.tg_type, n.tg_kind, '') AS tg_type,
               coalesce(n.description, '') AS description
        """
        rows: list[PipelineEntity] = []
        async with self._driver.session(database=self._db()) as session:
            result = await session.run(q, {"cid": collection_id})
            async for record in result:
                rows.append(
                    PipelineEntity(
                        id=str(record["id"]),
                        name=str(record["name"]),
                        tg_type=str(record["tg_type"] or ""),
                        description=str(record["description"] or ""),
                        resolved_id=None,
                    )
                )
        return rows

    async def fetch_subgraph_for_doc(
        self,
        collection_id: str,
        doc_id: str,
        publish_date: str,
        limit_chunks: int = 200,
    ) -> dict[str, Any]:
        pd = publish_date[:10] if len(publish_date) >= 10 else publish_date
        q = """
        MATCH (d:Document {doc_id: $doc_id, publish_date: $pd})-[:IN_COLLECTION]->(:Collection {collection_id: $cid})
        OPTIONAL MATCH (d)-[:HAS_CHUNK]->(c:Chunk)
        WITH d, collect(DISTINCT properties(c)) AS chunks
        OPTIONAL MATCH (d)-[:HAS_STATEMENT_EVENT]->(e:StatementEvent)
        RETURN properties(d) AS document, chunks, collect(DISTINCT properties(e)) AS events
        """
        async with self._driver.session(database=self._db()) as session:
            result = await session.run(q, {"cid": collection_id, "doc_id": doc_id, "pd": pd})
            rec = await result.single()
            if not rec:
                return {"document": None, "chunks": [], "events": []}
            ch = rec["chunks"] or []
            return {
                "document": rec["document"],
                "chunks": ch[:limit_chunks],
                "events": rec["events"] or [],
            }

    async def fetch_entity_neighborhood(
        self,
        collection_id: str,
        name_substring: str,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        q = """
        MATCH (:Collection {collection_id: $cid})<-[:IN_COLLECTION]-(:Document)-[:HAS_STATEMENT_EVENT]->(:StatementEvent)-[:MENTIONS_ENTITY]->(n:Entity)
        WHERE toLower(n.name) CONTAINS toLower($sub)
        OPTIONAL MATCH (n)-[r:TG_REL]-(m:Entity)
        WITH n, collect(DISTINCT {rel: type(r), other: m.name, pred: r.predicate}) AS edges
        RETURN properties(n) AS node, edges
        LIMIT $lim
        """
        async with self._driver.session(database=self._db()) as session:
            result = await session.run(q, {"cid": collection_id, "sub": name_substring, "lim": limit})
            out = []
            async for record in result:
                out.append({"node": record["node"], "edges": record["edges"]})
            return out
