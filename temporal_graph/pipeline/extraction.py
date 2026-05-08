from __future__ import annotations

import asyncio
import logging
import uuid
from collections import defaultdict
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from temporal_graph.llm.router import LLMRouter
from temporal_graph.models.api import IngestChunkItem, IngestPayload
from temporal_graph.models.pipeline import ExtractedTriplet, PipelineEntity, StatementEventPayload
from temporal_graph.neo4j.repository import GraphRepository
from temporal_graph.ontology.loader import OntologySpec
from temporal_graph.ontology.subtype import derive_normalized_subtype
from temporal_graph.pipeline.entity_enrichment import build_flat_entity_props
from temporal_graph.pipeline.entity_kinds import infer_kind, normalize_predicate, slug_uuid
from temporal_graph.pipeline.entity_resolution import resolve_entities_batch
from temporal_graph.pipeline.invalidation import run_batch_invalidation
from temporal_graph.pipeline.llm_schemas import (
    RawExtraction,
    RawStatement,
    RawStatementList,
    RawTemporalRange,
)
from temporal_graph.settings import Settings

logger = logging.getLogger(__name__)

EmitFn = Callable[[str, dict[str, Any]], Awaitable[None]]


def _parse_dt(s: str | None) -> datetime | None:
    if not s or not str(s).strip():
        return None
    t = str(s).strip().replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(t)
    except ValueError:
        pass
    for fmt in ("%Y-%m-%d", "%Y/%m/%d"):
        try:
            return datetime.strptime(str(s).strip()[:10], fmt)
        except ValueError:
            continue
    return None


def _publication_datetime(publish_date: str) -> datetime:
    return _parse_dt(publish_date) or datetime.now(timezone.utc)


@dataclass
class _Row:
    ev: StatementEventPayload
    triplets: list[ExtractedTriplet]
    entities: list[PipelineEntity]


class TemporalIngestionPipeline:
    """Chunk → statements → (temporal range ∥ triplets) → resolution → invalidation → Neo4j."""

    def __init__(
        self,
        router: LLMRouter,
        repo: GraphRepository,
        predicates: dict[str, str],
        settings: Settings,
        emit: EmitFn | None = None,
    ) -> None:
        self._router = router
        self._repo = repo
        self._predicates = predicates
        self._settings = settings
        self._emit = emit

    async def _e(self, typ: str, data: dict[str, Any]) -> None:
        if self._emit:
            await self._emit(typ, data)

    def _predicate_block(self) -> str:
        return "\n".join(f"- {k}: {v}" for k, v in sorted(self._predicates.items()))

    def _doc_summary(
        self,
        chunk: IngestChunkItem,
        ontology: OntologySpec,
        normalized_subtype: str,
        doc_id: str,
        collection_id: str,
    ) -> dict[str, Any]:
        pub = _publication_datetime(chunk.publish_date or "")
        return {
            "main_entity": None,
            "document_type": "Corporate disclosure",
            "publication_date": pub,
            "quarter": None,
            "document_chunk": None,
            "doc_id": doc_id,
            "collection_id": collection_id,
            "ontology_id": ontology.id,
            "ontology_version": ontology.ontology_version,
            "ontology_name": ontology.name,
            "canonical_event": chunk.canonical_event,
            "canonical_subevent": chunk.canonical_subevent,
            "normalized_subtype": normalized_subtype,
            "section_title": chunk.section_title,
            "title_summary": chunk.title_summary,
            "bundle_id": chunk.bundle_id,
            "chunk_type": chunk.type,
        }

    async def _extract_statements(
        self,
        chunk: IngestChunkItem,
        doc_summary: dict[str, Any],
    ) -> list[RawStatement]:
        sys = (
            "You extract atomic declarative statements from financial / corporate text. "
            "Respect the chunk's disclosure classification when deciding emphasis.\n"
            f"Ontology id: {doc_summary['ontology_id']} ({doc_summary['ontology_name']}).\n"
            f"Chunk canonical_event: {doc_summary['canonical_event']}\n"
            f"Chunk canonical_subevent: {doc_summary['canonical_subevent']}\n"
            f"Chunk normalized_subtype: {doc_summary['normalized_subtype']}\n"
            "Label each statement with temporal_type ATEMPORAL, STATIC, or DYNAMIC and "
            "statement_type FACT, OPINION, or PREDICTION."
        )
        user = (
            f"Section: {doc_summary.get('section_title') or 'N/A'}\n"
            f"Summary: {doc_summary.get('title_summary') or 'N/A'}\n\n"
            f"Text:\n{chunk.content}\n\n"
            "Return JSON matching the schema."
        )
        out = await self._router.complete_json_schema(
            "temporal_statement_extraction",
            sys,
            user,
            RawStatementList,
        )
        return list(out.statements)

    async def _extract_temporal_range(self, statement: RawStatement, doc_summary: dict[str, Any]) -> RawTemporalRange:
        if statement.temporal_type == "ATEMPORAL":
            return RawTemporalRange(valid_at=None, invalid_at=None)
        sys = (
            "Infer valid_at and invalid_at instants for the statement using the document publication "
            "context and temporal / episode typing rules."
        )
        user = (
            f"publication_date: {doc_summary['publication_date']}\n"
            f"temporal_type: {statement.temporal_type}\n"
            f"statement_type: {statement.statement_type}\n"
            f"ontology classification: {doc_summary['canonical_event']} / "
            f"{doc_summary['canonical_subevent']} / {doc_summary['normalized_subtype']}\n"
            f"statement: {statement.statement}\n"
            "Return JSON with valid_at and invalid_at as ISO-8601 strings or null."
        )
        out = await self._router.complete_json_schema(
            "temporal_date_extraction",
            sys,
            user,
            RawTemporalRange,
        )
        if statement.temporal_type == "STATIC":
            out = RawTemporalRange(valid_at=out.valid_at, invalid_at=None)
        if out.valid_at is None and doc_summary.get("publication_date"):
            pub = doc_summary["publication_date"]
            iso = pub.date().isoformat() if isinstance(pub, datetime) else str(pub)[:10]
            out = RawTemporalRange(valid_at=iso, invalid_at=out.invalid_at)
        return out

    async def _extract_triplets(self, statement: RawStatement) -> RawExtraction:
        sys = (
            "Extract subject-predicate-object triplets and entity mentions for knowledge graph ingestion. "
            "Use only predicates from the allowed list. Resolve vague references to concrete names.\n\n"
            "For each entity, fill `attributes` with optional snake_case fields from financial_entity_schema.md "
            "(e.g. company_ticker, company_sector, person_role, institution_type, news_headline, "
            "corp_event_direction, price_point_close). Only include values grounded in the statement.\n\n"
            f"Allowed predicates:\n{self._predicate_block()}"
        )
        user = f"Statement:\n{statement.statement}\n\nReturn JSON matching the schema."
        return await self._router.complete_json_schema(
            "temporal_triplet_extraction",
            sys,
            user,
            RawExtraction,
        )  # type: ignore[return-value]

    async def _process_statement(
        self,
        chunk: IngestChunkItem,
        doc_id: str,
        publish_date: str,
        statement: RawStatement,
        doc_summary: dict[str, Any],
        ontology: OntologySpec,
        normalized_subtype: str,
    ) -> tuple[StatementEventPayload, list[ExtractedTriplet], list[PipelineEntity]]:
        tr_task = self._extract_temporal_range(statement, doc_summary)
        ex_task = self._extract_triplets(statement)
        raw_tr, raw_ex = await asyncio.gather(tr_task, ex_task)
        emb = await self._router.embed("statement_embedding", statement.statement)
        eid = str(uuid.uuid4())
        valid_at = _parse_dt(raw_tr.valid_at)
        invalid_at = _parse_dt(raw_tr.invalid_at)
        created_at = datetime.now(timezone.utc)
        expired_at = created_at if (invalid_at is not None and statement.temporal_type == "DYNAMIC") else None

        ev = StatementEventPayload(
            id=eid,
            chunk_id=chunk.chunk_id,
            doc_id=doc_id,
            collection_id=doc_summary["collection_id"],
            publish_date=publish_date,
            statement=statement.statement,
            statement_type=statement.statement_type,
            temporal_type=statement.temporal_type,
            valid_at=valid_at,
            invalid_at=invalid_at,
            expired_at=expired_at,
            invalidated_by=None,
            created_at=created_at,
            embedding=emb,
            ontology_id=ontology.id,
            ontology_version=ontology.ontology_version,
            canonical_event=chunk.canonical_event,
            canonical_subevent=chunk.canonical_subevent,
            normalized_subtype=normalized_subtype,
        )

        triplets: list[ExtractedTriplet] = []
        entities: list[PipelineEntity] = []
        for rt in raw_ex.triplets:
            pred = normalize_predicate(rt.predicate, self._predicates)
            triplets.append(
                ExtractedTriplet(
                    id=str(uuid.uuid4()),
                    event_id=eid,
                    subject_name=rt.subject_name,
                    subject_id="",
                    predicate=pred,
                    object_name=rt.object_name,
                    object_id="",
                    value=rt.value,
                )
            )
        for re in raw_ex.entities:
            entities.append(
                PipelineEntity(
                    id=str(uuid.uuid4()),
                    name=re.name,
                    tg_type=re.type,
                    description=re.description or "",
                    financial=dict(re.attributes) if re.attributes else {},
                )
            )
        return ev, triplets, entities

    async def ingest(self, payload: IngestPayload, ontology: OntologySpec) -> dict[str, Any]:
        doc_id = payload.chunks[0].doc_id
        publish_date = payload.chunks[0].publish_date or ""
        pd_key = publish_date[:10] if len(publish_date) >= 10 else publish_date

        await self._repo.merge_document(
            doc_id,
            publish_date,
            payload.collection_id,
            {"ontology_id": ontology.id, "ontology_version": ontology.ontology_version},
        )
        global_ents = await self._repo.list_entities_for_resolution(payload.collection_id)

        rows: list[_Row] = []
        n_statements = 0

        for chunk in payload.chunks:
            ontology.validate_event(chunk.canonical_event, chunk.canonical_subevent)
            ns = derive_normalized_subtype(ontology, chunk.canonical_event, chunk.canonical_subevent, chunk.normalized_subtype)
            cd = chunk.model_dump()
            await self._repo.merge_chunk(
                doc_id,
                publish_date,
                cd,
                ontology.id,
                ontology.ontology_version,
                chunk.canonical_event,
                chunk.canonical_subevent,
                ns,
            )
            doc_summary = self._doc_summary(chunk, ontology, ns, doc_id, payload.collection_id)
            statements = await self._extract_statements(chunk, doc_summary)
            await self._e("chunk_progress", {"chunk_id": chunk.chunk_id, "statements": len(statements)})

            for stmt in statements:
                ev, trs, ents = await self._process_statement(
                    chunk, doc_id, publish_date, stmt, doc_summary, ontology, ns
                )
                rows.append(_Row(ev, trs, ents))
                n_statements += 1

        all_ents = [e for r in rows for e in r.entities]
        resolve_entities_batch(all_ents, global_ents)

        global_name_to_id: dict[str, str] = {}
        for e in all_ents:
            global_name_to_id[e.name] = e.resolved_id or e.id

        async def _ensure_named_entity(nm: str) -> None:
            if nm in global_name_to_id:
                return
            nid = slug_uuid(f"auto:{payload.collection_id}:{nm}")
            global_name_to_id[nm] = nid
            flat = build_flat_entity_props(nid, nm, "Company", "", None)
            await self._repo.merge_entity_node_merged(flat, ["Entity", "Company"])

        for r in rows:
            for t in r.triplets:
                await _ensure_named_entity(t.subject_name)
                await _ensure_named_entity(t.object_name)
                t.subject_id = global_name_to_id[t.subject_name]
                t.object_id = global_name_to_id[t.object_name]

        by_cid: dict[str, list[PipelineEntity]] = defaultdict(list)
        for e in all_ents:
            by_cid[e.resolved_id or e.id].append(e)

        seen_entity: set[str] = set()
        for cid, group in by_cid.items():
            fin: dict[str, Any] = {}
            for ent in group:
                fin.update(ent.financial)
            first = group[0]
            flat = build_flat_entity_props(cid, first.name, first.tg_type, first.description, fin)
            kind = infer_kind(first.tg_type)
            await self._repo.merge_entity_node_merged(flat, ["Entity", kind])
            seen_entity.add(cid)

        pred_groups = ontology.resolved_predicate_groups(self._settings.predicate_groups_path)
        await run_batch_invalidation(
            self._repo,
            self._router,
            ontology,
            self._settings,
            pred_groups,
            payload.collection_id,
            [(r.ev, r.triplets) for r in rows],
        )

        n_triplets = 0
        for r in rows:

            def _iso(ev2: StatementEventPayload, attr: str) -> str | None:
                dt = getattr(ev2, attr)
                if dt is None:
                    return None
                if isinstance(dt, datetime):
                    return dt.replace(microsecond=0).isoformat() + "Z"
                return str(dt)

            await self._repo.merge_statement_event(r.ev)
            for ent in r.entities:
                cid = ent.resolved_id or ent.id
                await self._repo.link_entity_to_statement(cid, r.ev.id)
            for t in r.triplets:
                await self._repo.merge_triplet_edge(t, _iso(r.ev, "valid_at"), _iso(r.ev, "invalid_at"))
            n_triplets += len(r.triplets)

        return {
            "doc_id": doc_id,
            "collection_id": payload.collection_id,
            "publish_date": pd_key,
            "ontology_id": ontology.id,
            "statements": n_statements,
            "triplets": n_triplets,
            "entities_touched": len(seen_entity),
        }
