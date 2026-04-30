"""Notebook-faithful invalidation: temporal window + embedding similarity + LLM (Neo4j-backed)."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from typing import Any

import numpy as np

from temporal_graph.llm.router import LLMRouter
from temporal_graph.models.pipeline import ExtractedTriplet, StatementEventPayload
from temporal_graph.neo4j.repository import GraphRepository
from temporal_graph.ontology.loader import OntologySpec
from temporal_graph.settings import Settings

logger = logging.getLogger(__name__)

EVENT_INVALIDATION_PROMPT = """Task: Analyze the primary event against the secondary event and determine if the primary event is invalidated by the secondary event.
Only set dates if they explicitly relate to the validity of the relationship described in the text.

IMPORTANT: Only invalidate events if they are directly invalidated by the other event given in the context. Do NOT use any external knowledge.
Only use dates that are directly stated to invalidate the relationship. The invalid_at for the invalidated event should be the valid_at of the event that caused the invalidation.

Invalidation Guidelines:
1. Dates are given in ISO 8601 format.
2. Where invalid_at is null, the event is still valid / ongoing.
3. Where invalid_at is defined, the event was previously invalidated.
4. An event can refine the invalid_at of a finished event to an earlier date only.
5. An event cannot invalidate an event that chronologically occurred after it.
6. An event cannot be invalidated by an event that chronologically occurred before it.
7. An event cannot invalidate itself.

---
Primary Event:
Statement: {p_stmt}
Triplet: {p_triplet}
Valid_at: {p_va}
Invalid_at: {p_ia}
---
Secondary Event:
Statement: {s_stmt}
Triplet: {s_triplet}
Valid_at: {s_va}
Invalid_at: {s_ia}
---

Reply with exactly True or False (no other text): True if the primary event is invalidated or its invalid_at is refined, else False.
"""


@dataclass
class InvEventView:
    id: str
    statement: str
    valid_at: datetime | None
    invalid_at: datetime | None
    expired_at: datetime | None
    temporal_type: str
    statement_type: str
    embedding: list[float]
    publish_date: str
    canonical_subevent: str = ""
    invalidated_by: str | None = None

    @staticmethod
    def from_payload(ev: StatementEventPayload) -> InvEventView:
        return InvEventView(
            id=ev.id,
            statement=ev.statement,
            valid_at=ev.valid_at,
            invalid_at=ev.invalid_at,
            expired_at=ev.expired_at,
            temporal_type=ev.temporal_type,
            statement_type=ev.statement_type,
            embedding=list(ev.embedding or []),
            publish_date=ev.publish_date,
            canonical_subevent=ev.canonical_subevent,
            invalidated_by=ev.invalidated_by,
        )

    @staticmethod
    def from_neo(props: dict[str, Any]) -> InvEventView:
        emb = props.get("embedding") or []
        if isinstance(emb, str):
            emb = []
        return InvEventView(
            id=str(props.get("id", "")),
            statement=str(props.get("statement", "")),
            valid_at=_parse_dt(props.get("valid_at")),
            invalid_at=_parse_dt(props.get("invalid_at")),
            expired_at=_parse_dt(props.get("expired_at")),
            temporal_type=str(props.get("temporal_type", "")),
            statement_type=str(props.get("statement_type", "")),
            embedding=[float(x) for x in emb] if emb else [],
            publish_date=str(props.get("publish_date", "")),
            canonical_subevent=str(props.get("canonical_subevent", "")),
            invalidated_by=str(props["invalidated_by"]) if props.get("invalidated_by") else None,
        )


def _parse_dt(v: Any) -> datetime | None:
    if v is None:
        return None
    if isinstance(v, datetime):
        return v
    s = str(v).strip().replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(s)
    except ValueError:
        return None


def _parse_publish_to_dt(pub: str) -> datetime | None:
    if not pub:
        return None
    p = pub.strip()[:10]
    for fmt in ("%Y-%m-%d", "%Y/%m/%d"):
        try:
            return datetime.strptime(p, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return _parse_dt(pub)


def publish_dates_within_threshold(
    incoming_publish: str,
    existing_publish: str,
    threshold_hours: float,
) -> bool:
    a = _parse_publish_to_dt(incoming_publish)
    b = _parse_publish_to_dt(existing_publish)
    if a is None or b is None:
        return False
    delta_sec = abs((a - b).total_seconds())
    return delta_sec <= threshold_hours * 3600.0


def predicate_to_group_map(groups: list[list[str]]) -> dict[str, list[str]]:
    m: dict[str, list[str]] = {}
    for g in groups:
        for p in g:
            m[str(p)] = [str(x) for x in g]
    return m


def cosine_similarity(v1: list[float], v2: list[float]) -> float:
    if not v1 or not v2 or len(v1) != len(v2):
        return 0.0
    a = np.array(v1, dtype=np.float64)
    b = np.array(v2, dtype=np.float64)
    na = np.linalg.norm(a)
    nb = np.linalg.norm(b)
    if na == 0 or nb == 0:
        return 0.0
    return float(np.dot(a, b) / (na * nb))


class InvalidationAgentNeo4j:
    def __init__(
        self,
        router: LLMRouter,
        repo: GraphRepository,
        ontology: OntologySpec,
        settings: Settings,
        predicate_groups: list[list[str]],
    ) -> None:
        self._router = router
        self._repo = repo
        self._ontology = ontology
        self._settings = settings
        self._pred_map = predicate_to_group_map(predicate_groups)
        self._sim_th = float(ontology.invalidation.similarity_threshold)
        self._top_k = int(ontology.invalidation.top_k)
        self._max_workers = 5

    def get_incoming_temporal_bounds(self, event: InvEventView) -> dict[str, datetime] | None:
        if event.temporal_type == "ATEMPORAL" or event.valid_at is None:
            return None
        temporal_bounds = {"start": event.valid_at, "end": event.valid_at}
        if event.temporal_type == "DYNAMIC" and event.invalid_at:
            temporal_bounds["end"] = event.invalid_at
        return temporal_bounds

    def select_events_temporally(
        self,
        triplet_events: list[tuple[ExtractedTriplet, InvEventView]],
        temp_bounds: dict[str, datetime],
        *,
        dynamic: bool = False,
    ) -> list[tuple[ExtractedTriplet, InvEventView]]:
        target_type = "DYNAMIC" if dynamic else "STATIC"
        filtered = [(t, e) for t, e in triplet_events if e.temporal_type == target_type]
        sorted_events = sorted(filtered, key=lambda te: te[1].valid_at or datetime.min.replace(tzinfo=timezone.utc))
        start, end = temp_bounds["start"], temp_bounds["end"]

        def _check_dynamic(ev: InvEventView, st: datetime, en: datetime) -> bool:
            if ev.temporal_type != "DYNAMIC":
                return False
            es = ev.valid_at or datetime.min.replace(tzinfo=timezone.utc)
            ee = ev.invalid_at
            if ee is not None and es <= st <= ee:
                return True
            if ee is None and es <= st:
                return True
            if st <= es <= en:
                return True
            return False

        def _check_static(ev: InvEventView, st: datetime, en: datetime) -> bool:
            if ev.temporal_type != "STATIC" or ev.valid_at is None:
                return False
            v = ev.valid_at
            return st <= v <= en or (v < en and v >= st)

        out: list[tuple[ExtractedTriplet, InvEventView]] = []
        for triplet, event in sorted_events:
            ok = _check_dynamic(event, start, end) if dynamic else _check_static(event, start, end)
            if ok:
                out.append((triplet, event))
        return out

    def filter_by_embedding_similarity(
        self,
        reference_event: InvEventView,
        candidate_pairs: list[tuple[ExtractedTriplet, InvEventView]],
    ) -> list[tuple[ExtractedTriplet, InvEventView]]:
        if not candidate_pairs or not reference_event.embedding:
            return []
        scored: list[tuple[float, ExtractedTriplet, InvEventView]] = []
        for t, e in candidate_pairs:
            if not e.embedding:
                continue
            sim = cosine_similarity(reference_event.embedding, e.embedding)
            if sim >= self._sim_th:
                scored.append((sim, t, e))
        scored.sort(key=lambda x: -x[0])
        return [(t, e) for _, t, e in scored[: self._top_k]]

    def filter_by_publish_date_proximity(
        self,
        incoming_subevent: str,
        incoming_publish: str,
        candidate_pairs: list[tuple[ExtractedTriplet, InvEventView]],
    ) -> list[tuple[ExtractedTriplet, InvEventView]]:
        th = self._ontology.publish_date_threshold_hours(incoming_subevent, self._settings)
        out: list[tuple[ExtractedTriplet, InvEventView]] = []
        for t, e in candidate_pairs:
            if publish_dates_within_threshold(incoming_publish, e.publish_date, th):
                out.append((t, e))
        return out

    def select_temporally_relevant_events_for_invalidation(
        self,
        incoming_event: InvEventView,
        candidate_triplet_events: list[tuple[ExtractedTriplet, InvEventView]],
    ) -> list[tuple[ExtractedTriplet, InvEventView]]:
        candidate_triplet_events = self.filter_by_publish_date_proximity(
            incoming_event.canonical_subevent,
            incoming_event.publish_date,
            candidate_triplet_events,
        )
        temporal_bounds = self.get_incoming_temporal_bounds(incoming_event)
        if not temporal_bounds:
            return []

        selected_statics = self.select_events_temporally(candidate_triplet_events, temporal_bounds, dynamic=False)
        selected_dynamics = self.select_events_temporally(candidate_triplet_events, temporal_bounds, dynamic=True)
        similar_static = self.filter_by_embedding_similarity(incoming_event, selected_statics)
        similar_dynamics = self.filter_by_embedding_similarity(incoming_event, selected_dynamics)
        return similar_static + similar_dynamics

    async def invalidation_step(
        self,
        primary_event: InvEventView,
        primary_triplet: ExtractedTriplet,
        secondary_event: InvEventView,
        secondary_triplet: ExtractedTriplet,
    ) -> InvEventView:
        prompt = EVENT_INVALIDATION_PROMPT.format(
            p_stmt=primary_event.statement,
            p_triplet=f"({primary_triplet.subject_name}, {primary_triplet.predicate}, {primary_triplet.object_name})",
            p_va=primary_event.valid_at,
            p_ia=primary_event.invalid_at,
            s_stmt=secondary_event.statement,
            s_triplet=f"({secondary_triplet.subject_name}, {secondary_triplet.predicate}, {secondary_triplet.object_name})",
            s_va=secondary_event.valid_at,
            s_ia=secondary_event.invalid_at,
        )
        text = await self._router.complete_text("invalidation_agent", "You output only True or False.", prompt)
        ok = text.strip().lower() == "true" or text.strip().lower().startswith("true")
        if not ok:
            return primary_event
        return replace(
            primary_event,
            invalid_at=secondary_event.valid_at,
            expired_at=datetime.now(timezone.utc),
            invalidated_by=secondary_event.id,
        )

    async def bi_directional_event_invalidation(
        self,
        incoming_triplet: ExtractedTriplet,
        incoming_event: InvEventView,
        existing_triplet_events: list[tuple[ExtractedTriplet, InvEventView]],
    ) -> tuple[InvEventView, list[InvEventView]]:
        changed_existing: list[InvEventView] = []
        updated_incoming = incoming_event

        dynamic_events_to_check = [(t, e) for t, e in existing_triplet_events if e.temporal_type == "DYNAMIC"]
        if dynamic_events_to_check:
            tasks = [
                self.invalidation_step(
                    primary_event=ex_ev,
                    primary_triplet=ex_tr,
                    secondary_event=incoming_event,
                    secondary_triplet=incoming_triplet,
                )
                for ex_tr, ex_ev in dynamic_events_to_check
            ]
            updated_events = await asyncio.gather(*tasks)
            for (ex_tr, ex_ev), upd in zip(dynamic_events_to_check, updated_events, strict=True):
                if upd.invalid_at != ex_ev.invalid_at or upd.invalidated_by != ex_ev.invalidated_by:
                    changed_existing.append(upd)

        if incoming_event.temporal_type == "DYNAMIC" and incoming_event.invalid_at is None:
            invalidating = [
                (t, e)
                for t, e in existing_triplet_events
                if incoming_event.valid_at and e.valid_at and incoming_event.valid_at < e.valid_at
            ]
            if invalidating:
                tasks = [
                    self.invalidation_step(
                        primary_event=incoming_event,
                        primary_triplet=incoming_triplet,
                        secondary_event=e,
                        secondary_triplet=t,
                    )
                    for t, e in invalidating
                ]
                updated_events = await asyncio.gather(*tasks)
                valid_inv = [(e.invalid_at, e.invalidated_by) for e in updated_events if e.invalid_at is not None]
                if valid_inv:
                    earliest = min(valid_inv, key=lambda x: x[0] or datetime.max.replace(tzinfo=timezone.utc))
                    updated_incoming = replace(
                        incoming_event,
                        invalid_at=earliest[0],
                        invalidated_by=earliest[1],
                        expired_at=datetime.now(timezone.utc),
                    )

        return updated_incoming, changed_existing

    @staticmethod
    def resolve_duplicate_invalidations(changed_events: list[InvEventView]) -> list[InvEventView]:
        if not changed_events:
            return []
        by_id: dict[str, InvEventView] = {}
        for ev in changed_events:
            cur = by_id.get(ev.id)
            if cur is None:
                by_id[ev.id] = ev
                continue
            if ev.invalid_at and (cur.invalid_at is None or ev.invalid_at < cur.invalid_at):
                by_id[ev.id] = ev
        return list(by_id.values())

    async def process_invalidations_in_parallel(
        self,
        incoming_triplets: list[ExtractedTriplet],
        incoming_events: list[InvEventView],
        existing_triplets: list[ExtractedTriplet],
        existing_events: list[dict[str, Any]],
    ) -> tuple[list[InvEventView], list[InvEventView]]:
        ev_map = {str(e["id"]): InvEventView.from_neo(e) for e in existing_events}
        incoming_event_map = {str(t.event_id): e for t, e in zip(incoming_triplets, incoming_events, strict=False)}
        tasks: list[asyncio.Task[tuple[InvEventView, list[InvEventView]]]] = []
        for inc_t in incoming_triplets:
            inc_ev = incoming_event_map[str(inc_t.event_id)]
            related = [
                (t, ev_map[str(t.event_id)])
                for t in existing_triplets
                if str(t.event_id) in ev_map
                and (str(t.subject_id) == str(inc_t.subject_id) or str(t.object_id) == str(inc_t.object_id))
            ]
            all_rel = self.select_temporally_relevant_events_for_invalidation(inc_ev, related)
            if not all_rel:
                continue
            tasks.append(
                asyncio.create_task(
                    self.bi_directional_event_invalidation(inc_t, inc_ev, all_rel),
                )
            )
        if not tasks:
            return [], []

        results = await asyncio.gather(*tasks, return_exceptions=True)
        updated_incoming: list[InvEventView] = []
        all_changed: list[InvEventView] = []
        for res in results:
            if isinstance(res, Exception):
                logger.exception("invalidation task failed: %s", res)
                continue
            inc_u, chg = res
            updated_incoming.append(inc_u)
            all_changed.extend(chg)

        dedup_existing = self.resolve_duplicate_invalidations(all_changed)
        dedup_incoming = self.resolve_duplicate_invalidations(updated_incoming)
        return dedup_incoming, dedup_existing


def relevant_predicates_for_triplets(
    incoming_triplets: list[ExtractedTriplet],
    pred_map: dict[str, list[str]],
) -> list[str]:
    preds: set[str] = set()
    for t in incoming_triplets:
        g = pred_map.get(str(t.predicate), [])
        preds.update(g)
    return sorted(preds)


async def run_batch_invalidation(
    repo: GraphRepository,
    router: LLMRouter,
    ontology: OntologySpec,
    settings: Settings,
    predicate_groups: list[list[str]],
    collection_id: str,
    rows: list[tuple[StatementEventPayload, list[ExtractedTriplet]]],
) -> None:
    """rows: (event, triplets_for_event). Mutates StatementEventPayload in place (notebook batch)."""
    if not ontology.invalidation.enabled:
        return
    if not await repo.has_statement_events(collection_id):
        return

    fact_triplets: list[ExtractedTriplet] = []
    fact_events_iv: list[InvEventView] = []
    ev_by_id: dict[str, StatementEventPayload] = {}

    for ev, trs in rows:
        if ev.statement_type != "FACT" or ev.temporal_type == "ATEMPORAL":
            continue
        for t in trs:
            if t.event_id != ev.id:
                continue
            fact_triplets.append(t)
            fact_events_iv.append(InvEventView.from_payload(ev))
            ev_by_id[ev.id] = ev

    if not fact_triplets:
        return

    pred_map = predicate_to_group_map(predicate_groups)
    eids = {str(t.subject_id) for t in fact_triplets} | {str(t.object_id) for t in fact_triplets}
    preds = relevant_predicates_for_triplets(fact_triplets, pred_map)
    if not preds:
        return

    ex_trs, ex_evs = await repo.fetch_related_triplet_events_for_invalidation(collection_id, list(eids), preds)
    if not ex_trs:
        return

    agent = InvalidationAgentNeo4j(router, repo, ontology, settings, predicate_groups)
    upd_inc, upd_ex = await agent.process_invalidations_in_parallel(
        fact_triplets,
        fact_events_iv,
        ex_trs,
        ex_evs,
    )

    for u in upd_ex:
        await repo.update_statement_event_invalidation(
            u.id,
            invalid_at=_iso(u.invalid_at),
            expired_at=_iso(u.expired_at),
            invalidated_by=u.invalidated_by,
        )
    for u in upd_inc:
        target = ev_by_id.get(u.id)
        if target:
            target.invalid_at = u.invalid_at
            target.expired_at = u.expired_at
            target.invalidated_by = u.invalidated_by


def _iso(dt: datetime | None) -> str | None:
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc).replace(microsecond=0).isoformat()
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat()
