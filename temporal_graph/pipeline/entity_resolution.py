"""Fuzzy entity resolution (notebook EntityResolution pattern, in-memory + Neo4j canonicals)."""

from __future__ import annotations

import string
import uuid

from rapidfuzz import fuzz

from temporal_graph.models.pipeline import PipelineEntity


def _clean(name: str) -> str:
    return name.lower().strip().translate(str.maketrans("", "", string.punctuation))


def group_entities_by_fuzzy_match(
    entities: list[PipelineEntity],
    threshold: float = 80.0,
) -> dict[str, list[PipelineEntity]]:
    name_to_entities: dict[str, list[PipelineEntity]] = {}
    cleaned_name_map: dict[str, str] = {}
    for entity in entities:
        name_to_entities.setdefault(entity.name, []).append(entity)
        cleaned_name_map[entity.name] = _clean(entity.name)
    unique_names = list(name_to_entities.keys())
    clustered: dict[str, list[PipelineEntity]] = {}
    used: set[str] = set()
    for name in unique_names:
        if name in used:
            continue
        clustered[name] = []
        for other_name in unique_names:
            if other_name in used:
                continue
            score = fuzz.partial_ratio(cleaned_name_map[name], cleaned_name_map[other_name])
            if score >= threshold:
                clustered[name].extend(name_to_entities[other_name])
                used.add(other_name)
        if not clustered[name]:
            clustered[name] = list(name_to_entities[name])
            used.add(name)
    return clustered


def set_medoid_as_canonical_entity(group: list[PipelineEntity]) -> PipelineEntity | None:
    if not group:
        return None
    if len(group) == 1:
        return group[0]
    best = group[0]
    best_score = 0.0
    for candidate in group:
        total = 0.0
        for other in group:
            if candidate is other:
                continue
            total += fuzz.ratio(_clean(candidate.name), _clean(other.name))
        if total >= best_score:
            best_score = total
            best = candidate
    return best


def resolve_entities_batch(
    batch_entities: list[PipelineEntity],
    global_canonicals: list[PipelineEntity],
    *,
    threshold: float = 80.0,
    acronym_thresh: float = 98.0,
) -> None:
    """Mutates batch_entities: sets resolved_id to canonical entity id (str UUID)."""
    type_groups: dict[str, list[PipelineEntity]] = {}
    for e in batch_entities:
        type_groups.setdefault(e.tg_type or "unknown", []).append(e)

    for entities in type_groups.values():
        clusters = group_entities_by_fuzzy_match(entities, threshold)
        for group in clusters.values():
            if not group:
                continue
            local_canon = set_medoid_as_canonical_entity(group)
            if local_canon is None:
                continue
            match = next(
                (c for c in global_canonicals if fuzz.ratio(_clean(c.name), _clean(local_canon.name)) >= threshold),
                None,
            )
            if local_canon.name and " " in local_canon.name:
                acronym = "".join(w[0] for w in local_canon.name.split() if w)
                acronym_match = next(
                    (
                        c
                        for c in global_canonicals
                        if fuzz.ratio(acronym, c.name) >= acronym_thresh and " " not in c.name
                    ),
                    None,
                )
                if acronym_match:
                    match = acronym_match
            if match:
                canonical_id = match.id
            else:
                canonical_id = str(uuid.uuid4())
                global_canonicals.append(
                    PipelineEntity(
                        id=canonical_id,
                        name=local_canon.name,
                        tg_type=local_canon.tg_type,
                        description=local_canon.description,
                        resolved_id=None,
                        financial=dict(local_canon.financial),
                    )
                )
            for entity in group:
                entity.resolved_id = canonical_id
