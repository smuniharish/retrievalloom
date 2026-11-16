"""Hierarchical evidence consolidation with contradiction-closed selection."""

from __future__ import annotations

import math
from itertools import combinations
from typing import TYPE_CHECKING

from retrievalloom.contracts.component import ComponentInfo, make_info
from retrievalloom.contracts.evidence import EvidenceConsolidatorABC
from retrievalloom.domain.hashing import stable_id
from retrievalloom.domain.records import ObjectKind
from retrievalloom.domain.retrieval import (
    CompletenessReport,
    Evidence,
    EvidenceRelation,
    EvidenceSegment,
    NLILabel,
    NLISummary,
    Provenance,
    RelationType,
    StageRecord,
)
from retrievalloom.errors import ComponentContractError, TenantIsolationError
from retrievalloom.evidence.dedup import unique

if TYPE_CHECKING:
    from collections.abc import Sequence

    from retrievalloom.contracts.evidence import ConsolidationContext
    from retrievalloom.domain.records import SourceReference
    from retrievalloom.domain.retrieval import Candidate

PAYLOAD_ORDINAL = "ordinal"
PAYLOAD_PARENT = "parent_object_id"
_MERGING = frozenset(
    {
        RelationType.SPAN_OVERLAP,
        RelationType.SAME_SECTION,
        RelationType.PARENT_CHILD,
        RelationType.COMPLEMENTARY,
    }
)


class _UnionFind:
    def __init__(self, keys: Sequence[str]) -> None:
        self._parent = {key: key for key in keys}

    def find(self, key: str) -> str:
        root = key
        while self._parent[root] != root:
            root = self._parent[root]
        while self._parent[key] != root:
            self._parent[key], key = root, self._parent[key]
        return root

    def union(self, a: str, b: str) -> None:
        self._parent[self.find(b)] = self.find(a)


def _document_keys(candidate: Candidate) -> set[tuple[str, int, str]]:
    return {
        (source.document_uid, source.document_version, source.representation_id)
        for source in candidate.sources
    }


def _iou(a: SourceReference, b: SourceReference) -> float:
    if a.span is None or b.span is None:
        return 0.0
    overlap = min(a.span.end, b.span.end) - max(a.span.start, b.span.start)
    if overlap <= 0:
        return 0.0
    union = max(a.span.end, b.span.end) - min(a.span.start, b.span.start)
    return overlap / union


def _same_version(a: SourceReference, b: SourceReference) -> bool:
    return (a.document_uid, a.document_version, a.representation_id) == (
        b.document_uid,
        b.document_version,
        b.representation_id,
    )


def _ordinal(candidate: Candidate) -> int | None:
    value = candidate.payload.get(PAYLOAD_ORDINAL)
    return value if isinstance(value, int) and not isinstance(value, bool) else None


def _structural(a: Candidate, b: Candidate, threshold: float) -> RelationType | None:
    """Strongest structural relation between two candidates of one document version."""
    pairs = [(x, y) for x in a.sources for y in b.sources if _same_version(x, y)]
    if any(_iou(x, y) >= threshold for x, y in pairs):
        return RelationType.SPAN_OVERLAP
    if a.payload.get(PAYLOAD_PARENT) == b.object_id or b.payload.get(PAYLOAD_PARENT) == a.object_id:
        return RelationType.PARENT_CHILD
    if any(x.section_path and x.section_path == y.section_path for x, y in pairs):
        return RelationType.SAME_SECTION
    ordinal_a, ordinal_b = _ordinal(a), _ordinal(b)
    if (
        a.kind == ObjectKind.CHUNK
        and b.kind == ObjectKind.CHUNK
        and ordinal_a is not None
        and ordinal_b is not None
        and abs(ordinal_a - ordinal_b) == 1
    ):
        return RelationType.COMPLEMENTARY
    return None


def _cosine(a: Sequence[float], b: Sequence[float]) -> float:
    norm = math.sqrt(sum(x * x for x in a)) * math.sqrt(sum(y * y for y in b))
    if len(a) != len(b) or norm == 0:
        return 0.0
    return sum(x * y for x, y in zip(a, b, strict=True)) / norm


def contradictions(candidates: Sequence[Candidate]) -> set[tuple[str, str]]:
    """Pairs judged entailment vs contradiction for the same hypothesis (sorted ids)."""
    by_hypothesis: dict[str, dict[NLILabel, set[str]]] = {}
    for candidate in candidates:
        for judgement in candidate.nli:
            labels = by_hypothesis.setdefault(judgement.hypothesis_ref, {})
            labels.setdefault(judgement.label, set()).add(candidate.candidate_id)
    pairs: set[tuple[str, str]] = set()
    for labels in by_hypothesis.values():
        for a in labels.get(NLILabel.ENTAILMENT, set()):
            for b in labels.get(NLILabel.CONTRADICTION, set()):
                if a != b:
                    pairs.add((min(a, b), max(a, b)))
    return pairs


def _final_score(candidate: Candidate) -> float:
    if candidate.scores.final is None:
        raise ComponentContractError(
            "consolidation needs final scores", details={"candidate_id": candidate.candidate_id}
        )
    return candidate.scores.final


class HierarchicalConsolidator(EvidenceConsolidatorABC):
    """Groups candidates into evidence (span overlap, structure, adjacency) and links rest.

    Merging relations (span overlap, parent/child, same section, adjacent chunks) join
    candidates into one evidence item with one segment per member. Semantic similarity and
    contradictions are relations only. Selection is contradiction-closed.
    """

    _INFO = make_info(component="consolidator", implementation="hierarchical", version="1")

    @property
    def info(self) -> ComponentInfo:
        """Component identity."""
        return self._INFO

    def _relations(
        self, candidates: Sequence[Candidate], context: ConsolidationContext
    ) -> list[EvidenceRelation]:
        relations: list[EvidenceRelation] = []
        buckets: dict[tuple[str, tuple[str, int, str]], list[Candidate]] = {}
        for candidate in candidates:
            for key in _document_keys(candidate):
                buckets.setdefault((candidate.scope.key, key), []).append(candidate)
        seen: set[tuple[str, str]] = set()
        for bucket in buckets.values():
            for a, b in combinations(bucket, 2):
                pair = (min(a.candidate_id, b.candidate_id), max(a.candidate_id, b.candidate_id))
                if pair in seen:
                    continue
                relation = _structural(a, b, context.span_iou_threshold)
                if relation is not None:
                    seen.add(pair)
                    relations.append(EvidenceRelation(a=pair[0], b=pair[1], type=relation))
        if context.semantic_relations and context.embeddings:
            vectors = context.embeddings
            for a, b in combinations(candidates, 2):
                pair = (min(a.candidate_id, b.candidate_id), max(a.candidate_id, b.candidate_id))
                if (
                    pair not in seen
                    and a.scope.key == b.scope.key
                    and a.candidate_id in vectors
                    and b.candidate_id in vectors
                    and _cosine(vectors[a.candidate_id], vectors[b.candidate_id])
                    >= context.semantic_threshold
                ):
                    relations.append(
                        EvidenceRelation(
                            a=pair[0], b=pair[1], type=RelationType.SEMANTICALLY_SIMILAR
                        )
                    )
        relations.extend(
            EvidenceRelation(a=a, b=b, type=RelationType.CONTRADICTS)
            for a, b in sorted(contradictions(candidates))
        )
        return relations

    def _merges(self, relation: EvidenceRelation, context: ConsolidationContext) -> bool:
        if relation.type not in _MERGING:
            return False
        if relation.type is RelationType.COMPLEMENTARY:
            return context.merge_adjacent
        if relation.type in {RelationType.SAME_SECTION, RelationType.PARENT_CHILD}:
            return context.group_structural
        return True

    async def consolidate(
        self, candidates: Sequence[Candidate], context: ConsolidationContext
    ) -> tuple[Sequence[Evidence], Sequence[str]]:
        """Consolidate final-ordered candidates; return evidence and truncated ids."""
        if not candidates:
            return [], []
        scope = candidates[0].scope
        for candidate in candidates:
            if candidate.scope != scope:
                raise TenantIsolationError("candidates of different scopes cannot be consolidated")
        by_id = {candidate.candidate_id: candidate for candidate in candidates}
        position = {candidate.candidate_id: index for index, candidate in enumerate(candidates)}
        relations = self._relations(candidates, context)
        groups = _UnionFind(list(by_id))
        for relation in relations:
            if self._merges(relation, context):
                groups.union(relation.a, relation.b)
        members: dict[str, list[str]] = {}
        for candidate_id in by_id:
            members.setdefault(groups.find(candidate_id), []).append(candidate_id)

        evidence_of: dict[str, str] = {}
        built: dict[str, list[str]] = {}
        for ids in members.values():
            ordered = sorted(ids, key=position.__getitem__)
            evidence_id = stable_id("ev", scope.key, *sorted(ids))
            built[evidence_id] = ordered
            for candidate_id in ordered:
                evidence_of[candidate_id] = evidence_id

        items: list[Evidence] = []
        for evidence_id, ordered in built.items():
            member_set = set(ordered)
            own = tuple(r for r in relations if r.a in member_set and r.b in member_set)
            linked = sorted(
                {
                    evidence_of[other]
                    for relation in relations
                    if relation.type is RelationType.CONTRADICTS
                    for mine, other in ((relation.a, relation.b), (relation.b, relation.a))
                    if mine in member_set and other not in member_set
                }
            )
            items.append(
                self._evidence(
                    evidence_id, [by_id[i] for i in ordered], own, tuple(linked), relations
                )
            )
        items.sort(key=lambda item: (-item.score, item.evidence_id))
        return _select(items, context)

    def _evidence(
        self,
        evidence_id: str,
        group: list[Candidate],
        own: tuple[EvidenceRelation, ...],
        linked: tuple[str, ...],
        relations: Sequence[EvidenceRelation],
    ) -> Evidence:
        best = max(group, key=_final_score)
        member_ids = {candidate.candidate_id for candidate in group}
        labels = [judgement.label for candidate in group for judgement in candidate.nli]
        has_contradiction = any(
            relation.type is RelationType.CONTRADICTS
            and (relation.a in member_ids or relation.b in member_ids)
            for relation in relations
        )
        reading = sorted(
            group,
            key=lambda c: (
                _ordinal(c) is None,
                _ordinal(c) or 0,
                group.index(c),
            ),
        )
        texts = [candidate.text for candidate in reading if candidate.text]
        sources = unique(source for candidate in group for source in candidate.sources)
        return Evidence(
            evidence_id=evidence_id,
            scope=group[0].scope,
            segments=tuple(
                EvidenceSegment(
                    candidate_id=candidate.candidate_id,
                    object_id=candidate.object_id,
                    text=candidate.text,
                    sources=candidate.sources,
                )
                for candidate in group
            ),
            member_candidate_ids=tuple(candidate.candidate_id for candidate in group),
            relations=own,
            linked_evidence_ids=linked,
            sources=sources,
            score=_final_score(best),
            scores=best.scores,
            nli_summary=NLISummary(
                entailment=labels.count(NLILabel.ENTAILMENT),
                contradiction=labels.count(NLILabel.CONTRADICTION),
                neutral=labels.count(NLILabel.NEUTRAL),
                has_contradiction=has_contradiction,
            ),
            completeness=CompletenessReport(
                missing_spans=tuple(
                    c.object_id
                    for c in group
                    if c.kind == ObjectKind.CHUNK and any(s.span is None for s in c.sources)
                ),
                stale_sources=tuple(c.object_id for c in group if c.flags.stale_version),
                unresolved_sources=tuple(c.object_id for c in group if c.flags.partial_coverage),
            ),
            provenance=Provenance(
                sources=sources,
                hits=unique(hit for candidate in group for hit in candidate.hits),
                stage_trail=(
                    StageRecord(
                        stage="consolidation",
                        component_id=self._INFO.name,
                        component_version=self._INFO.version,
                        config_fingerprint=self._INFO.fingerprint.config_digest,
                    ),
                ),
            ),
            display_text="\n\n".join(texts) if texts else None,
        )


def _select(
    items: list[Evidence], context: ConsolidationContext
) -> tuple[list[Evidence], list[str]]:
    """Top ``top_k`` items plus their contradicting items within the overflow budget."""
    by_id = {item.evidence_id: item for item in items}
    selected = items[: context.top_k]
    chosen = {item.evidence_id for item in selected}
    budget = context.contradiction_overflow
    truncated: list[str] = []
    for item in list(selected):
        for linked_id in item.linked_evidence_ids:
            if linked_id in chosen or linked_id in truncated:
                continue
            if budget > 0:
                budget -= 1
                chosen.add(linked_id)
                selected.append(by_id[linked_id])
            else:
                truncated.append(linked_id)
    selected.sort(key=lambda item: (-item.score, item.evidence_id))
    return selected, truncated
