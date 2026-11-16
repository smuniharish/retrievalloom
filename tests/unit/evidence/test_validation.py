"""EvidenceValidator: rejects fabricated, dropped or leaked provenance."""

from __future__ import annotations

import math
from typing import TYPE_CHECKING, Any

import pytest

from retrievalloom.domain.retrieval import EvidenceRelation, RelationType
from retrievalloom.errors import ProvenanceError, TenantIsolationError
from retrievalloom.evidence.consolidation import HierarchicalConsolidator
from retrievalloom.evidence.validation import EvidenceValidator
from tests.unit.evidence.helpers import OTHER_SCOPE, SCOPE, candidate, context, hit, source

if TYPE_CHECKING:
    from collections.abc import Callable

    from retrievalloom.domain.retrieval import Candidate, Evidence

VALIDATOR = EvidenceValidator()


async def build() -> tuple[list[Evidence], list[Candidate]]:
    candidates = [
        candidate("a", final=0.9, span=(0, 10)),
        candidate("b", final=0.8, span=(0, 10), hits=[hit("b"), hit("b", method="dense")]),
        candidate("c", final=0.7, document="d2"),
    ]
    evidence, _ = await HierarchicalConsolidator().consolidate(candidates, context())
    return list(evidence), candidates


def copy(item: Evidence, **update: Any) -> Evidence:
    return item.model_copy(update=update)


async def test_consolidated_evidence_is_valid() -> None:
    evidence, candidates = await build()
    assert [len(item.member_candidate_ids) for item in evidence] == [2, 1]
    VALIDATOR.validate(SCOPE, evidence, candidates)
    VALIDATOR.validate(SCOPE, [], [])


PROVENANCE_BREAKS: dict[str, Callable[[Evidence, Evidence], Evidence]] = {
    "non_finite_score": lambda item, _: copy(item, score=math.inf),
    "segments_not_members": lambda item, _: copy(
        item, member_candidate_ids=item.member_candidate_ids[::-1]
    ),
    "segment_object_differs": lambda item, other: copy(
        item,
        segments=(
            item.segments[0].model_copy(update={"object_id": other.segments[0].object_id}),
            *item.segments[1:],
        ),
    ),
    "segment_sources_differ": lambda item, other: copy(
        item,
        segments=(
            item.segments[0].model_copy(update={"sources": other.sources}),
            *item.segments[1:],
        ),
    ),
    "fabricated_source": lambda item, other: copy(item, sources=(*item.sources, *other.sources)),
    "dropped_provenance_source": lambda item, _: copy(
        item, provenance=item.provenance.model_copy(update={"sources": item.sources[:1]})
    ),
    "dropped_hit": lambda item, _: copy(
        item, provenance=item.provenance.model_copy(update={"hits": item.provenance.hits[:1]})
    ),
    "relation_to_non_member": lambda item, other: copy(
        item,
        relations=(
            EvidenceRelation(
                a=item.member_candidate_ids[0],
                b=other.member_candidate_ids[0],
                type=RelationType.SEMANTICALLY_SIMILAR,
            ),
        ),
    ),
    "self_link": lambda item, _: copy(item, linked_evidence_ids=(item.evidence_id,)),
}


@pytest.mark.parametrize("name", list(PROVENANCE_BREAKS))
async def test_inconsistent_provenance_is_rejected(name: str) -> None:
    evidence, candidates = await build()
    broken = PROVENANCE_BREAKS[name](evidence[0], evidence[1])
    with pytest.raises(ProvenanceError) as caught:
        VALIDATOR.validate(SCOPE, [broken, evidence[1]], candidates)
    assert caught.value.details["evidence_id"] == evidence[0].evidence_id


async def test_unknown_and_shared_members_are_rejected() -> None:
    evidence, candidates = await build()
    with pytest.raises(ProvenanceError, match="unknown candidate"):
        VALIDATOR.validate(SCOPE, evidence, candidates[1:])
    with pytest.raises(ProvenanceError, match="more than one"):
        VALIDATOR.validate(SCOPE, [evidence[1], evidence[1]], candidates)


async def test_foreign_scope_is_rejected() -> None:
    evidence, candidates = await build()
    item = evidence[1]
    with pytest.raises(TenantIsolationError, match="evidence belongs"):
        VALIDATOR.validate(OTHER_SCOPE, [item], candidates)
    foreign_source = copy(item, sources=(source("c", document="d2", scope=OTHER_SCOPE),))
    with pytest.raises(TenantIsolationError, match="source"):
        VALIDATOR.validate(SCOPE, [foreign_source], candidates)
    foreign_hit = copy(
        item,
        provenance=item.provenance.model_copy(update={"hits": (hit("c", scope=OTHER_SCOPE),)}),
    )
    with pytest.raises(TenantIsolationError, match="hit"):
        VALIDATOR.validate(SCOPE, [foreign_hit], candidates)
