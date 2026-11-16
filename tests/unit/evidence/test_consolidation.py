"""HierarchicalConsolidator: merging relations, E2/E3, contradiction-closed selection, P1."""

from __future__ import annotations

import pytest

from retrievalloom.domain.hashing import stable_id
from retrievalloom.domain.records import ObjectKind
from retrievalloom.domain.retrieval import CandidateFlags, NLILabel, RelationType
from retrievalloom.errors import ComponentContractError, TenantIsolationError
from retrievalloom.evidence.consolidation import (
    PAYLOAD_ORDINAL,
    PAYLOAD_PARENT,
    HierarchicalConsolidator,
    _UnionFind,
    contradictions,
)
from retrievalloom.evidence.validation import EvidenceValidator
from tests.unit.evidence.helpers import (
    OTHER_SCOPE,
    SCOPE,
    candidate,
    context,
    hit,
    judgement,
    object_id,
    source,
)

CONSOLIDATOR = HierarchicalConsolidator()


def relation_types(item: object) -> set[RelationType]:
    return {relation.type for relation in item.relations}  # type: ignore[attr-defined]


async def test_span_overlap_merges_and_preserves_provenance() -> None:
    a = candidate("a", final=0.9, span=(0, 100))
    b = candidate("b", final=0.7, span=(20, 100), hits=[hit("b"), hit("b", method="dense")])
    evidence, truncated = await CONSOLIDATOR.consolidate([a, b], context())
    assert truncated == []
    (item,) = evidence
    assert item.member_candidate_ids == (a.candidate_id, b.candidate_id)
    assert [segment.sources for segment in item.segments] == [a.sources, b.sources]
    assert item.sources == (*a.sources, *b.sources)
    assert item.provenance.sources == item.sources
    assert item.provenance.hits == (*a.hits, *b.hits)
    assert item.score == 0.9
    assert item.scores == a.scores
    assert relation_types(item) == {RelationType.SPAN_OVERLAP}
    assert item.evidence_id == stable_id("ev", SCOPE.key, *sorted([a.candidate_id, b.candidate_id]))
    (stage,) = item.provenance.stage_trail
    assert stage.stage == "consolidation"
    assert stage.component_id == CONSOLIDATOR.info.name
    assert item.display_text == "a\n\nb"
    EvidenceValidator().validate(SCOPE, evidence, [a, b])


@pytest.mark.parametrize(
    ("first", "second", "threshold", "merged"),
    [
        ((0, 100), (60, 140), 0.5, False),  # IoU 40/140 below threshold
        ((0, 100), (60, 140), 0.25, True),
        ((0, 10), (10, 20), 0.5, False),  # touching spans do not overlap
        ((0, 10), None, 0.5, False),  # a missing span never overlaps
    ],
)
async def test_span_iou_threshold(
    first: tuple[int, int],
    second: tuple[int, int] | None,
    threshold: float,
    merged: bool,
) -> None:
    a = candidate("a", final=0.9, span=first)
    b = candidate("b", final=0.8, span=second)
    evidence, _ = await CONSOLIDATOR.consolidate([a, b], context(span_iou_threshold=threshold))
    assert len(evidence) == (1 if merged else 2)


async def test_different_versions_or_documents_never_merge_structurally() -> None:
    a = candidate("a", final=0.9, span=(0, 50), version=1, section=("intro",))
    b = candidate("b", final=0.8, span=(0, 50), version=2, section=("intro",))
    c = candidate("c", final=0.7, span=(0, 50), document="d2", section=("intro",))
    evidence, _ = await CONSOLIDATOR.consolidate([a, b, c], context())
    assert [item.member_candidate_ids for item in evidence] == [
        (a.candidate_id,),
        (b.candidate_id,),
        (c.candidate_id,),
    ]


@pytest.mark.parametrize(
    ("relation", "option"),
    [
        (RelationType.PARENT_CHILD, "group_structural"),
        (RelationType.SAME_SECTION, "group_structural"),
        (RelationType.COMPLEMENTARY, "merge_adjacent"),
    ],
)
async def test_structural_relations_merge_unless_disabled(
    relation: RelationType, option: str
) -> None:
    if relation is RelationType.PARENT_CHILD:
        a = candidate("parent", final=0.9, kind=ObjectKind.STRUCTURE_NODE)
        b = candidate("child", final=0.8, payload={PAYLOAD_PARENT: object_id("parent")})
    elif relation is RelationType.SAME_SECTION:
        a = candidate("a", final=0.9, section=("ch1", "s2"))
        b = candidate("b", final=0.8, section=("ch1", "s2"))
    else:
        a = candidate("a", final=0.9, payload={PAYLOAD_ORDINAL: 4})
        b = candidate("b", final=0.8, payload={PAYLOAD_ORDINAL: 3})
    evidence, _ = await CONSOLIDATOR.consolidate([a, b], context())
    (item,) = evidence
    assert relation_types(item) == {relation}
    separate, _ = await CONSOLIDATOR.consolidate([a, b], context(**{option: False}))
    assert [item.member_candidate_ids for item in separate] == [
        (a.candidate_id,),
        (b.candidate_id,),
    ]
    assert all(item.relations == () for item in separate)


@pytest.mark.parametrize(
    ("first", "second"),
    [
        ({PAYLOAD_ORDINAL: 1}, {PAYLOAD_ORDINAL: 3}),  # not adjacent
        ({PAYLOAD_ORDINAL: True}, {PAYLOAD_ORDINAL: False}),  # booleans are not ordinals
        ({PAYLOAD_ORDINAL: "1"}, {PAYLOAD_ORDINAL: 2}),
        ({}, {PAYLOAD_ORDINAL: 2}),
        ({PAYLOAD_PARENT: "elsewhere"}, {}),
    ],
)
async def test_unrelated_payloads_do_not_merge(first: dict, second: dict) -> None:
    a = candidate("a", final=0.9, payload=first, section=())
    b = candidate("b", final=0.8, payload=second)
    evidence, _ = await CONSOLIDATOR.consolidate([a, b], context())
    assert len(evidence) == 2


async def test_adjacency_needs_two_chunks() -> None:
    a = candidate("a", final=0.9, payload={PAYLOAD_ORDINAL: 1}, kind=ObjectKind.STRUCTURE_NODE)
    b = candidate("b", final=0.8, payload={PAYLOAD_ORDINAL: 2})
    evidence, _ = await CONSOLIDATOR.consolidate([a, b], context())
    assert len(evidence) == 2


async def test_reading_order_follows_ordinals_then_input_order() -> None:
    section = ("chapter",)
    x = candidate("x", final=0.9, section=section, payload={PAYLOAD_ORDINAL: 2})
    y = candidate("y", final=0.8, section=section)
    z = candidate("z", final=0.7, section=section, payload={PAYLOAD_ORDINAL: 1})
    w = candidate("w", final=0.6, section=section, text=None)
    evidence, _ = await CONSOLIDATOR.consolidate([x, y, z, w], context())
    (item,) = evidence
    assert item.member_candidate_ids == tuple(c.candidate_id for c in (x, y, z, w))
    assert item.display_text == "z\n\nx\n\ny"
    assert item.segments[3].text is None


async def test_display_text_is_none_without_texts() -> None:
    evidence, _ = await CONSOLIDATOR.consolidate([candidate("a", text=None)], context())
    assert evidence[0].display_text is None


async def test_completeness_report() -> None:
    section = ("s",)
    chunk_without_span = candidate("a", final=0.9, section=section)
    node_without_span = candidate("b", final=0.8, section=section, kind=ObjectKind.STRUCTURE_NODE)
    stale = candidate(
        "c", final=0.7, section=section, span=(0, 5), flags=CandidateFlags(stale_version=True)
    )
    partial = candidate(
        "d", final=0.6, section=section, span=(9, 12), flags=CandidateFlags(partial_coverage=True)
    )
    evidence, _ = await CONSOLIDATOR.consolidate(
        [chunk_without_span, node_without_span, stale, partial], context()
    )
    (item,) = evidence
    assert item.completeness.missing_spans == (chunk_without_span.object_id,)
    assert item.completeness.stale_sources == (stale.object_id,)
    assert item.completeness.unresolved_sources == (partial.object_id,)


async def test_contradictions_are_linked_and_flagged() -> None:
    support = candidate("support", final=0.9, nli=[judgement(NLILabel.ENTAILMENT)])
    neutral = candidate(
        "neutral", final=0.5, document="d3", nli=[judgement(NLILabel.NEUTRAL, "h2")]
    )
    against = candidate(
        "against", final=0.2, document="d2", nli=[judgement(NLILabel.CONTRADICTION)]
    )
    evidence, truncated = await CONSOLIDATOR.consolidate(
        [support, neutral, against], context(top_k=1, overflow=1)
    )
    assert truncated == []
    assert [item.member_candidate_ids for item in evidence] == [
        (support.candidate_id,),
        (against.candidate_id,),
    ]
    first, second = evidence
    assert first.linked_evidence_ids == (second.evidence_id,)
    assert second.linked_evidence_ids == (first.evidence_id,)
    assert first.nli_summary.entailment == 1
    assert second.nli_summary.contradiction == 1
    assert first.nli_summary.has_contradiction
    assert second.nli_summary.has_contradiction
    assert first.relations == ()

    evidence, truncated = await CONSOLIDATOR.consolidate(
        [support, neutral, against], context(top_k=2, overflow=0)
    )
    assert [item.member_candidate_ids for item in evidence] == [
        (support.candidate_id,),
        (neutral.candidate_id,),
    ]
    assert not evidence[1].nli_summary.has_contradiction
    assert evidence[1].nli_summary.neutral == 1
    assert truncated == [second.evidence_id]


async def test_truncated_ids_are_reported_once_and_chosen_links_are_skipped() -> None:
    first = candidate("s1", final=0.9, nli=[judgement(NLILabel.ENTAILMENT)])
    second = candidate("s2", final=0.8, document="d2", nli=[judgement(NLILabel.ENTAILMENT)])
    against = candidate(
        "against", final=0.1, document="d3", nli=[judgement(NLILabel.CONTRADICTION)]
    )
    evidence, truncated = await CONSOLIDATOR.consolidate(
        [first, second, against], context(top_k=2, overflow=0)
    )
    assert len(evidence) == 2
    assert len(truncated) == 1
    assert evidence[0].linked_evidence_ids == evidence[1].linked_evidence_ids == tuple(truncated)

    evidence, truncated = await CONSOLIDATOR.consolidate(
        [first, second, against], context(top_k=3, overflow=0)
    )
    assert len(evidence) == 3
    assert truncated == []


async def test_contradiction_inside_one_group_is_an_own_relation() -> None:
    support = candidate("support", final=0.9, span=(0, 10), nli=[judgement(NLILabel.ENTAILMENT)])
    against = candidate("against", final=0.8, span=(0, 10), nli=[judgement(NLILabel.CONTRADICTION)])
    evidence, _ = await CONSOLIDATOR.consolidate([support, against], context())
    (item,) = evidence
    assert relation_types(item) == {RelationType.SPAN_OVERLAP, RelationType.CONTRADICTS}
    assert item.linked_evidence_ids == ()
    assert item.nli_summary.has_contradiction


def test_contradictions_pairs() -> None:
    torn = candidate(
        "torn", nli=[judgement(NLILabel.ENTAILMENT), judgement(NLILabel.CONTRADICTION)]
    )
    other = candidate("other", nli=[judgement(NLILabel.CONTRADICTION, "h2")])
    agree = candidate("agree", nli=[judgement(NLILabel.ENTAILMENT, "h2")])
    assert contradictions([torn]) == set()
    pair = tuple(sorted([other.candidate_id, agree.candidate_id]))
    assert contradictions([torn, other, agree]) == {pair}


async def test_semantic_relations_are_recorded_but_never_merge_documents() -> None:
    a = candidate("a", final=0.9, span=(0, 100))
    b = candidate("b", final=0.8, span=(25, 125))
    c = candidate("c", final=0.7, span=(50, 150))
    other_document = candidate("d", final=0.6, document="d2")
    no_vector = candidate("e", final=0.5, document="d3")
    wrong_size = candidate("f", final=0.4, document="d4")
    zero = candidate("g", final=0.3, document="d5")
    orthogonal = candidate("h", final=0.2, document="d6")
    vectors = {
        a.candidate_id: (1.0, 0.0),
        b.candidate_id: (1.0, 0.0),
        c.candidate_id: (1.0, 0.0),
        other_document.candidate_id: (2.0, 0.0),
        wrong_size.candidate_id: (1.0, 0.0, 0.0),
        zero.candidate_id: (0.0, 0.0),
        orthogonal.candidate_id: (0.0, 1.0),
    }
    candidates = [a, b, c, other_document, no_vector, wrong_size, zero, orthogonal]
    evidence, _ = await CONSOLIDATOR.consolidate(
        candidates, context(semantic_relations=True, embeddings=vectors)
    )
    assert evidence[0].member_candidate_ids == (a.candidate_id, b.candidate_id, c.candidate_id)
    assert all(len(item.member_candidate_ids) == 1 for item in evidence[1:])
    semantic = [r for r in evidence[0].relations if r.type is RelationType.SEMANTICALLY_SIMILAR]
    assert [(r.a, r.b) for r in semantic] == [tuple(sorted([a.candidate_id, c.candidate_id]))]
    assert relation_types(evidence[0]) == {
        RelationType.SPAN_OVERLAP,
        RelationType.SEMANTICALLY_SIMILAR,
    }

    plain, _ = await CONSOLIDATOR.consolidate(
        candidates, context(semantic_relations=False, embeddings=vectors)
    )
    assert relation_types(plain[0]) == {RelationType.SPAN_OVERLAP}


async def test_pair_sharing_several_documents_gets_one_relation() -> None:
    sources_a = [source("a", span=(0, 10)), source("a", document="d2", span=(0, 10))]
    sources_b = [source("b", span=(0, 10)), source("b", document="d2", span=(0, 10))]
    a = candidate("a", final=0.9, sources=sources_a)
    b = candidate("b", final=0.8, sources=sources_b)
    evidence, _ = await CONSOLIDATOR.consolidate([a, b], context())
    (item,) = evidence
    assert len(item.relations) == 1
    assert item.sources == (*sources_a, *sources_b)


async def test_transitive_groups_and_deterministic_ids() -> None:
    names = ["p", "q", "r", "s", "t"]
    candidates = [
        candidate(name, final=1.0 - index / 10, span=(index * 10, index * 10 + 15))
        for index, name in enumerate(names)
    ]
    evidence, _ = await CONSOLIDATOR.consolidate(candidates, context(span_iou_threshold=0.2))
    (item,) = evidence
    assert item.member_candidate_ids == tuple(c.candidate_id for c in candidates)
    again, _ = await CONSOLIDATOR.consolidate(candidates[::-1], context(span_iou_threshold=0.2))
    assert again[0].evidence_id == item.evidence_id
    assert again[0].score == item.score == 1.0


async def test_equal_scores_order_by_evidence_id() -> None:
    a = candidate("a", final=0.5, document="d1")
    b = candidate("b", final=0.5, document="d2")
    evidence, _ = await CONSOLIDATOR.consolidate([a, b], context())
    assert [item.evidence_id for item in evidence] == sorted(item.evidence_id for item in evidence)


def test_union_find_compresses_paths() -> None:
    groups = _UnionFind(["a", "b", "c", "d"])
    groups.union("b", "a")
    groups.union("c", "b")
    groups.union("d", "c")
    assert {groups.find(key) for key in "abcd"} == {"d"}
    assert groups._parent["a"] == "d"


async def test_rejects_mixed_scopes_and_missing_scores() -> None:
    a = candidate("a", final=0.9)
    foreign = candidate("b", final=0.8, scope=OTHER_SCOPE)
    with pytest.raises(TenantIsolationError):
        await CONSOLIDATOR.consolidate([a, foreign], context())
    unscored = candidate("c", final=None)
    with pytest.raises(ComponentContractError) as caught:
        await CONSOLIDATOR.consolidate([a, unscored], context())
    assert caught.value.details["candidate_id"] == unscored.candidate_id
    assert await CONSOLIDATOR.consolidate([], context()) == ([], [])
