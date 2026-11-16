"""Contract kits for evidence components: normalisation, fusion, dedup, consolidation."""

from __future__ import annotations

import math
from abc import abstractmethod
from typing import TYPE_CHECKING, Any

import pytest

from retrievalloom.contracts.evidence import ConsolidationContext, DedupResult, RankedEntry
from retrievalloom.domain.hashing import sha256_digest, stable_id
from retrievalloom.domain.inputs import Scope, TextSpan
from retrievalloom.domain.records import Channel, ObjectKind, SourceReference
from retrievalloom.domain.retrieval import (
    Candidate,
    MethodRef,
    NLIJudgement,
    NLILabel,
    ObjectRef,
    RetrievalHit,
    ScoreCard,
)
from retrievalloom.testing._base import ComponentContract

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

SCOPE = Scope(tenant_id="contract")


def entries(*pairs: tuple[str, float]) -> list[RankedEntry]:
    """Ranked entries from ``(item_id, score)`` pairs in rank order."""
    return [
        RankedEntry(item_id=item_id, rank=rank, score=score)
        for rank, (item_id, score) in enumerate(pairs, start=1)
    ]


def make_candidate(
    name: str,
    *,
    document: str,
    final: float,
    version: int = 1,
    span: tuple[int, int] | None = None,
    text: str | None = None,
    content: str | None = None,
    nli: Sequence[NLIJudgement] = (),
    method: str = "sparse",
) -> Candidate:
    """Build a candidate with one source and one hit (helper for consolidation kits)."""
    object_id = stable_id("obj", name)
    document_uid = stable_id("doc", document)
    source = SourceReference(
        scope=SCOPE,
        document_id=document,
        document_uid=document_uid,
        document_version=version,
        representation_id="text",
        object_id=object_id,
        span=TextSpan(start=span[0], end=span[1]) if span else None,
    )
    hit = RetrievalHit(
        object_ref=ObjectRef(scope_key=SCOPE.key, object_id=object_id, generation_id="gen"),
        method=MethodRef(channel=Channel.INDEX, method=method),
        backend_id="contract",
        raw_score=final,
        rank=1,
    )
    return Candidate(
        candidate_id=stable_id("cand", name),
        scope=SCOPE,
        object_id=object_id,
        kind=ObjectKind.CHUNK,
        text=text if text is not None else name,
        content_hash=sha256_digest(content if content is not None else name),
        sources=(source,),
        hits=(hit,),
        scores=ScoreCard(final=final),
        nli=tuple(nli),
    )


def judgement(label: NLILabel, hypothesis: str = "h1") -> NLIJudgement:
    """NLI judgement helper."""
    return NLIJudgement(
        label=label, confidence=0.9, model_identity="contract", hypothesis_ref=hypothesis
    )


class ScoreNormalizerContract(ComponentContract):
    """Kit for ``ScoreNormalizerABC``: one finite value per item, order preserving."""

    async def test_normalizes_every_item(self, component: Any) -> None:
        """Keys equal the input ids; better raw scores never normalise lower."""
        ranked = entries(("a", 9.0), ("b", 4.0), ("c", 1.0))
        values = await component.normalize(ranked)
        assert set(values) == {"a", "b", "c"}
        assert all(math.isfinite(value) for value in values.values())
        assert values["a"] >= values["b"] >= values["c"]

    async def test_equal_scores_and_empty_input(self, component: Any) -> None:
        """Ties stay finite and equal; empty input yields an empty mapping."""
        values = await component.normalize(entries(("a", 2.0), ("b", 2.0)))
        assert all(math.isfinite(value) for value in values.values())
        assert dict(await component.normalize([])) == {}


class FusionContract(ComponentContract):
    """Kit for ``FusionABC``: scores match ``reference`` and ordering is deterministic."""

    @abstractmethod
    def reference(
        self, lists: Mapping[str, Sequence[RankedEntry]], weights: Mapping[str, float]
    ) -> dict[str, float]:
        """Reference fused scores computed from the method's formula."""

    def lists(self) -> dict[str, list[RankedEntry]]:
        """Input lists of the kit (overlapping, with a tie candidate)."""
        return {
            "sparse": entries(("a", 12.0), ("b", 7.5), ("c", 3.0)),
            "dense": entries(("b", 0.91), ("d", 0.88), ("a", 0.42)),
            "keyword": entries(("e", 2.0)),
        }

    async def test_matches_reference_formula(self, component: Any) -> None:
        """Every item appears once with the reference score, best first."""
        lists = self.lists()
        weights = {"sparse": 2.0, "dense": 1.0, "keyword": 0.5}
        fused = await component.fuse(lists, weights)
        expected = self.reference(lists, weights)
        assert sorted(entry.item_id for entry in fused) == sorted(expected)
        for entry in fused:
            assert math.isclose(entry.score, expected[entry.item_id], rel_tol=1e-9, abs_tol=1e-12)
        scores = [entry.score for entry in fused]
        assert scores == sorted(scores, reverse=True)

    async def test_ties_are_deterministic(self, component: Any) -> None:
        """Equal fused scores order by best rank, then item id."""
        lists = {"x": entries(("b", 1.0)), "y": entries(("a", 1.0))}
        first = await component.fuse(lists, {})
        second = await component.fuse(dict(reversed(lists.items())), {})
        assert [entry.item_id for entry in first] == ["a", "b"]
        assert [entry.item_id for entry in second] == ["a", "b"]

    async def test_empty_input(self, component: Any) -> None:
        """No lists, nothing fused."""
        assert list(await component.fuse({}, {})) == []
        assert list(await component.fuse({"x": []}, {})) == []


class DeduplicatorContract(ComponentContract):
    """Kit for ``DeduplicatorABC``: duplicates merge without losing sources or hits (P1)."""

    async def test_merges_without_losing_provenance(self, component: Any) -> None:
        """Every input source and hit survives in the kept candidates."""
        candidates = [
            make_candidate("one", document="d1", final=0.9, content="same"),
            make_candidate("two", document="d2", final=0.8, content="same"),
            make_candidate("three", document="d3", final=0.7),
        ]
        result = await component.deduplicate(candidates)
        assert isinstance(result, DedupResult)
        kept_sources = {source for candidate in result.kept for source in candidate.sources}
        kept_hits = {hit for candidate in result.kept for hit in candidate.hits}
        for candidate in candidates:
            assert set(candidate.sources) <= kept_sources
            assert set(candidate.hits) <= kept_hits
        assert len(result.kept) <= len(candidates)
        assert result.kept[0].candidate_id == candidates[0].candidate_id

    async def test_distinct_candidates_are_kept(self, component: Any) -> None:
        """Unrelated candidates pass through in order."""
        candidates = [
            make_candidate("x", document="d1", final=0.9),
            make_candidate("y", document="d2", final=0.5),
        ]
        result = await component.deduplicate(candidates)
        assert [c.candidate_id for c in result.kept] == [c.candidate_id for c in candidates]


class EvidenceConsolidatorContract(ComponentContract):
    """Kit for ``EvidenceConsolidatorABC`` (invariants E1-E3 and P1)."""

    def context(self, top_k: int = 10, overflow: int = 0, **values: Any) -> ConsolidationContext:
        """Consolidation parameters."""
        return ConsolidationContext(top_k=top_k, contradiction_overflow=overflow, **values)

    async def test_identity_grouping(self, component: Any) -> None:
        """E1: unrelated candidates each become their own evidence, best first."""
        candidates = [
            make_candidate("a", document="d1", final=0.9),
            make_candidate("b", document="d2", final=0.6),
        ]
        evidence, truncated = await component.consolidate(candidates, self.context())
        assert [item.member_candidate_ids for item in evidence] == [
            (candidates[0].candidate_id,),
            (candidates[1].candidate_id,),
        ]
        assert list(truncated) == []
        assert list((await component.consolidate([], self.context()))[0]) == []

    async def test_provenance_is_preserved(self, component: Any) -> None:
        """P1: every source and hit of each consolidated member is kept."""
        candidates = [
            make_candidate("a", document="d1", final=0.9, span=(0, 10)),
            make_candidate("b", document="d1", final=0.8, span=(2, 10)),
        ]
        evidence, _ = await component.consolidate(candidates, self.context())
        members = {cid for item in evidence for cid in item.member_candidate_ids}
        sources = {source for item in evidence for source in item.provenance.sources}
        hits = {hit for item in evidence for hit in item.provenance.hits}
        for candidate in candidates:
            if candidate.candidate_id in members:
                assert set(candidate.sources) <= sources
                assert set(candidate.hits) <= hits
        for item in evidence:
            assert {segment.candidate_id for segment in item.segments} == set(
                item.member_candidate_ids
            )

    async def test_contradictions_are_kept(self, component: Any) -> None:
        """E2: a contradicting candidate is kept in the same or a linked evidence item."""
        support = make_candidate(
            "support", document="d1", final=0.9, nli=[judgement(NLILabel.ENTAILMENT)]
        )
        filler = make_candidate("filler", document="d3", final=0.5)
        against = make_candidate(
            "against", document="d2", final=0.1, nli=[judgement(NLILabel.CONTRADICTION)]
        )
        evidence, truncated = await component.consolidate(
            [support, filler, against], self.context(top_k=1, overflow=1)
        )
        members = {cid for item in evidence for cid in item.member_candidate_ids}
        assert {support.candidate_id, against.candidate_id} <= members
        assert any(item.nli_summary.has_contradiction for item in evidence)
        assert list(truncated) == []
        _, truncated = await component.consolidate(
            [support, filler, against], self.context(top_k=1, overflow=0)
        )
        assert len(truncated) == 1

    async def test_semantic_similarity_never_merges_documents(self, component: Any) -> None:
        """E3: semantically similar members of different documents stay separate."""
        a = make_candidate("a", document="d1", final=0.9)
        b = make_candidate("b", document="d2", final=0.8)
        context = self.context(
            semantic_relations=True,
            embeddings={a.candidate_id: (1.0, 0.0), b.candidate_id: (1.0, 0.0)},
        )
        evidence, _ = await component.consolidate([a, b], context)
        assert all(len(item.member_candidate_ids) == 1 for item in evidence)

    async def test_rejects_mixed_scopes(self, component: Any) -> None:
        """Candidates of different scopes are never consolidated together."""
        a = make_candidate("a", document="d1", final=0.9)
        foreign = a.model_copy(update={"scope": Scope(tenant_id="other")})
        with pytest.raises(Exception):  # noqa: B017, PT011
            await component.consolidate([a, foreign], self.context())


def rrf_reference(
    lists: Mapping[str, Sequence[RankedEntry]], weights: Mapping[str, float] | None, k: int = 60
) -> dict[str, float]:
    """``sum w_i / (k + rank_i)`` (``weights=None`` is plain RRF); weights renormalised."""
    total = sum(weights.get(source, 1.0) for source in lists if lists[source]) if weights else 1.0
    scores: dict[str, float] = {}
    for source, ranked in lists.items():
        weight = weights.get(source, 1.0) / total if weights else 1.0
        for entry in ranked:
            scores[entry.item_id] = scores.get(entry.item_id, 0.0) + weight / (k + entry.rank)
    return scores


def score_reference(
    lists: Mapping[str, Sequence[RankedEntry]],
    normalized: Mapping[str, Mapping[str, float]],
    *,
    weights: Mapping[str, float] | None = None,
    mnz: bool = False,
) -> dict[str, float]:
    """Weighted score (``weights`` given), CombSUM or CombMNZ (``mnz``) over normalised values."""
    total = sum(weights.get(source, 1.0) for source in lists if lists[source]) if weights else 1.0
    scores: dict[str, float] = {}
    counts: dict[str, int] = {}
    for source in lists:
        weight = weights.get(source, 1.0) / total if weights else 1.0
        for item_id, value in normalized.get(source, {}).items():
            scores[item_id] = scores.get(item_id, 0.0) + weight * value
            counts[item_id] = counts.get(item_id, 0) + 1
    if mnz:
        return {item_id: counts[item_id] * value for item_id, value in scores.items()}
    return scores
