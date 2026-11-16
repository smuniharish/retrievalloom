"""Normalisers and fusion: reference formulas, orientation, weights and factories."""

from __future__ import annotations

import math
from typing import TYPE_CHECKING, Any

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from retrievalloom.config import FusionConfig
from retrievalloom.contracts.evidence import RankedEntry
from retrievalloom.domain.retrieval import ScoreRange, ScoreSemantics
from retrievalloom.errors import ComponentContractError, ConfigurationError
from retrievalloom.evidence.fusion import (
    CombMNZFusion,
    CombSUMFusion,
    RRFFusion,
    WeightedRRFFusion,
    WeightedScoreFusion,
    make_fusion,
    order_fused,
    resolve_weights,
)
from retrievalloom.evidence.normalizers import (
    MinMaxNormalizer,
    RankNormalizer,
    SigmoidNormalizer,
    ZScoreNormalizer,
    make_normalizer,
    oriented_scores,
)

if TYPE_CHECKING:
    from collections.abc import Sequence

DISTANCE = ScoreSemantics(higher_is_better=False, range=ScoreRange.DISTANCE)


def ranked(*pairs: tuple[str, float | None], semantics: ScoreSemantics | None = None) -> list:
    return [
        RankedEntry(
            item_id=item_id, rank=rank, score=score, semantics=semantics or ScoreSemantics()
        )
        for rank, (item_id, score) in enumerate(pairs, start=1)
    ]


def as_dict(fused: Sequence[Any]) -> dict[str, float]:
    return {entry.item_id: entry.score for entry in fused}


# normalisers ----------------------------------------------------------------------------
def test_oriented_scores_negates_lower_is_better_and_keeps_first() -> None:
    entries = ranked(("a", 0.2), ("b", 0.7), ("a", 0.9), semantics=DISTANCE)
    assert oriented_scores(entries) == {"a": -0.2, "b": -0.7}


@pytest.mark.parametrize("bad", [None, math.nan, math.inf])
async def test_score_normalisers_reject_missing_or_non_finite_scores(bad: float | None) -> None:
    entry = RankedEntry(item_id="a", rank=1).model_copy(update={"score": bad})
    for normalizer in (MinMaxNormalizer(), ZScoreNormalizer(), SigmoidNormalizer()):
        with pytest.raises(ComponentContractError) as caught:
            await normalizer.normalize([entry])
        assert caught.value.details == {"item_id": "a"}


async def test_minmax() -> None:
    normalizer = MinMaxNormalizer()
    assert await normalizer.normalize(ranked(("a", 10.0), ("b", 5.0), ("c", 0.0))) == {
        "a": 1.0,
        "b": 0.5,
        "c": 0.0,
    }
    assert await normalizer.normalize(ranked(("a", 3.0), ("b", 3.0))) == {"a": 1.0, "b": 1.0}
    distances = await normalizer.normalize(ranked(("near", 0.1), ("far", 0.9), semantics=DISTANCE))
    assert distances == {"near": 1.0, "far": 0.0}
    assert await normalizer.normalize([]) == {}


async def test_zscore() -> None:
    normalizer = ZScoreNormalizer()
    values = await normalizer.normalize(ranked(("a", 3.0), ("b", 1.0)))
    assert values == pytest.approx({"a": 1.0, "b": -1.0})
    assert await normalizer.normalize(ranked(("a", 4.0), ("b", 4.0))) == {"a": 0.0, "b": 0.0}
    assert await normalizer.normalize([]) == {}


async def test_rank_normalizer_ignores_scores_and_keeps_best_rank() -> None:
    entries = [
        RankedEntry(item_id="b", rank=2),
        RankedEntry(item_id="a", rank=1),
        RankedEntry(item_id="b", rank=4),
        RankedEntry(item_id="c", rank=3),
    ]
    assert await RankNormalizer().normalize(entries) == {"a": 1.0, "b": 0.75, "c": 0.5}


async def test_sigmoid_is_stable_and_centered() -> None:
    normalizer = SigmoidNormalizer(center=2.0, temperature=0.5)
    values = await normalizer.normalize(
        ranked(("huge", 1e6), ("high", 3.0), ("center", 2.0), ("low", 1.0), ("tiny", -1e6))
    )
    assert values["center"] == 0.5
    assert values["high"] == pytest.approx(1 / (1 + math.exp(-2.0)))
    assert values["low"] == pytest.approx(1 / (1 + math.exp(2.0)))
    assert values["huge"] == 1.0
    assert values["tiny"] == 0.0
    assert values["high"] + values["low"] == pytest.approx(1.0)


@pytest.mark.parametrize(
    ("center", "temperature"), [(0.0, 0.0), (0.0, -1.0), (math.nan, 1.0), (0.0, math.inf)]
)
def test_sigmoid_rejects_invalid_parameters(center: float, temperature: float) -> None:
    with pytest.raises(ConfigurationError):
        SigmoidNormalizer(center=center, temperature=temperature)


@pytest.mark.parametrize(
    ("name", "cls"),
    [
        ("minmax", MinMaxNormalizer),
        ("zscore", ZScoreNormalizer),
        ("rank", RankNormalizer),
        ("sigmoid", SigmoidNormalizer),
    ],
)
def test_make_normalizer(name: str, cls: type) -> None:
    config = FusionConfig(normalizer=name, sigmoid_center=1.5, sigmoid_temperature=2.0)  # type: ignore[arg-type]
    normalizer = make_normalizer(config)
    assert type(normalizer) is cls
    if name == "sigmoid":
        assert normalizer.info == SigmoidNormalizer(1.5, 2.0).info


# fusion ---------------------------------------------------------------------------------
def test_resolve_weights() -> None:
    assert resolve_weights(["a", "b"], {"a": 3.0}) == {"a": 0.75, "b": 0.25}
    assert resolve_weights(["a", "b"], {"a": 0.0}) == {"a": 0.0, "b": 1.0}
    assert resolve_weights([], {"a": 1.0}) == {}


@pytest.mark.parametrize("weights", [{"a": -1.0}, {"a": math.nan}, {"b": math.inf}])
def test_resolve_weights_rejects_invalid(weights: dict[str, float]) -> None:
    with pytest.raises(ConfigurationError):
        resolve_weights(["a", "b"], weights)


def test_resolve_weights_rejects_all_zero() -> None:
    with pytest.raises(ConfigurationError):
        resolve_weights(["a", "b"], {"a": 0.0, "b": 0.0})


def test_order_fused_breaks_ties_by_best_rank_then_id() -> None:
    lists = {"x": ranked(("c", None), ("b", None)), "y": ranked(("a", None), ("b", None))}
    fused = order_fused({"a": 1.0, "b": 1.0, "c": 1.0, "d": 2.0}, {**lists, "z": ranked(("d", 0))})
    assert [entry.item_id for entry in fused] == ["d", "a", "c", "b"]


def test_rrf_rejects_invalid_k() -> None:
    with pytest.raises(ConfigurationError):
        RRFFusion(k=0)
    with pytest.raises(ConfigurationError):
        WeightedRRFFusion(k=0)


async def test_rrf_counts_each_item_once_per_list_and_ignores_weights() -> None:
    lists = {"x": ranked(("a", None), ("b", None), ("a", None)), "y": ranked(("b", None))}
    fused = await RRFFusion(k=1).fuse(lists, {"x": 100.0})
    assert as_dict(fused) == pytest.approx({"a": 1 / 2, "b": 1 / 3 + 1 / 2})
    assert [entry.item_id for entry in fused] == ["b", "a"]
    assert not RRFFusion().needs_scores
    assert await RRFFusion().fuse({}, {"x": 1.0}) == []


def test_score_fusion_fingerprint_depends_on_normalizer() -> None:
    zscore = WeightedScoreFusion(ZScoreNormalizer()).info.fingerprint.digest
    assert zscore != WeightedScoreFusion(MinMaxNormalizer()).info.fingerprint.digest
    assert zscore == WeightedScoreFusion(ZScoreNormalizer()).info.fingerprint.digest


async def test_weighted_rrf_renormalises_weights() -> None:
    lists = {"x": ranked(("a", None), ("b", None)), "y": ranked(("b", None)), "empty": []}
    fused = await WeightedRRFFusion(k=10).fuse(lists, {"x": 3.0, "y": 1.0, "empty": 50.0})
    assert as_dict(fused) == pytest.approx({"a": 0.75 / 11, "b": 0.75 / 12 + 0.25 / 11})
    assert await WeightedRRFFusion().fuse({"x": []}, {}) == []


@settings(max_examples=40, deadline=None)
@given(
    st.dictionaries(
        st.sampled_from(["s1", "s2", "s3"]),
        st.lists(st.sampled_from("abcdef"), min_size=1, max_size=6, unique=True),
        min_size=1,
    ),
    st.dictionaries(st.sampled_from(["s1", "s2", "s3"]), st.floats(0.1, 5.0)),
    st.integers(1, 100),
)
async def test_weighted_rrf_matches_reference(
    items: dict[str, list[str]], weights: dict[str, float], k: int
) -> None:
    lists = {source: ranked(*((item, None) for item in values)) for source, values in items.items()}
    total = sum(weights.get(source, 1.0) for source in lists)
    expected: dict[str, float] = {}
    for source, values in items.items():
        for rank, item in enumerate(values, start=1):
            share = weights.get(source, 1.0) / total / (k + rank)
            expected[item] = expected.get(item, 0.0) + share
    fused = await WeightedRRFFusion(k=k).fuse(lists, weights)
    assert as_dict(fused) == pytest.approx(expected, rel=1e-9)
    scores = [entry.score for entry in fused]
    assert scores == sorted(scores, reverse=True)


async def test_weighted_score_fusion_orients_and_weights_scores() -> None:
    lists = {
        "sparse": ranked(("a", 10.0), ("b", 0.0)),
        "dense": ranked(("b", 0.1), ("c", 0.5), semantics=DISTANCE),
    }
    fusion = WeightedScoreFusion()
    assert fusion.needs_scores
    fused = await fusion.fuse(lists, {"sparse": 1.0, "dense": 3.0})
    assert as_dict(fused) == pytest.approx({"a": 0.25, "b": 0.75, "c": 0.0})
    assert await fusion.fuse({}, {}) == []


async def test_score_fusion_rejects_rank_only_lists_unless_rank_normalised() -> None:
    lists = {"x": ranked(("a", None), ("b", None)), "y": ranked(("b", None))}
    with pytest.raises(ComponentContractError):
        await CombSUMFusion().fuse(lists, {})
    fusion = CombSUMFusion(RankNormalizer())
    assert not fusion.needs_scores
    assert as_dict(await fusion.fuse(lists, {})) == {"a": 1.0, "b": 1.5}


async def test_combsum_and_combmnz() -> None:
    lists = {
        "x": ranked(("a", 4.0), ("b", 2.0), ("c", 0.0)),
        "y": ranked(("b", 1.0), ("c", 0.0)),
    }
    assert as_dict(await CombSUMFusion().fuse(lists, {"x": 9.0})) == {
        "a": 1.0,
        "b": 1.5,
        "c": 0.0,
    }
    assert as_dict(await CombMNZFusion().fuse(lists, {})) == {"a": 1.0, "b": 3.0, "c": 0.0}
    assert await CombMNZFusion().fuse({"x": []}, {}) == []


@pytest.mark.parametrize(
    ("method", "cls"),
    [
        ("rrf", RRFFusion),
        ("weighted_rrf", WeightedRRFFusion),
        ("weighted_score", WeightedScoreFusion),
        ("combsum", CombSUMFusion),
        ("combmnz", CombMNZFusion),
    ],
)
def test_make_fusion(method: str, cls: type) -> None:
    fusion = make_fusion(FusionConfig(method=method, k=7, normalizer="rank"))  # type: ignore[arg-type]
    assert type(fusion) is cls
    assert fusion.info.implementation == method
    assert fusion.needs_scores is False
    if method in {"rrf", "weighted_rrf"}:
        assert fusion.info == cls(7).info
