"""Rank fusion: RRF, weighted RRF and weighted score (ranknexus), CombSUM and CombMNZ."""

from __future__ import annotations

import math
from typing import TYPE_CHECKING, Any

import ranknexus

from retrievalloom.contracts.component import ComponentInfo, make_info
from retrievalloom.contracts.evidence import FusedEntry, FusionABC
from retrievalloom.errors import ConfigurationError
from retrievalloom.evidence.normalizers import MinMaxNormalizer, make_normalizer

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from retrievalloom.config import FusionConfig
    from retrievalloom.contracts.evidence import RankedEntry, ScoreNormalizerABC

_ID = "id"
_SCORE = "score"


def _clean(lists: Mapping[str, Sequence[RankedEntry]]) -> dict[str, list[RankedEntry]]:
    """Non-empty lists sorted by rank, keeping each item's best rank once."""
    cleaned: dict[str, list[RankedEntry]] = {}
    for source, entries in lists.items():
        seen: set[str] = set()
        ordered: list[RankedEntry] = []
        for entry in sorted(entries, key=lambda item: (item.rank, item.item_id)):
            if entry.item_id not in seen:
                seen.add(entry.item_id)
                ordered.append(entry)
        if ordered:
            cleaned[source] = ordered
    return cleaned


def resolve_weights(sources: Sequence[str], weights: Mapping[str, float]) -> dict[str, float]:
    """Weights of ``sources`` (missing ones default to 1.0), renormalised to sum 1.

    Raises:
        ConfigurationError: A weight is negative or non-finite, or all weights are zero.
    """
    resolved = {source: float(weights.get(source, 1.0)) for source in sources}
    for source, weight in resolved.items():
        if not math.isfinite(weight) or weight < 0:
            raise ConfigurationError(
                "fusion weights must be finite and non-negative", details={"source": source}
            )
    total = sum(resolved.values())
    if sources and total <= 0:
        raise ConfigurationError("fusion weights must not all be zero")
    return {source: weight / total for source, weight in resolved.items()}


def order_fused(
    scores: Mapping[str, float], lists: Mapping[str, Sequence[RankedEntry]]
) -> list[FusedEntry]:
    """Deterministic order: score desc, best individual rank asc, item id asc."""
    best_rank: dict[str, int] = {}
    for entries in lists.values():
        for entry in entries:
            best_rank[entry.item_id] = min(best_rank.get(entry.item_id, entry.rank), entry.rank)
    ordered = sorted(scores, key=lambda key: (-scores[key], best_rank[key], key))
    return [FusedEntry(item_id=key, score=scores[key]) for key in ordered]


def _ranknexus(
    lists: Mapping[str, Sequence[RankedEntry]],
    *,
    method: str,
    k: int,
    weights: Mapping[str, float] | None = None,
    values: Mapping[str, Mapping[str, float]] | None = None,
) -> dict[str, float]:
    sources = list(lists)
    rankings: list[list[dict[str, Any]]] = [
        [
            {_ID: entry.item_id, _SCORE: (values[source][entry.item_id] if values else 0.0)}
            for entry in lists[source]
        ]
        for source in sources
    ]
    fused = ranknexus.fuse(
        rankings,
        method=method,
        id_field=_ID,
        score_field=_SCORE if values is not None else None,
        weights=[weights[source] for source in sources] if weights is not None else None,
        k=k,
        normalize=False,
    )
    return {str(result.document_id): float(result.score) for result in fused}


class RRFFusion(FusionABC):
    """Reciprocal rank fusion ``sum 1 / (k + rank)`` (1-based ranks); weights are ignored.

    Args:
        k: Smoothing constant (default 60).
    """

    def __init__(self, k: int = 60) -> None:
        if k < 1:
            raise ConfigurationError("RRF k must be >= 1")
        self._k = k
        self._info = make_info(
            component="fusion", implementation="rrf", version="1", config={"k": k}
        )

    @property
    def info(self) -> ComponentInfo:
        """Component identity."""
        return self._info

    async def fuse(
        self, lists: Mapping[str, Sequence[RankedEntry]], weights: Mapping[str, float]
    ) -> Sequence[FusedEntry]:
        """Fuse by reciprocal rank."""
        del weights
        cleaned = _clean(lists)
        if not cleaned:
            return []
        return order_fused(_ranknexus(cleaned, method="rrf", k=self._k), cleaned)


class WeightedRRFFusion(RRFFusion):
    """``sum w_i / (k + rank_i)`` with weights renormalised to sum 1."""

    def __init__(self, k: int = 60) -> None:
        super().__init__(k)
        self._info = make_info(
            component="fusion", implementation="weighted_rrf", version="1", config={"k": k}
        )

    async def fuse(
        self, lists: Mapping[str, Sequence[RankedEntry]], weights: Mapping[str, float]
    ) -> Sequence[FusedEntry]:
        """Fuse by weighted reciprocal rank."""
        cleaned = _clean(lists)
        if not cleaned:
            return []
        resolved = resolve_weights(list(cleaned), weights)
        scores = _ranknexus(cleaned, method="weighted_rrf", k=self._k, weights=resolved)
        return order_fused(scores, cleaned)


class _ScoreFusion(FusionABC):
    _NAME = ""

    def __init__(self, normalizer: ScoreNormalizerABC | None = None) -> None:
        self._normalizer = normalizer or MinMaxNormalizer()
        self._info = make_info(
            component="fusion",
            implementation=self._NAME,
            version="1",
            config={"normalizer": self._normalizer.info.fingerprint.digest},
        )

    @property
    def info(self) -> ComponentInfo:
        """Component identity."""
        return self._info

    @property
    def needs_scores(self) -> bool:
        """Score-based: rank-only sources are rejected unless normalised by rank."""
        return self._normalizer.info.implementation != "rank"

    async def _normalized(
        self, lists: Mapping[str, Sequence[RankedEntry]]
    ) -> dict[str, dict[str, float]]:
        return {
            source: dict(await self._normalizer.normalize(entries))
            for source, entries in lists.items()
        }


class WeightedScoreFusion(_ScoreFusion):
    """``sum w_i * n_i`` over normalised scores (absent counts 0), via ranknexus."""

    _NAME = "weighted_score"

    async def fuse(
        self, lists: Mapping[str, Sequence[RankedEntry]], weights: Mapping[str, float]
    ) -> Sequence[FusedEntry]:
        """Fuse by weighted normalised score."""
        cleaned = _clean(lists)
        if not cleaned:
            return []
        resolved = resolve_weights(list(cleaned), weights)
        values = await self._normalized(cleaned)
        scores = _ranknexus(cleaned, method="weighted_score", k=60, weights=resolved, values=values)
        return order_fused(scores, cleaned)


class CombSUMFusion(_ScoreFusion):
    """``sum n_i`` over normalised scores; weights are ignored."""

    _NAME = "combsum"

    async def fuse(
        self, lists: Mapping[str, Sequence[RankedEntry]], weights: Mapping[str, float]
    ) -> Sequence[FusedEntry]:
        """Fuse by summed normalised score."""
        del weights
        cleaned = _clean(lists)
        values = await self._normalized(cleaned)
        totals: dict[str, float] = {}
        counts: dict[str, int] = {}
        for scores in values.values():
            for key, value in scores.items():
                totals[key] = totals.get(key, 0.0) + value
                counts[key] = counts.get(key, 0) + 1
        return order_fused(self._combine(totals, counts), cleaned)

    def _combine(self, totals: dict[str, float], counts: dict[str, int]) -> dict[str, float]:
        del counts
        return totals


class CombMNZFusion(CombSUMFusion):
    """``|{i : d in L_i}| * sum n_i``."""

    _NAME = "combmnz"

    def _combine(self, totals: dict[str, float], counts: dict[str, int]) -> dict[str, float]:
        return {key: counts[key] * value for key, value in totals.items()}


def make_fusion(config: FusionConfig) -> FusionABC:
    """Build the fusion method named in ``config``."""
    if config.method == "weighted_rrf":
        return WeightedRRFFusion(config.k)
    if config.method == "weighted_score":
        return WeightedScoreFusion(make_normalizer(config))
    if config.method == "combsum":
        return CombSUMFusion(make_normalizer(config))
    if config.method == "combmnz":
        return CombMNZFusion(make_normalizer(config))
    return RRFFusion(config.k)
