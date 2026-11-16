"""Score normalisers: min-max, z-score, rank and sigmoid."""

from __future__ import annotations

import math
import statistics
from typing import TYPE_CHECKING

from retrievalloom.contracts.component import ComponentInfo, make_info
from retrievalloom.contracts.evidence import ScoreNormalizerABC
from retrievalloom.errors import ComponentContractError, ConfigurationError

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from retrievalloom.config import FusionConfig
    from retrievalloom.contracts.evidence import RankedEntry


def oriented_scores(entries: Sequence[RankedEntry]) -> dict[str, float]:
    """Raw scores keyed by item, negated for lower-is-better sources.

    Raises:
        ComponentContractError: An entry has no score or a non-finite score.
    """
    scores: dict[str, float] = {}
    for entry in entries:
        if entry.score is None or not math.isfinite(entry.score):
            raise ComponentContractError(
                "score-based normalisation needs a finite score for every entry",
                details={"item_id": entry.item_id},
            )
        value = entry.score if entry.semantics.higher_is_better else -entry.score
        scores.setdefault(entry.item_id, value)
    return scores


class MinMaxNormalizer(ScoreNormalizerABC):
    """``(x - min) / (max - min)``; all-equal lists map to 1.0."""

    _INFO = make_info(component="score_normalizer", implementation="minmax", version="1")

    @property
    def info(self) -> ComponentInfo:
        """Component identity."""
        return self._INFO

    async def normalize(self, entries: Sequence[RankedEntry]) -> Mapping[str, float]:
        """Normalise ``entries`` to [0, 1]."""
        scores = oriented_scores(entries)
        if not scores:
            return {}
        low, high = min(scores.values()), max(scores.values())
        if high == low:
            return dict.fromkeys(scores, 1.0)
        return {key: (value - low) / (high - low) for key, value in scores.items()}


class ZScoreNormalizer(ScoreNormalizerABC):
    """``(x - mean) / stdev`` (population); zero deviation maps to 0.0."""

    _INFO = make_info(component="score_normalizer", implementation="zscore", version="1")

    @property
    def info(self) -> ComponentInfo:
        """Component identity."""
        return self._INFO

    async def normalize(self, entries: Sequence[RankedEntry]) -> Mapping[str, float]:
        """Standardise ``entries``."""
        scores = oriented_scores(entries)
        if not scores:
            return {}
        mean = statistics.fmean(scores.values())
        deviation = statistics.pstdev(scores.values(), mu=mean)
        if deviation == 0.0:
            return dict.fromkeys(scores, 0.0)
        return {key: (value - mean) / deviation for key, value in scores.items()}


class RankNormalizer(ScoreNormalizerABC):
    """``1 - (rank - 1) / |L|``; works for rank-only sources."""

    _INFO = make_info(component="score_normalizer", implementation="rank", version="1")

    @property
    def info(self) -> ComponentInfo:
        """Component identity."""
        return self._INFO

    async def normalize(self, entries: Sequence[RankedEntry]) -> Mapping[str, float]:
        """Map ranks to (0, 1]."""
        size = len(entries)
        result: dict[str, float] = {}
        for entry in sorted(entries, key=lambda item: item.rank):
            result.setdefault(entry.item_id, 1.0 - (entry.rank - 1) / size)
        return result


class SigmoidNormalizer(ScoreNormalizerABC):
    """``1 / (1 + exp(-(x - center) / temperature))``.

    Args:
        center: Score mapped to 0.5.
        temperature: Positive scale.
    """

    def __init__(self, center: float = 0.0, temperature: float = 1.0) -> None:
        if not math.isfinite(center) or not math.isfinite(temperature) or temperature <= 0:
            raise ConfigurationError("sigmoid needs a finite center and a positive temperature")
        self._center = center
        self._temperature = temperature
        self._info = make_info(
            component="score_normalizer",
            implementation="sigmoid",
            version="1",
            config={"center": center, "temperature": temperature},
        )

    @property
    def info(self) -> ComponentInfo:
        """Component identity."""
        return self._info

    def _squash(self, value: float) -> float:
        exponent = -(value - self._center) / self._temperature
        if exponent >= 0:
            return math.exp(-exponent) / (1.0 + math.exp(-exponent))
        return 1.0 / (1.0 + math.exp(exponent))

    async def normalize(self, entries: Sequence[RankedEntry]) -> Mapping[str, float]:
        """Map scores into (0, 1)."""
        return {key: self._squash(value) for key, value in oriented_scores(entries).items()}


def make_normalizer(config: FusionConfig) -> ScoreNormalizerABC:
    """Build the normaliser named in ``config``."""
    if config.normalizer == "zscore":
        return ZScoreNormalizer()
    if config.normalizer == "rank":
        return RankNormalizer()
    if config.normalizer == "sigmoid":
        return SigmoidNormalizer(config.sigmoid_center, config.sigmoid_temperature)
    return MinMaxNormalizer()
