"""Evidence intelligence contracts: normalisation, fusion, dedup, consolidation."""

from __future__ import annotations

from abc import abstractmethod
from typing import TYPE_CHECKING

from pydantic import Field

from retrievalloom.contracts.component import ComponentABC
from retrievalloom.domain.base import FrozenModel, PositiveInt
from retrievalloom.domain.retrieval import Candidate, Evidence, ScoreSemantics

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence


class RankedEntry(FrozenModel):
    """One entry of a ranked list fed to normalisation or fusion."""

    item_id: str
    rank: PositiveInt
    score: float | None = None
    semantics: ScoreSemantics = Field(default_factory=ScoreSemantics)


class FusedEntry(FrozenModel):
    """Fusion output entry."""

    item_id: str
    score: float


class ScoreNormalizerABC(ComponentABC):
    """Maps raw scores of one ranked list to comparable values."""

    @abstractmethod
    async def normalize(self, entries: Sequence[RankedEntry]) -> Mapping[str, float]:
        """Return normalised scores keyed by ``item_id``."""


class FusionABC(ComponentABC):
    """Fuses several ranked lists into one ordering."""

    @property
    def needs_scores(self) -> bool:
        """Whether the method needs raw scores (rank-only sources are rejected)."""
        return False

    @abstractmethod
    async def fuse(
        self,
        lists: Mapping[str, Sequence[RankedEntry]],
        weights: Mapping[str, float],
    ) -> Sequence[FusedEntry]:
        """Fuse ``lists`` (keyed by source name) using ``weights`` (keyed by name)."""


class DedupResult(FrozenModel):
    """Deduplication output: kept candidates and what each one absorbed."""

    kept: tuple[Candidate, ...]
    merged: dict[str, tuple[str, ...]] = Field(default_factory=dict)


class DeduplicatorABC(ComponentABC):
    """Merges duplicate candidates without dropping sources or hits."""

    @abstractmethod
    async def deduplicate(self, candidates: Sequence[Candidate]) -> DedupResult:
        """Deduplicate ``candidates`` (order preserved by representative)."""


class ConsolidationContext(FrozenModel):
    """Parameters of one consolidation run."""

    top_k: PositiveInt
    contradiction_overflow: int = Field(ge=0)
    group_structural: bool = True
    merge_adjacent: bool = True
    span_iou_threshold: float = Field(default=0.5, gt=0.0, le=1.0)
    semantic_relations: bool = False
    semantic_threshold: float = Field(default=0.9, gt=0.0, le=1.0)
    embeddings: dict[str, tuple[float, ...]] | None = None


class EvidenceConsolidatorABC(ComponentABC):
    """Groups final-ordered candidates into evidence. Always executed by the engine."""

    @abstractmethod
    async def consolidate(
        self, candidates: Sequence[Candidate], context: ConsolidationContext
    ) -> tuple[Sequence[Evidence], Sequence[str]]:
        """Return selected evidence and ids of contradicting evidence left out by overflow."""
