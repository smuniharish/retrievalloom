"""Custom evidence components: percentile normaliser, weighted CombSUM, near-duplicate
dedup and a document-diversity consolidator."""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

from retrievalloom.contracts.component import ComponentInfo, make_info
from retrievalloom.contracts.evidence import (
    DeduplicatorABC,
    DedupResult,
    EvidenceConsolidatorABC,
    FusedEntry,
    FusionABC,
    ScoreNormalizerABC,
)
from retrievalloom.domain.retrieval import NLILabel
from retrievalloom.errors import ConfigurationError, TenantIsolationError
from retrievalloom.evidence.consolidation import HierarchicalConsolidator
from retrievalloom.evidence.dedup import IdentityAndHashDeduplicator, merge_candidates, unique
from retrievalloom.evidence.fusion import _clean, order_fused, resolve_weights
from retrievalloom.evidence.normalizers import oriented_scores
from retrievalloom.index.text import fold

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from retrievalloom.contracts.component import ComponentContext
    from retrievalloom.contracts.evidence import ConsolidationContext, RankedEntry
    from retrievalloom.domain.retrieval import Candidate, Evidence

_WORD = re.compile(r"\w+", re.UNICODE)


def percentiles(entries: Sequence[RankedEntry]) -> dict[str, float]:
    """Mid-rank percentile of each oriented score in [0, 1]; a single entry maps to 1.0."""
    scores = oriented_scores(entries)
    if len(scores) <= 1:
        return dict.fromkeys(scores, 1.0)
    values = sorted(scores.values())
    span = len(values) - 1
    result: dict[str, float] = {}
    for key, value in scores.items():
        below = sum(1 for other in values if other < value)
        equal = sum(1 for other in values if other == value)
        result[key] = (below + (equal - 1) / 2) / span
    return result


class PercentileNormalizer(ScoreNormalizerABC):
    """Maps each score to its percentile within its list.

    Robust to outliers and to score scales that differ wildly between methods (BM25 vs
    cosine vs cross-encoder logits) - only the order within a list matters, but unlike
    pure rank normalisation, ties stay ties.
    """

    _INFO = make_info(component="score_normalizer", implementation="percentile", version="1")

    @property
    def info(self) -> ComponentInfo:
        """Component identity."""
        return self._INFO

    async def normalize(self, entries: Sequence[RankedEntry]) -> Mapping[str, float]:
        """Percentiles keyed by item id."""
        return percentiles(entries)


class WeightedCombSUMFusion(FusionABC):
    """``sum_i w_i * n_i(d)`` over normalised scores; weights renormalised to sum 1.

    ``FusionConfig.normalizer`` selects only the built-in normalisers by name; a custom
    ``ScoreNormalizerABC`` plugs in here instead.

    Args:
        normalizer: Per-list score normaliser (default ``PercentileNormalizer``).
    """

    def __init__(self, normalizer: ScoreNormalizerABC | None = None) -> None:
        self._normalizer = normalizer or PercentileNormalizer()
        self._info = make_info(
            component="fusion",
            implementation="weighted-combsum",
            version="1",
            config={"normalizer": self._normalizer.info.fingerprint.digest},
        )

    @property
    def info(self) -> ComponentInfo:
        """Component identity (includes the normaliser's fingerprint)."""
        return self._info

    @property
    def needs_scores(self) -> bool:
        """Raw scores are required by the normaliser."""
        return True

    async def initialize(self, context: ComponentContext) -> None:
        """Initialise the normaliser."""
        await self._normalizer.initialize(context)

    async def close(self) -> None:
        """Close the normaliser."""
        await self._normalizer.close()

    async def fuse(
        self, lists: Mapping[str, Sequence[RankedEntry]], weights: Mapping[str, float]
    ) -> Sequence[FusedEntry]:
        """Fuse by weighted sum of normalised scores (absent items contribute 0)."""
        cleaned = _clean(lists)
        if not cleaned:
            return []
        resolved = resolve_weights(list(cleaned), weights)
        totals: dict[str, float] = {}
        for source, entries in cleaned.items():
            for item_id, value in (await self._normalizer.normalize(entries)).items():
                totals[item_id] = totals.get(item_id, 0.0) + resolved[source] * value
        return order_fused(totals, cleaned)


def _shingles(text: str, size: int) -> frozenset[tuple[str, ...]]:
    words = _WORD.findall(fold(text))
    if len(words) < size:
        return frozenset({tuple(words)}) if words else frozenset()
    return frozenset(tuple(words[i : i + size]) for i in range(len(words) - size + 1))


class NearDuplicateDeduplicator(DeduplicatorABC):
    """Identity + content-hash dedup, then Jaccard similarity of word shingles.

    Catches the same paragraph pasted into two documents with small edits (a FAQ copied
    into a release note, a boilerplate disclaimer). Never merges across scopes; the first
    candidate of a group (best-first) is the representative and every source and hit of
    the group is kept.

    Args:
        threshold: Minimum Jaccard similarity to merge (0, 1].
        shingle_size: Words per shingle.
    """

    def __init__(self, *, threshold: float = 0.8, shingle_size: int = 3) -> None:
        if not 0 < threshold <= 1 or shingle_size < 1:
            raise ConfigurationError("threshold must be in (0, 1] and shingle_size >= 1")
        self._threshold = threshold
        self._size = shingle_size
        self._exact = IdentityAndHashDeduplicator()
        self._info = make_info(
            component="deduplicator",
            implementation="near-duplicate",
            version="1",
            config={"threshold": threshold, "shingle_size": shingle_size},
        )

    @property
    def info(self) -> ComponentInfo:
        """Component identity."""
        return self._info

    async def deduplicate(self, candidates: Sequence[Candidate]) -> DedupResult:
        """Merge exact duplicates, then near duplicates within each scope."""
        exact = await self._exact.deduplicate(candidates)
        groups: list[tuple[Candidate, frozenset[tuple[str, ...]], list[Candidate]]] = []
        for candidate in exact.kept:
            shingles = _shingles(candidate.text or "", self._size)
            for head, head_shingles, members in groups:
                if head.scope.key != candidate.scope.key or not shingles or not head_shingles:
                    continue
                overlap = len(shingles & head_shingles) / len(shingles | head_shingles)
                if overlap >= self._threshold:
                    members.append(candidate)
                    break
            else:
                groups.append((candidate, shingles, []))
        merged = {key: list(values) for key, values in exact.merged.items()}
        kept: list[Candidate] = []
        for head, _, members in groups:
            kept.append(merge_candidates(head, members))
            for member in members:
                merged.setdefault(head.candidate_id, []).extend(
                    [member.candidate_id, *merged.pop(member.candidate_id, [])]
                )
        return DedupResult(
            kept=tuple(kept), merged={key: unique(values) for key, values in merged.items()}
        )


def _document(candidate: Candidate) -> str:
    return candidate.sources[0].document_uid


def _contradicts(candidate: Candidate) -> bool:
    return any(judgement.label == NLILabel.CONTRADICTION for judgement in candidate.nli)


class DocumentDiversityConsolidator(EvidenceConsolidatorABC):
    """Caps how many candidates one document contributes, then consolidates hierarchically.

    Long manuals otherwise fill every slot with neighbouring chunks of one document. Only
    non-contradicting candidates beyond ``max_per_document`` are dropped, so a
    contradiction is never hidden by the cap (E2); grouping, provenance and the
    contradiction overflow are delegated to ``HierarchicalConsolidator``.

    Args:
        max_per_document: Candidates kept per source document (best first).
        inner: Consolidator applied after the cap.
    """

    def __init__(
        self, *, max_per_document: int = 2, inner: EvidenceConsolidatorABC | None = None
    ) -> None:
        if max_per_document < 1:
            raise ConfigurationError("max_per_document must be at least 1")
        self._cap = max_per_document
        self._inner = inner or HierarchicalConsolidator()
        self._info = make_info(
            component="evidence_consolidator",
            implementation="document-diversity",
            version="1",
            config={"max_per_document": max_per_document, "inner": self._inner.info.name},
        )

    @property
    def info(self) -> ComponentInfo:
        """Component identity."""
        return self._info

    async def consolidate(
        self, candidates: Sequence[Candidate], context: ConsolidationContext
    ) -> tuple[Sequence[Evidence], Sequence[str]]:
        """Apply the per-document cap, then the inner consolidator."""
        if len({candidate.scope for candidate in candidates}) > 1:
            raise TenantIsolationError("candidates of different scopes cannot be consolidated")
        seen: dict[str, int] = {}
        kept: list[Candidate] = []
        for candidate in candidates:
            document = _document(candidate)
            if _contradicts(candidate) or seen.get(document, 0) < self._cap:
                seen[document] = seen.get(document, 0) + 1
                kept.append(candidate)
        return await self._inner.consolidate(kept, context)
