"""Sparse lexical BM25 index (in memory, updated in place) scored with bm25s formulas."""

from __future__ import annotations

import math
from array import array
from collections import Counter
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

import numpy as np

from retrievalloom.contracts.component import ComponentInfo, make_info
from retrievalloom.contracts.indexes import SparseIndexABC
from retrievalloom.domain.records import Channel
from retrievalloom.domain.retrieval import (
    MethodRef,
    ObjectRef,
    RetrievalHit,
    ScoreRange,
    ScoreSemantics,
)
from retrievalloom.errors import ComponentContractError, ConfigurationError
from retrievalloom.index.memory import InMemoryGenerationalIndex
from retrievalloom.runtime.offloader import WorkloadKind

if TYPE_CHECKING:
    from collections.abc import Sequence

    from retrievalloom.contracts.indexes import GenerationSpec, IndexItem, SparseQuery
    from retrievalloom.domain.filters import FilterExpression
    from retrievalloom.domain.inputs import Scope

_METHOD = MethodRef(channel=Channel.INDEX, method="sparse")
_SEMANTICS = ScoreSemantics(higher_is_better=True, range=ScoreRange.UNBOUNDED)
_NONOCCURRENCE_METHODS = frozenset({"bm25l", "bm25+"})
# bm25s.BM25's default ``delta`` of the BM25L and BM25+ variants.
_DELTA = 0.5
# Compaction runs once more slots are dead than live, and at least this many.
_COMPACT_MIN_DEAD = 1024


# bm25s's idf and term-frequency formulas (Kamphuis et al., ECIR 2020), restated
# because bm25s keeps them private. ``test_sparse_scores_match_bm25s`` pins them.
def _idf_robertson(df: int, n: int) -> float:
    return math.log(max((n - df + 0.5) / (df + 0.5), 1.0))


def _idf_lucene(df: int, n: int) -> float:
    return math.log(1 + (n - df + 0.5) / (df + 0.5))


def _idf_atire(df: int, n: int) -> float:
    return math.log(n / df)


def _idf_bm25l(df: int, n: int) -> float:
    return math.log((n + 1) / (df + 0.5))


def _idf_bm25plus(df: int, n: int) -> float:
    return math.log((n + 1) / df)


def _tfc_robertson(tf: Any, l_d: Any, l_avg: float, k1: float, b: float, delta: float) -> Any:
    del delta
    return tf / (k1 * ((1 - b) + b * l_d / l_avg) + tf)


def _tfc_atire(tf: Any, l_d: Any, l_avg: float, k1: float, b: float, delta: float) -> Any:
    del delta
    return (tf * (k1 + 1)) / (tf + k1 * (1 - b + b * l_d / l_avg))


def _tfc_bm25l(tf: Any, l_d: Any, l_avg: float, k1: float, b: float, delta: float) -> Any:
    c = tf / (1 - b + b * l_d / l_avg)
    return ((k1 + 1) * (c + delta)) / (k1 + c + delta)


def _tfc_bm25plus(tf: Any, l_d: Any, l_avg: float, k1: float, b: float, delta: float) -> Any:
    return ((k1 + 1) * tf) / (k1 * (1 - b + b * l_d / l_avg) + tf) + delta


_FORMULAS = {
    "robertson": (_idf_robertson, _tfc_robertson),
    "lucene": (_idf_lucene, _tfc_robertson),
    "atire": (_idf_atire, _tfc_atire),
    "bm25l": (_idf_bm25l, _tfc_bm25l),
    "bm25+": (_idf_bm25plus, _tfc_bm25plus),
}

type _Posting = tuple[array[int], array[int]]
type _Analysis = list[tuple[str, int, Counter[str]]]


@dataclass(slots=True)
class _SparseStructure:
    """Term postings over object slots; removed objects leave dead slots until compaction.

    ``ids[slot]`` is ``""`` once the slot's object is removed. Postings hold
    ``(slots, term frequencies)`` in increasing slot order.
    """

    slots: dict[str, int] = field(default_factory=dict)
    ids: list[str] = field(default_factory=list)
    lengths: np.ndarray = field(default_factory=lambda: np.zeros(0, dtype=np.int64))
    live: np.ndarray = field(default_factory=lambda: np.zeros(0, dtype=np.bool_))
    postings: dict[str, _Posting] = field(default_factory=dict)
    live_count: int = 0
    total_length: int = 0

    @property
    def dead(self) -> int:
        """Slots of removed objects."""
        return len(self.ids) - self.live_count

    def insert(self, analysis: _Analysis) -> None:
        """Append objects that are not in the structure."""
        size = len(self.ids)
        needed = size + len(analysis)
        if needed > len(self.lengths):
            capacity = max(needed, 2 * len(self.lengths), 64)
            lengths = np.zeros(capacity, dtype=np.int64)
            live = np.zeros(capacity, dtype=np.bool_)
            lengths[:size] = self.lengths[:size]
            live[:size] = self.live[:size]
            # Replaced, not resized: searches keep their own copies.
            self.lengths, self.live = lengths, live
        for object_id, length, counts in analysis:
            slot = len(self.ids)
            self.ids.append(object_id)
            self.slots[object_id] = slot
            self.lengths[slot] = length
            self.live[slot] = True
            self.live_count += 1
            self.total_length += length
            for term, frequency in counts.items():
                posting = self.postings.get(term)
                if posting is None:
                    posting = self.postings[term] = (array("i"), array("i"))
                posting[0].append(slot)
                posting[1].append(frequency)

    def delete(self, object_ids: list[str]) -> None:
        """Mark the slots of present objects dead."""
        for object_id in object_ids:
            slot = self.slots.pop(object_id)
            self.ids[slot] = ""
            self.live[slot] = False
            self.live_count -= 1
            self.total_length -= int(self.lengths[slot])

    def compacted(self) -> _SparseStructure:
        """A copy without dead slots (pure; runs in a worker thread)."""
        size = len(self.ids)
        keep = self.live[:size]
        renumber = np.cumsum(keep, dtype=np.int64) - 1
        postings: dict[str, _Posting] = {}
        for term, (slots, frequencies) in self.postings.items():
            old = np.frombuffer(slots, dtype=np.intc)
            kept = keep[old]
            if kept.any():
                new_slots, new_frequencies = array("i"), array("i")
                new_slots.frombytes(renumber[old[kept]].astype(np.intc).tobytes())
                new_frequencies.frombytes(np.frombuffer(frequencies, dtype=np.intc)[kept].tobytes())
                postings[term] = (new_slots, new_frequencies)
        ids = [object_id for object_id in self.ids if object_id]
        return _SparseStructure(
            slots={object_id: slot for slot, object_id in enumerate(ids)},
            ids=ids,
            lengths=self.lengths[:size][keep],
            live=np.ones(len(ids), dtype=np.bool_),
            postings=postings,
            live_count=self.live_count,
            total_length=self.total_length,
        )


class BM25sIndex(SparseIndexABC, InMemoryGenerationalIndex):
    """BM25 over engine-tokenized objects (``IndexItem.tokens`` is required).

    bm25s has no incremental API and bakes corpus statistics into its score matrix, so
    this index keeps term-frequency postings of the live objects and applies bm25s's
    idf and term-frequency formulas at query time. ``N``, document frequencies and the
    average length always cover the live objects, so scores equal those of a fresh
    bm25s index over them (up to float32 rounding there) however the generation was
    updated. Filters do not change these statistics. Objects scoring zero are never
    returned.

    Args:
        k1: BM25 ``k1``.
        b: BM25 ``b``.
        method: bm25s scoring variant (``lucene``, ``robertson``, ``atire``, ``bm25l``,
            ``bm25+``).
    """

    workload = WorkloadKind.SPARSE_INDEX

    def __init__(self, *, k1: float = 1.5, b: float = 0.75, method: str = "lucene") -> None:
        super().__init__()
        if k1 <= 0 or not 0.0 <= b <= 1.0:
            raise ConfigurationError("invalid BM25 parameters")
        formulas = _FORMULAS.get(method)
        if formulas is None:
            raise ConfigurationError(f"unknown BM25 method {method!r}")
        self._idf, self._tfc = formulas
        self._k1 = k1
        self._b = b
        self._nonoccurrence = method in _NONOCCURRENCE_METHODS
        self._params = {"k1": k1, "b": b, "method": method}
        self._info = make_info(
            component="sparse_index", implementation="bm25s", version="1", config=self._params
        )

    @property
    def info(self) -> ComponentInfo:
        """Component identity."""
        return self._info

    def _empty(self, spec: GenerationSpec) -> _SparseStructure:
        del spec
        return _SparseStructure()

    def _analyze(self, structure: _SparseStructure, items: list[IndexItem]) -> _Analysis:
        del structure
        analysis: _Analysis = []
        for item in items:
            if item.tokens is None:
                raise ComponentContractError(
                    "sparse items need tokens", details={"object_id": item.object_id}
                )
            analysis.append((item.object_id, len(item.tokens), Counter(item.tokens)))
        return analysis

    def _insert(self, structure: _SparseStructure, analysis: _Analysis) -> None:
        structure.insert(analysis)

    def _delete(self, structure: _SparseStructure, object_ids: list[str]) -> None:
        structure.delete(object_ids)

    async def _maintain(self, scope: Scope, structure: _SparseStructure) -> _SparseStructure:
        if structure.dead < max(_COMPACT_MIN_DEAD, structure.live_count + 1):
            return structure
        return await self.offloader.run(
            WorkloadKind.SPARSE_INDEX, structure.compacted, tenant=scope.tenant_id
        )

    def _score_sync(
        self,
        postings: list[tuple[np.ndarray, np.ndarray]],
        live: np.ndarray,
        lengths: np.ndarray,
        live_count: int,
        total_length: int,
    ) -> tuple[np.ndarray, np.ndarray]:
        """Scores of live objects with a positive score, best first (worker thread)."""
        base = 0.0
        contributions: list[tuple[np.ndarray, Any]] = []
        for all_slots, all_frequencies in postings:
            present = live[all_slots]
            slots = all_slots[present]
            if not len(slots):
                continue
            # A live posting implies a live object with a token, so both are positive.
            average = total_length / live_count
            idf = self._idf(len(slots), live_count)
            parameters = {"l_avg": average, "k1": self._k1, "b": self._b, "delta": _DELTA}
            frequencies = all_frequencies[present].astype(np.float64)
            part = idf * self._tfc(frequencies, lengths[slots], **parameters)
            if self._nonoccurrence:
                # BM25L/BM25+ also score objects lacking the term (as bm25s does).
                missing = idf * self._tfc(0, average, **parameters)
                base += missing
                part = part - missing
            contributions.append((slots, part))
        scores = np.full(len(live), base, dtype=np.float64)
        for slots, part in contributions:
            scores[slots] += part
        candidates = np.flatnonzero(live & (scores > 0))
        order = candidates[np.argsort(-scores[candidates], kind="stable")]
        return order, scores[order]

    async def search(
        self,
        scope: Scope,
        generation_id: str,
        query: SparseQuery,
        limit: int,
        filters: FilterExpression | None,
    ) -> Sequence[RetrievalHit]:
        """Return BM25-ranked objects."""
        generation = self.sealed_generation(scope, generation_id)
        structure: _SparseStructure = generation.built
        terms = sorted({token for token in query.tokens if token in structure.postings})
        if not terms:
            return []
        # Copies: the structure may change while scoring runs in a worker thread.
        postings = [
            (np.array(slots, dtype=np.intc), np.array(frequencies, dtype=np.intc))
            for slots, frequencies in (structure.postings[term] for term in terms)
        ]
        size = len(structure.ids)
        ids = structure.ids
        order, scores = await self.offloader.run(
            WorkloadKind.SPARSE_QUERY,
            self._score_sync,
            postings,
            structure.live[:size].copy(),
            structure.lengths[:size].copy(),
            structure.live_count,
            structure.total_length,
            tenant=scope.tenant_id,
        )
        kept: list[tuple[str, float]] = []
        for slot, score in zip(order.tolist(), scores.tolist(), strict=True):
            # Scores descend; collect past ``limit`` only while scores tie the last kept.
            if len(kept) >= limit and (not kept or score < kept[-1][1]):
                break
            # Objects removed meanwhile have no metadata (and an empty id).
            if not generation.admits(ids[slot], filters):
                continue
            kept.append((ids[slot], score))
        ordered = sorted(kept, key=lambda pair: (-pair[1], pair[0]))[:limit]
        return [
            RetrievalHit(
                object_ref=ObjectRef(
                    scope_key=scope.key, object_id=object_id, generation_id=generation_id
                ),
                method=_METHOD,
                backend_id=self._info.implementation,
                raw_score=score,
                score_semantics=_SEMANTICS,
                rank=rank,
            )
            for rank, (object_id, score) in enumerate(ordered, start=1)
        ]
