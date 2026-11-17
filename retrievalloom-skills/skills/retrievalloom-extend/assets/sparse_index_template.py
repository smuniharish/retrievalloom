"""A sparse (BM25-style) index backend to adapt to your search service.

Copy this file, keep the method signatures, and replace each ``self._generations``
operation with the call to your backend that its comment names. The storage
here is one dictionary per ``(scope key, generation id)``, which is what every
backend must emulate: generations of different scopes never share data, even
with equal generation ids.

Prove it with ``test_sparse_index_contract.py`` (``SparseIndexContract``).
"""

from __future__ import annotations

import math
from collections import Counter
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from retrievalloom.contracts.component import make_info
from retrievalloom.contracts.indexes import (
    GenerationStats,
    IndexCapabilities,
    SparseIndexABC,
    WriteReport,
)
from retrievalloom.domain.filters import evaluate
from retrievalloom.domain.records import Channel
from retrievalloom.domain.retrieval import (
    MatchDetail,
    MethodRef,
    ObjectRef,
    RetrievalHit,
    ScoreRange,
    ScoreSemantics,
)
from retrievalloom.errors import GenerationError, LifecycleError
from retrievalloom.runtime.offloader import WorkloadKind

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from retrievalloom.contracts.component import ComponentContext, ComponentInfo
    from retrievalloom.contracts.indexes import GenerationSpec, IndexItem, SparseQuery
    from retrievalloom.domain.base import Metadata
    from retrievalloom.domain.filters import FilterExpression
    from retrievalloom.domain.inputs import Scope
    from retrievalloom.runtime.offloader import Offloader

METHOD = MethodRef(channel=Channel.INDEX, method="sparse")
SEMANTICS = ScoreSemantics(higher_is_better=True, range=ScoreRange.UNBOUNDED)
K1, B = 1.2, 0.75

# (object id, terms, filter metadata) of one stored object
Row = tuple[str, tuple[str, ...], dict[str, object]]


@dataclass(slots=True)
class Generation:
    """One generation: a table or collection named after the scope key and generation id."""

    spec: GenerationSpec
    rows: dict[str, Row] = field(default_factory=dict)
    sealed: bool = False


class TemplateSparseIndex(SparseIndexABC):
    """Scores ``SparseQuery.tokens`` with BM25 and returns positive matches only."""

    def __init__(self, *, k1: float = K1, b: float = B) -> None:
        self._k1, self._b = k1, b
        self._generations: dict[tuple[str, str], Generation] = {}
        self._offloader: Offloader | None = None
        # Every setting that changes the index content belongs in the fingerprint.
        self._info = make_info(
            component="sparse_index",
            implementation="template-sparse",
            version="1",
            config={"k1": k1, "b": b},
        )

    @property
    def info(self) -> ComponentInfo:
        return self._info

    @property
    def capabilities(self) -> IndexCapabilities:
        # mutable_in_place=False would make the engine build a new generation per change,
        # and write() must then raise GenerationError after seal().
        return IndexCapabilities(
            mutable_in_place=True, durable=False, filter_pushdown=True, max_batch=256
        )

    async def initialize(self, context: ComponentContext) -> None:
        # Open the client here. Idempotent: a second call must not open a second one.
        self._offloader = context.offloader

    async def close(self) -> None:
        # Close only what initialize() opened; never a client the application passed in.
        self._generations.clear()
        self._offloader = None

    def _generation(self, scope: Scope, generation_id: str) -> Generation:
        found = self._generations.get((scope.key, generation_id))
        if found is None:
            raise GenerationError("generation not found", details={"generation_id": generation_id})
        return found

    async def create_generation(self, scope: Scope, spec: GenerationSpec) -> None:
        # CREATE TABLE/collection IF NOT EXISTS: idempotent.
        self._generations.setdefault((scope.key, spec.generation_id), Generation(spec))

    async def write(
        self, scope: Scope, generation_id: str, items: Sequence[IndexItem]
    ) -> WriteReport:
        # UPSERT by object_id: rewriting an object replaces it.
        generation = self._generation(scope, generation_id)
        for item in items:
            terms = item.tokens if item.tokens is not None else tuple(item.text.casefold().split())
            generation.rows[item.object_id] = (item.object_id, terms, dict(item.metadata))
        return WriteReport(written=len(items))

    async def remove(self, scope: Scope, generation_id: str, object_ids: Sequence[str]) -> int:
        # DELETE ... WHERE object_id IN (...). Always accepted, also after seal and for an
        # unknown generation; removed objects must be unsearchable when this returns.
        generation = self._generations.get((scope.key, generation_id))
        if generation is None:
            return 0
        present = [oid for oid in dict.fromkeys(object_ids) if oid in generation.rows]
        for object_id in present:
            del generation.rows[object_id]
        return len(present)

    async def update_metadata(
        self, scope: Scope, generation_id: str, updates: Mapping[str, Metadata]
    ) -> int:
        # UPDATE the filter payload of present objects; ignore absent ones.
        generation = self._generation(scope, generation_id)
        updated = 0
        for object_id, metadata in updates.items():
            if object_id in generation.rows:
                _, terms, _ = generation.rows[object_id]
                generation.rows[object_id] = (object_id, terms, dict(metadata))
                updated += 1
        return updated

    async def seal(self, scope: Scope, generation_id: str) -> GenerationStats:
        # Flush/commit/refresh so searches see every write. Idempotent.
        generation = self._generation(scope, generation_id)
        generation.sealed = True
        return GenerationStats(object_count=len(generation.rows))

    async def drop_generation(self, scope: Scope, generation_id: str) -> None:
        # DROP TABLE/collection IF EXISTS: idempotent.
        self._generations.pop((scope.key, generation_id), None)

    async def list_generations(self, scope: Scope) -> Sequence[str]:
        # Only this scope's generations.
        return sorted(gid for key, gid in self._generations if key == scope.key)

    async def search(
        self,
        scope: Scope,
        generation_id: str,
        query: SparseQuery,
        limit: int,
        filters: FilterExpression | None,
    ) -> Sequence[RetrievalHit]:
        if self._offloader is None:
            raise LifecycleError(f"{self.info.name} is not initialised")
        generation = self._generation(scope, generation_id)
        # Snapshot on the event loop; score in a worker so the loop never blocks.
        scored = await self._offloader.run(
            WorkloadKind.SPARSE_QUERY,
            self._score,
            list(generation.rows.values()),
            query.tokens,
            limit,
            filters,
            tenant=scope.tenant_id,
        )
        return [
            RetrievalHit(
                object_ref=ObjectRef(
                    scope_key=scope.key, object_id=object_id, generation_id=generation_id
                ),
                method=METHOD,
                backend_id=self._info.implementation,
                raw_score=score,
                score_semantics=SEMANTICS,
                rank=rank,
                matched=tuple(MatchDetail(kind="term", term=term) for term in terms),
            )
            for rank, (object_id, score, terms) in enumerate(scored, start=1)
        ]

    def _score(
        self,
        rows: list[Row],
        tokens: tuple[str, ...],
        limit: int,
        filters: FilterExpression | None,
    ) -> list[tuple[str, float, tuple[str, ...]]]:
        """BM25 over a snapshot of the rows (worker thread; reads only its arguments)."""
        if not rows or not tokens:
            return []
        total = len(rows)
        average = sum(len(terms) for _, terms, _ in rows) / total or 1.0
        frequency = Counter(term for _, terms, _ in rows for term in set(terms))
        query = list(dict.fromkeys(tokens))
        scored = []
        for object_id, terms, metadata in rows:
            if not evaluate(filters, metadata):
                continue  # filter pushdown: drop objects whose metadata does not match
            counts = Counter(terms)
            matched = tuple(term for term in query if counts[term])
            if not matched:
                continue  # never return an object that matches no query term
            score = 0.0
            for term in matched:
                idf = math.log(1 + (total - frequency[term] + 0.5) / (frequency[term] + 0.5))
                norm = self._k1 * (1 - self._b + self._b * len(terms) / average)
                score += idf * counts[term] * (self._k1 + 1) / (counts[term] + norm)
            scored.append((object_id, score, matched))
        scored.sort(key=lambda row: (-row[1], row[0]))
        return scored[:limit]
