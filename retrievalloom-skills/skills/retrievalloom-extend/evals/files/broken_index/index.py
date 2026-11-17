"""A keyword-overlap sparse index that fails SparseIndexContract: the input of eval 2."""

from __future__ import annotations

import time
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
from retrievalloom.domain.retrieval import MethodRef, ObjectRef, RetrievalHit

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from retrievalloom.contracts.component import ComponentInfo
    from retrievalloom.contracts.indexes import GenerationSpec, IndexItem, SparseQuery
    from retrievalloom.domain.base import Metadata
    from retrievalloom.domain.filters import FilterExpression
    from retrievalloom.domain.inputs import Scope


class OverlapSparseIndex(SparseIndexABC):
    """Scores objects by how many query tokens they contain."""

    def __init__(self) -> None:
        self._data: dict[str, dict[str, tuple[set[str], dict[str, object]]]] = {}
        self._sealed: set[str] = set()

    @property
    def info(self) -> ComponentInfo:
        return make_info(component="sparse_index", implementation="overlap", version="1")

    @property
    def capabilities(self) -> IndexCapabilities:
        return IndexCapabilities(mutable_in_place=False, durable=False)

    async def create_generation(self, scope: Scope, spec: GenerationSpec) -> None:
        self._data[spec.generation_id] = {}

    async def write(
        self, scope: Scope, generation_id: str, items: Sequence[IndexItem]
    ) -> WriteReport:
        rows = self._data[generation_id]
        for item in items:
            rows[item.object_id] = (set(item.tokens or ()), dict(item.metadata))
        return WriteReport(written=len(items))

    async def remove(self, scope: Scope, generation_id: str, object_ids: Sequence[str]) -> int:
        if generation_id in self._sealed:
            raise RuntimeError("generation is sealed")
        rows = self._data[generation_id]
        for object_id in object_ids:
            del rows[object_id]
        return len(object_ids)

    async def update_metadata(
        self, scope: Scope, generation_id: str, updates: Mapping[str, Metadata]
    ) -> int:
        rows = self._data[generation_id]
        for object_id, metadata in updates.items():
            rows[object_id] = (rows.get(object_id, (set(), {}))[0], dict(metadata))
        return len(updates)

    async def seal(self, scope: Scope, generation_id: str) -> GenerationStats:
        self._sealed.add(generation_id)
        return GenerationStats(object_count=len(self._data[generation_id]))

    async def drop_generation(self, scope: Scope, generation_id: str) -> None:
        del self._data[generation_id]

    async def list_generations(self, scope: Scope) -> Sequence[str]:
        return list(self._data)

    def _score(
        self, generation_id: str, tokens: Sequence[str]
    ) -> list[tuple[str, float, dict[str, object]]]:
        time.sleep(0.01)  # stands in for the blocking backend call
        scored = []
        for object_id, (terms, metadata) in self._data[generation_id].items():
            score = float(len(terms & set(tokens)))
            scored.append((object_id, score, metadata))
        scored.sort(key=lambda row: -row[1])
        return scored

    async def search(
        self,
        scope: Scope,
        generation_id: str,
        query: SparseQuery,
        limit: int,
        filters: FilterExpression | None,
    ) -> Sequence[RetrievalHit]:
        scored = self._score(generation_id, query.tokens)
        hits = []
        for rank, (object_id, score, metadata) in enumerate(scored[:limit]):
            if not evaluate(filters, metadata):
                continue
            hits.append(
                RetrievalHit(
                    object_ref=ObjectRef(
                        scope_key=scope.key, object_id=object_id, generation_id=generation_id
                    ),
                    method=MethodRef(channel=Channel.INDEX, method="sparse"),
                    backend_id="overlap",
                    raw_score=score,
                    rank=rank,
                )
            )
        return hits
