"""Sparse lexical BM25 index built on bm25s (in memory, immutable)."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import bm25s
import numpy as np
from bm25s.tokenization import Tokenized

from retrievalloom.contracts.component import ComponentInfo, make_info
from retrievalloom.contracts.indexes import SparseIndexABC
from retrievalloom.domain.filters import evaluate
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


@dataclass(slots=True, frozen=True)
class _SparseStructure:
    retriever: bm25s.BM25 | None
    vocabulary: dict[str, int]
    object_ids: list[str]


class BM25sIndex(SparseIndexABC, InMemoryGenerationalIndex):
    """BM25 over engine-tokenized objects (``IndexItem.tokens`` is required).

    Tombstoned and filtered-out objects are excluded with a ``weight_mask``; objects
    scoring zero (no query term) are never returned.

    Args:
        k1: BM25 ``k1``.
        b: BM25 ``b``.
        method: bm25s scoring variant (``lucene``, ``robertson``, ``atire``, ``bm25l``,
            ``bm25+``).
    """

    def __init__(self, *, k1: float = 1.5, b: float = 0.75, method: str = "lucene") -> None:
        super().__init__()
        if k1 <= 0 or not 0.0 <= b <= 1.0:
            raise ConfigurationError("invalid BM25 parameters")
        self._params = {"k1": k1, "b": b, "method": method}
        self._info = make_info(
            component="sparse_index", implementation="bm25s", version="1", config=self._params
        )

    @property
    def info(self) -> ComponentInfo:
        """Component identity."""
        return self._info

    def _build_sync(self, items: list[IndexItem]) -> _SparseStructure:
        vocabulary: dict[str, int] = {}
        corpus: list[list[int]] = []
        object_ids: list[str] = []
        for item in sorted(items, key=lambda value: value.object_id):
            if item.tokens is None:
                raise ComponentContractError(
                    "sparse items need tokens", details={"object_id": item.object_id}
                )
            corpus.append([vocabulary.setdefault(token, len(vocabulary)) for token in item.tokens])
            object_ids.append(item.object_id)
        if not vocabulary:
            # No object has a token: nothing can match, and bm25s would divide by a zero
            # average document length.
            return _SparseStructure(retriever=None, vocabulary={}, object_ids=object_ids)
        retriever = bm25s.BM25(
            k1=self._params["k1"], b=self._params["b"], method=self._params["method"]
        )
        retriever.index(Tokenized(ids=corpus, vocab=vocabulary), show_progress=False)
        return _SparseStructure(retriever=retriever, vocabulary=vocabulary, object_ids=object_ids)

    async def _build(self, scope: Scope, spec: GenerationSpec, items: list[IndexItem]) -> Any:
        del spec
        return await self.offloader.run(
            WorkloadKind.SPARSE_INDEX, self._build_sync, items, tenant=scope.tenant_id
        )

    @staticmethod
    def _query_sync(
        structure: _SparseStructure, token_ids: list[int], mask: np.ndarray, k: int
    ) -> list[tuple[int, float]]:
        assert structure.retriever is not None
        documents, scores = structure.retriever.retrieve(
            Tokenized(ids=[token_ids], vocab=structure.vocabulary),
            k=k,
            show_progress=False,
            weight_mask=mask,
            n_threads=0,
        )
        return [
            (int(index), float(score))
            for index, score in zip(documents[0], scores[0], strict=True)
            if score > 0
        ]

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
        if structure.retriever is None:
            return []
        token_ids = sorted(
            {structure.vocabulary[token] for token in query.tokens if token in structure.vocabulary}
        )
        if not token_ids:
            return []
        mask = np.fromiter(
            (
                0.0
                if object_id in generation.tombstones
                or not evaluate(filters, generation.metadata.get(object_id, {}))
                else 1.0
                for object_id in structure.object_ids
            ),
            dtype=np.float64,
            count=len(structure.object_ids),
        )
        if not mask.any():
            return []
        k = min(limit, len(structure.object_ids))
        ranked = await self.offloader.run(
            WorkloadKind.SPARSE_QUERY,
            self._query_sync,
            structure,
            token_ids,
            mask,
            k,
            tenant=scope.tenant_id,
        )
        ordered = sorted(
            ((structure.object_ids[index], score) for index, score in ranked),
            key=lambda pair: (-pair[1], pair[0]),
        )
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
