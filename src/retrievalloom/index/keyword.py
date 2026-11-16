"""Exact dictionary keyword index built on ahocorasick-rs (in memory, immutable)."""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from retrievalloom.contracts.component import ComponentInfo, make_info
from retrievalloom.contracts.indexes import KeywordIndexABC
from retrievalloom.domain.filters import evaluate
from retrievalloom.domain.inputs import TextSpan
from retrievalloom.domain.records import Channel
from retrievalloom.domain.retrieval import (
    MatchDetail,
    MethodRef,
    ObjectRef,
    RetrievalHit,
    ScoreRange,
    ScoreSemantics,
)
from retrievalloom.index.memory import InMemoryGenerationalIndex
from retrievalloom.index.text import PhraseMatcher, fold
from retrievalloom.runtime.offloader import WorkloadKind

if TYPE_CHECKING:
    from collections.abc import Sequence

    from retrievalloom.contracts.indexes import GenerationSpec, IndexItem, KeywordQuery
    from retrievalloom.domain.filters import FilterExpression
    from retrievalloom.domain.inputs import Scope

_METHOD = MethodRef(channel=Channel.INDEX, method="keyword")
_SEMANTICS = ScoreSemantics(higher_is_better=True, range=ScoreRange.UNBOUNDED)


@dataclass(slots=True, frozen=True)
class _KeywordStructure:
    matcher: PhraseMatcher
    postings: dict[str, dict[str, int]]
    document_count: int


def build_keyword_structure(spec: GenerationSpec, items: list[IndexItem]) -> _KeywordStructure:
    """Build the automaton and canonical-term postings (runs in a worker thread)."""
    phrases: dict[str, str] = {}
    for entry in spec.keyword_entries:
        for surface in entry.surface_forms:
            phrases.setdefault(surface, entry.canonical)
    matcher = PhraseMatcher(phrases)
    postings: dict[str, dict[str, int]] = {}
    for item in items:
        for canonical, _surface, _start, _end in matcher.find(item.text):
            bucket = postings.setdefault(canonical, {})
            bucket[item.object_id] = bucket.get(item.object_id, 0) + 1
    return _KeywordStructure(matcher=matcher, postings=postings, document_count=len(items))


class AhoCorasickKeywordIndex(KeywordIndexABC, InMemoryGenerationalIndex):
    """Dictionary keyword index.

    The generation's dictionary (``GenerationSpec.keyword_entries``) is compiled into an
    Aho-Corasick automaton; object texts are scanned once at ``seal`` to build postings of
    canonical keywords. Queries are scanned with the same automaton; objects are scored by
    the sum of ``idf`` of matched canonical keywords (``log(1 + N/df)``).
    """

    def __init__(self) -> None:
        super().__init__()
        self._info = make_info(component="keyword_index", implementation="ahocorasick", version="1")

    @property
    def info(self) -> ComponentInfo:
        """Component identity."""
        return self._info

    async def _build(self, scope: Scope, spec: GenerationSpec, items: list[IndexItem]) -> Any:
        return await self.offloader.run(
            WorkloadKind.KEYWORD_BUILD, build_keyword_structure, spec, items, tenant=scope.tenant_id
        )

    async def search(
        self,
        scope: Scope,
        generation_id: str,
        query: KeywordQuery,
        limit: int,
        filters: FilterExpression | None,
    ) -> Sequence[RetrievalHit]:
        """Return objects containing dictionary keywords found in the query."""
        generation = self.sealed_generation(scope, generation_id)
        structure: _KeywordStructure = generation.built
        spans_valid = len(fold(query.text)) == len(query.text)
        matched: dict[str, tuple[str, int, int]] = {}
        for canonical, surface, start, end in structure.matcher.find(query.text):
            matched.setdefault(canonical, (surface, start, end))
        scores: dict[str, float] = {}
        details: dict[str, list[MatchDetail]] = {}
        for canonical, (surface, start, end) in matched.items():
            posting = structure.postings.get(canonical, {})
            live = [
                object_id
                for object_id in posting
                if object_id not in generation.tombstones
                and evaluate(filters, generation.metadata.get(object_id, {}))
            ]
            if not live:
                continue
            idf = math.log(1.0 + structure.document_count / len(posting))
            detail = MatchDetail(
                kind="keyword",
                term=canonical,
                matched=surface,
                query_span=TextSpan(start=start, end=end) if spans_valid else None,
                score=idf,
            )
            for object_id in live:
                scores[object_id] = scores.get(object_id, 0.0) + idf
                details.setdefault(object_id, []).append(detail)
        ordered = sorted(scores.items(), key=lambda pair: (-pair[1], pair[0]))[:limit]
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
                matched=tuple(details[object_id]),
            )
            for rank, (object_id, score) in enumerate(ordered, start=1)
        ]
