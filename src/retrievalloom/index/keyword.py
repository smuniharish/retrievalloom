"""Exact dictionary keyword index built on ahocorasick-rs (in memory, updated in place)."""

from __future__ import annotations

import heapq
import math
from collections import Counter
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from retrievalloom.contracts.component import ComponentInfo, make_info
from retrievalloom.contracts.indexes import KeywordIndexABC
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


@dataclass(slots=True)
class _KeywordStructure:
    """The generation's automaton plus canonical-keyword postings of the live objects."""

    matcher: PhraseMatcher
    postings: dict[str, dict[str, int]] = field(default_factory=dict)
    forward: dict[str, tuple[str, ...]] = field(default_factory=dict)


class AhoCorasickKeywordIndex(KeywordIndexABC, InMemoryGenerationalIndex):
    """Dictionary keyword index.

    The generation's dictionary (``GenerationSpec.keyword_entries``) is compiled once
    into an Aho-Corasick automaton; each written object's text is scanned once to post
    the canonical keywords it contains, and removed objects are unposted, so updates cost
    time proportional to the changed objects. Queries are scanned with the same
    automaton; objects are scored by the sum of ``idf`` of matched canonical keywords
    (``log(1 + N/df)`` over the live objects).
    """

    workload = WorkloadKind.KEYWORD_BUILD

    def __init__(self) -> None:
        super().__init__()
        self._info = make_info(component="keyword_index", implementation="ahocorasick", version="1")

    @property
    def info(self) -> ComponentInfo:
        """Component identity."""
        return self._info

    def _empty(self, spec: GenerationSpec) -> _KeywordStructure:
        phrases: dict[str, str] = {}
        for entry in spec.keyword_entries:
            for surface in entry.surface_forms:
                phrases.setdefault(surface, entry.canonical)
        return _KeywordStructure(matcher=PhraseMatcher(phrases))

    def _analyze(
        self, structure: _KeywordStructure, items: list[IndexItem]
    ) -> list[tuple[str, Counter[str]]]:
        return [
            (item.object_id, Counter(match[0] for match in structure.matcher.find(item.text)))
            for item in items
        ]

    def _insert(
        self, structure: _KeywordStructure, analysis: list[tuple[str, Counter[str]]]
    ) -> None:
        for object_id, counts in analysis:
            structure.forward[object_id] = tuple(counts)
            for canonical, count in counts.items():
                structure.postings.setdefault(canonical, {})[object_id] = count

    def _delete(self, structure: _KeywordStructure, object_ids: list[str]) -> None:
        for object_id in object_ids:
            for canonical in structure.forward.pop(object_id):
                posting = structure.postings[canonical]
                del posting[object_id]
                if not posting:
                    del structure.postings[canonical]

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
        document_count = len(structure.forward)
        terms: list[tuple[dict[str, int], float, MatchDetail]] = []
        for canonical, (surface, start, end) in matched.items():
            posting = structure.postings.get(canonical)
            if not posting:
                continue
            idf = math.log(1.0 + document_count / len(posting))
            detail = MatchDetail(
                kind="keyword",
                term=canonical,
                matched=surface,
                query_span=TextSpan(start=start, end=end) if spans_valid else None,
                score=idf,
            )
            terms.append((posting, idf, detail))
        # Scores are accumulated per posting; filters are evaluated once per object and
        # details built only for the top hits.
        scores: dict[str, float] = {}
        for posting, idf, _detail in terms:
            for object_id in posting:
                scores[object_id] = scores.get(object_id, 0.0) + idf
        top = heapq.nsmallest(
            limit,
            generation.matching(scores, filters),
            key=lambda object_id: (-scores[object_id], object_id),
        )
        ordered = [(object_id, scores[object_id]) for object_id in top]
        details = {
            object_id: [detail for posting, _idf, detail in terms if object_id in posting]
            for object_id in top
        }
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
