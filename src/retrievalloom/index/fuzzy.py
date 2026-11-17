"""Fuzzy vocabulary index built on RapidFuzz (in memory, updated in place)."""

from __future__ import annotations

import heapq
import sys
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

import numpy as np
from cachetools import LRUCache
from rapidfuzz import fuzz, process

from retrievalloom.contracts.component import ComponentInfo, make_info
from retrievalloom.contracts.indexes import FuzzyIndexABC
from retrievalloom.domain.records import Channel
from retrievalloom.domain.retrieval import (
    MatchDetail,
    MethodRef,
    ObjectRef,
    RetrievalHit,
    ScoreRange,
    ScoreSemantics,
)
from retrievalloom.errors import ConfigurationError
from retrievalloom.index.memory import InMemoryGenerationalIndex
from retrievalloom.index.text import PhraseMatcher, fold
from retrievalloom.runtime.offloader import WorkloadKind

if TYPE_CHECKING:
    from collections.abc import Sequence

    from retrievalloom.contracts.indexes import FuzzyQuery, GenerationSpec, IndexItem
    from retrievalloom.domain.filters import FilterExpression
    from retrievalloom.domain.inputs import Scope

_METHOD = MethodRef(channel=Channel.INDEX, method="fuzzy")
_SEMANTICS = ScoreSemantics(higher_is_better=True, range=ScoreRange.UNBOUNDED)
_MATCHES_PER_NGRAM = 5
# Query n-grams whose matches each generation memoises.
_MATCH_CACHE_SIZE = 4096
# rapidfuzz's overloaded scorer stubs do not satisfy its own extract() overloads.
_SCORER: Any = fuzz.WRatio

type _GramMatches = tuple[tuple[str, float], ...]


@dataclass(slots=True)
class _FuzzyStructure:
    """Vocabulary postings of the live objects and a lazily refreshed sorted vocabulary."""

    matcher: PhraseMatcher
    postings: dict[str, set[str]] = field(default_factory=dict)
    forward: dict[str, tuple[str, ...]] = field(default_factory=dict)
    vocabulary: list[str] = field(default_factory=list)
    added: set[str] = field(default_factory=set)
    removed: set[str] = field(default_factory=set)
    # Top matches per query n-gram against ``matched_against`` (LRU).
    matched_against: list[str] | None = None
    matches: LRUCache[str, _GramMatches] = field(
        default_factory=lambda: LRUCache(maxsize=_MATCH_CACHE_SIZE)
    )

    def sorted_vocabulary(self) -> list[str]:
        """The sorted vocabulary; a new list after changes, so readers keep a stable one."""
        if self.added or self.removed:
            kept = [term for term in self.vocabulary if term not in self.removed]
            # Both runs are sorted, so Timsort merges them in linear time.
            self.vocabulary = sorted(kept + sorted(self.added))
            self.added = set()
            self.removed = set()
        return self.vocabulary

    def post(self, term: str, object_id: str) -> None:
        """Post ``object_id`` under ``term``."""
        bucket = self.postings.get(term)
        if bucket is not None:
            bucket.add(object_id)
            return
        self.postings[term] = {object_id}
        if term in self.removed:
            self.removed.discard(term)
        else:
            self.added.add(term)

    def unpost(self, term: str, object_id: str) -> None:
        """Remove ``object_id`` from ``term``'s posting."""
        bucket = self.postings[term]
        bucket.discard(object_id)
        if bucket:
            return
        del self.postings[term]
        if term in self.added:
            self.added.discard(term)
        else:
            self.removed.add(term)


class RapidFuzzIndex(FuzzyIndexABC, InMemoryGenerationalIndex):
    """Fuzzy matching of query n-grams against a vocabulary (never against full texts).

    The vocabulary holds the generation's dictionary surface forms (posted to objects
    that contain them, word-bounded) and, when ``corpus_vocabulary`` is set, corpus
    tokens of at least ``min_token_length`` characters. Writes and removals update the
    postings in place; the sorted vocabulary is refreshed on the next search. Query
    n-grams (1..``max_ngram`` tokens) are scored with ``rapidfuzz.process.cdist``
    (``WRatio``, GIL released) and the best five vocabulary terms per n-gram are kept;
    an object's score is the sum over matched vocabulary terms of the best
    similarity / 100.

    Args:
        score_cutoff: Minimum similarity (0-100).
        max_ngram: Longest query n-gram.
        corpus_vocabulary: Add corpus tokens to the vocabulary.
        min_token_length: Shortest token or n-gram considered.
    """

    workload = WorkloadKind.KEYWORD_BUILD

    def __init__(
        self,
        *,
        score_cutoff: float = 85.0,
        max_ngram: int = 3,
        corpus_vocabulary: bool = True,
        min_token_length: int = 3,
    ) -> None:
        super().__init__()
        if not 0.0 <= score_cutoff <= 100.0:
            raise ConfigurationError("score_cutoff must be within [0, 100]")
        if max_ngram < 1 or min_token_length < 1:
            raise ConfigurationError("max_ngram and min_token_length must be positive")
        self._cutoff = score_cutoff
        self._max_ngram = max_ngram
        self._corpus = corpus_vocabulary
        self._min_length = min_token_length
        self._info = make_info(
            component="fuzzy_index",
            implementation="rapidfuzz",
            version="1",
            config={
                "score_cutoff": score_cutoff,
                "max_ngram": max_ngram,
                "corpus_vocabulary": corpus_vocabulary,
                "min_token_length": min_token_length,
                "scorer": "WRatio",
            },
        )

    @property
    def info(self) -> ComponentInfo:
        """Component identity."""
        return self._info

    def _empty(self, spec: GenerationSpec) -> _FuzzyStructure:
        phrases = {
            surface: fold(surface)
            for entry in spec.keyword_entries
            for surface in entry.surface_forms
        }
        return _FuzzyStructure(matcher=PhraseMatcher(phrases))

    def _build(self, spec: GenerationSpec, items: list[IndexItem]) -> _FuzzyStructure:
        structure = super()._build(spec, items)
        structure.sorted_vocabulary()  # sort the full vocabulary here, not on the event loop
        return structure

    def _analyze(
        self, structure: _FuzzyStructure, items: list[IndexItem]
    ) -> list[tuple[str, tuple[str, ...]]]:
        analysis: list[tuple[str, tuple[str, ...]]] = []
        for item in items:
            terms = [surface for _canonical, surface, _s, _e in structure.matcher.find(item.text)]
            if self._corpus:
                terms.extend(token for token in item.tokens or () if len(token) >= self._min_length)
            analysis.append(
                (item.object_id, tuple(sys.intern(term) for term in dict.fromkeys(terms)))
            )
        return analysis

    def _insert(
        self, structure: _FuzzyStructure, analysis: list[tuple[str, tuple[str, ...]]]
    ) -> None:
        for object_id, terms in analysis:
            structure.forward[object_id] = terms
            for term in terms:
                structure.post(term, object_id)

    def _delete(self, structure: _FuzzyStructure, object_ids: list[str]) -> None:
        for object_id in object_ids:
            for term in structure.forward.pop(object_id):
                structure.unpost(term, object_id)

    def _ngrams(self, tokens: Sequence[str]) -> list[str]:
        grams: list[str] = []
        for size in range(1, self._max_ngram + 1):
            for start in range(len(tokens) - size + 1):
                gram = " ".join(tokens[start : start + size])
                if len(gram) >= self._min_length and gram not in grams:
                    grams.append(gram)
        return grams

    def _match_sync(self, vocabulary: list[str], grams: list[str]) -> list[_GramMatches]:
        # cdist releases the GIL while scoring (process.extract does not), so the
        # offloaded match cannot stall the application's event loop.
        matrix = process.cdist(
            grams,
            vocabulary,
            scorer=_SCORER,
            score_cutoff=self._cutoff,
            dtype=np.float64,
            workers=1,
        )
        matches: list[_GramMatches] = []
        for row in matrix:
            candidates = np.flatnonzero(row >= self._cutoff).tolist()
            candidates.sort(key=lambda index: (-row[index], index))
            matches.append(
                tuple(
                    (vocabulary[index], float(row[index]))
                    for index in candidates[:_MATCHES_PER_NGRAM]
                )
            )
        return matches

    async def _matches(
        self, scope: Scope, structure: _FuzzyStructure, grams: list[str]
    ) -> dict[str, tuple[float, str]]:
        """Best ``(similarity, gram)`` per matched term; per-gram results are memoised.

        The vocabulary list is replaced (never mutated) when terms change, so results
        memoised against the current list are exactly what matching would return.
        """
        vocabulary = structure.sorted_vocabulary()
        if structure.matched_against is not vocabulary:
            structure.matched_against = vocabulary
            structure.matches.clear()
        cache = structure.matches
        results: dict[str, _GramMatches] = {}
        for gram in grams:
            hit = cache.get(gram)
            if hit is not None:
                results[gram] = hit
        missing = [gram for gram in grams if gram not in results]
        if missing:
            found = await self.offloader.run(
                WorkloadKind.FUZZY_MATCH,
                self._match_sync,
                vocabulary,
                missing,
                tenant=scope.tenant_id,
            )
            results.update(zip(missing, found, strict=True))
            # The vocabulary may have changed while matching ran; matches against the old
            # list must not be memoised for the new one.
            if structure.matched_against is vocabulary:
                cache.update(zip(missing, found, strict=True))
        best: dict[str, tuple[float, str]] = {}
        for gram in grams:
            for term, similarity in results[gram]:
                if term not in best or similarity > best[term][0]:
                    best[term] = (similarity, gram)
        return best

    async def search(
        self,
        scope: Scope,
        generation_id: str,
        query: FuzzyQuery,
        limit: int,
        filters: FilterExpression | None,
    ) -> Sequence[RetrievalHit]:
        """Return objects posting fuzzily matched vocabulary terms."""
        generation = self.sealed_generation(scope, generation_id)
        structure: _FuzzyStructure = generation.built
        grams = self._ngrams([fold(token) for token in query.tokens])
        if not grams or not structure.sorted_vocabulary():
            return []
        best = await self._matches(scope, structure, grams)
        # A term removed while matching ran has no posting any more.
        matched = [
            (
                structure.postings.get(term, set()),
                similarity / 100.0,
                MatchDetail(kind="fuzzy", term=gram, matched=term, score=similarity),
            )
            for term, (similarity, gram) in sorted(best.items())
        ]
        # Common terms post to most of the corpus, so only scores are accumulated per
        # posting; filters are evaluated once per object and details built for the top hits.
        scores: dict[str, float] = {}
        for posting, weight, _detail in matched:
            for object_id in posting:
                scores[object_id] = scores.get(object_id, 0.0) + weight
        top = heapq.nsmallest(
            limit,
            generation.matching(scores, filters),
            key=lambda object_id: (-scores[object_id], object_id),
        )
        ordered = [(object_id, scores[object_id]) for object_id in top]
        details = {
            object_id: [detail for posting, _weight, detail in matched if object_id in posting]
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
