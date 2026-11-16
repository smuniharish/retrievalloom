"""Fuzzy vocabulary index built on RapidFuzz (in memory, updated in place)."""

from __future__ import annotations

import sys
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

import numpy as np
from rapidfuzz import fuzz, process

from retrievalloom.contracts.component import ComponentInfo, make_info
from retrievalloom.contracts.indexes import FuzzyIndexABC
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
# rapidfuzz's overloaded scorer stubs do not satisfy its own extract() overloads.
_SCORER: Any = fuzz.WRatio


@dataclass(slots=True)
class _FuzzyStructure:
    """Vocabulary postings of the live objects and a lazily refreshed sorted vocabulary."""

    matcher: PhraseMatcher
    postings: dict[str, set[str]] = field(default_factory=dict)
    forward: dict[str, tuple[str, ...]] = field(default_factory=dict)
    vocabulary: list[str] = field(default_factory=list)
    added: set[str] = field(default_factory=set)
    removed: set[str] = field(default_factory=set)

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

    def _match_sync(self, vocabulary: list[str], grams: list[str]) -> dict[str, tuple]:
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
        best: dict[str, tuple[float, str]] = {}
        for gram, row in zip(grams, matrix, strict=True):
            candidates = np.flatnonzero(row >= self._cutoff).tolist()
            candidates.sort(key=lambda index: (-row[index], index))
            for index in candidates[:_MATCHES_PER_NGRAM]:
                term, similarity = vocabulary[index], float(row[index])
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
        vocabulary = structure.sorted_vocabulary()
        if not grams or not vocabulary:
            return []
        best = await self.offloader.run(
            WorkloadKind.FUZZY_MATCH, self._match_sync, vocabulary, grams, tenant=scope.tenant_id
        )
        scores: dict[str, float] = {}
        details: dict[str, list[MatchDetail]] = {}
        for term, (similarity, gram) in sorted(best.items()):
            detail = MatchDetail(kind="fuzzy", term=gram, matched=term, score=similarity)
            # A term removed while matching ran has no posting any more.
            for object_id in structure.postings.get(term, ()):
                if not evaluate(filters, generation.metadata.get(object_id, {})):
                    continue
                scores[object_id] = scores.get(object_id, 0.0) + similarity / 100.0
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
