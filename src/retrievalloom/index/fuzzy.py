"""Fuzzy vocabulary index built on RapidFuzz (in memory, immutable)."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

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


@dataclass(slots=True, frozen=True)
class _FuzzyStructure:
    vocabulary: list[str]
    postings: dict[str, frozenset[str]]


class RapidFuzzIndex(FuzzyIndexABC, InMemoryGenerationalIndex):
    """Fuzzy matching of query n-grams against a vocabulary (never against full texts).

    The vocabulary holds the generation's dictionary surface forms (posted to objects
    that contain them, word-bounded) and, when ``corpus_vocabulary`` is set, corpus
    tokens of at least ``min_token_length`` characters. Query n-grams (1..``max_ngram``
    tokens) are matched with ``rapidfuzz.process.extract`` (``WRatio``); an object's score
    is the sum over matched vocabulary terms of the best similarity / 100.

    Args:
        score_cutoff: Minimum similarity (0-100).
        max_ngram: Longest query n-gram.
        corpus_vocabulary: Add corpus tokens to the vocabulary.
        min_token_length: Shortest token or n-gram considered.
    """

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

    def _build_sync(self, spec: GenerationSpec, items: list[IndexItem]) -> _FuzzyStructure:
        postings: dict[str, set[str]] = {}
        phrases = {
            surface: fold(surface)
            for entry in spec.keyword_entries
            for surface in entry.surface_forms
        }
        matcher = PhraseMatcher(phrases)
        for item in items:
            for _canonical, surface, _start, _end in matcher.find(item.text):
                postings.setdefault(surface, set()).add(item.object_id)
            if self._corpus:
                for token in item.tokens or ():
                    if len(token) >= self._min_length:
                        postings.setdefault(token, set()).add(item.object_id)
        vocabulary = sorted(postings)
        return _FuzzyStructure(
            vocabulary=vocabulary,
            postings={term: frozenset(ids) for term, ids in postings.items()},
        )

    async def _build(self, scope: Scope, spec: GenerationSpec, items: list[IndexItem]) -> Any:
        return await self.offloader.run(
            WorkloadKind.KEYWORD_BUILD, self._build_sync, spec, items, tenant=scope.tenant_id
        )

    def _ngrams(self, tokens: Sequence[str]) -> list[str]:
        grams: list[str] = []
        for size in range(1, self._max_ngram + 1):
            for start in range(len(tokens) - size + 1):
                gram = " ".join(tokens[start : start + size])
                if len(gram) >= self._min_length and gram not in grams:
                    grams.append(gram)
        return grams

    def _match_sync(self, structure: _FuzzyStructure, grams: list[str]) -> dict[str, tuple]:
        best: dict[str, tuple[float, str]] = {}
        for gram in grams:
            for term, similarity, _index in process.extract(
                gram,
                structure.vocabulary,
                scorer=_SCORER,
                score_cutoff=self._cutoff,
                limit=_MATCHES_PER_NGRAM,
            ):
                if term not in best or similarity > best[term][0]:
                    best[term] = (float(similarity), gram)
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
        if not grams or not structure.vocabulary:
            return []
        best = await self.offloader.run(
            WorkloadKind.FUZZY_MATCH, self._match_sync, structure, grams, tenant=scope.tenant_id
        )
        scores: dict[str, float] = {}
        details: dict[str, list[MatchDetail]] = {}
        for term, (similarity, gram) in sorted(best.items()):
            detail = MatchDetail(kind="fuzzy", term=gram, matched=term, score=similarity)
            for object_id in structure.postings[term]:
                if object_id in generation.tombstones or not evaluate(
                    filters, generation.metadata.get(object_id, {})
                ):
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
