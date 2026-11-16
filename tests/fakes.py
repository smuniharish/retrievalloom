"""Deterministic test doubles for application-supplied models."""

from __future__ import annotations

import hashlib
import itertools
import math
import re
from typing import TYPE_CHECKING

from retrievalloom.contracts.component import ComponentInfo, make_info
from retrievalloom.contracts.models import (
    NLIABC,
    EmbeddingModelABC,
    GraphExtraction,
    GraphExtractorABC,
    RerankerABC,
    RerankScore,
)
from retrievalloom.domain.inputs import GraphEntityInput, GraphRelationshipInput
from retrievalloom.domain.retrieval import NLIJudgement, NLILabel

if TYPE_CHECKING:
    from collections.abc import Sequence

    from retrievalloom.contracts.models import ExtractionSource, NLIPair, RerankItem

_WORD = re.compile(r"[a-z0-9]+")
_NEGATIONS = frozenset({"no", "not", "nobody", "never", "none", "nothing"})


def words(text: str) -> list[str]:
    """Lower-cased alphanumeric words."""
    return _WORD.findall(text.casefold())


class HashEmbedding(EmbeddingModelABC):
    """Hashed bag-of-words embedding (unit length, one bias dimension)."""

    def __init__(self, dimensions: int = 32, batch_size: int = 8) -> None:
        self._dimensions = dimensions
        self._batch_size = batch_size
        self._info = make_info(
            component="embedding",
            implementation="hash",
            version="1",
            model_identity="test/hash-embedding",
            dimensions=dimensions,
        )
        self.calls = 0

    @property
    def info(self) -> ComponentInfo:
        """Component identity."""
        return self._info

    @property
    def dimensions(self) -> int:
        """Vector size."""
        return self._dimensions

    @property
    def max_batch_size(self) -> int:
        """Largest batch."""
        return self._batch_size

    def vector(self, text: str) -> list[float]:
        """Embedding of ``text``."""
        values = [0.0] * self._dimensions
        values[-1] = 0.1
        for word in words(text):
            bucket = int.from_bytes(hashlib.blake2b(word.encode(), digest_size=4).digest())
            values[bucket % (self._dimensions - 1)] += 1.0
        norm = math.sqrt(sum(value * value for value in values))
        return [value / norm for value in values]

    async def embed_documents(self, texts: Sequence[str]) -> Sequence[Sequence[float]]:
        """Embed documents."""
        self.calls += 1
        return [self.vector(text) for text in texts]

    async def embed_query(self, text: str) -> Sequence[float]:
        """Embed a query."""
        self.calls += 1
        return self.vector(text)


class OverlapReranker(RerankerABC):
    """Scores items by query-word overlap."""

    _INFO = make_info(
        component="reranker", implementation="overlap", version="1", model_identity="test/overlap"
    )

    @property
    def info(self) -> ComponentInfo:
        """Component identity."""
        return self._INFO

    async def rerank(self, query: str, items: Sequence[RerankItem]) -> Sequence[RerankScore]:
        """Fraction of query words present in each item."""
        terms = set(words(query))
        return [
            RerankScore(
                item_id=item.item_id,
                score=len(terms & set(words(item.text))) / max(len(terms), 1),
            )
            for item in items
        ]


class RuleNLI(NLIABC):
    """Negated hypotheses contradict, overlapping ones entail, others are neutral."""

    _INFO = make_info(
        component="nli", implementation="rules", version="1", model_identity="test/rules"
    )

    @property
    def info(self) -> ComponentInfo:
        """Component identity."""
        return self._INFO

    async def judge(self, pairs: Sequence[NLIPair]) -> Sequence[NLIJudgement]:
        """Judge each pair."""
        judgements = []
        for pair in pairs:
            hypothesis = set(words(pair.hypothesis))
            if hypothesis & _NEGATIONS:
                label = NLILabel.CONTRADICTION
            elif hypothesis & set(words(pair.premise)):
                label = NLILabel.ENTAILMENT
            else:
                label = NLILabel.NEUTRAL
            judgements.append(
                NLIJudgement(
                    label=label,
                    confidence=0.9,
                    model_identity="test/rules",
                    hypothesis_ref=pair.hypothesis_ref,
                )
            )
        return judgements


class CapitalizedExtractor(GraphExtractorABC):
    """Capitalised words become entities; consecutive ones are linked by ``mentions``."""

    _INFO = make_info(component="graph_extractor", implementation="capitalized", version="1")

    @property
    def info(self) -> ComponentInfo:
        """Component identity."""
        return self._INFO

    async def extract(self, source: ExtractionSource) -> GraphExtraction:
        """Extract entities and relationships."""
        names: list[str] = []
        for text in source.texts:
            for name in re.findall(r"\b[A-Z][A-Za-z]+\b", text):
                if name not in names:
                    names.append(name)
        return GraphExtraction(
            entities=tuple(GraphEntityInput(key=name, name=name) for name in names),
            relationships=tuple(
                GraphRelationshipInput(source_key=a, target_key=b, relation_type="mentions")
                for a, b in itertools.pairwise(names)
            ),
        )
