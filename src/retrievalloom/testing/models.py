"""Contract kits for application-supplied models."""

from __future__ import annotations

import math
from typing import TYPE_CHECKING, Any, ClassVar

from retrievalloom.contracts.models import ExtractionSource, GraphExtraction, NLIPair, RerankItem
from retrievalloom.domain.hashing import stable_id
from retrievalloom.domain.inputs import Scope
from retrievalloom.domain.retrieval import NLIJudgement, NLILabel
from retrievalloom.testing._base import ComponentContract

if TYPE_CHECKING:
    from collections.abc import Sequence

TEXTS = (
    "Paris is the capital of France.",
    "Bananas are rich in potassium.",
    "The Eiffel Tower stands in Paris.",
)


def _check_vector(vector: Sequence[float], dimensions: int) -> None:
    assert len(vector) == dimensions
    assert all(isinstance(value, float | int) and math.isfinite(value) for value in vector)
    assert any(value != 0 for value in vector)


class EmbeddingModelContract(ComponentContract):
    """Kit for ``EmbeddingModelABC``.

    Attributes:
        tolerance: Absolute tolerance when comparing repeated or batched embeddings.
    """

    tolerance: ClassVar[float] = 1e-4

    def assert_close(self, a: Sequence[float], b: Sequence[float]) -> None:
        """Element-wise closeness within ``tolerance``."""
        assert len(a) == len(b)
        assert all(math.isclose(x, y, abs_tol=self.tolerance) for x, y in zip(a, b, strict=True))

    async def test_fingerprint_declares_model(self, component: Any) -> None:
        """The fingerprint carries the model identity and dimensionality."""
        assert component.dimensions >= 1
        assert component.max_batch_size >= 1
        fingerprint = component.info.fingerprint
        assert fingerprint.model_identity
        assert fingerprint.dimensions == component.dimensions

    async def test_embed_documents(self, component: Any) -> None:
        """One finite, non-zero vector per text, in order and independent of batching."""
        vectors = await component.embed_documents(list(TEXTS))
        assert len(vectors) == len(TEXTS)
        for vector in vectors:
            _check_vector(vector, component.dimensions)
        single = await component.embed_documents([TEXTS[1]])
        self.assert_close(single[0], vectors[1])

    async def test_embed_query_is_deterministic(self, component: Any) -> None:
        """Query embeddings have the model dimensionality and are repeatable."""
        first = await component.embed_query("capital of France")
        second = await component.embed_query("capital of France")
        _check_vector(first, component.dimensions)
        self.assert_close(first, second)


class RerankerContract(ComponentContract):
    """Kit for ``RerankerABC``.

    Attributes:
        check_relevance: Also require that the relevant passage outscores the others.
    """

    check_relevance: ClassVar[bool] = True

    async def test_scores_every_item_once(self, component: Any) -> None:
        """Every item gets exactly one finite score."""
        items = [RerankItem(item_id=f"i{index}", text=text) for index, text in enumerate(TEXTS)]
        scores = await component.rerank("What is the capital of France?", items)
        ids = [score.item_id for score in scores]
        assert sorted(ids) == sorted(item.item_id for item in items)
        assert all(math.isfinite(score.score) for score in scores)
        if self.check_relevance:
            by_id = {score.item_id: score.score for score in scores}
            assert by_id["i0"] > by_id["i1"]

    async def test_empty_input(self, component: Any) -> None:
        """No items, no scores."""
        assert list(await component.rerank("anything", [])) == []


class NLIContract(ComponentContract):
    """Kit for ``NLIABC``.

    Attributes:
        check_semantics: Also require the obvious entailment and contradiction labels.
    """

    check_semantics: ClassVar[bool] = True

    async def test_judges_each_pair_in_order(self, component: Any) -> None:
        """One valid judgement per pair, aligned to the pair's hypothesis."""
        pairs = [
            NLIPair(
                pair_id="p1",
                premise="A man is playing a guitar on stage.",
                hypothesis="A man is playing an instrument.",
                hypothesis_ref="h1",
            ),
            NLIPair(
                pair_id="p2",
                premise="A man is playing a guitar on stage.",
                hypothesis="Nobody is playing any instrument.",
                hypothesis_ref="h2",
            ),
        ]
        judgements = await component.judge(pairs)
        assert len(judgements) == len(pairs)
        for pair, judgement in zip(pairs, judgements, strict=True):
            assert isinstance(judgement, NLIJudgement)
            assert judgement.hypothesis_ref == pair.hypothesis_ref
            assert judgement.model_identity
            assert 0.0 <= judgement.confidence <= 1.0
        if self.check_semantics:
            assert judgements[0].label is NLILabel.ENTAILMENT
            assert judgements[1].label is NLILabel.CONTRADICTION

    async def test_empty_input(self, component: Any) -> None:
        """No pairs, no judgements."""
        assert list(await component.judge([])) == []


class GraphExtractorContract(ComponentContract):
    """Kit for ``GraphExtractorABC``: valid, deterministic, self-consistent output."""

    def source(self) -> ExtractionSource:
        """Extraction input used by the kit."""
        return ExtractionSource(
            scope=Scope(tenant_id="contract"),
            document_id="doc-1",
            document_uid=stable_id("doc", "contract"),
            version=1,
            representation_id="text",
            texts=("Guido van Rossum created Python. Python runs on CPython.",),
        )

    async def test_extracts_consistent_graph(self, component: Any) -> None:
        """Output is a ``GraphExtraction`` whose relationships reference extracted entities."""
        extraction = await component.extract(self.source())
        assert isinstance(extraction, GraphExtraction)
        keys = {entity.key or entity.name for entity in extraction.entities}
        for relationship in extraction.relationships:
            assert relationship.source_key in keys
            assert relationship.target_key in keys

    async def test_extraction_is_deterministic(self, component: Any) -> None:
        """The same source yields the same graph."""
        assert await component.extract(self.source()) == await component.extract(self.source())
