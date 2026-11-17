"""Contract kits against the example model adapters (FlagEmbedding, real local models).

Needs ``RL_TEST_MODELS_DIR`` (``bge-m3``, ``bge-reranker-v2-m3``, ``UltraBERT-150M-nli``);
the learned-sparse index also needs ``RL_TEST_QDRANT_URL``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pytest

from examples.adapters import (
    BgeCrossEncoderReranker,
    BgeM3Embedding,
    BgeM3SparseIndex,
    UltraBertNLI,
)
from retrievalloom.contracts.indexes import SparseQuery
from retrievalloom.testing import (
    EmbeddingModelContract,
    NLIContract,
    RerankerContract,
    SparseIndexContract,
)
from retrievalloom.testing.indexes import DOC_A, DOC_B, DOC_C, SCOPE
from tests.live.conftest import drop_collections
from tests.support import SharedResources

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from pathlib import Path

pytestmark = [
    pytest.mark.models,
    # FlagEmbedding/transformers emit deprecation warnings on import and first encode.
    pytest.mark.filterwarnings("ignore::DeprecationWarning"),
    pytest.mark.filterwarnings("ignore::FutureWarning"),
    pytest.mark.filterwarnings("ignore::UserWarning"),
]


class _ModelKit(SharedResources):
    @pytest.fixture(autouse=True)
    def _models(self, models_dir: Path) -> None:
        self.models = models_dir


class TestBgeM3FlagEmbedding(_ModelKit, EmbeddingModelContract):
    async def make_component(self, tmp_path: Path) -> BgeM3Embedding:
        return BgeM3Embedding(self.models / "bge-m3")


class TestBgeCrossEncoderReranker(_ModelKit, RerankerContract):
    async def make_component(self, tmp_path: Path) -> BgeCrossEncoderReranker:
        return BgeCrossEncoderReranker(self.models / "bge-reranker-v2-m3")


class TestUltraBertNLI(_ModelKit, NLIContract):
    async def make_component(self, tmp_path: Path) -> UltraBertNLI:
        return UltraBertNLI(self.models / "UltraBERT-150M-nli")


@pytest.mark.qdrant
class TestBgeM3SparseIndex(_ModelKit, SparseIndexContract):
    """Learned-sparse (bge-m3 lexical weights) in Qdrant sparse vectors."""

    @pytest.fixture(autouse=True)
    async def _collection(self, qdrant_url: str, prefix: str) -> AsyncIterator[None]:
        self.url, self.prefix = qdrant_url, prefix
        yield
        await drop_collections(qdrant_url, prefix)

    async def make_component(self, tmp_path: Path) -> BgeM3SparseIndex:
        return BgeM3SparseIndex(self.models / "bge-m3", url=self.url, collection_prefix=self.prefix)

    async def test_explains_matches_with_learned_terms(self, component: Any) -> None:
        """Hits name the shared vocabulary terms; the raw text is encoded, not the tokens."""
        await self.build(component, SCOPE, "gen_explain", [DOC_A, DOC_B, DOC_C])
        stemmed = SparseQuery(tokens=("pyth", "packag"), text="Python packaging?")
        hits = await component.search(SCOPE, "gen_explain", stemmed, 3, None)
        assert hits[0].object_ref.object_id == DOC_C
        assert any("packag" in detail.term for detail in hits[0].matched)
        assert all(detail.kind == "learned_term" for hit in hits for detail in hit.matched)
