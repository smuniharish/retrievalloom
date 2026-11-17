"""Built-in adapters run against the public contract kits."""

from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar

import pytest

from retrievalloom.contracts.indexes import IndexCapabilities, WriteReport
from retrievalloom.errors import GenerationError
from retrievalloom.evidence.consolidation import HierarchicalConsolidator
from retrievalloom.evidence.dedup import IdentityAndHashDeduplicator
from retrievalloom.evidence.fusion import (
    CombMNZFusion,
    CombSUMFusion,
    RRFFusion,
    WeightedRRFFusion,
    WeightedScoreFusion,
)
from retrievalloom.evidence.normalizers import (
    MinMaxNormalizer,
    RankNormalizer,
    SigmoidNormalizer,
    ZScoreNormalizer,
)
from retrievalloom.index.fuzzy import RapidFuzzIndex
from retrievalloom.index.keyword import AhoCorasickKeywordIndex
from retrievalloom.index.sparse import BM25sIndex
from retrievalloom.index.vector import QdrantVectorStore
from retrievalloom.infra.cache import InMemoryLRUCache, NoCache
from retrievalloom.storage.base import MetadataTransactionABC
from retrievalloom.storage.sqlite import SQLiteMetadataStore, SQLiteTransaction
from retrievalloom.testing import (
    CacheContract,
    DeduplicatorContract,
    EmbeddingModelContract,
    EvidenceConsolidatorContract,
    FusionContract,
    FuzzyIndexContract,
    GraphExtractorContract,
    GraphStoreContract,
    KeywordIndexContract,
    MetadataStoreContract,
    NLIContract,
    RerankerContract,
    ScoreNormalizerContract,
    SparseIndexContract,
    StructureEngineContract,
    VectorStoreContract,
)
from retrievalloom.testing.evidence import rrf_reference, score_reference
from tests.fakes import CapitalizedExtractor, HashEmbedding, OverlapReranker, RuleNLI
from tests.support import SharedResources

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence
    from pathlib import Path

    from retrievalloom.contracts.evidence import RankedEntry
    from retrievalloom.contracts.indexes import IndexItem
    from retrievalloom.domain.inputs import Scope


# indexes ---------------------------------------------------------------------------------
class TestAhoCorasickKeywordIndex(SharedResources, KeywordIndexContract):
    async def make_component(self, tmp_path: Path) -> AhoCorasickKeywordIndex:
        return AhoCorasickKeywordIndex()


class TestRapidFuzzIndex(SharedResources, FuzzyIndexContract):
    async def make_component(self, tmp_path: Path) -> RapidFuzzIndex:
        return RapidFuzzIndex()


class SealOnceFuzzyIndex(RapidFuzzIndex):
    """Immutable variant: exercises the kit's path for backends that reject sealed writes."""

    @property
    def capabilities(self) -> IndexCapabilities:
        return IndexCapabilities(mutable_in_place=False, durable=False, filter_pushdown=True)

    async def write(
        self, scope: Scope, generation_id: str, items: Sequence[IndexItem]
    ) -> WriteReport:
        if self.generation(scope, generation_id).sealed:
            raise GenerationError("generation is sealed")
        return await super().write(scope, generation_id, items)


class TestSealOnceFuzzyIndex(SharedResources, FuzzyIndexContract):
    async def make_component(self, tmp_path: Path) -> SealOnceFuzzyIndex:
        return SealOnceFuzzyIndex()


class TestBM25sIndex(SharedResources, SparseIndexContract):
    async def make_component(self, tmp_path: Path) -> BM25sIndex:
        return BM25sIndex()


class TestQdrantMemory(SharedResources, VectorStoreContract):
    async def make_component(self, tmp_path: Path) -> QdrantVectorStore:
        return QdrantVectorStore()


class TestQdrantLocalPath(SharedResources, VectorStoreContract):
    async def make_component(self, tmp_path: Path) -> QdrantVectorStore:
        return QdrantVectorStore(path=str(tmp_path / "qdrant"))


@pytest.mark.structure
class TestTreeSearchMemory(SharedResources, StructureEngineContract):
    async def make_component(self, tmp_path: Path) -> object:
        from retrievalloom.structure.treesearch_engine import TreeSearchStructureEngine

        return TreeSearchStructureEngine()


@pytest.mark.structure
class TestTreeSearchDirectory(SharedResources, StructureEngineContract):
    async def make_component(self, tmp_path: Path) -> object:
        from retrievalloom.structure.treesearch_engine import TreeSearchStructureEngine

        return TreeSearchStructureEngine(tmp_path / "structure")


@pytest.mark.graph
class TestLadybugMemory(SharedResources, GraphStoreContract):
    async def make_component(self, tmp_path: Path) -> object:
        from retrievalloom.graph.ladybug_store import LadybugGraphStore

        return LadybugGraphStore()


@pytest.mark.graph
class TestLadybugFile(SharedResources, GraphStoreContract):
    async def make_component(self, tmp_path: Path) -> object:
        from retrievalloom.graph.ladybug_store import LadybugGraphStore

        return LadybugGraphStore(tmp_path / "graph" / "db.lbug")


# models (test doubles prove the kits; real models run in tests/live) --------------------
class TestHashEmbedding(SharedResources, EmbeddingModelContract):
    async def make_component(self, tmp_path: Path) -> HashEmbedding:
        return HashEmbedding()


class TestOverlapReranker(SharedResources, RerankerContract):
    async def make_component(self, tmp_path: Path) -> OverlapReranker:
        return OverlapReranker()


class TestRuleNLI(SharedResources, NLIContract):
    async def make_component(self, tmp_path: Path) -> RuleNLI:
        return RuleNLI()


class TestCapitalizedExtractor(SharedResources, GraphExtractorContract):
    async def make_component(self, tmp_path: Path) -> CapitalizedExtractor:
        return CapitalizedExtractor()


# evidence -------------------------------------------------------------------------------
class TestMinMaxNormalizer(SharedResources, ScoreNormalizerContract):
    async def make_component(self, tmp_path: Path) -> MinMaxNormalizer:
        return MinMaxNormalizer()


class TestZScoreNormalizer(SharedResources, ScoreNormalizerContract):
    async def make_component(self, tmp_path: Path) -> ZScoreNormalizer:
        return ZScoreNormalizer()


class TestRankNormalizer(SharedResources, ScoreNormalizerContract):
    async def make_component(self, tmp_path: Path) -> RankNormalizer:
        return RankNormalizer()


class TestSigmoidNormalizer(SharedResources, ScoreNormalizerContract):
    async def make_component(self, tmp_path: Path) -> SigmoidNormalizer:
        return SigmoidNormalizer(center=2.0, temperature=3.0)


def minmax(lists: Mapping[str, Sequence[RankedEntry]]) -> dict[str, dict[str, float]]:
    """Independent min-max normalisation (all-equal lists map to 1.0)."""
    result: dict[str, dict[str, float]] = {}
    for source, entries in lists.items():
        scores = {entry.item_id: float(entry.score or 0.0) for entry in entries}
        low, high = min(scores.values(), default=0.0), max(scores.values(), default=0.0)
        result[source] = {
            key: 1.0 if high == low else (value - low) / (high - low)
            for key, value in scores.items()
        }
    return result


class TestRRFFusion(SharedResources, FusionContract):
    async def make_component(self, tmp_path: Path) -> RRFFusion:
        return RRFFusion()

    def reference(
        self, lists: Mapping[str, Sequence[RankedEntry]], weights: Mapping[str, float]
    ) -> dict[str, float]:
        return rrf_reference(lists, None)


class TestWeightedRRFFusion(SharedResources, FusionContract):
    async def make_component(self, tmp_path: Path) -> WeightedRRFFusion:
        return WeightedRRFFusion(k=10)

    def reference(
        self, lists: Mapping[str, Sequence[RankedEntry]], weights: Mapping[str, float]
    ) -> dict[str, float]:
        return rrf_reference(lists, weights, k=10)


class TestWeightedScoreFusion(SharedResources, FusionContract):
    async def make_component(self, tmp_path: Path) -> WeightedScoreFusion:
        return WeightedScoreFusion()

    def reference(
        self, lists: Mapping[str, Sequence[RankedEntry]], weights: Mapping[str, float]
    ) -> dict[str, float]:
        return score_reference(lists, minmax(lists), weights=weights)


class TestCombSUMFusion(SharedResources, FusionContract):
    async def make_component(self, tmp_path: Path) -> CombSUMFusion:
        return CombSUMFusion()

    def reference(
        self, lists: Mapping[str, Sequence[RankedEntry]], weights: Mapping[str, float]
    ) -> dict[str, float]:
        return score_reference(lists, minmax(lists))


class TestCombMNZFusion(SharedResources, FusionContract):
    async def make_component(self, tmp_path: Path) -> CombMNZFusion:
        return CombMNZFusion()

    def reference(
        self, lists: Mapping[str, Sequence[RankedEntry]], weights: Mapping[str, float]
    ) -> dict[str, float]:
        return score_reference(lists, minmax(lists), mnz=True)


class TestIdentityAndHashDeduplicator(SharedResources, DeduplicatorContract):
    async def make_component(self, tmp_path: Path) -> IdentityAndHashDeduplicator:
        return IdentityAndHashDeduplicator()


class TestHierarchicalConsolidator(SharedResources, EvidenceConsolidatorContract):
    async def make_component(self, tmp_path: Path) -> HierarchicalConsolidator:
        return HierarchicalConsolidator()


# infra and storage ----------------------------------------------------------------------
class TestInMemoryLRUCache(SharedResources, CacheContract):
    async def make_component(self, tmp_path: Path) -> InMemoryLRUCache:
        return InMemoryLRUCache()


class TestNoCache(SharedResources, CacheContract):
    stores_values: ClassVar[bool] = False

    async def make_component(self, tmp_path: Path) -> NoCache:
        return NoCache()


class TestSQLiteMemoryStore(SharedResources, MetadataStoreContract):
    async def make_component(self, tmp_path: Path) -> SQLiteMetadataStore:
        return SQLiteMetadataStore()


class TestSQLiteFileStore(SharedResources, MetadataStoreContract):
    async def make_component(self, tmp_path: Path) -> SQLiteMetadataStore:
        return SQLiteMetadataStore(tmp_path / "meta.db")


class _DefaultsTransaction(SQLiteTransaction):
    """Falls back to the base-class forms of the optional batched methods."""

    snapshot_marker = MetadataTransactionABC.snapshot_marker
    get_change_seqs = MetadataTransactionABC.get_change_seqs
    set_documents_objects_state = MetadataTransactionABC.set_documents_objects_state
    retract_contributions = MetadataTransactionABC.retract_contributions
    get_lineage_many = MetadataTransactionABC.get_lineage_many
    close_visibility_many = MetadataTransactionABC.close_visibility_many


class _DefaultsStore(SQLiteMetadataStore):
    def transaction(self, scope: Scope | None = None, *, write: bool) -> SQLiteTransaction:
        return _DefaultsTransaction(self, scope, write=write)


class TestStoreWithBaseDefaults(SharedResources, MetadataStoreContract):
    """A store implementing only the abstract methods still passes the contract."""

    async def make_component(self, tmp_path: Path) -> SQLiteMetadataStore:
        return _DefaultsStore()
