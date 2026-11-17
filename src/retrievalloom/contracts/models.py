"""Application-supplied model contracts: embeddings, reranking, NLI, extraction, tokens."""

from __future__ import annotations

from abc import abstractmethod
from typing import TYPE_CHECKING

from pydantic import Field

from retrievalloom.contracts.component import ComponentABC
from retrievalloom.domain.base import FrozenModel, Metadata, PositiveInt
from retrievalloom.domain.hashing import digest_json
from retrievalloom.domain.inputs import GraphEntityInput, GraphRelationshipInput, Scope

if TYPE_CHECKING:
    from collections.abc import Sequence

    from retrievalloom.domain.retrieval import NLIJudgement


class EmbeddingModelABC(ComponentABC):
    """Embedding model supplied by the application.

    ``info.fingerprint`` must carry ``model_identity`` and ``dimensions``. Every call is
    validated by the engine (count, dimensions, finiteness, non-zero norm).
    """

    @property
    @abstractmethod
    def dimensions(self) -> int:
        """Embedding dimensionality."""

    @property
    def max_batch_size(self) -> int:
        """Largest batch passed to ``embed_documents``; the engine splits larger inputs."""
        return 64

    @abstractmethod
    async def embed_documents(self, texts: Sequence[str]) -> Sequence[Sequence[float]]:
        """Embed document texts, one vector per input in order."""

    @abstractmethod
    async def embed_query(self, text: str) -> Sequence[float]:
        """Embed a query."""


class RerankItem(FrozenModel):
    """Item to rerank."""

    item_id: str
    text: str


class RerankScore(FrozenModel):
    """Reranker score aligned to ``item_id``."""

    item_id: str
    score: float


class RerankerABC(ComponentABC):
    """Cross-encoder style reranker; must score every item exactly once."""

    @abstractmethod
    async def rerank(self, query: str, items: Sequence[RerankItem]) -> Sequence[RerankScore]:
        """Score ``items`` against ``query``."""


class NLIPair(FrozenModel):
    """Premise/hypothesis pair."""

    pair_id: str
    premise: str
    hypothesis: str
    hypothesis_ref: str


class NLIABC(ComponentABC):
    """Natural language inference model; returns one judgement per pair in order."""

    @abstractmethod
    async def judge(self, pairs: Sequence[NLIPair]) -> Sequence[NLIJudgement]:
        """Judge each pair."""


class ExtractionSource(FrozenModel):
    """Document version view handed to a graph extractor."""

    scope: Scope
    document_id: str
    document_uid: str
    version: PositiveInt
    representation_id: str
    texts: tuple[str, ...] = Field(min_length=1)
    metadata: Metadata = Field(default_factory=dict)


class GraphExtraction(FrozenModel):
    """Raw extractor output."""

    entities: tuple[GraphEntityInput, ...] = ()
    relationships: tuple[GraphRelationshipInput, ...] = ()


class NormalizedEntity(FrozenModel):
    """Entity with a canonical key."""

    canonical_key: str = Field(min_length=1)
    entity_type: str
    name: str
    aliases: tuple[str, ...] = ()
    description: str | None = None
    properties: Metadata = Field(default_factory=dict)
    source_keys: tuple[str, ...] = ()


class NormalizedRelationship(FrozenModel):
    """Relationship between canonical entity keys."""

    source_key: str
    source_type: str
    target_key: str
    target_type: str
    relation_type: str
    qualifier: str | None = None
    description: str | None = None
    properties: Metadata = Field(default_factory=dict)
    directed: bool = True


class NormalizedGraph(FrozenModel):
    """Normaliser output."""

    entities: tuple[NormalizedEntity, ...] = ()
    relationships: tuple[NormalizedRelationship, ...] = ()


class GraphExtractorABC(ComponentABC):
    """Extracts raw entities and relationships from a document version."""

    @abstractmethod
    async def extract(self, source: ExtractionSource) -> GraphExtraction:
        """Extract a graph from ``source``."""


class GraphNormalizerABC(ComponentABC):
    """Assigns canonical keys, merges aliases and maps types."""

    @abstractmethod
    async def normalize(self, extraction: GraphExtraction, scope: Scope) -> NormalizedGraph:
        """Normalise ``extraction``."""


class TokenizerABC(ComponentABC):
    """Tokenizer used by sparse and fuzzy methods."""

    @abstractmethod
    async def tokenize(self, texts: Sequence[str]) -> Sequence[Sequence[str]]:
        """Tokenize each text."""


class KeywordEntry(FrozenModel):
    """Dictionary entry: surface forms that map to a canonical keyword."""

    canonical: str = Field(min_length=1)
    aliases: tuple[str, ...] = ()

    @property
    def surface_forms(self) -> tuple[str, ...]:
        """Canonical form followed by aliases."""
        return (self.canonical, *self.aliases)


class KeywordDictionaryABC(ComponentABC):
    """Supplies the keyword (FlashText-style) dictionary of a scope."""

    @abstractmethod
    async def entries(self, scope: Scope) -> Sequence[KeywordEntry]:
        """Return the dictionary entries for ``scope``."""

    async def revision(self, scope: Scope) -> str:
        """A token that changes whenever ``entries(scope)`` changes.

        The indexer checks it on every pass to decide whether keyword generations must
        be rebuilt. The default digests ``entries``; override it with something cheaper
        when computing the entries reads the whole scope.
        """
        return digest_json([entry.model_dump(mode="json") for entry in await self.entries(scope)])
