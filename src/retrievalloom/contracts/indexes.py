"""Index and storage backend contracts sharing one generational base."""

from __future__ import annotations

from abc import abstractmethod
from enum import StrEnum
from typing import TYPE_CHECKING, Any

from pydantic import Field

from retrievalloom.contracts.component import ComponentABC
from retrievalloom.contracts.models import KeywordEntry
from retrievalloom.domain.base import Digest, FrozenModel, Metadata

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from retrievalloom.domain.filters import FilterExpression
    from retrievalloom.domain.inputs import Scope
    from retrievalloom.domain.retrieval import RetrievalHit


class GenerationSpec(FrozenModel):
    """Build parameters of one index generation."""

    generation_id: str
    method: str
    dependency_fingerprint: Digest
    dimensions: int | None = None
    keyword_entries: tuple[KeywordEntry, ...] = ()
    options: Metadata = Field(default_factory=dict)


class IndexItem(FrozenModel):
    """One derived object prepared for a backend (text, tokens, vector, filter metadata)."""

    object_id: str
    document_uid: str | None = None
    version: int | None = None
    kind: str
    text: str
    metadata: Metadata = Field(default_factory=dict)
    tokens: tuple[str, ...] | None = None
    vector: tuple[float, ...] | None = None
    payload: dict[str, Any] = Field(default_factory=dict)


class WriteReport(FrozenModel):
    """Result of a backend write."""

    written: int = 0


class GenerationStats(FrozenModel):
    """Statistics produced when sealing a generation."""

    object_count: int
    details: Metadata = Field(default_factory=dict)


class IndexCapabilities(FrozenModel):
    """What a backend can do.

    Attributes:
        mutable_in_place: Writes are allowed into a sealed (active) generation.
        durable: Data survives process restarts and may be shared between engines.
        filter_pushdown: The backend evaluates ``FilterExpression`` itself.
        max_batch: Largest write batch.
        max_depth: Deepest graph traversal a graph store supports (``None``: no limit,
            or not a graph store). The engine checks the configured depth against it
            at ``initialize``.
    """

    mutable_in_place: bool
    durable: bool
    filter_pushdown: bool = True
    max_batch: int = Field(default=256, ge=1)
    max_depth: int | None = Field(default=None, ge=1)


class GenerationalIndexABC(ComponentABC):
    """Generation-aware backend.

    All methods are idempotent by ``(generation_id, object_id)`` and must reject data
    of another scope. Immutable backends accept ``write`` only before ``seal``;
    ``remove`` is always accepted, also after ``seal``, and must make the removed
    objects unsearchable at once (erasure relies on it).
    """

    @property
    @abstractmethod
    def capabilities(self) -> IndexCapabilities:
        """Backend capabilities."""

    @abstractmethod
    async def create_generation(self, scope: Scope, spec: GenerationSpec) -> None:
        """Create an empty generation (idempotent)."""

    @abstractmethod
    async def write(
        self, scope: Scope, generation_id: str, items: Sequence[IndexItem]
    ) -> WriteReport:
        """Write or overwrite items."""

    @abstractmethod
    async def remove(self, scope: Scope, generation_id: str, object_ids: Sequence[str]) -> int:
        """Remove items; returns how many were present."""

    @abstractmethod
    async def update_metadata(
        self, scope: Scope, generation_id: str, updates: Mapping[str, Metadata]
    ) -> int:
        """Replace the filter metadata of existing items."""

    @abstractmethod
    async def seal(self, scope: Scope, generation_id: str) -> GenerationStats:
        """Finish building a generation and return its statistics."""

    @abstractmethod
    async def drop_generation(self, scope: Scope, generation_id: str) -> None:
        """Delete a generation (idempotent)."""

    @abstractmethod
    async def list_generations(self, scope: Scope) -> Sequence[str]:
        """List generations present in the backend for ``scope``."""


class KeywordQuery(FrozenModel):
    """Keyword query: raw query text scanned with the dictionary automaton."""

    text: str


class FuzzyQuery(FrozenModel):
    """Fuzzy query: query tokens; n-grams are formed by the backend."""

    tokens: tuple[str, ...]


class SparseQuery(FrozenModel):
    """Sparse query: the engine tokenizer's tokens plus the raw query text.

    Term-statistics indexes (BM25) score ``tokens``. Learned-sparse indexes, which run
    their own model tokenizer, encode ``text`` instead; it is empty only when a caller
    builds a query from tokens alone.
    """

    tokens: tuple[str, ...]
    text: str = ""


class KeywordIndexABC(GenerationalIndexABC):
    """Exact dictionary keyword index."""

    @abstractmethod
    async def search(
        self,
        scope: Scope,
        generation_id: str,
        query: KeywordQuery,
        limit: int,
        filters: FilterExpression | None,
    ) -> Sequence[RetrievalHit]:
        """Return ranked hits."""


class FuzzyIndexABC(GenerationalIndexABC):
    """Fuzzy vocabulary index."""

    @abstractmethod
    async def search(
        self,
        scope: Scope,
        generation_id: str,
        query: FuzzyQuery,
        limit: int,
        filters: FilterExpression | None,
    ) -> Sequence[RetrievalHit]:
        """Return ranked hits."""


class SparseIndexABC(GenerationalIndexABC):
    """Sparse lexical (BM25) index."""

    @abstractmethod
    async def search(
        self,
        scope: Scope,
        generation_id: str,
        query: SparseQuery,
        limit: int,
        filters: FilterExpression | None,
    ) -> Sequence[RetrievalHit]:
        """Return ranked hits."""


class VectorStoreABC(GenerationalIndexABC):
    """Dense vector store; ``GenerationSpec.dimensions`` is mandatory."""

    @abstractmethod
    async def search(
        self,
        scope: Scope,
        generation_id: str,
        vector: Sequence[float],
        limit: int,
        filters: FilterExpression | None,
    ) -> Sequence[RetrievalHit]:
        """Return ranked hits."""


class GraphNode(FrozenModel):
    """Node reached by a traversal."""

    object_id: str
    distance: int
    path: tuple[str, ...] = ()


class GraphSubgraph(FrozenModel):
    """Traversal result: reached entities and traversed relationships."""

    nodes: tuple[GraphNode, ...] = ()
    relationships: tuple[GraphNode, ...] = ()


class GraphStoreABC(GenerationalIndexABC):
    """Property graph store.

    Items are graph aggregates: ``IndexItem.kind`` is ``graph_entity`` (payload
    ``entity_type``, ``name``, ``aliases``) or ``graph_relationship`` (payload
    ``source_entity_id``, ``target_entity_id``, ``relation_type``). Relationships
    must be written after their endpoint entities.
    """

    @abstractmethod
    async def find_entities(
        self,
        scope: Scope,
        generation_id: str,
        terms: Sequence[str],
        limit: int,
    ) -> Sequence[RetrievalHit]:
        """Match entities whose name or alias equals one of ``terms`` (casefolded)."""

    @abstractmethod
    async def neighborhood(
        self,
        scope: Scope,
        generation_id: str,
        seeds: Sequence[str],
        depth: int,
        limit: int,
    ) -> GraphSubgraph:
        """Return entities and relationships within ``depth`` hops of ``seeds``."""


class GraphRetrieverABC(ComponentABC):
    """Traversal strategy over a ``GraphStoreABC``."""

    @abstractmethod
    async def retrieve(
        self,
        scope: Scope,
        generation_id: str,
        query: str,
        store: GraphStoreABC,
        limit: int,
    ) -> Sequence[RetrievalHit]:
        """Return ranked graph hits for ``query``."""


class NavigationDirection(StrEnum):
    """Structure navigation direction."""

    PARENT = "parent"
    CHILDREN = "children"
    SIBLINGS = "siblings"
    PATH = "path"


class StructureEngineABC(GenerationalIndexABC):
    """Document-structure search engine.

    Items are ``structure_node`` objects whose payload carries ``parent_object_id``,
    ``title``, ``level`` and ``ordinal``; nodes of one document version are written together.
    """

    @abstractmethod
    async def search(
        self,
        scope: Scope,
        generation_id: str,
        query: str,
        limit: int,
        filters: FilterExpression | None,
    ) -> Sequence[RetrievalHit]:
        """Return ranked node hits."""

    @abstractmethod
    async def navigate(
        self,
        scope: Scope,
        generation_id: str,
        object_id: str,
        direction: NavigationDirection,
    ) -> Sequence[str]:
        """Return related node object ids."""
