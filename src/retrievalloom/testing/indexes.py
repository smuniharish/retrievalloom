"""Contract kits for generational index backends."""

from __future__ import annotations

import asyncio
import math
from abc import abstractmethod
from typing import TYPE_CHECKING, Any, ClassVar

import pytest

from retrievalloom.contracts.indexes import (
    FuzzyQuery,
    GenerationalIndexABC,
    GenerationSpec,
    GenerationStats,
    GraphStoreABC,
    IndexCapabilities,
    IndexItem,
    KeywordQuery,
    NavigationDirection,
    SparseQuery,
    StructureEngineABC,
    VectorStoreABC,
)
from retrievalloom.contracts.models import KeywordEntry
from retrievalloom.domain.filters import Eq
from retrievalloom.domain.hashing import sha256_digest, stable_id
from retrievalloom.domain.inputs import Scope
from retrievalloom.domain.records import Channel, ObjectKind
from retrievalloom.domain.retrieval import RetrievalHit
from retrievalloom.errors import RetrievalLoomError
from retrievalloom.testing._base import ComponentContract

if TYPE_CHECKING:
    from collections.abc import Sequence

    from retrievalloom.domain.filters import FilterExpression

SCOPE = Scope(tenant_id="contract-a")
OTHER_SCOPE = Scope(tenant_id="contract-b")

DOC_A = stable_id("obj", "contract", "a")
DOC_B = stable_id("obj", "contract", "b")
DOC_C = stable_id("obj", "contract", "c")

CORPUS: dict[str, tuple[str, dict[str, Any]]] = {
    DOC_A: ("python asyncio event loop", {"lang": "en"}),
    DOC_B: ("rust tokio runtime", {"lang": "en"}),
    DOC_C: ("python packaging with uv", {"lang": "de"}),
}
VOCABULARY = ("python", "asyncio", "event", "loop", "rust", "tokio", "runtime", "packaging", "uv")
QUERY = "python"


def bag_of_words(text: str) -> tuple[float, ...]:
    """Deterministic unit-norm bag-of-words vector over ``VOCABULARY`` (plus a bias dim)."""
    words = text.casefold().split()
    vector = [float(words.count(term)) for term in VOCABULARY] + [0.1]
    norm = math.sqrt(sum(value * value for value in vector))
    return tuple(value / norm for value in vector)


class GenerationalIndexContract(ComponentContract):
    """Checks every ``GenerationalIndexABC`` obligation through a text query.

    Specialised kits implement ``search`` (and may override ``spec``/``item``); the
    corpus has three objects where the query ``python`` matches ``DOC_A`` and ``DOC_C``.
    """

    channel: ClassVar[Channel] = Channel.INDEX
    method: ClassVar[str] = "contract"

    def spec(self, generation_id: str) -> GenerationSpec:
        """Generation build parameters."""
        return GenerationSpec(
            generation_id=generation_id,
            method=self.method,
            dependency_fingerprint=sha256_digest("contract"),
            dimensions=len(VOCABULARY) + 1,
            keyword_entries=tuple(KeywordEntry(canonical=term) for term in VOCABULARY),
        )

    def item(self, object_id: str, text: str, metadata: dict[str, Any]) -> IndexItem:
        """Index item for one corpus object (tokens and vector included)."""
        return IndexItem(
            object_id=object_id,
            document_uid=stable_id("doc", object_id),
            version=1,
            kind=ObjectKind.CHUNK,
            text=text,
            metadata=metadata,
            tokens=tuple(text.casefold().split()),
            vector=bag_of_words(text),
        )

    @abstractmethod
    async def search(
        self,
        component: Any,
        scope: Scope,
        generation_id: str,
        text: str,
        limit: int,
        filters: FilterExpression | None,
    ) -> Sequence[RetrievalHit]:
        """Run the specialisation's search for ``text``."""

    def items(self, *object_ids: str) -> list[IndexItem]:
        """Items of the given corpus objects."""
        return [self.item(object_id, *CORPUS[object_id]) for object_id in object_ids]

    async def build(
        self, component: Any, scope: Scope, generation_id: str, object_ids: Sequence[str]
    ) -> GenerationStats:
        """Create, write and seal a generation."""
        await component.create_generation(scope, self.spec(generation_id))
        await component.write(scope, generation_id, self.items(*object_ids))
        return await component.seal(scope, generation_id)

    async def matching(
        self,
        component: Any,
        scope: Scope,
        generation_id: str,
        filters: FilterExpression | None = None,
        limit: int = 10,
    ) -> list[str]:
        """Object ids of hits whose score is positive (ranked order)."""
        hits = await self.search(component, scope, generation_id, QUERY, limit, filters)
        self.check_hits(hits, scope, generation_id)
        return [hit.object_ref.object_id for hit in hits if self.relevant(hit)]

    def relevant(self, hit: RetrievalHit) -> bool:
        """Whether a hit counts as a match (dense stores return every object)."""
        del hit
        return True

    def check_hits(self, hits: Sequence[RetrievalHit], scope: Scope, generation_id: str) -> None:
        """Common hit invariants."""
        assert [hit.rank for hit in hits] == list(range(1, len(hits) + 1))
        for hit in hits:
            assert isinstance(hit, RetrievalHit)
            assert hit.object_ref.scope_key == scope.key
            assert hit.generation_id == generation_id
            assert hit.method.channel == self.channel
        ids = [hit.object_ref.object_id for hit in hits]
        assert len(ids) == len(set(ids))

    async def test_capabilities(self, component: GenerationalIndexABC) -> None:
        """Capabilities are declared."""
        capabilities = component.capabilities
        assert isinstance(capabilities, IndexCapabilities)
        assert capabilities.max_batch >= 1

    async def test_search_returns_matching_objects(self, component: Any) -> None:
        """Only the matching objects are returned, best first."""
        stats = await self.build(component, SCOPE, "gen_one", list(CORPUS))
        assert stats.object_count == len(CORPUS)
        assert set(await self.matching(component, SCOPE, "gen_one")) == {DOC_A, DOC_C}
        assert len(await self.matching(component, SCOPE, "gen_one", limit=1)) == 1

    async def test_create_and_write_are_idempotent(self, component: Any) -> None:
        """Creating a generation twice and rewriting an object do not duplicate it."""
        await component.create_generation(SCOPE, self.spec("gen_idem"))
        await component.create_generation(SCOPE, self.spec("gen_idem"))
        await component.write(SCOPE, "gen_idem", self.items(DOC_A, DOC_B))
        await component.write(SCOPE, "gen_idem", self.items(DOC_A))
        stats = await component.seal(SCOPE, "gen_idem")
        assert stats.object_count == 2
        assert await self.matching(component, SCOPE, "gen_idem") == [DOC_A]

    async def test_remove_hides_objects(self, component: Any) -> None:
        """Removed objects disappear; removing them again reports nothing."""
        await self.build(component, SCOPE, "gen_rm", list(CORPUS))
        assert await component.remove(SCOPE, "gen_rm", [DOC_A]) == 1
        assert await component.remove(SCOPE, "gen_rm", [DOC_A]) == 0
        assert await self.matching(component, SCOPE, "gen_rm") == [DOC_C]

    async def test_filters_restrict_results(self, component: Any) -> None:
        """Filters are honoured (pushed down or applied by the backend)."""
        await self.build(component, SCOPE, "gen_filter", list(CORPUS))
        found = await self.matching(component, SCOPE, "gen_filter", Eq(field="lang", value="de"))
        assert found == [DOC_C]

    async def test_update_metadata_changes_filtering(self, component: Any) -> None:
        """Metadata updates replace the filterable metadata of present objects only."""
        await self.build(component, SCOPE, "gen_meta", list(CORPUS))
        updated = await component.update_metadata(
            SCOPE, "gen_meta", {DOC_C: {"lang": "en"}, stable_id("obj", "absent"): {}}
        )
        assert updated == 1
        de = Eq(field="lang", value="de")
        assert await self.matching(component, SCOPE, "gen_meta", de) == []

    async def test_scopes_are_isolated(self, component: Any) -> None:
        """Objects of one scope never surface in another, even with equal generation ids."""
        await self.build(component, SCOPE, "gen_shared", [DOC_A])
        await self.build(component, OTHER_SCOPE, "gen_shared", [DOC_C])
        await self.build(component, OTHER_SCOPE, "gen_other", [DOC_B])
        assert await self.matching(component, SCOPE, "gen_shared") == [DOC_A]
        assert await self.matching(component, OTHER_SCOPE, "gen_shared") == [DOC_C]
        assert "gen_other" not in await component.list_generations(SCOPE)

    async def test_sealed_generation_mutability(self, component: Any) -> None:
        """Immutable backends reject writes after ``seal``; mutable ones apply them."""
        await self.build(component, SCOPE, "gen_seal", [DOC_A])
        if component.capabilities.mutable_in_place:
            await component.write(SCOPE, "gen_seal", self.items(DOC_A, DOC_C))
            assert set(await self.matching(component, SCOPE, "gen_seal")) == {DOC_A, DOC_C}
        else:
            with pytest.raises(RetrievalLoomError):
                await component.write(SCOPE, "gen_seal", self.items(DOC_A, DOC_C))
            assert await self.matching(component, SCOPE, "gen_seal") == [DOC_A]

    async def test_drop_generation_is_idempotent(self, component: Any) -> None:
        """Dropped generations are no longer listed."""
        await self.build(component, SCOPE, "gen_drop", [DOC_A])
        assert "gen_drop" in await component.list_generations(SCOPE)
        await component.drop_generation(SCOPE, "gen_drop")
        await component.drop_generation(SCOPE, "gen_drop")
        assert "gen_drop" not in await component.list_generations(SCOPE)

    async def test_cancelled_search_leaves_backend_usable(self, component: Any) -> None:
        """Cancelling a search propagates ``CancelledError`` and corrupts nothing."""
        await self.build(component, SCOPE, "gen_cancel", list(CORPUS))
        task = asyncio.create_task(self.search(component, SCOPE, "gen_cancel", QUERY, 10, None))
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert set(await self.matching(component, SCOPE, "gen_cancel")) == {DOC_A, DOC_C}


class KeywordIndexContract(GenerationalIndexContract):
    """Kit for ``KeywordIndexABC``."""

    method = "keyword"

    async def search(
        self,
        component: Any,
        scope: Scope,
        generation_id: str,
        text: str,
        limit: int,
        filters: FilterExpression | None,
    ) -> Sequence[RetrievalHit]:
        """Keyword search over the raw query text."""
        return await component.search(scope, generation_id, KeywordQuery(text=text), limit, filters)


class FuzzyIndexContract(GenerationalIndexContract):
    """Kit for ``FuzzyIndexABC``; also checks typo tolerance."""

    method = "fuzzy"

    async def search(
        self,
        component: Any,
        scope: Scope,
        generation_id: str,
        text: str,
        limit: int,
        filters: FilterExpression | None,
    ) -> Sequence[RetrievalHit]:
        """Fuzzy search over query tokens."""
        query = FuzzyQuery(tokens=tuple(text.casefold().split()))
        return await component.search(scope, generation_id, query, limit, filters)

    async def test_typo_matches(self, component: Any) -> None:
        """A misspelt query term still finds the objects containing the term."""
        await self.build(component, SCOPE, "gen_typo", list(CORPUS))
        hits = await self.search(component, SCOPE, "gen_typo", "pythn", 10, None)
        assert {hit.object_ref.object_id for hit in hits} == {DOC_A, DOC_C}


class SparseIndexContract(GenerationalIndexContract):
    """Kit for ``SparseIndexABC``."""

    method = "sparse"

    async def search(
        self,
        component: Any,
        scope: Scope,
        generation_id: str,
        text: str,
        limit: int,
        filters: FilterExpression | None,
    ) -> Sequence[RetrievalHit]:
        """Sparse search over query tokens."""
        query = SparseQuery(tokens=tuple(text.casefold().split()), text=text)
        return await component.search(scope, generation_id, query, limit, filters)


class VectorStoreContract(GenerationalIndexContract):
    """Kit for ``VectorStoreABC`` (bag-of-words vectors; zero-similarity hits ignored)."""

    method = "dense"

    def relevant(self, hit: RetrievalHit) -> bool:
        """Objects sharing no query term only have the bias similarity."""
        text = CORPUS[hit.object_ref.object_id][0]
        similarity = sum(
            a * b for a, b in zip(bag_of_words(QUERY), bag_of_words(text), strict=True)
        )
        return similarity > 0.1

    async def search(
        self,
        component: VectorStoreABC,
        scope: Scope,
        generation_id: str,
        text: str,
        limit: int,
        filters: FilterExpression | None,
    ) -> Sequence[RetrievalHit]:
        """Vector search with the query's bag-of-words vector."""
        return await component.search(scope, generation_id, bag_of_words(text), limit, filters)


STRUCTURE_ROOT = stable_id("obj", "contract", "root")


class StructureEngineContract(GenerationalIndexContract):
    """Kit for ``StructureEngineABC``: corpus objects become children of one root node."""

    channel = Channel.STRUCTURE
    method = "structure"

    def item(self, object_id: str, text: str, metadata: dict[str, Any]) -> IndexItem:
        """Structure node item under ``STRUCTURE_ROOT``."""
        ordinal = sorted(CORPUS).index(object_id) + 1 if object_id in CORPUS else 0
        return IndexItem(
            object_id=object_id,
            document_uid=stable_id("doc", "contract"),
            version=1,
            kind=ObjectKind.STRUCTURE_NODE,
            text=text,
            metadata=metadata,
            payload={
                "parent_object_id": None if object_id == STRUCTURE_ROOT else STRUCTURE_ROOT,
                "title": text.split()[0],
                "level": 0 if object_id == STRUCTURE_ROOT else 1,
                "ordinal": ordinal,
            },
        )

    def items(self, *object_ids: str) -> list[IndexItem]:
        """The root node followed by the requested nodes."""
        root = self.item(STRUCTURE_ROOT, "contract manual", {"lang": "en"})
        return [root, *super().items(*object_ids)]

    async def search(
        self,
        component: StructureEngineABC,
        scope: Scope,
        generation_id: str,
        text: str,
        limit: int,
        filters: FilterExpression | None,
    ) -> Sequence[RetrievalHit]:
        """Structure search over node titles and texts."""
        return await component.search(scope, generation_id, text, limit, filters)

    async def test_create_and_write_are_idempotent(self, component: Any) -> None:
        """Rewriting a document's nodes replaces them (the root counts as an object)."""
        await component.create_generation(SCOPE, self.spec("gen_idem"))
        await component.create_generation(SCOPE, self.spec("gen_idem"))
        await component.write(SCOPE, "gen_idem", self.items(DOC_A, DOC_B))
        await component.write(SCOPE, "gen_idem", self.items(DOC_A, DOC_B))
        stats = await component.seal(SCOPE, "gen_idem")
        assert stats.object_count == 3
        assert await self.matching(component, SCOPE, "gen_idem") == [DOC_A]

    async def test_search_returns_matching_objects(self, component: Any) -> None:
        """Only the matching nodes are returned, best first."""
        stats = await self.build(component, SCOPE, "gen_one", list(CORPUS))
        assert stats.object_count == len(CORPUS) + 1
        assert set(await self.matching(component, SCOPE, "gen_one")) == {DOC_A, DOC_C}
        assert len(await self.matching(component, SCOPE, "gen_one", limit=1)) == 1

    async def test_navigation(self, component: StructureEngineABC) -> None:
        """Parent, children, siblings and path follow the written tree."""
        await self.build(component, SCOPE, "gen_nav", list(CORPUS))
        parent = await component.navigate(SCOPE, "gen_nav", DOC_A, NavigationDirection.PARENT)
        assert list(parent) == [STRUCTURE_ROOT]
        children = await component.navigate(
            SCOPE, "gen_nav", STRUCTURE_ROOT, NavigationDirection.CHILDREN
        )
        assert list(children) == sorted(CORPUS)
        siblings = await component.navigate(SCOPE, "gen_nav", DOC_A, NavigationDirection.SIBLINGS)
        assert set(siblings) == {DOC_B, DOC_C}
        path = await component.navigate(SCOPE, "gen_nav", DOC_A, NavigationDirection.PATH)
        assert set(path) >= {STRUCTURE_ROOT}
        unknown = stable_id("obj", "unknown")
        assert await component.navigate(SCOPE, "gen_nav", unknown, NavigationDirection.PATH) == []


ENTITY_PY = stable_id("obj", "contract", "python")
ENTITY_ASYNC = stable_id("obj", "contract", "asyncio")
ENTITY_RUST = stable_id("obj", "contract", "rust")
RELATION = stable_id("obj", "contract", "part_of")


class GraphStoreContract(ComponentContract):
    """Kit for ``GraphStoreABC``: entity lookup, traversal, removal and isolation."""

    def entity(self, object_id: str, name: str, aliases: tuple[str, ...] = ()) -> IndexItem:
        """Entity item."""
        return IndexItem(
            object_id=object_id,
            kind=ObjectKind.GRAPH_ENTITY,
            text=name,
            payload={"entity_type": "technology", "name": name, "aliases": list(aliases)},
        )

    def relationship(self, object_id: str, source: str, target: str) -> IndexItem:
        """Relationship item."""
        return IndexItem(
            object_id=object_id,
            kind=ObjectKind.GRAPH_RELATIONSHIP,
            text="part of",
            payload={
                "source_entity_id": source,
                "target_entity_id": target,
                "relation_type": "part_of",
            },
        )

    async def build(self, component: GraphStoreABC, scope: Scope, generation_id: str) -> None:
        """Write ``asyncio -part_of-> python`` plus an unconnected ``rust`` entity."""
        spec = GenerationSpec(
            generation_id=generation_id,
            method="graph",
            dependency_fingerprint=sha256_digest("contract"),
        )
        await component.create_generation(scope, spec)
        await component.write(
            scope,
            generation_id,
            [
                self.relationship(RELATION, ENTITY_ASYNC, ENTITY_PY),
                self.entity(ENTITY_PY, "Python", ("CPython",)),
                self.entity(ENTITY_ASYNC, "asyncio"),
                self.entity(ENTITY_RUST, "Rust"),
            ],
        )
        await component.seal(scope, generation_id)

    async def test_find_entities_by_name_and_alias(self, component: GraphStoreABC) -> None:
        """Names and aliases match case-insensitively."""
        await self.build(component, SCOPE, "gen_find")
        hits = await component.find_entities(SCOPE, "gen_find", ["cpython", "RUST"], 10)
        assert {hit.object_ref.object_id for hit in hits} == {ENTITY_PY, ENTITY_RUST}
        assert [hit.rank for hit in hits] == [1, 2]
        assert await component.find_entities(SCOPE, "gen_find", ["unknown"], 10) == []

    async def test_neighborhood(self, component: GraphStoreABC) -> None:
        """Traversal reaches the related entity through the relationship."""
        await self.build(component, SCOPE, "gen_hood")
        subgraph = await component.neighborhood(SCOPE, "gen_hood", [ENTITY_ASYNC], 1, 10)
        assert [node.object_id for node in subgraph.nodes] == [ENTITY_PY]
        assert subgraph.nodes[0].distance == 1
        assert [node.object_id for node in subgraph.relationships] == [RELATION]
        isolated = await component.neighborhood(SCOPE, "gen_hood", [ENTITY_RUST], 2, 10)
        assert isolated.nodes == ()

    async def test_remove_and_isolation(self, component: GraphStoreABC) -> None:
        """Removed entities vanish; other scopes never see the graph."""
        await self.build(component, SCOPE, "gen_rm")
        assert await component.remove(SCOPE, "gen_rm", [ENTITY_RUST]) == 1
        assert await component.find_entities(SCOPE, "gen_rm", ["rust"], 10) == []
        assert await component.find_entities(OTHER_SCOPE, "gen_rm", ["python"], 10) == []
        assert await component.list_generations(OTHER_SCOPE) == []
        await component.drop_generation(SCOPE, "gen_rm")
        assert "gen_rm" not in await component.list_generations(SCOPE)
