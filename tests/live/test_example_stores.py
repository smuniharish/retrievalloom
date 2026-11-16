"""Contract kits against the example Neo4j graph store and pgvector vector store.

Skipped unless ``RL_TEST_NEO4J_URL`` (plus ``RL_TEST_NEO4J_USER``/``RL_TEST_NEO4J_PASSWORD``)
or ``RL_TEST_PGVECTOR_DSN`` are set. Each test uses its own Neo4j label / table prefix and
removes it afterwards, so runs never share server state.
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING, Any

import pytest

from examples.adapters import Neo4jGraphStore, PgVectorStore
from retrievalloom.domain.filters import And, Eq, In, Not
from retrievalloom.domain.hashing import random_id
from retrievalloom.errors import ComponentContractError, ConfigurationError
from retrievalloom.testing import GraphStoreContract, VectorStoreContract
from retrievalloom.testing.indexes import (
    DOC_A,
    DOC_B,
    DOC_C,
    ENTITY_ASYNC,
    ENTITY_PY,
    ENTITY_RUST,
    SCOPE,
    bag_of_words,
)
from tests.support import SharedResources

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from pathlib import Path

    from retrievalloom.contracts.component import ComponentContext

NEO4J_URL = os.environ.get("RL_TEST_NEO4J_URL")
NEO4J_USER = os.environ.get("RL_TEST_NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.environ.get("RL_TEST_NEO4J_PASSWORD", "retrievalloom")
PGVECTOR_DSN = os.environ.get("RL_TEST_PGVECTOR_DSN")


def _unique(prefix: str) -> str:
    return f"{prefix}_{random_id('t').replace('-', '_')}"[:40].lower()


@pytest.mark.graph
@pytest.mark.skipif(not NEO4J_URL, reason="set RL_TEST_NEO4J_URL to run Neo4j tests")
class TestNeo4jGraphStore(SharedResources, GraphStoreContract):
    @pytest.fixture(autouse=True)
    async def _label(self, component_context: ComponentContext) -> AsyncIterator[None]:
        self.label = "RLT" + _unique("g")
        yield
        cleaner = self.store()
        await cleaner.initialize(component_context)
        try:
            await cleaner.drop_label()
        finally:
            await cleaner.close()

    def store(self) -> Neo4jGraphStore:
        assert NEO4J_URL is not None
        return Neo4jGraphStore(
            NEO4J_URL, user=NEO4J_USER, password=NEO4J_PASSWORD, label=self.label
        )

    async def make_component(self, tmp_path: Path) -> Neo4jGraphStore:
        return self.store()

    async def test_two_hop_paths_and_generation_isolation(self, component: Any) -> None:
        """Depth counts entity hops; paths run seed -> relationship -> entity."""
        await self.build(component, SCOPE, "gen_paths")
        sub = await component.neighborhood(SCOPE, "gen_paths", [ENTITY_PY], 2, 10)
        assert [(n.object_id, n.distance) for n in sub.nodes] == [(ENTITY_ASYNC, 1)]
        assert sub.nodes[0].path[0] == ENTITY_PY
        assert sub.nodes[0].path[-1] == ENTITY_ASYNC
        assert await component.find_entities(SCOPE, "other_gen", ["python"], 10) == []
        with pytest.raises(ConfigurationError):
            await component.neighborhood(SCOPE, "gen_paths", [ENTITY_PY], 6, 10)

    async def test_relationship_needs_endpoints(self, component: Any) -> None:
        """Dangling relationships are rejected and the batch is rolled back."""
        await self.build(component, SCOPE, "gen_dangling")
        with pytest.raises(ComponentContractError):
            await component.write(
                SCOPE, "gen_dangling", [self.relationship("rel-x", ENTITY_RUST, "missing")]
            )
        stats = await component.seal(SCOPE, "gen_dangling")
        assert stats.details == {"entity": 3, "relationship": 1}


@pytest.mark.skipif(not PGVECTOR_DSN, reason="set RL_TEST_PGVECTOR_DSN to run pgvector tests")
class TestPgVectorStore(SharedResources, VectorStoreContract):
    @pytest.fixture(autouse=True)
    async def _prefix(self, component_context: ComponentContext) -> AsyncIterator[None]:
        self.prefix = _unique("rlt")
        yield
        cleaner = self.store()
        await cleaner.initialize(component_context)
        try:
            await cleaner.drop_tables()
        finally:
            await cleaner.close()

    def store(self) -> PgVectorStore:
        assert PGVECTOR_DSN is not None
        return PgVectorStore(PGVECTOR_DSN, table_prefix=self.prefix)

    async def make_component(self, tmp_path: Path) -> PgVectorStore:
        return self.store()

    async def test_compound_filters_are_pushed_down(self, component: Any) -> None:
        """And/Not/In translate to SQL and agree with the reference evaluator."""
        await self.build(component, SCOPE, "gen_filters", [DOC_A, DOC_B, DOC_C])
        not_de = Not(clause=Eq(field="lang", value="de"))
        cases = [
            (In(field="lang", values=("de", "fr")), {DOC_C}),
            (And(clauses=(Eq(field="lang", value="en"), not_de)), {DOC_A, DOC_B}),
            (Not(clause=Eq(field="lang", value="en")), {DOC_C}),
        ]
        for expression, expected in cases:
            hits = await component.search(
                SCOPE, "gen_filters", bag_of_words("python"), 10, expression
            )
            assert {hit.object_ref.object_id for hit in hits} == expected, expression

    async def test_restart_keeps_generations(
        self, component: Any, component_context: ComponentContext
    ) -> None:
        """A second store instance sees the same generations and rows (durability)."""
        await self.build(component, SCOPE, "gen_durable", [DOC_A, DOC_C])
        second = self.store()
        await second.initialize(component_context)
        try:
            assert "gen_durable" in await second.list_generations(SCOPE)
            hits = await second.search(SCOPE, "gen_durable", bag_of_words("python"), 5, None)
            assert {hit.object_ref.object_id for hit in hits} == {DOC_A, DOC_C}
            with pytest.raises(ComponentContractError):
                await second.search(SCOPE, "gen_durable", (1.0, 0.0), 5, None)
        finally:
            await second.close()
