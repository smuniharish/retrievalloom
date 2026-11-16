"""Backend factories for the combination matrix (``test_combination_matrix.py``).

One :class:`ComboRun` owns every server-side namespace of a single matrix cell (a
Postgres schema, a Qdrant collection prefix, a pgvector table prefix, a Neo4j label) and
drops them all on exit, so cells never share state and leave nothing behind.
Every ``*_store()`` call builds a fresh component on the same durable location, which is
what an engine restart sees.
"""

from __future__ import annotations

import os
import uuid
from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING, Any, Self

import pytest

from retrievalloom._logging import get_logger
from retrievalloom.contracts.component import ComponentContext
from retrievalloom.domain.base import utc_now
from retrievalloom.domain.hashing import random_id
from retrievalloom.index.vector import QdrantVectorStore
from retrievalloom.infra.metrics import NullMetrics
from retrievalloom.runtime.offloader import Offloader, OffloaderConfig
from retrievalloom.storage.sqlite import SQLiteMetadataStore
from tests.live.conftest import MODELS_DIR, QDRANT_URL, drop_collections
from tests.support import shared_manager

if TYPE_CHECKING:
    from pathlib import Path
    from types import TracebackType

    from retrievalloom.contracts.indexes import GraphStoreABC, StructureEngineABC, VectorStoreABC
    from retrievalloom.storage.base import DocumentMetadataStoreABC

POSTGRES_DSN = os.environ.get("RL_TEST_POSTGRES_DSN")
PGVECTOR_DSN = os.environ.get("RL_TEST_PGVECTOR_DSN")
# Same variables (and defaults) as tests/live/test_example_stores.py.
NEO4J_URL = os.environ.get("RL_TEST_NEO4J_URL")
NEO4J_USER = os.environ.get("RL_TEST_NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.environ.get("RL_TEST_NEO4J_PASSWORD", "retrievalloom")


class Meta(StrEnum):
    SQLITE = "sqlite"
    POSTGRES = "postgres"


class Vector(StrEnum):
    QDRANT_MEMORY = "qdrant-memory"
    QDRANT_SERVER = "qdrant-server"
    PGVECTOR = "pgvector"


class Graph(StrEnum):
    NONE = "nograph"
    LADYBUG = "ladybug"
    NEO4J = "neo4j"


@dataclass(frozen=True)
class Combo:
    """One cell of the backend matrix."""

    meta: Meta
    vector: Vector
    graph: Graph
    structure: bool

    @property
    def id(self) -> str:
        return "-".join(
            (self.meta, self.vector, self.graph, "struct" if self.structure else "nostruct")
        )

    @property
    def durable_vectors(self) -> bool:
        return self.vector is not Vector.QDRANT_MEMORY

    def marks(self, *, models: bool = False) -> list[Any]:
        """Markers plus a skip naming every missing service variable."""
        marks: list[Any] = []
        missing: list[str] = []
        if self.meta is Meta.POSTGRES and not POSTGRES_DSN:
            missing.append("RL_TEST_POSTGRES_DSN")
        if self.vector is Vector.QDRANT_SERVER:
            marks.append(pytest.mark.qdrant)
            if not QDRANT_URL:
                missing.append("RL_TEST_QDRANT_URL")
        if self.vector is Vector.PGVECTOR and not PGVECTOR_DSN:
            missing.append("RL_TEST_PGVECTOR_DSN")
        if self.graph is not Graph.NONE:
            marks.append(pytest.mark.graph)
        if self.graph is Graph.NEO4J and not NEO4J_URL:
            missing.append("RL_TEST_NEO4J_URL")
        if self.structure:
            marks.append(pytest.mark.structure)
        if models:
            marks.append(pytest.mark.models)
            # The learned-sparse index of the model slice lives on the Qdrant server.
            if not QDRANT_URL and "RL_TEST_QDRANT_URL" not in missing:
                missing.append("RL_TEST_QDRANT_URL")
            if not MODELS_DIR:
                missing.append("RL_TEST_MODELS_DIR")
        if missing:
            marks.append(pytest.mark.skip(reason=f"set {', '.join(missing)} to run {self.id}"))
        return marks


def _suffix() -> str:
    return uuid.uuid4().hex[:16]


class ComboRun:
    """Unique, self-cleaning backend namespaces for one combination run."""

    def __init__(self, combo: Combo, data_dir: Path) -> None:
        self.combo = combo
        self.data_dir = data_dir
        suffix = _suffix()
        self.suffix = suffix
        self.schema = f"rl_combo_{suffix}"
        self.qdrant_prefix = f"rlcombo_{suffix}"
        self.pg_prefix = f"rlc_{suffix}"
        self.neo4j_label = f"RLCombo_{suffix}"

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        await self.cleanup()

    # factories ----------------------------------------------------------------------------
    def metadata_store(self) -> DocumentMetadataStoreABC:
        if self.combo.meta is Meta.SQLITE:
            return SQLiteMetadataStore(self.data_dir / "metadata.db")
        from retrievalloom.storage.postgres import PostgresMetadataStore

        assert POSTGRES_DSN is not None
        return PostgresMetadataStore(POSTGRES_DSN, schema=self.schema)

    def vector_store(self) -> VectorStoreABC:
        if self.combo.vector is Vector.QDRANT_MEMORY:
            return QdrantVectorStore()
        if self.combo.vector is Vector.QDRANT_SERVER:
            assert QDRANT_URL is not None
            return QdrantVectorStore(url=QDRANT_URL, collection_prefix=self.qdrant_prefix)
        from examples.adapters import PgVectorStore

        assert PGVECTOR_DSN is not None
        return PgVectorStore(PGVECTOR_DSN, table_prefix=self.pg_prefix)

    def graph_store(self) -> GraphStoreABC | None:
        if self.combo.graph is Graph.NONE:
            return None
        if self.combo.graph is Graph.LADYBUG:
            from retrievalloom.graph.ladybug_store import LadybugGraphStore

            return LadybugGraphStore(self.data_dir / "graph" / "graph.lbug")
        from examples.adapters import Neo4jGraphStore

        assert NEO4J_URL is not None
        return Neo4jGraphStore(
            NEO4J_URL, user=NEO4J_USER, password=NEO4J_PASSWORD, label=self.neo4j_label
        )

    def structure_engine(self) -> StructureEngineABC | None:
        if not self.combo.structure:
            return None
        from retrievalloom.structure.treesearch_engine import TreeSearchStructureEngine

        return TreeSearchStructureEngine(self.data_dir / "structure")

    # cleanup ------------------------------------------------------------------------------
    async def cleanup(self) -> None:
        """Drop every server-side namespace this run may have created."""
        errors: list[BaseException] = []
        if self.combo.meta is Meta.POSTGRES:
            try:
                await self._drop_schema()
            except Exception as exc:
                errors.append(exc)
        if QDRANT_URL:
            try:
                await drop_collections(QDRANT_URL, self.qdrant_prefix)
            except Exception as exc:
                errors.append(exc)
        offloader = Offloader(OffloaderConfig(max_workers=1), resource_manager=shared_manager())
        await offloader.start()
        context = ComponentContext(
            engine_id=random_id("engine"),
            offloader=offloader,
            clock=utc_now,
            logger=get_logger("tests.combo"),
            metrics=NullMetrics(),
        )
        try:
            await self._drop_example_stores(context, errors)
        finally:
            await offloader.close()
        if errors:
            raise errors[0]

    async def _drop_example_stores(
        self, context: ComponentContext, errors: list[BaseException]
    ) -> None:
        if self.combo.vector is Vector.PGVECTOR:
            from examples.adapters import PgVectorStore

            assert PGVECTOR_DSN is not None
            pg = PgVectorStore(PGVECTOR_DSN, table_prefix=self.pg_prefix)
            try:
                await pg.initialize(context)
                await pg.drop_tables()
            except Exception as exc:
                errors.append(exc)
            finally:
                await pg.close()
        if self.combo.graph is Graph.NEO4J:
            from examples.adapters import Neo4jGraphStore

            assert NEO4J_URL is not None
            neo = Neo4jGraphStore(
                NEO4J_URL, user=NEO4J_USER, password=NEO4J_PASSWORD, label=self.neo4j_label
            )
            try:
                await neo.initialize(context)
                await neo.drop_label()
            except Exception as exc:
                errors.append(exc)
            finally:
                await neo.close()

    async def _drop_schema(self) -> None:
        import sqlalchemy as sa

        from retrievalloom.storage.postgres import PostgresMetadataStore

        assert POSTGRES_DSN is not None
        admin = PostgresMetadataStore(POSTGRES_DSN, schema=self.schema)
        await admin.initialize()
        try:
            async with admin.engine.begin() as connection:
                await connection.execute(sa.text(f'DROP SCHEMA IF EXISTS "{self.schema}" CASCADE'))
        finally:
            await admin.close()
