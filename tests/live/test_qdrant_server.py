"""Qdrant server mode: contract kit over REST and gRPC, and multi-engine lifecycle behaviour."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any

import pytest

from retrievalloom import (
    ChunkInput,
    ChunksRepresentation,
    Components,
    DocumentInput,
    RetrievalConfig,
    RetrievalExecutionConfig,
    Scope,
    TextRepresentation,
)
from retrievalloom.config import (
    ChannelsConfig,
    FuzzyConfig,
    IndexChannelConfig,
    LifecycleConfig,
    MethodConfig,
)
from retrievalloom.domain.records import GenerationState, TaskState
from retrievalloom.domain.retrieval import ResultStatus
from retrievalloom.index.vector import QdrantVectorStore
from retrievalloom.storage.sqlite import SQLiteMetadataStore
from retrievalloom.testing import VectorStoreContract
from tests.fakes import HashEmbedding
from tests.live.conftest import QDRANT_GRPC_PORT, drop_collections, running_engine
from tests.support import SharedResources

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from contextlib import AbstractAsyncContextManager
    from pathlib import Path

    from retrievalloom import RetrievalEngine, RetrievalResult

pytestmark = pytest.mark.qdrant

DENSE_ONLY = RetrievalConfig(
    channels=ChannelsConfig(
        index=IndexChannelConfig(
            keyword=MethodConfig(enabled=False),
            fuzzy=FuzzyConfig(enabled=False),
            sparse=MethodConfig(enabled=False),
        )
    ),
)
DENSE = RetrievalExecutionConfig(dense=True)
CORPUS = {
    "loops": "Python asyncio event loops schedule coroutines cooperatively.",
    "vectors": "Qdrant stores dense vectors in collections with payload filters.",
    "bm25": "BM25 ranks documents by sparse term statistics.",
}


def documents(corpus: dict[str, str]) -> list[DocumentInput]:
    """One text representation per document."""
    return [
        DocumentInput(_id=key, representations=(TextRepresentation(text=text),))
        for key, text in corpus.items()
    ]


def document_ids(result: RetrievalResult) -> list[str]:
    """Document ids of the evidence, best first."""
    return [item.sources[0].document_id for item in result.evidence]


async def active_generations(engine: RetrievalEngine, scope: Scope) -> set[str]:
    """Ids of the active generations of `scope`."""
    status = await engine.status(scope=scope)
    return {
        item.generation_id for item in status.generations if item.state is GenerationState.ACTIVE
    }


class _ServerKit(SharedResources, VectorStoreContract):
    """Contract kit bound to a unique collection prefix that is dropped afterwards."""

    @pytest.fixture(autouse=True)
    async def _collections(self, qdrant_url: str, prefix: str) -> AsyncIterator[None]:
        self.url, self.prefix = qdrant_url, prefix
        self.clients: list[Any] = []
        yield
        for client in self.clients:
            await client.close()
        await drop_collections(qdrant_url, prefix)


class TestQdrantServerRest(_ServerKit):
    async def make_component(self, tmp_path: Path) -> QdrantVectorStore:
        return QdrantVectorStore(url=self.url, collection_prefix=self.prefix)


class TestQdrantServerGrpc(_ServerKit):
    async def make_component(self, tmp_path: Path) -> QdrantVectorStore:
        from qdrant_client import AsyncQdrantClient

        client = AsyncQdrantClient(url=self.url, prefer_grpc=True, grpc_port=QDRANT_GRPC_PORT)
        self.clients.append(client)
        return QdrantVectorStore(client=client, collection_prefix=self.prefix)


@pytest.fixture
async def server(qdrant_url: str, prefix: str) -> AsyncIterator[dict[str, Any]]:
    """Builder kwargs for engines sharing one server prefix; collections dropped after."""
    yield {"url": qdrant_url, "collection_prefix": prefix}
    await drop_collections(qdrant_url, prefix)


def dense_engine(
    db: Path, server: dict[str, Any], embedding: HashEmbedding | None = None, **layers: Any
) -> AbstractAsyncContextManager[RetrievalEngine]:
    """Dense-only engine on a shared metadata file and a shared Qdrant server prefix."""
    return running_engine(
        Components(
            metadata_store=SQLiteMetadataStore(db),
            embedding=embedding or HashEmbedding(),
            vector_store=QdrantVectorStore(**server),
        ),
        retrieval=DENSE_ONLY,
        **layers,
    )


async def test_engines_share_pointers_and_generations(
    tmp_path: Path, server: dict[str, Any]
) -> None:
    """A second engine on the same metadata DB and server sees the first engine's writes."""
    scope = Scope(tenant_id="acme")
    db = tmp_path / "meta.db"
    async with dense_engine(db, server) as writer, dense_engine(db, server) as reader:
        report = await writer.ingest(documents(CORPUS), scope=scope)
        assert all(item.outcome.value == "created" for item in report.documents)
        assert not report.warnings
        result = await reader.retrieve("asyncio event loops", scope=scope, config=DENSE)
        assert result.status is ResultStatus.COMPLETE
        assert document_ids(result)[0] == "loops"

        status_writer = await writer.status(scope=scope)
        status_reader = await reader.status(scope=scope)
        assert status_writer.generations == status_reader.generations

        await reader.ingest(
            documents({"tenancy": "Tenant isolation keeps every scope apart."}), scope=scope
        )
        result = await writer.retrieve("tenant isolation scope", scope=scope, config=DENSE)
        assert document_ids(result)[0] == "tenancy"


async def test_reader_lease_of_another_engine_holds_back_retirement(
    tmp_path: Path, server: dict[str, Any]
) -> None:
    """A generation another engine has read is only dropped once that engine lets go."""
    scope = Scope(tenant_id="acme")
    db = tmp_path / "meta.db"
    fast = LifecycleConfig(poll_interval=0.2)
    async with dense_engine(db, server, lifecycle=fast) as owner:
        await owner.ingest(documents(CORPUS), scope=scope)
        (old,) = await active_generations(owner, scope)
        async with dense_engine(
            db, server, lifecycle=LifecycleConfig(background=False)
        ) as other:  # never retires anything itself
            result = await other.retrieve("dense vectors", scope=scope, config=DENSE)
            assert document_ids(result)[0] == "vectors"  # holds a lease on `old`
            ref = await owner.rebuild(scope=scope, channel="index", method="dense")
            assert (await owner.wait_for(ref, timeout=60)).state is TaskState.SUCCEEDED
            await asyncio.sleep(1)  # several maintenance rounds
            states = {
                g.generation_id: g.state for g in (await owner.status(scope=scope)).generations
            }
            assert states[old] is GenerationState.RETIRING
        async with asyncio.timeout(30):  # `other` closed and released its lease
            while True:
                status = await owner.status(scope=scope)
                states = {g.generation_id: g.state for g in status.generations}
                if states.get(old, GenerationState.RETIRED) is GenerationState.RETIRED:
                    break
                await asyncio.sleep(0.2)
        result = await owner.retrieve("dense vectors", scope=scope, config=DENSE)
        assert document_ids(result)[0] == "vectors"


async def test_rebuild_switches_generation_under_concurrent_retrieval(
    tmp_path: Path, server: dict[str, Any]
) -> None:
    """Retrievals running while a rebuild publishes and retires generations never fail."""
    scope = Scope(tenant_id="acme")
    async with dense_engine(tmp_path / "meta.db", server) as engine:
        await engine.ingest(documents(CORPUS), scope=scope)
        before = await active_generations(engine, scope)
        stop = asyncio.Event()
        outcomes: list[RetrievalResult] = []

        async def reader() -> None:
            while not stop.is_set():
                outcomes.append(await engine.retrieve("dense vectors", scope=scope, config=DENSE))

        readers = [asyncio.create_task(reader()) for _ in range(4)]
        try:
            for _ in range(3):
                ref = await engine.rebuild(scope=scope, channel="index", method="dense")
                assert (await engine.wait_for(ref, timeout=60)).state is TaskState.SUCCEEDED
        finally:
            stop.set()
            await asyncio.gather(*readers)

        after = await active_generations(engine, scope)
        assert after.isdisjoint(before)
        assert outcomes
        for result in outcomes:
            assert result.status is ResultStatus.COMPLETE
            assert document_ids(result)[0] == "vectors"


async def test_restart_reconciles_from_durable_state(
    tmp_path: Path, server: dict[str, Any]
) -> None:
    """After a restart the engine serves from the published server generation directly."""
    scope = Scope(tenant_id="acme")
    db = tmp_path / "meta.db"
    async with dense_engine(db, server) as engine:
        await engine.ingest(documents(CORPUS), scope=scope)
        generations = (await engine.status(scope=scope)).generations
    embedding = HashEmbedding()
    async with dense_engine(db, server, embedding) as restarted:
        assert (await restarted.status(scope=scope)).generations == generations
        calls = embedding.calls
        result = await restarted.retrieve("BM25 sparse term statistics", scope=scope, config=DENSE)
        assert document_ids(result)[0] == "bm25"
        assert embedding.calls == calls + 1  # only the query was embedded, nothing re-indexed


async def test_tenants_never_see_each_other(tmp_path: Path, server: dict[str, Any]) -> None:
    """Identical document ids in two tenants stay isolated on a shared server collection."""
    alpha, beta = Scope(tenant_id="alpha"), Scope(tenant_id="beta")
    async with dense_engine(tmp_path / "meta.db", server) as engine:
        await engine.ingest(
            [
                DocumentInput(
                    _id="shared",
                    representations=(
                        ChunksRepresentation(chunks=(ChunkInput(text="alpha secret roadmap"),)),
                    ),
                )
            ],
            scope=alpha,
        )
        await engine.ingest(documents({"shared": "beta public changelog"}), scope=beta)
        for scope, own, foreign in ((alpha, "alpha", "beta"), (beta, "beta", "alpha")):
            for query in ("alpha secret roadmap", "beta public changelog"):
                result = await engine.retrieve(query, scope=scope, config=DENSE, top_k=10)
                assert result.evidence
                for item in result.evidence:
                    assert item.scope == scope
                    assert all(source.scope == scope for source in item.sources)
                    text = " ".join(segment.text or "" for segment in item.segments)
                    assert own in text
                    assert foreign not in text
