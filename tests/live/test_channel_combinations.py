"""Channel on/off scenarios on live durable backends, and automatic outage recovery.

The scenarios are the offline ones (``tests/unit/retrieval/channel_scenarios.py``)
run on Postgres or SQLite metadata with a Qdrant server, a Ladybug graph and a
TreeSearch structure engine, restarting the engine with other channels in between.

``test_dense_coverage_recovers_after_qdrant_outage`` is opt-in with
``RL_TEST_FAULTS=1`` (needs the ``podman`` CLI): it creates its own ``rl-chan-qdrant``
container (image of ``rl-qdrant``), stops it in the middle of ingestion, starts it
again and expects coverage to return to ``ready`` without a backfill.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import time
from typing import TYPE_CHECKING

import pytest

from retrievalloom import (
    Components,
    DocumentInput,
    IndexWait,
    IngestionConfig,
    LifecycleConfig,
    RerankingConfig,
    RetrievalConfig,
    Scope,
    TextRepresentation,
)
from retrievalloom.domain.retrieval import ResultStatus
from retrievalloom.index.vector import QdrantVectorStore
from retrievalloom.storage.sqlite import SQLiteMetadataStore
from tests.fakes import CapitalizedExtractor, HashEmbedding
from tests.live._reliability_helpers import Container, image_of, podman_available, service_host
from tests.live.combo_backends import Combo, ComboRun, Graph, Meta, Vector
from tests.live.conftest import drop_collections, running_engine
from tests.unit.retrieval import channel_scenarios as scenarios

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Awaitable, Callable, Iterator
    from pathlib import Path

    from retrievalloom import RetrievalEngine

pytestmark = [pytest.mark.graph, pytest.mark.structure]

SCOPE = Scope(tenant_id="channels", namespace="live")
BACKENDS = [
    pytest.param(
        Combo(meta, Vector.QDRANT_SERVER, Graph.LADYBUG, structure=True),
        id=f"{meta}-qdrant-server",
        marks=Combo(meta, Vector.QDRANT_SERVER, Graph.LADYBUG, structure=True).marks(),
    )
    for meta in (Meta.SQLITE, Meta.POSTGRES)
]
SCENARIOS = [
    scenarios.every_channel_combination,
    scenarios.enable_later,
    scenarios.catch_up_window,
    scenarios.disable_change_and_reenable,
    scenarios.swap_channels,
]


def _opener(run: ComboRun) -> scenarios.Opener:
    @contextlib.asynccontextmanager
    async def open_engine(
        channels: frozenset[str], lifecycle: LifecycleConfig
    ) -> AsyncIterator[RetrievalEngine]:
        components = Components(
            metadata_store=run.metadata_store(),
            vector_store=run.vector_store(),
            graph_store=run.graph_store(),
            structure_engine=run.structure_engine(),
            embedding=HashEmbedding(),
            graph_extractor=CapitalizedExtractor(),
        )
        async with running_engine(
            components,
            run.data_dir,
            retrieval=scenarios.retrieval_config(channels),
            lifecycle=lifecycle,
        ) as engine:
            yield engine

    return open_engine


@pytest.mark.parametrize("combo", BACKENDS)
@pytest.mark.parametrize("scenario", SCENARIOS, ids=lambda scenario: scenario.__name__)
async def test_channel_scenario(
    combo: Combo,
    scenario: Callable[[scenarios.Opener, Scope], Awaitable[None]],
    tmp_path: Path,
) -> None:
    async with ComboRun(combo, tmp_path) as run:
        await scenario(_opener(run), SCOPE)


# automatic recovery after a transient outage -------------------------------------------------
FAULTS = os.environ.get("RL_TEST_FAULTS") == "1"
RECOVERY_PORT = int(os.environ.get("RL_TEST_CHANNELS_QDRANT_PORT", "6353"))
RECOVERY_BOUND = 120.0


@pytest.fixture(scope="module")
def outage_qdrant() -> Iterator[tuple[Container, str]]:
    """A dedicated Qdrant container (never one of the shared ``rl-*`` services)."""
    container = Container(
        name="rl-chan-qdrant",
        image=image_of("rl-qdrant", "docker.io/qdrant/qdrant:latest"),
        ports=((RECOVERY_PORT, 6333),),
    )
    container.create()
    try:
        yield container, f"http://{service_host()}:{RECOVERY_PORT}"
    finally:
        container.remove()


async def _qdrant_ready(url: str) -> None:
    import httpx

    deadline = time.monotonic() + RECOVERY_BOUND
    async with httpx.AsyncClient(timeout=2) as client:
        while True:
            with contextlib.suppress(httpx.HTTPError):
                if (await client.get(f"{url}/readyz")).status_code == 200:
                    return
            assert time.monotonic() < deadline, "Qdrant did not become ready"
            await asyncio.sleep(0.5)


async def _dense_coverage(
    engine: RetrievalEngine, done: Callable[[dict[str, int]], bool]
) -> dict[str, int]:
    deadline = time.monotonic() + RECOVERY_BOUND
    while True:
        counts = (await engine.status(scope=SCOPE)).coverage.get("index.dense", {})
        if done(counts):
            return counts
        assert time.monotonic() < deadline, counts
        await asyncio.sleep(0.5)


def _docs(prefix: str) -> list[DocumentInput]:
    return [
        DocumentInput(
            _id=f"{prefix}-{n}", representations=(TextRepresentation(text=f"{prefix}word {n}"),)
        )
        for n in range(5)
    ]


@pytest.mark.qdrant
@pytest.mark.skipif(not FAULTS, reason="set RL_TEST_FAULTS=1 to run fault injection")
@pytest.mark.skipif(FAULTS and not podman_available(), reason="needs the podman CLI")
async def test_dense_coverage_recovers_after_qdrant_outage(
    outage_qdrant: tuple[Container, str], tmp_path: Path, prefix: str
) -> None:
    container, url = outage_qdrant
    await _qdrant_ready(url)
    components = Components(
        metadata_store=SQLiteMetadataStore(tmp_path / "metadata.db"),
        vector_store=QdrantVectorStore(url=url, collection_prefix=prefix, timeout=3),
        embedding=HashEmbedding(),
    )
    lifecycle = LifecycleConfig(
        background=True,
        poll_interval=0.2,
        max_attempts=2,
        retry_failed_after=1.0,
        retry_failed_max_delay=4.0,
    )
    try:
        async with running_engine(
            components,
            tmp_path,
            retrieval=RetrievalConfig(reranking=RerankingConfig(enabled=False)),
            ingestion=IngestionConfig(index_wait=IndexWait.NONE),
            lifecycle=lifecycle,
        ) as engine:
            await engine.ingest(_docs("base"), scope=SCOPE)
            await _dense_coverage(engine, lambda counts: counts == {"ready": 5})

            await container.stop()
            await engine.ingest(_docs("outage"), scope=SCOPE)
            failed = await _dense_coverage(engine, lambda counts: counts.get("failed", 0) == 5)
            assert failed == {"ready": 5, "failed": 5}

            await container.start()
            await _qdrant_ready(url)
            # No backfill: the lifecycle re-queues the retryable failures by itself.
            await _dense_coverage(engine, lambda counts: counts == {"ready": 10})
            result = await engine.retrieve("outageword", scope=SCOPE, top_k=10)
            assert result.status is ResultStatus.COMPLETE, result.warnings
            dense = {
                source.document_id
                for item in result.evidence
                if any(hit.method.method == "dense" for hit in item.provenance.hits)
                for source in item.sources
            }
            assert {f"outage-{n}" for n in range(5)} <= dense
    finally:
        await drop_collections(url, prefix)
