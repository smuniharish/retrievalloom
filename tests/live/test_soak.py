"""Load and soak: event-loop responsiveness, tenant backpressure and graceful shutdown."""

from __future__ import annotations

import asyncio
import gc
import time
from typing import TYPE_CHECKING

import pytest

from retrievalloom import (
    Components,
    DocumentInput,
    LifecycleConfig,
    LifecycleError,
    OperationTimeoutError,
    RetrievalConfig,
    RetrievalExecutionConfig,
    Scope,
    TextRepresentation,
)
from retrievalloom.config import TenantPolicy
from retrievalloom.domain.retrieval import ResultStatus
from retrievalloom.runtime.offloader import WorkloadKind
from tests.fakes import HashEmbedding
from tests.live.conftest import running_engine

if TYPE_CHECKING:
    from collections.abc import Sequence

    from retrievalloom.contracts.component import ComponentContext
    from retrievalloom.runtime.offloader import Offloader

pytestmark = pytest.mark.benchmark

DOCUMENTS = [
    DocumentInput(
        _id=f"doc-{index}",
        representations=(
            TextRepresentation(text=f"Document {index} covers topic {index % 7} and item {index}."),
        ),
    )
    for index in range(200)
]


class BlockingEmbedding(HashEmbedding):
    """Hash embedding whose inference blocks a worker thread, like a real CPU model."""

    def __init__(self, block_seconds: float) -> None:
        super().__init__()
        self._block = block_seconds
        self._offloader: Offloader | None = None

    async def initialize(self, context: ComponentContext) -> None:
        """Keep the offloader for inference."""
        self._offloader = context.offloader

    def _blocking(self, texts: list[str]) -> list[list[float]]:
        time.sleep(self._block)
        return [self.vector(text) for text in texts]

    async def embed_documents(self, texts: Sequence[str]) -> Sequence[Sequence[float]]:
        """Embed documents on a worker thread."""
        assert self._offloader is not None
        self.calls += 1
        return await self._offloader.run(WorkloadKind.EMBEDDING, self._blocking, list(texts))

    async def embed_query(self, text: str) -> Sequence[float]:
        """Embed a query on a worker thread."""
        assert self._offloader is not None
        self.calls += 1
        return (await self._offloader.run(WorkloadKind.EMBEDDING, self._blocking, [text]))[0]


class LagProbe:
    """Measures the worst event-loop scheduling delay while it runs."""

    def __init__(self, interval: float = 0.01) -> None:
        self.interval = interval
        self.worst = 0.0
        self._task: asyncio.Task[None] | None = None

    async def _run(self) -> None:
        loop = asyncio.get_running_loop()
        while True:
            start = loop.time()
            await asyncio.sleep(self.interval)
            self.worst = max(self.worst, loop.time() - start - self.interval)

    async def __aenter__(self) -> LagProbe:
        # Earlier live tests leave large model heaps behind; a full collection over them
        # pauses the interpreter for ~0.3s regardless of library behaviour. Freezing the
        # pre-existing heap makes the probe measure stalls caused by the code under test.
        gc.collect()
        gc.freeze()
        self._task = asyncio.create_task(self._run())
        return self

    async def __aexit__(self, *exc: object) -> None:
        assert self._task is not None
        self._task.cancel()
        await asyncio.gather(self._task, return_exceptions=True)
        gc.unfreeze()


async def test_event_loop_stays_responsive_under_load(tmp_path: object) -> None:
    """Ingest and 120 concurrent retrievals never stall the loop: blocking work is offloaded."""
    scope = Scope(tenant_id="acme")
    retrieval = RetrievalConfig(
        embedding_cache_size=0,
        default_tenant_policy=TenantPolicy(max_concurrent_requests=64),
    )
    async with running_engine(
        Components(embedding=BlockingEmbedding(0.02)), retrieval=retrieval
    ) as engine:
        async with LagProbe() as probe:
            report = await engine.ingest(DOCUMENTS, scope=scope)
            assert not report.warnings
            results = await asyncio.gather(
                *(engine.retrieve(f"topic {i % 7} item {i}", scope=scope) for i in range(120))
            )
        assert all(result.status is ResultStatus.COMPLETE for result in results)
        assert all(result.evidence for result in results)
    assert probe.worst < 0.25, f"event loop stalled for {probe.worst:.3f}s"


async def test_tenant_backpressure_isolates_noisy_tenant() -> None:
    """A flooded tenant is shed with a retryable timeout while another tenant is unaffected."""
    noisy, quiet = Scope(tenant_id="noisy"), Scope(tenant_id="quiet")
    retrieval = RetrievalConfig(
        embedding_cache_size=0,
        tenant_policies={"noisy": TenantPolicy(max_concurrent_requests=2)},
    )
    dense = RetrievalExecutionConfig(dense=True, timeout=1.0)
    async with running_engine(
        Components(embedding=BlockingEmbedding(0.3)), retrieval=retrieval
    ) as engine:
        await engine.ingest(DOCUMENTS[:20], scope=noisy)
        await engine.ingest(DOCUMENTS[:20], scope=quiet)
        flood = [
            asyncio.create_task(engine.retrieve(f"topic {i}", scope=noisy, config=dense))
            for i in range(12)
        ]
        await asyncio.sleep(0.05)
        started = time.perf_counter()
        quiet_result = await engine.retrieve("topic 3", scope=quiet, config=dense)
        quiet_latency = time.perf_counter() - started
        outcomes = await asyncio.gather(*flood, return_exceptions=True)

    assert quiet_result.status is ResultStatus.COMPLETE
    assert quiet_latency < 1.0
    shed = [item for item in outcomes if isinstance(item, OperationTimeoutError)]
    served = [item for item in outcomes if not isinstance(item, BaseException)]
    assert shed, "the noisy tenant was never shed"
    assert served, "the noisy tenant was starved completely"
    assert all(item.retryable for item in shed)
    assert len(shed) + len(served) == len(flood)


async def test_graceful_shutdown_drains_in_flight_work() -> None:
    """``close`` lets in-flight retrievals finish, then rejects new work."""
    scope = Scope(tenant_id="acme")
    retrieval = RetrievalConfig(embedding_cache_size=0)
    async with running_engine(
        Components(embedding=BlockingEmbedding(0.2)),
        retrieval=retrieval,
        lifecycle=LifecycleConfig(background=True, poll_interval=0.05, shutdown_timeout=10),
    ) as engine:
        await engine.ingest(DOCUMENTS[:50], scope=scope)
        in_flight = [
            asyncio.create_task(engine.retrieve(f"topic {i}", scope=scope)) for i in range(16)
        ]
        await asyncio.sleep(0.05)
        await engine.close()
        results = await asyncio.gather(*in_flight)
        assert all(result.status is ResultStatus.COMPLETE for result in results)
        with pytest.raises(LifecycleError):
            await engine.retrieve("after close", scope=scope)
