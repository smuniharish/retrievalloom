"""Engine surface: lifecycle state machine, operations, ownership, shutdown, isolation."""

from __future__ import annotations

import asyncio
import contextlib
import functools
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any

import pytest

from retrievalloom import (
    AccessDeniedError,
    Components,
    ConfigurationError,
    Eq,
    IndexWait,
    IngestionConfig,
    LifecycleConfig,
    LifecycleError,
    OperationTimeoutError,
    ProvenanceError,
    RetrievalConfig,
    RetrievalExecutionConfig,
    Scope,
    TenantIsolationError,
    TenantPolicy,
    ValidationError,
    WarningCode,
)
from retrievalloom.contracts.infra import Action
from retrievalloom.domain.records import Channel, TaskKind, TaskState
from retrievalloom.engine import EngineState
from retrievalloom.index.vector import QdrantVectorStore
from retrievalloom.infra.cache import NoCache
from retrievalloom.integrations.agxai import AgxaiProvenanceRecorder
from tests.fakes import HashEmbedding
from tests.unit.retrieval.helpers import (
    ACME,
    GLOBEX,
    RecordingRecorder,
    TablePolicy,
    build_engine,
    chunks_doc,
    opened,
    runtime,
    text_doc,
    unit,
)

if TYPE_CHECKING:
    from pathlib import Path

    from retrievalloom import RetrievalEngine
    from retrievalloom.domain.retrieval import RetrievalHit

DOCS = [
    text_doc("d1", "Python asyncio event loops schedule coroutines.", lang="en"),
    chunks_doc(
        "d2", "Qdrant stores dense vectors.", "BM25 ranks sparse tokens for asyncio.", lang="de"
    ),
]


def _docs(result: Any) -> set[str]:
    return {s.document_id for e in result.evidence for s in e.sources}


def _slow_search(monkeypatch: pytest.MonkeyPatch, engine: RetrievalEngine, delay: float) -> None:
    backend = unit(engine, "index.sparse").backend
    original = functools.partial(type(backend).search, backend)

    async def search(*args: Any, **kwargs: Any) -> list[RetrievalHit]:
        await asyncio.sleep(delay)
        return list(await original(*args, **kwargs))

    monkeypatch.setattr(backend, "search", search)


# lifecycle state machine ------------------------------------------------------------------
async def test_state_machine() -> None:
    engine = build_engine()
    assert engine.state == EngineState.NEW
    assert engine.engine_id.startswith("engine")
    assert engine.instance_id.startswith("inst")
    assert engine.config.lifecycle.background is False
    with pytest.raises(LifecycleError):
        await engine.retrieve("asyncio", scope=ACME)
    async with engine as entered:
        assert entered is engine
        assert engine.state == EngineState.OPEN
        await engine.initialize()
        assert engine.state == EngineState.OPEN
    assert engine.state == EngineState.CLOSED
    await engine.close()
    with pytest.raises(LifecycleError):
        await engine.initialize()
    with pytest.raises(LifecycleError):
        await engine.status(scope=ACME)

    never_opened = build_engine()
    await never_opened.close()
    assert never_opened.state == EngineState.CLOSED
    with pytest.raises(LifecycleError):
        await never_opened.initialize()


async def test_failed_build_cleans_up_and_stays_new() -> None:
    policy = TablePolicy()
    engine = build_engine(
        components=Components(vector_store=QdrantVectorStore(), access_policy=policy)
    )
    with pytest.raises(ConfigurationError, match="embedding"):
        await engine.initialize()
    assert engine.state == EngineState.NEW
    assert not policy.closed


async def test_failed_startup_round_stops_the_coordinator(monkeypatch: pytest.MonkeyPatch) -> None:
    from retrievalloom.lifecycle.coordinator import LifecycleCoordinator

    stopped: list[bool] = []
    original_stop = LifecycleCoordinator.stop

    async def run_once(self: Any, scope: Scope | None = None) -> Any:
        raise RuntimeError("startup round")

    async def stop(self: Any) -> None:
        stopped.append(True)
        await original_stop(self)

    monkeypatch.setattr(LifecycleCoordinator, "run_once", run_once)
    monkeypatch.setattr(LifecycleCoordinator, "stop", stop)
    engine = build_engine()
    with pytest.raises(RuntimeError, match="startup round"):
        await engine.initialize()
    assert stopped == [True]
    assert engine.state == EngineState.NEW


async def test_application_components_are_never_closed(monkeypatch: pytest.MonkeyPatch) -> None:
    policy = TablePolicy()
    closed: list[str] = []

    async def failing_close(self: Any) -> None:
        closed.append("cache")
        raise RuntimeError("close failed")

    monkeypatch.setattr(NoCache, "close", failing_close)
    async with opened(components=Components(access_policy=policy)) as engine:
        await engine.ingest(DOCS, scope=ACME)
    assert engine.state == EngineState.CLOSED
    assert closed == ["cache"]
    assert not policy.closed


# shutdown ---------------------------------------------------------------------------------
async def test_close_drains_in_flight_requests(monkeypatch: pytest.MonkeyPatch) -> None:
    engine = build_engine()
    await engine.initialize()
    await engine.ingest(DOCS, scope=ACME)
    _slow_search(monkeypatch, engine, 0.3)
    request = asyncio.ensure_future(engine.retrieve("asyncio", scope=ACME))
    await asyncio.sleep(0.05)
    await engine.close()
    assert request.done()
    assert _docs(request.result())
    assert engine.state == EngineState.CLOSED


async def test_close_waits_for_and_cancels_background_rounds(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ingestion = IngestionConfig(index_wait_timeout=0.05)
    finished: list[str] = []

    def slow_round(delay: float) -> Any:
        async def run_once(scope: Scope | None = None) -> Any:
            try:
                await asyncio.sleep(delay)
            except asyncio.CancelledError:
                finished.append("cancelled")
                raise
            finished.append("done")
            return SimpleNamespace(executions=())

        return run_once

    engine = build_engine(ingestion=ingestion)
    await engine.initialize()
    monkeypatch.setattr(runtime(engine).coordinator, "run_once", slow_round(0.2))
    report = await engine.ingest(DOCS, scope=ACME)
    assert [w.code for w in report.warnings] == [WarningCode.INDEX_WAIT_TIMEOUT]
    await engine.close()
    assert finished == ["done"]

    lifecycle = LifecycleConfig(background=False, shutdown_timeout=0.1)
    engine = build_engine(ingestion=ingestion, lifecycle=lifecycle)
    await engine.initialize()
    await engine.ingest(DOCS, scope=ACME)
    await asyncio.gather(*engine._background)
    monkeypatch.setattr(runtime(engine).coordinator, "run_once", slow_round(10))
    await engine.ingest([text_doc("d3", "late asyncio document")], scope=ACME)
    _slow_search(monkeypatch, engine, 10)
    # an in-flight request keeps the engine from draining within shutdown_timeout
    stuck = asyncio.ensure_future(engine.retrieve("asyncio", scope=ACME))
    await asyncio.sleep(0.05)
    assert not stuck.done()
    await engine.close()
    assert finished == ["done", "cancelled"]
    assert engine.state == EngineState.CLOSED
    stuck.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await stuck


# writes -----------------------------------------------------------------------------------
async def test_ingest_authorization_and_batch_limit() -> None:
    policy = TablePolicy(denied={Action.INGEST, Action.DELETE})
    retrieval = RetrievalConfig(tenant_policies={"acme": TenantPolicy(max_ingest_batch=1)})
    async with opened(components=Components(access_policy=policy), retrieval=retrieval) as engine:
        with pytest.raises(AccessDeniedError) as denied:
            await engine.ingest(DOCS[:1], scope=GLOBEX)
        with pytest.raises(AccessDeniedError):
            await engine.delete(["d1"], scope=GLOBEX)
        policy.denied = set()
        with pytest.raises(ValidationError) as limited:
            await engine.ingest(DOCS, scope=ACME)
        await engine.ingest(DOCS, scope=GLOBEX)
    assert denied.value.details == {"action": "ingest", "reason": "listed"}
    assert limited.value.details == {"count": 2, "max": 1}


async def test_index_after_write_reports_failures(monkeypatch: pytest.MonkeyPatch) -> None:
    async with opened() as engine:
        coordinator = runtime(engine).coordinator

        async def broken(scope: Scope | None = None) -> Any:
            raise RuntimeError("round")

        monkeypatch.setattr(coordinator, "run_once", broken)
        crashed = await engine.ingest(DOCS[:1], scope=ACME)

        async def partial(scope: Scope | None = None) -> Any:
            executions = [
                SimpleNamespace(resource_id="a", succeeded=False),
                SimpleNamespace(resource_id="a", succeeded=False),
                SimpleNamespace(resource_id="b", succeeded=True),
            ]
            return SimpleNamespace(executions=executions)

        monkeypatch.setattr(coordinator, "run_once", partial)
        failed = await engine.ingest(DOCS[1:], scope=ACME)
    [crash] = crashed.warnings
    assert (crash.code, crash.details) == (
        WarningCode.STAGE_FAILED_DEGRADED,
        {"stage": "indexing", "error_type": "RuntimeError"},
    )
    [fail] = failed.warnings
    assert fail.details == {"stage": "indexing", "failed": 1}


async def test_index_wait_none_leaves_indexing_to_lifecycle() -> None:
    async with opened(ingestion=IngestionConfig(index_wait=IndexWait.NONE)) as engine:
        report = await engine.ingest(DOCS, scope=ACME)
        before = await engine.retrieve("asyncio", scope=ACME)
        await engine.run_lifecycle_once(ACME)
        after = await engine.retrieve("asyncio", scope=ACME)
    assert report.warnings == ()
    assert before.evidence == ()
    assert WarningCode.INDEX_MISSING in {w.code for w in before.warnings}
    assert _docs(after) == {"d1", "d2"}


async def test_write_recording_failures() -> None:
    recorder = RecordingRecorder(RuntimeError("down"))
    async with opened(components=Components(provenance_recorder=recorder)) as engine:
        report = await engine.ingest(DOCS, scope=ACME)
        deleted = await engine.delete(["d1"], scope=ACME)
        recorder.error = TenantIsolationError("leak")
        with pytest.raises(TenantIsolationError):
            await engine.delete(["d2"], scope=ACME)
    assert [w.code for w in report.warnings] == [WarningCode.PROVENANCE_RECORDING_FAILED]
    assert deleted.deleted == ("d1",)

    strict = RetrievalConfig(fail_on_recording_error=True)
    recorder = RecordingRecorder(RuntimeError("down"))
    async with opened(retrieval=strict, components=Components(provenance_recorder=recorder)) as e:
        with pytest.raises(ProvenanceError):
            await e.ingest(DOCS, scope=ACME)


# retrieve ---------------------------------------------------------------------------------
async def test_invalid_request_reports_error_locations() -> None:
    async with opened() as engine:
        with pytest.raises(ValidationError) as caught:
            await engine.retrieve("", scope=ACME)
    assert caught.value.details == {"errors": [{"loc": "query", "type": "string_too_short"}]}


async def test_tenant_concurrency_slot_times_out(monkeypatch: pytest.MonkeyPatch) -> None:
    retrieval = RetrievalConfig(tenant_policies={"acme": TenantPolicy(max_concurrent_requests=1)})
    async with opened(retrieval=retrieval) as engine:
        await engine.ingest(DOCS, scope=ACME)
        await engine.ingest(DOCS, scope=GLOBEX)
        _slow_search(monkeypatch, engine, 0.5)
        busy = asyncio.ensure_future(engine.retrieve("asyncio", scope=ACME))
        await asyncio.sleep(0.05)
        quick = RetrievalExecutionConfig(timeout=0.05)
        with pytest.raises(OperationTimeoutError) as caught:
            await engine.retrieve("asyncio", scope=ACME, config=quick)
        # another tenant has its own slots
        other = await engine.retrieve("asyncio", scope=GLOBEX)
        assert _docs(await busy) == {"d1", "d2"}
    assert caught.value.details == {"tenant": "acme"}
    assert caught.value.retryable
    assert _docs(other) == {"d1", "d2"}


async def test_row_filter_and_retrieve_authorization() -> None:
    policy = TablePolicy(filters={"bob": Eq(field="lang", value="de")})
    async with opened(components=Components(access_policy=policy)) as engine:
        await engine.ingest(DOCS, scope=ACME)
        bob = await engine.retrieve("asyncio", scope=ACME, principal="bob")
        alice = await engine.retrieve("asyncio", scope=ACME, principal="alice")
        policy.denied = {Action.RETRIEVE}
        with pytest.raises(AccessDeniedError):
            await engine.retrieve("asyncio", scope=ACME, principal="bob")
    assert _docs(bob) == {"d2"}
    assert _docs(alice) == {"d1", "d2"}


async def test_tenants_are_isolated_across_operations() -> None:
    async with opened() as engine:
        await engine.ingest([text_doc("a1", "shared asyncio words")], scope=ACME)
        await engine.ingest([text_doc("b1", "shared asyncio words")], scope=GLOBEX)
        await engine.delete(["a1"], scope=GLOBEX)
        acme = await engine.retrieve("shared asyncio", scope=ACME)
        globex = await engine.retrieve("shared asyncio", scope=GLOBEX)
        acme_status = await engine.status(scope=ACME)
    assert _docs(acme) == {"a1"}
    assert _docs(globex) == {"b1"}
    assert acme_status.documents_active == 1
    assert acme_status.documents_deleted == 0


# lifecycle operations ----------------------------------------------------------------------
async def test_backfill_rebuild_wait_status() -> None:
    async with opened() as engine:
        await engine.ingest(DOCS, scope=ACME)
        backfill = await engine.backfill(scope=ACME, channels=["index"], methods=["sparse"])
        done = await engine.wait_for(backfill, timeout=30)
        rebuild = await engine.rebuild(scope=ACME, channel=Channel.INDEX, method="keyword")
        rebuilt = await engine.wait_for(rebuild, timeout=30)
        everything = await engine.backfill(scope=ACME)
        await engine.wait_for(everything, timeout=30)
        status = await engine.status(scope=ACME)
        with pytest.raises(ConfigurationError, match="unknown channel"):
            await engine.backfill(scope=ACME, channels=["bogus"])
        with pytest.raises(ConfigurationError) as unmatched:
            await engine.rebuild(scope=ACME, channel="graph")
        with pytest.raises(ConfigurationError):
            await engine.backfill(scope=ACME, methods=["dense"])
        result = await engine.retrieve("asyncio", scope=ACME)
    assert (backfill.kind, done.state) == (TaskKind.BACKFILL, TaskState.SUCCEEDED)
    assert (rebuild.kind, rebuilt.state) == (TaskKind.REBUILD, TaskState.SUCCEEDED)
    assert unmatched.value.details == {"channels": ["graph"], "methods": None}
    assert status.documents_active == 2
    assert status.tasks == {"succeeded": 3}
    assert {g.method for g in status.generations} >= {"index.keyword", "index.sparse"}
    assert status.coverage
    assert _docs(result) == {"d1", "d2"}


async def test_schedule_backfill_reuses_active_tasks() -> None:
    engine = build_engine(ingestion=IngestionConfig(index_wait=IndexWait.NONE))
    assert await engine._schedule_backfill(ACME, []) is None
    async with engine:
        units = [unit(engine, "index.sparse")]
        await engine.rebuild(scope=ACME, channel="index", method="sparse")
        first = await engine._schedule_backfill(ACME, units)
        again = await engine._schedule_backfill(ACME, units)
        wider = await engine._schedule_backfill(ACME, [*units, unit(engine, "index.keyword")])
    assert first is not None
    assert again == first
    assert wider not in {None, first}


async def test_health_reports_component_failures(monkeypatch: pytest.MonkeyPatch) -> None:
    async with opened() as engine:
        healthy = await engine.health()

        async def broken() -> Any:
            raise RuntimeError("sick")

        monkeypatch.setattr(runtime(engine).access, "health", broken)
        sick = await engine.health()
    assert healthy.status.value == "healthy"
    assert healthy.details["units"] == ["index.fuzzy", "index.keyword", "index.sparse"]
    assert sick.status.value == "unhealthy"
    [report] = [c for c in sick.components if c.status.value == "unhealthy"]
    assert report.details == {"error_type": "RuntimeError"}


# defaults and background --------------------------------------------------------------------
async def test_xai_runtime_selects_the_agxai_recorder() -> None:
    async with opened(xai_runtime=SimpleNamespace(current_run=None)) as engine:
        assert isinstance(runtime(engine).recorder, AgxaiProvenanceRecorder)
        await engine.ingest(DOCS, scope=ACME)
        assert _docs(await engine.retrieve("asyncio", scope=ACME)) == {"d1", "d2"}


async def test_persistent_defaults_under_data_dir(tmp_path: Path) -> None:
    components = Components(embedding=HashEmbedding())
    async with opened(components=components, data_dir=tmp_path) as engine:
        await engine.ingest(DOCS, scope=ACME)
        result = await engine.retrieve("asyncio", scope=ACME)
    assert "index.dense" in result.snapshot.generations
    assert (tmp_path / "metadata.db").exists()
    assert (tmp_path / "qdrant").exists()


@pytest.mark.graph
@pytest.mark.structure
async def test_graph_and_structure_defaults_under_data_dir(tmp_path: Path) -> None:
    from retrievalloom import ChannelsConfig, GraphChannelConfig, StructureChannelConfig

    retrieval = RetrievalConfig(
        channels=ChannelsConfig(
            graph=GraphChannelConfig(enabled=True), structure=StructureChannelConfig(enabled=True)
        )
    )
    async with opened(retrieval=retrieval, data_dir=tmp_path) as engine:
        assert {"graph.graph", "structure.structure"} <= set(runtime(engine).units)
    assert (tmp_path / "graph").exists()


async def test_background_lifecycle_indexes_without_waiting() -> None:
    lifecycle = LifecycleConfig(background=True, poll_interval=0.05)
    ingestion = IngestionConfig(index_wait=IndexWait.NONE)
    async with opened(lifecycle=lifecycle, ingestion=ingestion) as engine:
        await engine.ingest(DOCS, scope=ACME)
        result = None
        for _ in range(200):
            result = await engine.retrieve("asyncio", scope=ACME)
            if _docs(result) == {"d1", "d2"}:
                break
            await asyncio.sleep(0.05)
        assert result is not None
        assert _docs(result) == {"d1", "d2"}
        status = await engine.status(scope=ACME)
    assert status.documents_active == 2
    assert engine.state == EngineState.CLOSED
