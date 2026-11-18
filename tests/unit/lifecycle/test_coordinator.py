"""Lifecycle coordinator: refresh-engine rounds, tasks, maintenance, background scheduling."""

from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import TYPE_CHECKING

import pytest

from retrievalloom.config import LifecycleConfig
from retrievalloom.domain.records import CoverageState, TaskKind, TaskState
from retrievalloom.errors import (
    BackendUnavailableError,
    ConfigurationError,
    LifecycleError,
    OperationTimeoutError,
    TenantIsolationError,
)
from retrievalloom.lifecycle.canonical import document_uid
from retrievalloom.lifecycle.coordinator import resource_id
from tests.unit.lifecycle.helpers import (
    OTHER,
    SCOPE,
    DictIndex,
    Harness,
    build,
    fail_transaction,
    index_unit,
    text_doc,
)

if TYPE_CHECKING:
    from retrievalloom.domain.records import CoverageRecord
    from retrievalloom.storage.sqlite import SQLiteMetadataStore


def harness(
    store: SQLiteMetadataStore, backend: DictIndex | None = None, **config: object
) -> Harness:
    lifecycle = LifecycleConfig.model_validate({"background": False, **config})
    return build(store, [index_unit(backend or DictIndex())], lifecycle=lifecycle)


async def coverage(h: Harness, document_id: str) -> CoverageRecord:
    uid = document_uid(SCOPE, document_id)
    async with h.store.transaction(SCOPE, write=False) as transaction:
        return (await transaction.get_coverage(h.unit.key(SCOPE), [uid]))[uid]


async def test_unit_lookup_and_resource_ids(store: SQLiteMetadataStore) -> None:
    h = harness(store)
    assert h.coordinator.units == (h.unit,)
    assert h.coordinator.unit("index.sparse") is h.unit
    with pytest.raises(ConfigurationError) as caught:
        h.coordinator.unit("index.dense")
    assert caught.value.details == {"unit": "index.dense"}
    assert resource_id(SCOPE, h.unit) == f"{SCOPE.key}:index.sparse:inst_local"
    durable = index_unit(DictIndex(durable=True))
    assert resource_id(SCOPE, durable) == f"{SCOPE.key}:index.sparse:-"
    with pytest.raises(LifecycleError):
        await h.coordinator.process("nope")


async def test_round_indexes_every_scope_that_lags(store: SQLiteMetadataStore) -> None:
    h = harness(store)
    await h.ingestion.ingest([text_doc("a", "x")], SCOPE)
    await h.ingestion.ingest([text_doc("b", "y")], OTHER)
    result = await h.coordinator.run_once()
    assert not result.errors
    assert {e.resource_id for e in result.executions if e.succeeded} == {
        resource_id(SCOPE, h.unit),
        resource_id(OTHER, h.unit),
    }
    assert (await coverage(h, "a")).state == CoverageState.READY
    # Nothing lags any more: the next round has nothing to do.
    again = await h.coordinator.run_once()
    assert not again.executions
    # A scoped round only touches that scope.
    await h.ingestion.ingest([text_doc("a", "x2")], SCOPE)
    await h.ingestion.ingest([text_doc("b", "y2")], OTHER)
    await h.coordinator.run_once(SCOPE)
    assert (await coverage(h, "a")).indexed_version == 2
    assert await h.indexer.needs_work(OTHER, h.unit)


async def test_failing_unit_fails_its_task_after_max_attempts(
    store: SQLiteMetadataStore,
) -> None:
    backend = DictIndex()
    h = harness(store, backend, max_attempts=2)
    await h.ingestion.ingest([text_doc("a", "x")], SCOPE)
    backend.fail["create_generation"] = BackendUnavailableError("index down")
    ref = await h.coordinator.request(SCOPE, TaskKind.BACKFILL, [h.unit])

    result = await h.coordinator.run_once()
    assert result.errors
    assert [m for m, _ in backend.calls].count("create_generation") == 2  # retried once
    assert "lifecycle_round_errors" in h.logger.events("warning")
    running = await h.coordinator.task_result(ref.task_id, SCOPE)
    assert (running.state, running.attempts, running.error) == (TaskState.RUNNING, 1, None)

    await h.coordinator.run_once()
    failed = await h.coordinator.task_result(ref.task_id, SCOPE)
    assert (failed.state, failed.attempts) == (TaskState.FAILED, 2)
    assert failed.error is not None
    assert failed.error.code == "lifecycle_error"
    assert failed.details == {"units": ["index.sparse"]}

    # Terminal tasks are never touched again.
    await h.coordinator._bump(SCOPE, ref.task_id)
    await h.coordinator._finish(SCOPE, ref.task_id, TaskState.SUCCEEDED)
    await h.coordinator._bump(SCOPE, "task_missing")
    await h.coordinator._finish(SCOPE, "task_missing", TaskState.SUCCEEDED)
    assert (await h.coordinator.task_result(ref.task_id, SCOPE)).state == TaskState.FAILED


async def test_non_retryable_errors_are_not_retried(store: SQLiteMetadataStore) -> None:
    backend = DictIndex()
    h = harness(store, backend, max_attempts=3)
    await h.ingestion.ingest([text_doc("a", "x")], SCOPE)
    backend.fail["create_generation"] = RuntimeError("bug")
    result = await h.coordinator.run_once()
    assert result.errors
    assert [m for m, _ in backend.calls].count("create_generation") == 1
    assert "lifecycle_round_errors" in h.logger.events("warning")


async def test_backfill_retries_failed_rows_and_completes(store: SQLiteMetadataStore) -> None:
    backend = DictIndex()
    h = harness(store, backend, max_attempts=1)
    await h.ingestion.ingest([text_doc("a", "x")], SCOPE)
    await h.coordinator.run_once()
    await h.ingestion.ingest([text_doc("a", "x2")], SCOPE)
    backend.fail["write"] = BackendUnavailableError("index down")
    await h.coordinator.run_once()
    assert (await coverage(h, "a")).state == CoverageState.FAILED
    assert not await h.indexer.needs_work(SCOPE, h.unit)  # failed rows wait for a backfill

    del backend.fail["write"]
    ref = await h.coordinator.request(SCOPE, TaskKind.BACKFILL, [h.unit])
    row = await coverage(h, "a")
    assert (row.state, row.attempts) == (CoverageState.PENDING, 0)
    result = await h.coordinator.wait_for(ref, timeout=10)
    assert result.state == TaskState.SUCCEEDED
    row = await coverage(h, "a")
    assert (row.state, row.indexed_version) == (CoverageState.READY, 2)


async def test_backfill_of_up_to_date_units_succeeds(store: SQLiteMetadataStore) -> None:
    h = harness(store)
    ref = await h.coordinator.request(SCOPE, TaskKind.BACKFILL, [h.unit])
    assert (await h.coordinator.wait_for(ref, timeout=10)).state == TaskState.SUCCEEDED


@pytest.mark.parametrize("kind", [TaskKind.BACKFILL, TaskKind.REBUILD])
async def test_tasks_for_unconfigured_units_fail(
    store: SQLiteMetadataStore, kind: TaskKind
) -> None:
    h = harness(store)
    stranger = index_unit(DictIndex(), method="dense")
    ref = await h.coordinator.request(SCOPE, kind, [h.unit, stranger])
    result = await h.coordinator.wait_for(ref, timeout=10)
    assert result.state == TaskState.FAILED
    assert result.error is not None
    assert result.error.details == {"units": ["index.dense"]}


async def test_backfill_waits_while_the_unit_still_lags(store: SQLiteMetadataStore) -> None:
    h = harness(store)
    await h.ingestion.ingest([text_doc("a", "x")], SCOPE)
    ref = await h.coordinator.request(SCOPE, TaskKind.BACKFILL, [h.unit])
    await h.ingestion.ingest([text_doc("b", "y")], OTHER)
    await h.coordinator.run_once(OTHER)  # SCOPE is not processed: the task stays queued
    assert (await h.coordinator.task_result(ref.task_id, SCOPE)).state == TaskState.QUEUED
    await h.coordinator.run_once()
    assert (await h.coordinator.task_result(ref.task_id, SCOPE)).state == TaskState.SUCCEEDED


async def test_rebuild_task_and_wait_for(store: SQLiteMetadataStore) -> None:
    h = harness(store)
    await h.ingestion.ingest([text_doc("a", "x")], SCOPE)
    await h.coordinator.run_once()
    ref = await h.coordinator.request(SCOPE, TaskKind.REBUILD, [h.unit])
    assert ref.kind == TaskKind.REBUILD
    # Settling before the rebuild ran leaves the task untouched.
    await h.coordinator._settle_tasks(None, await h.coordinator.run_once(OTHER))
    assert (await h.coordinator.task_result(ref.task_id, SCOPE)).state == TaskState.QUEUED
    result = await h.coordinator.wait_for(ref, timeout=10)
    assert result.state == TaskState.SUCCEEDED
    assert "lifecycle_task_queued" in h.logger.events("info")


async def test_rebuild_of_several_units_rebuilds_every_unit(store: SQLiteMetadataStore) -> None:
    lifecycle = LifecycleConfig(background=False)
    units = [index_unit(DictIndex()), index_unit(DictIndex(), method="dense")]
    h = build(store, units, lifecycle=lifecycle)
    await h.ingestion.ingest([text_doc("a", "x")], SCOPE)
    await h.coordinator.run_once()

    async def active() -> dict[str, str]:
        async with store.transaction(SCOPE, write=False) as transaction:
            pointers = {u.name: await transaction.get_pointer(u.key(SCOPE)) for u in units}
        return {name: p.generation_id for name, p in pointers.items() if p is not None}

    before = await active()
    ref = await h.coordinator.request(SCOPE, TaskKind.REBUILD, units)
    # One unit at a time: the task must keep driving the second unit after the first.
    await h.indexer.run(SCOPE, units[0])
    assert (await h.coordinator.task_result(ref.task_id, SCOPE)).state == TaskState.RUNNING
    assert await h.indexer.needs_work(SCOPE, units[1])
    assert not await h.indexer.needs_work(SCOPE, units[0])
    await h.indexer.run(SCOPE, units[1])
    result = await h.coordinator.wait_for(ref, timeout=10)
    assert result.state == TaskState.SUCCEEDED
    after = await active()
    assert set(after) == {"index.sparse", "index.dense"}
    assert all(after[name] != before[name] for name in after)


async def test_wait_for_times_out_and_leaves_the_store_intact(store: SQLiteMetadataStore) -> None:
    h = harness(store)
    await h.ingestion.ingest([text_doc(f"d{i}", f"text {i}") for i in range(20)], SCOPE)
    ref = await h.coordinator.request(SCOPE, TaskKind.REBUILD, [h.unit])
    for _ in range(5):
        # The deadline cancels in-flight metadata operations; the in-memory database
        # must survive (a cancelled driver call used to replace it with an empty one).
        with pytest.raises(OperationTimeoutError) as caught:
            await h.coordinator.wait_for(ref, timeout=0)
        assert caught.value.details == {"task_id": ref.task_id}
    assert (await h.coordinator.wait_for(ref, timeout=10)).state == TaskState.SUCCEEDED
    assert (await coverage(h, "d0")).state == CoverageState.READY


async def test_task_result_is_scoped(store: SQLiteMetadataStore) -> None:
    h = harness(store)
    ref = await h.coordinator.request(SCOPE, TaskKind.REBUILD, [h.unit])
    with pytest.raises(TenantIsolationError):
        await h.coordinator.task_result(ref.task_id, OTHER)
    with pytest.raises(LifecycleError) as caught:
        await h.coordinator.task_result("task_missing", SCOPE)
    assert caught.value.details == {"task_id": "task_missing"}


async def test_start_without_background_only_registers(store: SQLiteMetadataStore) -> None:
    h = harness(store)
    await h.coordinator.start()
    assert not h.coordinator.background
    h.coordinator.kick()  # no scheduler: no-op
    later = h.clock() + timedelta(days=1)
    async with store.transaction(write=True) as transaction:
        assert await transaction.expired_instances(now=later) == [h.instance_id]
        await transaction.put_reader_lease("gen", h.instance_id, expires_at=later, now=h.clock())
        await transaction.put_reader_lease("gen", "inst_gone", expires_at=h.clock(), now=h.clock())
    await h.coordinator.maintain()  # purges the expired lease
    async with store.transaction(write=False) as transaction:
        assert await transaction.count_reader_leases("gen", now=h.clock() - timedelta(1)) == 1
    await h.close()
    async with store.transaction(write=False) as transaction:
        assert await transaction.expired_instances(now=later) == []
        assert await transaction.count_reader_leases("gen", now=h.clock()) == 0
    assert "engine_instance_deregistered" in h.logger.events("info")


async def test_tick_reports_failures_without_raising(
    store: SQLiteMetadataStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    h = harness(store)
    await h.ingestion.ingest([text_doc("a", "x")], SCOPE)
    await h.coordinator.tick()
    assert (await coverage(h, "a")).state == CoverageState.READY
    fail_transaction(monkeypatch, "heartbeat_instance", lambda: RuntimeError("db gone"))
    await h.coordinator.tick()
    _, _, fields = next(r for r in h.logger.records if r[1] == "lifecycle_tick_failed")
    assert fields == {
        "error_type": "RuntimeError",
        "error_code": "unexpected_error",
        "error": "RuntimeError",
        "details": {},
    }


async def test_background_scheduler_runs_rounds(store: SQLiteMetadataStore) -> None:
    h = harness(store, background=True, poll_interval=0.05)
    assert not h.coordinator.background
    h.coordinator.kick()  # no scheduler yet: no-op
    await h.coordinator.start()
    try:
        assert h.coordinator.background
        await h.ingestion.ingest([text_doc("a", "x")], SCOPE)
        ref = await h.coordinator.request(SCOPE, TaskKind.BACKFILL, [h.unit])
        h.coordinator.kick()  # a kicked round is already running: no-op
        result = await h.coordinator.wait_for(ref, timeout=10)
        assert result.state == TaskState.SUCCEEDED
        assert (await coverage(h, "a")).state == CoverageState.READY
    finally:
        await h.close()
    assert not h.coordinator.background
    await h.close()  # idempotent


async def test_kicked_round_failure_is_logged(
    store: SQLiteMetadataStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    h = harness(store, background=True, poll_interval=60)
    await h.coordinator.start()
    try:
        await asyncio.sleep(0.05)  # let the immediate scheduled round finish

        async def broken(scope: object = None) -> None:
            raise RuntimeError("round failed")

        monkeypatch.setattr(h.coordinator, "run_once", broken)
        h.coordinator.kick(SCOPE)
        for _ in range(100):
            if "lifecycle_round_failed" in h.logger.events("error"):
                break
            await asyncio.sleep(0.01)
        assert "lifecycle_round_failed" in h.logger.events("error")
    finally:
        await h.close()


async def test_stop_cancels_a_kicked_round(
    store: SQLiteMetadataStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    h = harness(store, background=True, poll_interval=60)
    await h.coordinator.start()
    started = asyncio.Event()

    async def slow(scope: object = None) -> None:
        started.set()
        await asyncio.sleep(60)

    monkeypatch.setattr(h.coordinator, "run_once", slow)
    h.coordinator.kick()
    await started.wait()
    await h.close()
    assert "lifecycle_round_failed" not in h.logger.events("error")


# automatic retry of failed coverage -------------------------------------------------------
async def failed_after_outage(h: Harness, backend: DictIndex, error: Exception) -> None:
    """Index ``a`` and ``b``, then fail their update until their coverage is ``failed``."""
    await h.ingestion.ingest([text_doc("a", "x"), text_doc("b", "y")], SCOPE)
    await h.coordinator.run_once()
    backend.fail["write"] = error
    await h.ingestion.ingest([text_doc("a", "x2"), text_doc("b", "y2")], SCOPE)
    await run_until_failed(h)


async def run_until_failed(h: Harness) -> None:
    for _ in range(4):  # a round stops at its first failing batch
        await h.coordinator.run_once()
    assert (await coverage(h, "a")).state == CoverageState.FAILED
    assert (await coverage(h, "b")).state == CoverageState.FAILED


def requeued(h: Harness) -> list[int]:
    return [f["documents"] for _, e, f in h.logger.records if e == "failed_coverage_requeued"]


async def test_retryable_failures_are_requeued_with_exponential_backoff(
    store: SQLiteMetadataStore,
) -> None:
    backend = DictIndex(durable=True)
    h = harness(
        store,
        backend,
        max_attempts=1,
        batch_size=1,
        retry_failed_after=10.0,
        retry_failed_max_delay=25.0,
    )
    await failed_after_outage(h, backend, BackendUnavailableError("qdrant down"))
    delays = []
    for _ in range(3):
        waited = 0
        while (await coverage(h, "a")).state == CoverageState.FAILED:
            h.clock.advance(5)
            waited += 5
            await h.coordinator.maintain(housekeeping=False)
        delays.append(waited)
        assert (await coverage(h, "b")).state == CoverageState.PENDING
        await run_until_failed(h)  # fails again: one more attempt, a longer delay
    assert delays == [10, 20, 25]
    assert (await coverage(h, "a")).attempts == 4
    assert requeued(h) == [2, 2, 2]
    del backend.fail["write"]
    h.clock.advance(25)
    await h.coordinator.tick()
    assert (await coverage(h, "a")).state == CoverageState.READY
    assert (await coverage(h, "b")).state == CoverageState.READY


async def test_the_retry_sweep_is_throttled(store: SQLiteMetadataStore) -> None:
    backend = DictIndex()
    h = harness(store, backend, max_attempts=1, retry_failed_after=10.0)
    await failed_after_outage(h, backend, BackendUnavailableError("down"))
    await h.coordinator.maintain(housekeeping=False)  # sweeps: not due yet
    h.clock.advance(9.9)
    await h.coordinator.maintain(housekeeping=False)  # sweeps: still not due
    h.clock.advance(0.2)
    await h.coordinator.maintain(housekeeping=False)  # due, but swept 0.2 s ago: skipped
    assert (await coverage(h, "a")).state == CoverageState.FAILED
    h.clock.advance(4.9)
    await h.coordinator.maintain(housekeeping=False)
    assert (await coverage(h, "a")).state == CoverageState.PENDING


@pytest.mark.parametrize(
    ("error", "config"),
    [
        (RuntimeError("bug"), {"retry_failed_after": 1.0}),
        (BackendUnavailableError("down"), {"retry_failed_after": None}),
    ],
    ids=["non-retryable", "disabled"],
)
async def test_failures_stay_failed_until_a_backfill(
    store: SQLiteMetadataStore, error: Exception, config: dict[str, object]
) -> None:
    backend = DictIndex()
    h = harness(store, backend, max_attempts=1, **config)
    await failed_after_outage(h, backend, error)
    for _ in range(3):
        h.clock.advance(3600)
        await h.coordinator.maintain(housekeeping=False)
    assert (await coverage(h, "a")).state == CoverageState.FAILED
    assert requeued(h) == []


async def test_unit_timeout_signals_overload(store: SQLiteMetadataStore) -> None:
    backend = DictIndex()
    h = harness(store, backend, unit_timeout=0.05)
    await h.ingestion.ingest([text_doc("a", "x")], SCOPE)

    async def stall() -> None:
        await asyncio.sleep(5)

    backend.hooks["create_generation"] = stall
    result = await h.coordinator.run_once()
    assert h.coordinator._limiter.state()["reason"].startswith("overload")
    assert {issue.exception_type for issue in result.errors} <= {"OperationTimeoutError"}
