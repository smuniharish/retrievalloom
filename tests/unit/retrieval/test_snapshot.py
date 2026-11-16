"""Snapshot capture: commit sequence, generation pinning, reader registration, coverage flags."""

from __future__ import annotations

import contextlib
import dataclasses
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, cast

import pytest

from retrievalloom import (
    CoverageError,
    IndexWait,
    IngestionConfig,
    RetrievalConfig,
    RetrievalExecutionConfig,
    Scope,
    Severity,
    WarningCode,
)
from retrievalloom.domain.base import utc_now
from retrievalloom.domain.records import CoverageRecord, CoverageState
from retrievalloom.domain.retrieval import RetrievalRequest
from retrievalloom.index.text import SimpleTokenizer
from retrievalloom.lifecycle.indexer import ReaderTracker
from retrievalloom.retrieval.planning import resolve_plan
from retrievalloom.retrieval.snapshot import SnapshotReader
from tests.unit.retrieval.helpers import ACME, opened, runtime, text_doc

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Callable

    from retrievalloom import RetrievalEngine
    from retrievalloom.domain.records import IndexGeneration
    from retrievalloom.lifecycle.units import IndexUnit
    from retrievalloom.storage.base import PointerRow, UnitKey

KEYWORD = "index.keyword"


def _plan(engine: RetrievalEngine, units: dict[str, IndexUnit] | None = None, **execution: Any):
    rt = runtime(engine)
    request = RetrievalRequest(
        request_id="r",
        scope=ACME,
        query="python",
        top_k=3,
        execution=RetrievalExecutionConfig(**execution),
    )
    return resolve_plan(
        request,
        engine.config.retrieval,
        rt.units if units is None else units,
        method_fusion=None,
        channel_fusion=None,
        has_reranker=False,
        has_nli=False,
    )


def _reader(
    engine: RetrievalEngine,
    readers: ReaderTracker | None = None,
    store: Any = None,
    schedule: Any = None,
) -> SnapshotReader:
    rt = runtime(engine)
    return SnapshotReader(
        store or rt.store,
        readers or ReaderTracker(),
        instance_id=engine.instance_id,
        schedule_backfill=schedule,
    )


async def _pointer(engine: RetrievalEngine, name: str, scope: Scope = ACME) -> str:
    rt = runtime(engine)
    async with rt.store.transaction(scope, write=False) as tx:
        pointer = await tx.get_pointer(rt.units[name].key(scope))
    assert pointer is not None
    return pointer.generation_id


async def test_empty_scope_returns_without_acquiring() -> None:
    async with opened() as engine:
        readers = ReaderTracker()
        snapshot = await _reader(engine, readers).capture(Scope(tenant_id="nobody"), _plan(engine))
    assert snapshot.active_documents == 0
    assert snapshot.methods == []
    assert snapshot.acquired == []


async def test_scope_with_only_deleted_documents_reports_zero_fraction() -> None:
    async with opened() as engine:
        await engine.ingest([text_doc("d1", "python")], scope=ACME)
        await engine.delete(["d1"], scope=ACME)
        snapshot = await _reader(engine).capture(ACME, _plan(engine))
    assert snapshot.active_documents == 0
    assert snapshot.methods == []
    assert set(snapshot.coverage.fraction_ready.values()) <= {0.0}
    assert snapshot.model().generations == {}


async def test_capture_pins_generations_and_release_is_idempotent() -> None:
    async with opened() as engine:
        await engine.ingest([text_doc("d1", "python asyncio")], scope=ACME)
        readers = ReaderTracker()
        reader = _reader(engine, readers)
        snapshot = await reader.capture(ACME, _plan(engine))
        generation = await _pointer(engine, KEYWORD)
        assert snapshot.seq > 0
        assert snapshot.active_documents == 1
        assert snapshot.warnings == []
        assert readers.readers(generation) == 1
        model = snapshot.model()
        assert model.generations[KEYWORD] == generation
        assert model.fingerprints[KEYWORD] == runtime(engine).units[KEYWORD].fingerprint
        assert snapshot.coverage.fraction_ready[KEYWORD] == 1.0
        assert not any(item.partial for item in snapshot.methods)
        reader.release(snapshot)
        reader.release(snapshot)
        assert readers.readers(generation) == 0


async def test_missing_indexes_skip_fail_and_backfill() -> None:
    async with opened(ingestion=IngestionConfig(index_wait=IndexWait.NONE)) as engine:
        await engine.ingest([text_doc("d1", "python asyncio")], scope=ACME)
        snapshot = await _reader(engine).capture(ACME, _plan(engine))
        assert snapshot.methods == []
        assert {w.code for w in snapshot.warnings} == {WarningCode.INDEX_MISSING}
        assert len(snapshot.warnings) == 3

        with pytest.raises(CoverageError) as caught:
            await _reader(engine).capture(ACME, _plan(engine, missing_index_policy="fail"))
        assert sorted(caught.value.details["methods"]) == [
            "index.fuzzy",
            "index.keyword",
            "index.sparse",
        ]

        scheduled: list[list[str]] = []

        async def schedule(scope: Scope, units: Any) -> str | None:
            scheduled.append([u.name for u in units])
            return None if len(scheduled) > 1 else "task-1"

        plan = _plan(engine, missing_index_policy="schedule_backfill")
        first = await _reader(engine, schedule=schedule).capture(ACME, plan)
        second = await _reader(engine, schedule=schedule).capture(ACME, plan)
        # without a scheduler nothing is scheduled
        third = await _reader(engine).capture(ACME, plan)
    backfill = [w for w in first.warnings if w.code == WarningCode.BACKFILL_SCHEDULED]
    assert backfill[0].severity == Severity.INFO
    assert backfill[0].details["task_id"] == "task-1"
    assert all(w.code != WarningCode.BACKFILL_SCHEDULED for w in second.warnings)
    assert all(w.code != WarningCode.BACKFILL_SCHEDULED for w in third.warnings)
    assert len(scheduled) == 2


async def test_failure_after_acquire_releases_everything() -> None:
    async with opened() as engine:
        await engine.ingest([text_doc("d1", "python asyncio")], scope=ACME)
        units = dict(runtime(engine).units)
        ghost = dataclasses.replace(units[KEYWORD], method="dense")
        units[ghost.name] = ghost
        readers = ReaderTracker()
        with pytest.raises(CoverageError):
            await _reader(engine, readers).capture(
                ACME, _plan(engine, units, missing_index_policy="fail", dense=True)
            )
        assert readers.readers(await _pointer(engine, KEYWORD)) == 0


async def test_stale_fingerprint_is_not_read() -> None:
    async with opened() as engine:
        await engine.ingest([text_doc("d1", "python asyncio")], scope=ACME)
        units = dict(runtime(engine).units)
        units[KEYWORD] = dataclasses.replace(units[KEYWORD], dependencies=(SimpleTokenizer(),))
        snapshot = await _reader(engine).capture(ACME, _plan(engine, units))
    assert KEYWORD not in snapshot.model().generations
    stale = [w for w in snapshot.warnings if w.code == WarningCode.STALE_COVERAGE]
    assert [w.method for w in stale] == ["keyword"]


async def test_partial_coverage_and_failed_rows_schedule_backfill() -> None:
    async with opened(
        ingestion=IngestionConfig(index_wait=IndexWait.NONE),
        retrieval=RetrievalConfig(missing_index_policy="schedule_backfill"),
    ) as engine:
        await engine.ingest([text_doc("d1", "python asyncio")], scope=ACME)
        await engine.run_lifecycle_once(ACME)
        await engine.ingest([text_doc("d2", "python loops")], scope=ACME)
        rt = runtime(engine)
        keyword = rt.units[KEYWORD]
        async with rt.store.transaction(ACME, write=True) as tx:
            documents = await tx.list_documents(ACME.key)
            fresh = next(d for d in documents if d.document_id == "d2")
            await tx.upsert_coverage(
                [
                    CoverageRecord(
                        scope_key=ACME.key,
                        document_uid=fresh.document_uid,
                        channel=keyword.channel,
                        method=keyword.method,
                        backend_instance_id=keyword.instance,
                        target_version=fresh.version,
                        state=CoverageState.FAILED,
                        updated_at=utc_now(),
                    )
                ]
            )
        units_seen: list[list[str]] = []

        async def schedule(scope: Scope, units: Any) -> str | None:
            units_seen.append([u.name for u in units])
            return "t"

        snapshot = await _reader(engine, schedule=schedule).capture(
            ACME, _plan(engine, missing_index_policy="schedule_backfill")
        )
    partial = {w.method: w for w in snapshot.warnings if w.code == WarningCode.PARTIAL_COVERAGE}
    assert set(partial) == {"keyword", "fuzzy", "sparse"}
    assert partial["keyword"].details["failed"] == 1
    assert partial["keyword"].details["active"] == 2
    assert all(item.partial for item in snapshot.methods)
    assert units_seen == [[KEYWORD]]
    assert snapshot.coverage.fraction_ready[KEYWORD] == 0.5


@dataclasses.dataclass
class _Twist:
    """Rewrites what the store returns for one unit on the first and later reads."""

    real: IndexGeneration
    second: Callable[[PointerRow | None, IndexGeneration], tuple[Any, Any]]
    calls: int = 0


class _Tx:
    def __init__(self, inner: Any, twist: _Twist, call: int) -> None:
        self._inner = inner
        self._twist = twist
        self._call = call

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)

    async def get_pointer(self, key: UnitKey) -> Any:
        pointer = await self._inner.get_pointer(key)
        if pointer is None or pointer.generation_id != self._twist.real.generation_id:
            return pointer
        if self._call == 1:
            return pointer.model_copy(update={"generation_id": "old"})
        return self._twist.second(pointer, self._twist.real)[0]

    async def get_generation(self, generation_id: str) -> Any:
        if generation_id == "old":
            return self._twist.real.model_copy(update={"generation_id": "old"})
        if self._call > 1 and generation_id == self._twist.real.generation_id:
            return self._twist.second(None, self._twist.real)[1]
        return await self._inner.get_generation(generation_id)


class _Store:
    def __init__(self, inner: Any, twist: _Twist) -> None:
        self._inner = inner
        self._twist = twist

    @contextlib.asynccontextmanager
    async def transaction(self, scope: Scope, *, write: bool) -> AsyncGenerator[_Tx]:
        self._twist.calls += 1
        async with self._inner.transaction(scope, write=write) as tx:
            yield _Tx(tx, self._twist, self._twist.calls)


@pytest.mark.parametrize(
    ("case", "expect_bound"),
    [("replaced", True), ("gone", False), ("refingerprinted", False), ("also_retiring", False)],
)
async def test_retiring_generation_rereads_pointer(case: str, expect_bound: bool) -> None:
    async with opened() as engine:
        await engine.ingest([text_doc("d1", "python asyncio")], scope=ACME)
        rt = runtime(engine)
        real_id = await _pointer(engine, KEYWORD)
        async with rt.store.transaction(ACME, write=False) as tx:
            real = await tx.get_generation(real_id)
        assert real is not None

        def second(pointer: Any, generation: IndexGeneration) -> tuple[Any, Any]:
            if case == "gone":
                return None, None
            if case == "refingerprinted":
                return pointer, generation.model_copy(update={"dependency_fingerprint": "x" * 64})
            return pointer, generation

        readers = ReaderTracker()
        readers.mark_retiring("old")
        if case == "also_retiring":
            readers.mark_retiring(real_id)
        store = _Store(rt.store, _Twist(real=real, second=second))
        units = {KEYWORD: rt.units[KEYWORD]}
        snapshot = await _reader(engine, readers, store=cast("Any", store)).capture(
            ACME, _plan(engine, units)
        )
    if expect_bound:
        assert snapshot.model().generations == {KEYWORD: real_id}
        assert snapshot.acquired == [real_id]
        assert snapshot.warnings == []
    else:
        assert snapshot.methods == []
        assert [w.code for w in snapshot.warnings] == [WarningCode.STAGE_SKIPPED]


async def test_durable_units_hold_a_reader_lease(monkeypatch: pytest.MonkeyPatch) -> None:
    async with opened() as engine:
        await engine.ingest([text_doc("d1", "python asyncio")], scope=ACME)
        rt = runtime(engine)
        generation = await _pointer(engine, KEYWORD)
        durable = dataclasses.replace(rt.units[KEYWORD], instance="")
        method = cast("Any", SimpleNamespace(unit=durable))
        readers = ReaderTracker()
        reader = _reader(engine, readers)
        assert await reader._register(ACME, method, generation)
        assert readers.readers(generation) == 1
        async with rt.store.transaction(ACME, write=False) as tx:
            assert await tx.count_reader_leases(generation, now=utc_now()) == 1
        assert not await reader._register(ACME, method, "gen_unknown")  # nothing to lease
        assert readers.readers("gen_unknown") == 0

        async def unavailable(*_: Any, **__: Any) -> bool:
            raise RuntimeError("store down")

        monkeypatch.setattr(readers, "lease", unavailable)
        with pytest.raises(RuntimeError, match="store down"):
            await reader._register(ACME, method, generation)
        assert readers.readers(generation) == 1  # the failed registration was undone


async def test_retiring_generation_with_unchanged_pointer_is_skipped() -> None:
    async with opened() as engine:
        await engine.ingest([text_doc("d1", "python asyncio")], scope=ACME)
        readers = ReaderTracker()
        readers.mark_retiring(await _pointer(engine, KEYWORD))
        snapshot = await _reader(engine, readers).capture(ACME, _plan(engine))
    assert KEYWORD not in snapshot.model().generations
    assert [w.method for w in snapshot.warnings if w.code == WarningCode.STAGE_SKIPPED] == [
        "keyword"
    ]
