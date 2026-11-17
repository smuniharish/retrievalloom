"""Physical erasure, tombstone purge, retention housekeeping and startup rebuilds."""

from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import TYPE_CHECKING, Any
from unittest.mock import ANY

import pytest

from retrievalloom.config import LifecycleConfig
from retrievalloom.domain.records import (
    Channel,
    CoverageState,
    DocumentState,
    GenerationState,
    ObjectKind,
    TaskState,
)
from retrievalloom.errors import BackendUnavailableError, LifecycleError, VersionConflictError
from retrievalloom.graph.normalizer import IdentityGraphNormalizer
from retrievalloom.lifecycle.canonical import document_uid
from retrievalloom.lifecycle.coordinator import LifecycleCoordinator
from retrievalloom.lifecycle.ingestion import GraphDerivation
from retrievalloom.lifecycle.purge import DocumentEraser
from tests.fakes import CapitalizedExtractor
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
    from collections.abc import Sequence

    from retrievalloom.domain.inputs import Scope
    from retrievalloom.lifecycle.units import IndexUnit
    from retrievalloom.storage.sqlite import SQLiteMetadataStore

DAY = 86_400.0


def uid(document_id: str, scope: Scope = SCOPE) -> str:
    return document_uid(scope, document_id)


def harness(store: SQLiteMetadataStore, *, graph: bool = False, **lifecycle: Any) -> Harness:
    units: list[IndexUnit] = [index_unit(DictIndex())]
    derivation = None
    if graph:
        units.append(index_unit(DictIndex(), method="graph", channel=Channel.GRAPH))
        derivation = GraphDerivation(IdentityGraphNormalizer(), CapitalizedExtractor())
    config = LifecycleConfig(background=False, **lifecycle)
    return build(store, units, lifecycle=config, graph=derivation)


async def index_all(h: Harness, scope: Scope = SCOPE) -> None:
    for unit in h.units:
        await h.indexer.run(scope, unit)


def items(h: Harness, unit: IndexUnit, scope: Scope = SCOPE) -> dict[str, str]:
    """``object_id -> text`` across every generation of ``unit``'s backend in ``scope``."""
    backend = unit.backend
    assert isinstance(backend, DictIndex)
    return {
        oid: item.text
        for (key, _), generation in backend.generations.items()
        if key == scope.key
        for oid, item in generation.items.items()
    }


async def leftovers(h: Harness, document_id: str, scope: Scope = SCOPE) -> dict[str, int]:
    """Rows still referencing a document."""
    doc = uid(document_id, scope)
    async with h.store.transaction(scope, write=False) as tx:
        return {
            "documents": int(await tx.get_document(scope, document_id) is not None),
            "versions": len(await tx.get_versions(doc)),
            "objects": len(await tx.list_document_objects(scope.key, doc)),
            "contributions": len(await tx.list_document_contributions(scope.key, doc)),
            "lineage": len(await tx.get_lineage(scope.key, doc)),
            "coverage": sum(
                [len(await tx.get_coverage(unit.key(scope), [doc])) for unit in h.units]
            ),
        }


GONE = {
    "documents": 0,
    "versions": 0,
    "objects": 0,
    "contributions": 0,
    "lineage": 0,
    "coverage": 0,
}


# explicit purge -------------------------------------------------------------------------
async def test_purge_erases_backends_and_metadata(store: SQLiteMetadataStore) -> None:
    h = harness(store, graph=True)
    chunks, graph = h.units
    await h.ingestion.ingest(
        [text_doc("d1", "Alice met Bob"), text_doc("d2", "Alice again")], SCOPE
    )
    await h.ingestion.ingest([text_doc("d1", "Alice met Bob twice")], SCOPE)
    await index_all(h)
    assert set(items(h, graph).values()) == {"Alice", "Bob", "Alice mentions Bob"}

    report = await h.eraser.purge(["d1", "d1", "missing"], SCOPE)

    assert report.purged == ("d1",)
    assert report.not_found == ("missing",)
    assert len(report.task_ids) == 1
    assert await leftovers(h, "d1") == GONE
    assert "Alice met Bob twice" not in items(h, chunks).values()
    assert sorted(items(h, graph).values()) == ["Alice"]  # still supported by d2
    async with store.transaction(SCOPE, write=False) as tx:
        events = await tx.list_pending_events()
        alice = [
            obj
            for obj in await tx.list_current_objects(SCOPE.key, [ObjectKind.GRAPH_ENTITY])
            if obj.text == "Alice"
        ]
        task = await tx.get_task(report.task_ids[0])
    assert events == []
    assert [source.document_id for source in alice[0].sources] == ["d2"]
    assert task is not None
    assert task.target == {"units": [graph.name]}
    # The queued rebuild republishes the graph without the purged document.
    await h.indexer.run(SCOPE, graph)
    assert sorted(items(h, graph).values()) == ["Alice"]
    assert (await h.eraser.purge(["d1"], SCOPE)).not_found == ("d1",)
    assert "documents_purged" in h.logger.events("info")


async def test_purge_of_a_tombstone_skips_finished_graph_rebuilds(
    store: SQLiteMetadataStore,
) -> None:
    h = harness(store, graph=True)
    await h.ingestion.ingest([text_doc("d1", "Alice met Bob"), text_doc("d2", "plain")], SCOPE)
    await index_all(h)
    await h.ingestion.delete(["d1", "d2"], SCOPE)
    await index_all(h)  # the graph unit rebuilt and marked d1 deleted
    report = await h.eraser.purge(["d1", "d2"], SCOPE)  # d2 has no graph objects
    assert report.purged == ("d1", "d2")
    assert report.task_ids == ()
    assert await leftovers(h, "d1") == GONE
    assert await leftovers(h, "d2") == GONE


async def test_purge_removes_from_every_live_generation(store: SQLiteMetadataStore) -> None:
    h = harness(store)
    await h.ingestion.ingest([text_doc("d1", "alpha")], SCOPE)
    await index_all(h)
    async with store.transaction(SCOPE, write=False) as tx:
        [active] = await tx.list_generations(scope_key=SCOPE.key)
    retiring = active.model_copy(update={"state": GenerationState.RETIRING})
    async with store.transaction(SCOPE, write=True) as tx:
        await tx.put_generation(retiring, expected_revision=active.row_revision)
    await h.eraser.purge(["d1"], SCOPE)
    assert items(h, h.unit) == {}


async def test_backend_failure_keeps_metadata_for_a_retry(store: SQLiteMetadataStore) -> None:
    h = harness(store)
    await h.ingestion.ingest([text_doc("d1", "alpha")], SCOPE)
    await index_all(h)
    backend = h.unit.backend
    assert isinstance(backend, DictIndex)
    backend.fail["remove"] = BackendUnavailableError("index down")
    with pytest.raises(BackendUnavailableError):
        await h.eraser.purge(["d1"], SCOPE)
    async with store.transaction(SCOPE, write=False) as tx:
        tombstone = await tx.get_document(SCOPE, "d1")
    assert tombstone is not None
    assert tombstone.state == DocumentState.DELETED
    assert (await leftovers(h, "d1"))["versions"] == 1
    del backend.fail["remove"]
    report = await h.eraser.purge(["d1"], SCOPE)
    assert report.purged == ("d1",)
    assert items(h, h.unit) == {}
    assert await leftovers(h, "d1") == GONE


async def test_purge_is_tenant_isolated(store: SQLiteMetadataStore) -> None:
    h = harness(store)
    await h.ingestion.ingest([text_doc("d1", "alpha")], SCOPE)
    await h.ingestion.ingest([text_doc("d1", "beta")], OTHER)
    await index_all(h)
    await index_all(h, OTHER)
    await h.eraser.purge(["d1"], SCOPE)
    assert await leftovers(h, "d1") == GONE
    assert (await leftovers(h, "d1", OTHER))["versions"] == 1
    assert list(items(h, h.unit, OTHER).values()) == ["beta"]


async def test_concurrent_change_is_retried(store: SQLiteMetadataStore) -> None:
    h = harness(store)
    await h.ingestion.ingest([text_doc("d1", "alpha")], SCOPE)
    await index_all(h)
    backend = h.unit.backend
    assert isinstance(backend, DictIndex)

    async def touch() -> None:
        async with store.transaction(SCOPE, write=True) as tx:
            current = await tx.get_document(SCOPE, "d1")
            assert current is not None
            await tx.put_document(
                current.model_copy(update={"metadata": {"touched": True}}),
                expected_revision=current.row_revision,
            )

    backend.hooks["remove"] = touch
    assert (await h.eraser.purge(["d1"], SCOPE)).purged == ("d1",)
    assert await leftovers(h, "d1") == GONE


async def test_document_purged_concurrently_is_a_success(store: SQLiteMetadataStore) -> None:
    h = harness(store)
    await h.ingestion.ingest([text_doc("d1", "alpha")], SCOPE)
    await index_all(h)
    backend = h.unit.backend
    assert isinstance(backend, DictIndex)

    async def purge_elsewhere() -> None:
        async with store.transaction(SCOPE, write=True) as tx:
            await tx.purge_document(SCOPE.key, uid("d1"))

    backend.hooks["remove"] = purge_elsewhere
    assert (await h.eraser.purge(["d1"], SCOPE)).purged == ("d1",)


async def test_persistent_conflicts_raise(
    store: SQLiteMetadataStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    h = harness(store)
    await h.ingestion.ingest([text_doc("d1", "alpha")], SCOPE)

    async def conflict(*_: object) -> None:
        raise VersionConflictError("busy")

    monkeypatch.setattr(h.eraser, "_delete_rows", conflict)
    with pytest.raises(VersionConflictError):
        await h.eraser.purge(["d1"], SCOPE)


async def test_revived_document_is_not_erased(store: SQLiteMetadataStore) -> None:
    h = harness(store)
    await h.ingestion.ingest([text_doc("d1", "alpha")], SCOPE)
    eraser = h.eraser
    assert isinstance(eraser, DocumentEraser)
    with pytest.raises(LifecycleError, match="re-ingested"):
        await eraser._erase(SCOPE, ["d1"], [])
    await eraser._erase(SCOPE, ["gone"], [])  # purged meanwhile: nothing to do
    assert (await leftovers(h, "d1"))["documents"] == 1


# background purge and retention ---------------------------------------------------------
async def test_expired_tombstones_are_purged_by_maintenance(store: SQLiteMetadataStore) -> None:
    h = harness(store, purge_after=DAY, batch_size=1)
    await h.ingestion.ingest([text_doc(f"d{i}", f"text {i}") for i in range(3)], SCOPE)
    await index_all(h)
    await h.ingestion.delete(["d0", "d1"], SCOPE)
    h.clock.advance(2 * DAY)
    await h.coordinator.maintain()  # coverage is still pending: not purgeable yet
    assert (await leftovers(h, "d0"))["documents"] == 1
    await index_all(h)
    async with store.transaction(SCOPE, write=False) as tx:
        [row] = (await tx.get_coverage(h.unit.key(SCOPE), [uid("d0")])).values()
    assert row.state == CoverageState.DELETED
    await h.coordinator.maintain()
    assert await leftovers(h, "d0") == GONE
    assert await leftovers(h, "d1") == GONE
    assert (await leftovers(h, "d2"))["documents"] == 1
    _, _, fields = next(r for r in h.logger.records if r[1] == "tombstones_purged")
    assert fields == {"scope": SCOPE.key, "purged": 2}


async def test_recent_tombstones_are_kept(store: SQLiteMetadataStore) -> None:
    h = harness(store, purge_after=DAY)
    await h.ingestion.ingest([text_doc("d1", "alpha")], SCOPE)
    await h.ingestion.delete(["d1"], SCOPE)
    await index_all(h)
    h.clock.advance(DAY - 1)
    await h.coordinator.maintain()
    assert (await leftovers(h, "d1"))["documents"] == 1
    assert "tombstones_purged" not in h.logger.events("info")


async def test_retention_prunes_versions_and_events(store: SQLiteMetadataStore) -> None:
    h = harness(store, max_versions=1, event_retention=DAY, batch_size=1)
    for text in ("one", "two", "three"):
        await h.ingestion.ingest([text_doc("d1", text)], SCOPE)
    await h.coordinator.maintain()
    async with store.transaction(SCOPE, write=False) as tx:
        # Versions referenced by retained ingestion events are kept.
        assert [v.version for v in await tx.get_versions(uid("d1"))] == [1, 2, 3]
    h.clock.advance(DAY + 1)
    await h.coordinator.maintain()  # events go first, which unblocks the versions
    async with store.transaction(SCOPE, write=False) as tx:
        assert [v.version for v in await tx.get_versions(uid("d1"))] == [3]
        assert await tx.get_event(SCOPE.key, (await tx.get_versions(uid("d1")))[0].event_id) is None
    pruned = [fields for _, event, fields in h.logger.records if event == "history_pruned"]
    assert pruned == [{"scope": SCOPE.key, "versions": 2, "events": 3, "purge_log": 0}]


async def test_housekeeping_can_be_disabled(store: SQLiteMetadataStore) -> None:
    h = harness(store, purge_after=None, max_versions=None, event_retention=None)
    for text in ("one", "two"):
        await h.ingestion.ingest([text_doc("d1", text)], SCOPE)
    await h.ingestion.delete(["d1"], SCOPE)
    await index_all(h)
    h.clock.advance(365 * DAY)
    await h.coordinator.maintain()
    assert (await leftovers(h, "d1"))["versions"] == 2
    assert "history_pruned" not in h.logger.events("info")


async def test_housekeeping_failure_is_logged(
    store: SQLiteMetadataStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    h = harness(store)
    await h.ingestion.ingest([text_doc("d1", "alpha")], SCOPE)
    fail_transaction(monkeypatch, "prune_versions", lambda: RuntimeError("db gone"))
    await h.coordinator.maintain()
    _, _, fields = next(r for r in h.logger.records if r[1] == "lifecycle_housekeeping_failed")
    assert fields == {"scope": SCOPE.key, "error_type": "RuntimeError"}


async def test_background_ticks_housekeep_at_most_every_five_minutes(
    store: SQLiteMetadataStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    h = harness(store)
    await h.ingestion.ingest([text_doc("d1", "alpha")], SCOPE)
    calls: list[str] = []

    async def housekeep(scope: Scope) -> None:
        calls.append(scope.key)

    monkeypatch.setattr(h.coordinator, "housekeep", housekeep)
    await h.coordinator.tick()
    await h.coordinator.tick()
    assert calls == [SCOPE.key]
    h.clock.advance(300)
    await h.coordinator.tick()
    assert calls == [SCOPE.key, SCOPE.key]
    await h.coordinator.maintain()  # explicit maintenance always housekeeps
    assert len(calls) == 3


# startup rebuild ------------------------------------------------------------------------
async def test_start_with_rebuild_indexes_before_returning(store: SQLiteMetadataStore) -> None:
    h = harness(store)
    await h.ingestion.ingest([text_doc("d1", "alpha")], SCOPE)
    await h.coordinator.start(rebuild=True)
    async with store.transaction(SCOPE, write=False) as tx:
        [row] = (await tx.get_coverage(h.unit.key(SCOPE), [uid("d1")])).values()
    assert row.state == CoverageState.READY
    await h.close()


async def test_start_rebuild_timeout_continues_in_background(
    store: SQLiteMetadataStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    h = build(
        store,
        [index_unit(DictIndex())],
        lifecycle=LifecycleConfig(background=True, poll_interval=60),
    )

    async def slow(scope: object = None) -> None:
        await asyncio.sleep(60)

    monkeypatch.setattr(h.coordinator, "run_once", slow)
    await h.coordinator.start(rebuild=True, rebuild_timeout=0.01)
    try:
        assert h.coordinator.background
        _, _, fields = next(r for r in h.logger.records if r[1] == "startup_rebuild_timeout")
        assert fields == {"timeout": 0.01}
    finally:
        await h.close()


async def test_start_rebuild_failure_stops_the_coordinator(
    store: SQLiteMetadataStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    h = harness(store)

    async def broken(scope: object = None) -> None:
        raise RuntimeError("rebuild failed")

    monkeypatch.setattr(h.coordinator, "run_once", broken)
    with pytest.raises(RuntimeError, match="rebuild failed"):
        await h.coordinator.start(rebuild=True)
    assert not h.coordinator.background
    assert "engine_instance_deregistered" in h.logger.events("info")


async def test_task_state_of_queued_rebuild(store: SQLiteMetadataStore) -> None:
    h = harness(store, graph=True)
    await h.ingestion.ingest([text_doc("d1", "Alice met Bob")], SCOPE)
    await index_all(h)
    report = await h.eraser.purge(["d1"], SCOPE)
    result = await h.coordinator.task_result(report.task_ids[0], SCOPE)
    assert result.state == TaskState.QUEUED
    await h.coordinator.run_once(SCOPE)
    assert items(h, h.units[1]) == {}


# batching -------------------------------------------------------------------------------
def record_removals(monkeypatch: pytest.MonkeyPatch, backend: DictIndex) -> list[list[str]]:
    removed: list[list[str]] = []
    original = backend.remove

    async def remove(scope: Scope, generation_id: str, object_ids: Sequence[str]) -> int:
        removed.append(list(object_ids))
        return await original(scope, generation_id, object_ids)

    monkeypatch.setattr(backend, "remove", remove)
    return removed


def eraser_of(h: Harness, batch_size: int) -> DocumentEraser:
    return DocumentEraser(
        h.store,
        h.ingestion,
        h.indexer,
        h.units,
        clock=h.clock,
        logger=h.logger,
        batch_size=batch_size,
    )


async def test_purge_removes_a_batch_with_one_call_per_generation(
    store: SQLiteMetadataStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    h = harness(store)
    await h.ingestion.ingest([text_doc(f"d{i}", f"text {i}") for i in range(5)], SCOPE)
    await index_all(h)
    backend = h.unit.backend
    assert isinstance(backend, DictIndex)
    removed = record_removals(monkeypatch, backend)
    report = await eraser_of(h, 2).purge([f"d{i}" for i in range(5)], SCOPE)
    assert report.purged == ("d0", "d1", "d2", "d3", "d4")
    assert [len(ids) for ids in removed] == [2, 2, 1]
    assert items(h, h.unit) == {}
    for i in range(5):
        assert await leftovers(h, f"d{i}") == GONE
    with pytest.raises(ValueError, match="at least 1"):
        eraser_of(h, 0)


async def test_only_changed_documents_are_retried(
    store: SQLiteMetadataStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    h = harness(store)
    await h.ingestion.ingest([text_doc("d1", "alpha"), text_doc("d2", "beta")], SCOPE)
    await index_all(h)
    backend = h.unit.backend
    assert isinstance(backend, DictIndex)

    async def touch() -> None:
        async with store.transaction(SCOPE, write=True) as tx:
            current = await tx.get_document(SCOPE, "d2")
            assert current is not None
            await tx.put_document(
                current.model_copy(update={"metadata": {"touched": True}}),
                expected_revision=current.row_revision,
            )

    backend.hooks["remove"] = touch
    removed = record_removals(monkeypatch, backend)
    assert (await h.eraser.purge(["d1", "d2"], SCOPE)).purged == ("d1", "d2")
    assert [len(ids) for ids in removed] == [2, 1]
    assert await leftovers(h, "d1") == GONE
    assert await leftovers(h, "d2") == GONE


async def test_documents_that_keep_changing_raise(
    store: SQLiteMetadataStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    h = harness(store)
    await h.ingestion.ingest([text_doc("d1", "alpha"), text_doc("d2", "beta")], SCOPE)

    async def changed(scope: Scope, plans: Sequence[Any]) -> list[str]:
        return [plans[-1].document.document_id]

    monkeypatch.setattr(h.eraser, "_delete_rows", changed)
    with pytest.raises(VersionConflictError) as caught:
        await h.eraser.purge(["d1", "d2"], SCOPE)
    assert caught.value.details == {"document_ids": ["d2"]}


async def test_aggregate_shared_with_a_changed_document_waits_for_its_retry(
    store: SQLiteMetadataStore,
) -> None:
    h = harness(store, graph=True)
    _, graph = h.units
    await h.ingestion.ingest(
        [text_doc("d1", "Alice met Bob"), text_doc("d2", "Alice again")], SCOPE
    )
    await index_all(h)
    await h.ingestion.delete(["d1", "d2"], SCOPE)
    await index_all(h)
    backend = graph.backend
    assert isinstance(backend, DictIndex)

    async def touch() -> None:
        async with store.transaction(SCOPE, write=True) as tx:
            current = await tx.get_document(SCOPE, "d2")
            assert current is not None
            await tx.put_document(
                current.model_copy(update={"metadata": {"touched": True}}),
                expected_revision=current.row_revision,
            )

    backend.hooks["remove"] = touch
    assert (await h.eraser.purge(["d1", "d2"], SCOPE)).purged == ("d1", "d2")
    assert await leftovers(h, "d1") == GONE
    assert await leftovers(h, "d2") == GONE
    async with store.transaction(SCOPE, write=False) as tx:
        assert list(await tx.list_current_objects(SCOPE.key, [ObjectKind.GRAPH_ENTITY])) == []
        log = await tx.list_purge_log(SCOPE.key, after_seq=0)
    assert [entry.document_uid for entry in log] == [uid("d1"), uid("d2")]
    assert all(entry.object_ids for entry in log)


# eviction on other engines --------------------------------------------------------------
def engine(store: SQLiteMetadataStore, name: str, **lifecycle: Any) -> Harness:
    config = LifecycleConfig(background=False, **lifecycle)
    unit = index_unit(DictIndex(), instance_id=f"inst_{name}")
    return build(store, [unit], lifecycle=config, instance_id=f"inst_{name}")


def texts(h: Harness) -> set[str]:
    return set(items(h, h.unit).values())


async def test_other_engines_evict_purged_objects(store: SQLiteMetadataStore) -> None:
    a = engine(store, "a")
    b = engine(store, "b")
    await b.coordinator.start()
    await a.coordinator.start()
    await a.ingestion.ingest([text_doc("d1", "alpha"), text_doc("d2", "beta")], SCOPE)
    await a.coordinator.run_once()
    await b.coordinator.run_once()
    assert texts(b) == {"alpha", "beta"}

    await a.eraser.purge(["d1"], SCOPE)
    assert texts(a) == {"beta"}
    assert texts(b) == {"alpha", "beta"}  # until its next lifecycle round
    await b.coordinator.run_once()
    assert texts(b) == {"beta"}
    _, _, fields = next(r for r in b.logger.records if r[1] == "purged_objects_evicted")
    assert fields == {"scope": SCOPE.key, "documents": 1, "objects": 1}
    assert await b.eraser.evict_purged() == 0  # already applied
    await a.coordinator.run_once()  # the purging engine's own entry is harmless
    assert texts(a) == {"beta"}
    await a.close()
    await b.close()


async def test_eviction_pages_through_the_log(store: SQLiteMetadataStore) -> None:
    a = engine(store, "a")
    b = engine(store, "b")
    await a.ingestion.ingest([text_doc(f"d{i}", f"text {i}") for i in range(3)], SCOPE)
    await a.ingestion.ingest([text_doc("d0", "other scope")], OTHER)
    await index_all(b)
    await index_all(b, OTHER)
    follower = eraser_of(b, 1)
    assert await follower.evict_purged() == 0  # the first call only starts following
    await a.eraser.purge(["d0", "d1", "d2"], SCOPE)
    await a.eraser.purge(["d0"], OTHER)
    assert await follower.evict_purged() == 4
    assert texts(b) == set()
    assert items(b, b.unit, OTHER) == {}


async def test_missed_log_entries_rebuild_engine_local_units(store: SQLiteMetadataStore) -> None:
    a = engine(store, "a")
    b = engine(store, "b")
    await a.ingestion.ingest([text_doc(f"d{i}", f"text {i}") for i in range(4)], SCOPE)
    await index_all(b)
    await b.eraser.follow_purge_log()
    for document_id in ("d0", "d1", "d2"):
        await a.eraser.purge([document_id], SCOPE)
    async with store.transaction(SCOPE, write=True) as tx:
        far = a.clock() + timedelta(days=365)
        assert await tx.prune_purge_log(SCOPE.key, before=far) == 2
    assert await b.eraser.evict_purged() == 1
    _, _, fields = next(r for r in b.logger.records if r[1] == "purge_log_entries_missed")
    assert fields == {"scope": SCOPE.key, "after_seq": 0, "next_seq": 3}
    assert texts(b) == {"text 3"}


async def test_missed_log_entries_of_an_unbuilt_unit_need_no_rebuild(
    store: SQLiteMetadataStore,
) -> None:
    a = engine(store, "a")
    b = engine(store, "b")
    await a.ingestion.ingest([text_doc(f"d{i}", f"text {i}") for i in range(3)], SCOPE)
    await b.eraser.follow_purge_log()
    for document_id in ("d0", "d1", "d2"):
        await a.eraser.purge([document_id], SCOPE)
    async with store.transaction(SCOPE, write=True) as tx:
        assert await tx.prune_purge_log(SCOPE.key, before=a.clock() + timedelta(days=1)) == 2
    assert await b.eraser.evict_purged() == 1
    async with store.transaction(SCOPE, write=False) as tx:
        assert await tx.get_pointer(b.unit.key(SCOPE)) is None


async def test_engines_without_engine_local_units_skip_the_log(
    store: SQLiteMetadataStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    h = build(store, [index_unit(DictIndex(durable=True))])
    fail_transaction(monkeypatch, "purge_log_heads", lambda: RuntimeError("unused"))
    assert await h.eraser.evict_purged() == 0


async def test_eviction_failure_is_logged_and_the_round_continues(
    store: SQLiteMetadataStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    h = harness(store)
    await h.ingestion.ingest([text_doc("d1", "alpha")], SCOPE)

    async def broken() -> None:
        raise RuntimeError("db gone")

    monkeypatch.setattr(h.eraser, "evict_purged", broken)
    await h.coordinator.run_once()
    _, _, fields = next(r for r in h.logger.records if r[1] == "purge_eviction_failed")
    assert fields == {"error_type": "RuntimeError"}
    assert texts(h) == {"alpha"}


async def test_housekeeping_prunes_the_purge_log(store: SQLiteMetadataStore) -> None:
    h = harness(store, purge_log_retention=DAY, batch_size=1)
    await h.ingestion.ingest([text_doc(f"d{i}", f"text {i}") for i in range(3)], SCOPE)
    for document_id in ("d0", "d1", "d2"):
        await h.eraser.purge([document_id], SCOPE)
    await h.coordinator.maintain()
    h.clock.advance(DAY + 1)
    await h.coordinator.maintain()
    async with store.transaction(SCOPE, write=False) as tx:
        assert [entry.seq for entry in await tx.list_purge_log(SCOPE.key, after_seq=0)] == [3]
    pruned = [fields for _, event, fields in h.logger.records if event == "history_pruned"]
    assert pruned == [{"scope": SCOPE.key, "versions": 0, "events": 0, "purge_log": 2}]


async def test_coordinator_without_eraser_skips_eviction(store: SQLiteMetadataStore) -> None:
    h = harness(store)
    coordinator = LifecycleCoordinator(
        store,
        h.indexer,
        h.units,
        LifecycleConfig(background=False),
        instance_id=h.instance_id,
        engine_id="engine-1",
        clock=h.clock,
        logger=h.logger,
    )
    await h.ingestion.ingest([text_doc("d1", "alpha")], SCOPE)
    await coordinator.start()
    await coordinator.run_once()
    assert texts(h) == {"alpha"}
    await coordinator.stop()


async def test_purging_documents_without_objects_of_a_unit_skips_it(
    store: SQLiteMetadataStore,
) -> None:
    h = harness(store, graph=True)
    _, graph = h.units
    await h.ingestion.ingest([text_doc("d1", "plain text")], SCOPE)
    await index_all(h)
    backend = graph.backend
    assert isinstance(backend, DictIndex)
    assert (await h.eraser.purge(["d1"], SCOPE)).purged == ("d1",)
    assert ("remove", ANY) not in backend.calls
