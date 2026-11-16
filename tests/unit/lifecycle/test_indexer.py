"""Unit indexer: full builds, pointer CAS, incremental updates, retirement, crash recovery."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any

import pytest

from retrievalloom.config import IngestionConfig, LifecycleConfig
from retrievalloom.contracts.models import KeywordEntry
from retrievalloom.domain.hashing import random_id
from retrievalloom.domain.inputs import (
    DocumentInput,
    StructureNodeInput,
    StructureRepresentation,
    TextRepresentation,
)
from retrievalloom.domain.records import (
    Channel,
    CoverageState,
    GenerationState,
    IndexGeneration,
    LifecycleTask,
    ObjectKind,
    TaskKind,
    TaskState,
)
from retrievalloom.errors import BackendUnavailableError, VersionConflictError
from retrievalloom.graph.normalizer import IdentityGraphNormalizer
from retrievalloom.index.text import StaticKeywordDictionary
from retrievalloom.lifecycle.canonical import document_uid
from retrievalloom.lifecycle.derivation import derive_chunks
from retrievalloom.lifecycle.indexer import ReaderTracker, _supporting_versions, task_units
from retrievalloom.lifecycle.ingestion import GraphDerivation
from retrievalloom.lifecycle.units import DICTIONARY_DIGEST, ItemPreparer
from retrievalloom.storage.sqlite import SQLiteTransaction
from tests.fakes import CapitalizedExtractor
from tests.unit.lifecycle.helpers import (
    SCOPE,
    Dependency,
    DictIndex,
    FakeClock,
    Harness,
    build,
    fail_transaction,
    index_unit,
    text_doc,
)
from tests.unit.lifecycle.test_derivation import record

if TYPE_CHECKING:
    from retrievalloom.domain.records import CoverageRecord
    from retrievalloom.lifecycle.units import IndexUnit
    from retrievalloom.storage.base import PointerRow
    from retrievalloom.storage.sqlite import SQLiteMetadataStore


def uid(document_id: str) -> str:
    return document_uid(SCOPE, document_id)


async def coverage(h: Harness, document_id: str, unit: IndexUnit | None = None) -> CoverageRecord:
    unit = unit or h.unit
    async with h.store.transaction(SCOPE, write=False) as transaction:
        rows = await transaction.get_coverage(unit.key(SCOPE), [uid(document_id)])
    return rows[uid(document_id)]


async def pointer(h: Harness, unit: IndexUnit | None = None) -> PointerRow | None:
    unit = unit or h.unit
    async with h.store.transaction(SCOPE, write=False) as transaction:
        return await transaction.get_pointer(unit.key(SCOPE))


async def generation(h: Harness, generation_id: str) -> IndexGeneration:
    async with h.store.transaction(SCOPE, write=False) as transaction:
        found = await transaction.get_generation(generation_id)
    assert found is not None
    return found


async def visible(h: Harness, document_id: str, unit: IndexUnit | None = None) -> set[int]:
    """Versions of the document visible to readers of ``unit`` now."""
    unit = unit or h.unit
    async with h.store.transaction(SCOPE, write=False) as transaction:
        seq = await transaction.current_commit_seq()
        versions = await transaction.visible_versions(unit.key(SCOPE), [uid(document_id)], seq)
    return set(versions.get(uid(document_id), frozenset()))


async def active_items(h: Harness, unit: IndexUnit | None = None) -> dict[str, str | None]:
    """``object_id -> text`` of the published generation."""
    unit = unit or h.unit
    ref = await pointer(h, unit)
    assert ref is not None
    backend = unit.backend
    assert isinstance(backend, DictIndex)
    return {oid: item.text for oid, item in backend.live(SCOPE, ref.generation_id).items()}


def harness(store: SQLiteMetadataStore, backend: DictIndex | None = None, **kwargs: Any) -> Harness:
    return build(store, [index_unit(backend or DictIndex())], **kwargs)


def tree_doc(document_id: str) -> DocumentInput:
    return DocumentInput.model_validate(
        {
            "_id": document_id,
            "representations": (StructureRepresentation(root=StructureNodeInput(text="t")),),
        }
    )


# reader tracker -------------------------------------------------------------------------
async def test_reader_tracker() -> None:
    tracker = ReaderTracker()
    assert tracker.try_acquire("g")
    assert tracker.try_acquire("g")
    tracker.release("g")
    assert tracker.readers("g") == 1
    async with tracker.reading("g") as acquired:
        assert acquired
        assert tracker.readers("g") == 2
    tracker.release("g")
    assert tracker.readers("g") == 0
    tracker.mark_retiring("g")
    async with tracker.reading("g") as acquired:
        assert not acquired
    assert tracker.readers("g") == 0
    tracker.forget("g")
    assert tracker.try_acquire("g")


# needs_work / first build ---------------------------------------------------------------
async def test_first_build_publishes_a_generation(store: SQLiteMetadataStore) -> None:
    h = harness(store)
    assert await h.indexer.needs_work(SCOPE, h.unit)  # no pointer yet
    await h.ingestion.ingest([text_doc("d1", "alpha"), tree_doc("t")], SCOPE)
    await h.indexer.run(SCOPE, h.unit)

    ref = await pointer(h)
    assert ref is not None
    built = await generation(h, ref.generation_id)
    assert built.state == GenerationState.ACTIVE
    assert built.owner_id == h.instance_id
    assert built.backend_instance_id == "inst_local"
    assert built.stats["object_count"] == 1
    assert list((await active_items(h)).values()) == ["alpha"]
    ready = await coverage(h, "d1")
    assert (ready.state, ready.indexed_version, ready.generation_id) == (
        CoverageState.READY,
        1,
        ref.generation_id,
    )
    assert (await coverage(h, "t")).state == CoverageState.NOT_APPLICABLE
    assert await visible(h, "d1") == {1}
    assert h.logger.events("info")[-2:] == ["generation_build_started", "generation_published"]
    assert not await h.indexer.needs_work(SCOPE, h.unit)


async def test_needs_work_detects_each_kind_of_lag(store: SQLiteMetadataStore) -> None:
    backend = DictIndex()
    h = build(store, [index_unit(backend, dependencies=[Dependency(1)])])
    await h.ingestion.ingest([text_doc("d1", "a")], SCOPE)
    await h.indexer.run(SCOPE, h.unit)
    assert not await h.indexer.needs_work(SCOPE, h.unit)

    await h.ingestion.ingest([text_doc("d1", "b")], SCOPE)  # pending coverage row
    assert await h.indexer.needs_work(SCOPE, h.unit)
    await h.indexer.run(SCOPE, h.unit)

    await h.ingestion.ingest([text_doc("d2", "c")], SCOPE)  # no coverage row yet
    assert await h.indexer.needs_work(SCOPE, h.unit)
    await h.indexer.run(SCOPE, h.unit)
    assert not await h.indexer.needs_work(SCOPE, h.unit)

    changed = index_unit(backend, dependencies=[Dependency(2)])
    assert await h.indexer.needs_work(SCOPE, changed)  # dependency fingerprint changed

    await h.coordinator.request(SCOPE, TaskKind.REBUILD, [h.unit])
    assert await h.indexer.needs_work(SCOPE, h.unit)


async def test_pointer_to_a_missing_generation_triggers_a_build(
    store: SQLiteMetadataStore,
) -> None:
    h = harness(store)
    await h.ingestion.ingest([text_doc("d1", "a")], SCOPE)
    async with store.transaction(SCOPE, write=True) as transaction:
        await transaction.set_pointer(h.unit.key(SCOPE), "gen_lost", expected_revision=None)
    assert await h.indexer.needs_work(SCOPE, h.unit)
    await h.indexer.run(SCOPE, h.unit)
    ref = await pointer(h)
    assert ref is not None
    assert ref.generation_id != "gen_lost"
    assert (await generation(h, ref.generation_id)).predecessor_id == "gen_lost"


async def test_pages_through_documents_in_batches(store: SQLiteMetadataStore) -> None:
    h = harness(
        store, DictIndex(max_batch=2), lifecycle=LifecycleConfig(background=False, batch_size=1)
    )
    await h.ingestion.ingest([text_doc(f"d{i}", f"text {i}") for i in range(3)], SCOPE)
    await h.indexer.run(SCOPE, h.unit)
    assert set((await active_items(h)).values()) == {"text 0", "text 1", "text 2"}
    assert {(await coverage(h, f"d{i}")).state for i in range(3)} == {CoverageState.READY}
    backend = h.unit.backend
    assert isinstance(backend, DictIndex)
    assert [method for method, _ in backend.calls].count("write") == 3


# incremental ----------------------------------------------------------------------------
async def test_incremental_update_replaces_the_previous_version(
    store: SQLiteMetadataStore,
) -> None:
    h = harness(store)
    await h.ingestion.ingest([text_doc("d1", "old"), text_doc("d2", "same")], SCOPE)
    await h.indexer.run(SCOPE, h.unit)
    first = await pointer(h)
    await h.ingestion.ingest([text_doc("d1", "new"), tree_doc("t")], SCOPE)
    await h.indexer.run(SCOPE, h.unit)
    assert await pointer(h) == first  # updated in place, no new generation
    assert set((await active_items(h)).values()) == {"new", "same"}
    row = await coverage(h, "d1")
    assert (row.state, row.indexed_version, row.attempts) == (CoverageState.READY, 2, 0)
    assert (await coverage(h, "t")).state == CoverageState.NOT_APPLICABLE
    assert await visible(h, "d1") == {2}


class MutableDictionary(StaticKeywordDictionary):
    """Keyword dictionary whose entries the test changes between runs."""

    def __init__(self) -> None:
        super().__init__()
        self.current: tuple[KeywordEntry, ...] = ()

    async def entries(self, scope: Any) -> tuple[KeywordEntry, ...]:
        del scope
        return self.current


async def test_keyword_dictionary_change_forces_a_full_build(store: SQLiteMetadataStore) -> None:
    dictionary = MutableDictionary()
    preparer = ItemPreparer(tokenizer=None, embedder=None, dictionary=dictionary)
    h = build(store, [index_unit(DictIndex(), method="keyword")], preparer=preparer)

    async def run(document_id: str) -> IndexGeneration:
        await h.ingestion.ingest([text_doc(document_id, document_id)], SCOPE)
        await h.indexer.run(SCOPE, h.unit)
        ref = await pointer(h)
        assert ref is not None
        return await generation(h, ref.generation_id)

    first = await run("d1")
    assert DICTIONARY_DIGEST not in first.stats  # no dictionary entries
    assert (await run("d2")).generation_id == first.generation_id  # updated in place
    dictionary.current = (KeywordEntry(canonical="d1"),)
    second = await run("d3")
    # Keyword postings of unchanged objects depend on the dictionary: rebuilt.
    assert second.generation_id != first.generation_id
    assert isinstance(second.stats[DICTIONARY_DIGEST], str)
    assert (await run("d4")).generation_id == second.generation_id
    dictionary.current = (KeywordEntry(canonical="d1", aliases=("first",)),)
    assert (await run("d5")).generation_id != second.generation_id
    assert set((await active_items(h)).values()) == {"d1", "d2", "d3", "d4", "d5"}


async def test_document_changed_while_writing_is_written_again(
    store: SQLiteMetadataStore,
) -> None:
    backend = DictIndex()
    h = harness(store, backend)
    await h.ingestion.ingest([text_doc("d1", "v1")], SCOPE)
    await h.indexer.run(SCOPE, h.unit)
    await h.ingestion.ingest([text_doc("d1", "v2")], SCOPE)

    async def concurrent_update() -> None:
        await h.ingestion.ingest([text_doc("d1", "v3")], SCOPE)

    backend.hooks["write"] = concurrent_update
    await h.indexer.run(SCOPE, h.unit)
    assert list((await active_items(h)).values()) == ["v3"]
    assert (await coverage(h, "d1")).indexed_version == 3
    assert [m for m, _ in backend.calls].count("write") == 3  # build, v2 (discarded), v3


async def test_incremental_failures_are_counted_then_fail_the_row(
    store: SQLiteMetadataStore,
) -> None:
    backend = DictIndex()
    h = harness(store, backend, lifecycle=LifecycleConfig(background=False, max_attempts=2))
    await h.ingestion.ingest([text_doc("d1", "v1")], SCOPE)
    await h.indexer.run(SCOPE, h.unit)
    await h.ingestion.ingest([text_doc("d1", "v2")], SCOPE)
    backend.fail["write"] = BackendUnavailableError("index down")
    with pytest.raises(BackendUnavailableError):
        await h.indexer.run(SCOPE, h.unit)
    row = await coverage(h, "d1")
    assert (row.state, row.attempts) == (CoverageState.PENDING, 1)
    assert row.last_error is not None
    assert row.last_error.code == "backend_unavailable"
    with pytest.raises(BackendUnavailableError):
        await h.indexer.run(SCOPE, h.unit)
    row = await coverage(h, "d1")
    assert (row.state, row.attempts) == (CoverageState.FAILED, 2)
    assert await visible(h, "d1") == {1}  # readers still see v1


async def test_cancelled_incremental_write_leaves_rows_pending(
    store: SQLiteMetadataStore,
) -> None:
    backend = DictIndex()
    h = harness(store, backend)
    await h.ingestion.ingest([text_doc("d1", "v1")], SCOPE)
    await h.indexer.run(SCOPE, h.unit)
    await h.ingestion.ingest([text_doc("d1", "v2")], SCOPE)
    backend.fail["write"] = asyncio.CancelledError()
    with pytest.raises(asyncio.CancelledError):
        await h.indexer.run(SCOPE, h.unit)
    row = await coverage(h, "d1")
    assert (row.state, row.attempts, row.last_error) == (CoverageState.PENDING, 0, None)


# deletes --------------------------------------------------------------------------------
async def test_deleted_documents_are_removed_from_the_active_generation(
    store: SQLiteMetadataStore,
) -> None:
    h = harness(store)
    await h.ingestion.ingest([text_doc("d1", "a"), text_doc("d2", "b"), tree_doc("t")], SCOPE)
    await h.indexer.run(SCOPE, h.unit)
    await h.ingestion.delete(["d1", "t"], SCOPE)
    assert await h.indexer.needs_work(SCOPE, h.unit)
    await h.indexer.run(SCOPE, h.unit)
    assert list((await active_items(h)).values()) == ["b"]
    assert (await coverage(h, "d1")).state == CoverageState.DELETED
    assert (await coverage(h, "t")).state == CoverageState.DELETED
    assert await visible(h, "d1") == set()
    assert not await h.indexer.needs_work(SCOPE, h.unit)

    # A deleted document without indexable objects needs no backend call.
    await h.ingestion.ingest([tree_doc("t2")], SCOPE)
    await h.indexer.run(SCOPE, h.unit)
    backend = h.unit.backend
    assert isinstance(backend, DictIndex)
    removes = [m for m, _ in backend.calls].count("remove")
    await h.ingestion.delete(["t2"], SCOPE)
    await h.indexer.run(SCOPE, h.unit)
    assert (await coverage(h, "t2")).state == CoverageState.DELETED
    assert [m for m, _ in backend.calls].count("remove") == removes


async def test_document_deleted_during_incremental_pass_is_left_for_next_round(
    store: SQLiteMetadataStore,
) -> None:
    backend = DictIndex()
    h = harness(store, backend, lifecycle=LifecycleConfig(background=False, batch_size=1))
    await h.ingestion.ingest([text_doc("d1", "a"), text_doc("d2", "b")], SCOPE)
    await h.indexer.run(SCOPE, h.unit)
    await h.ingestion.ingest([text_doc("d1", "a2"), text_doc("d2", "b2")], SCOPE)

    async def delete_other() -> None:
        await h.ingestion.delete(["d1", "d2"], SCOPE)

    backend.hooks["write"] = delete_other
    await h.indexer.run(SCOPE, h.unit)
    assert {(await coverage(h, d)).state for d in ("d1", "d2")} == {CoverageState.PENDING}
    assert await h.indexer.needs_work(SCOPE, h.unit)
    await h.indexer.run(SCOPE, h.unit)
    assert await active_items(h) == {}
    assert {(await coverage(h, d)).state for d in ("d1", "d2")} == {CoverageState.DELETED}


async def test_document_revived_during_removal_stays_pending(
    store: SQLiteMetadataStore,
) -> None:
    backend = DictIndex()
    h = harness(store, backend)
    await h.ingestion.ingest([text_doc("d1", "a")], SCOPE)
    await h.indexer.run(SCOPE, h.unit)
    await h.ingestion.delete(["d1"], SCOPE)

    async def revive() -> None:
        await h.ingestion.ingest([text_doc("d1", "a")], SCOPE)

    backend.hooks["remove"] = revive
    await h.indexer.run(SCOPE, h.unit)
    assert list((await active_items(h)).values()) == ["a"]  # re-added incrementally
    row = await coverage(h, "d1")
    assert row.state == CoverageState.READY
    assert await visible(h, "d1") == {1}


async def test_document_purged_during_removal_stays_purged(store: SQLiteMetadataStore) -> None:
    backend = DictIndex()
    h = harness(store, backend)
    await h.ingestion.ingest([text_doc("d1", "a")], SCOPE)
    await h.indexer.run(SCOPE, h.unit)
    await h.ingestion.delete(["d1"], SCOPE)

    async def purge() -> None:
        async with store.transaction(SCOPE, write=True) as transaction:
            await transaction.purge_document(SCOPE.key, uid("d1"))

    backend.hooks["remove"] = purge
    await h.indexer.run(SCOPE, h.unit)
    async with store.transaction(SCOPE, write=False) as transaction:
        assert await transaction.get_coverage(h.unit.key(SCOPE), [uid("d1")]) == {}
    assert await active_items(h) == {}


# full rebuilds and publication ----------------------------------------------------------
async def test_immutable_backend_rebuilds_and_retires_the_old_generation(
    store: SQLiteMetadataStore,
) -> None:
    backend = DictIndex(mutable=False)
    h = harness(store, backend)
    await h.ingestion.ingest([text_doc("d1", "a"), text_doc("d2", "b")], SCOPE)
    await h.indexer.run(SCOPE, h.unit)
    first = await pointer(h)
    assert first is not None
    await h.ingestion.ingest([text_doc("d1", "a2")], SCOPE)
    await h.ingestion.delete(["d2"], SCOPE)
    await h.indexer.run(SCOPE, h.unit)
    second = await pointer(h)
    assert second is not None
    assert second.generation_id != first.generation_id
    assert list((await active_items(h)).values()) == ["a2"]
    assert (await generation(h, first.generation_id)).state == GenerationState.RETIRED
    assert (SCOPE.key, first.generation_id) not in backend.generations
    assert (await coverage(h, "d2")).state == CoverageState.DELETED
    assert "generation_retired" in h.logger.events("info")


async def test_rebuild_task_forces_a_new_generation(store: SQLiteMetadataStore) -> None:
    h = harness(store)
    await h.ingestion.ingest([text_doc("d1", "a")], SCOPE)
    await h.indexer.run(SCOPE, h.unit)
    first = await pointer(h)
    ref = await h.coordinator.request(SCOPE, TaskKind.REBUILD, [h.unit])
    await h.indexer.run(SCOPE, h.unit)
    second = await pointer(h)
    assert first is not None
    assert second is not None
    assert second.generation_id != first.generation_id
    result = await h.coordinator.task_result(ref.task_id, SCOPE)
    assert result.state == TaskState.SUCCEEDED

    # A task cancelled while its build runs keeps its terminal state.
    cancelled = await h.coordinator.request(SCOPE, TaskKind.REBUILD, [h.unit])

    async def cancel_task() -> None:
        async with store.transaction(SCOPE, write=True) as transaction:
            task = await transaction.get_task(cancelled.task_id)
            assert task is not None
            await transaction.put_task(
                task.model_copy(update={"state": TaskState.CANCELLED}),
                expected_revision=task.row_revision,
            )

    backend = h.unit.backend
    assert isinstance(backend, DictIndex)
    backend.hooks["seal"] = cancel_task
    await h.indexer.run(SCOPE, h.unit)
    assert (await h.coordinator.task_result(cancelled.task_id, SCOPE)).state == (
        TaskState.CANCELLED
    )


async def test_retention_and_readers_delay_retirement(store: SQLiteMetadataStore) -> None:
    clock = FakeClock()
    backend = DictIndex(mutable=False)
    h = harness(
        store,
        backend,
        clock=clock,
        lifecycle=LifecycleConfig(background=False, generation_retention=60),
    )
    await h.ingestion.ingest([text_doc("d1", "a")], SCOPE)
    await h.indexer.run(SCOPE, h.unit)
    first = await pointer(h)
    assert first is not None
    old = first.generation_id
    assert h.readers.try_acquire(old)
    await h.ingestion.ingest([text_doc("d1", "b")], SCOPE)
    await h.indexer.run(SCOPE, h.unit)
    assert (await generation(h, old)).state == GenerationState.RETIRING
    assert not h.readers.try_acquire(old)  # no new readers of a retiring generation
    clock.advance(61)
    assert not await h.indexer.retire(SCOPE, h.unit, old)  # one reader still active
    h.readers.release(old)
    original = SQLiteTransaction.get_generation

    async def retired_concurrently(self: SQLiteTransaction, generation_id: str) -> Any:
        row = await original(self, generation_id)
        if self._write and row is not None and row.state == GenerationState.RETIRING:
            await self.put_generation(
                row.model_copy(update={"state": GenerationState.RETIRED}),
                expected_revision=row.row_revision,
            )
            return await original(self, generation_id)
        return row

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(SQLiteTransaction, "get_generation", retired_concurrently)
        assert not await h.indexer.retire(SCOPE, h.unit, old)  # fenced by another engine
    assert (SCOPE.key, old) in backend.generations  # its drop is left to that engine
    assert (await generation(h, old)).state == GenerationState.RETIRED
    assert not await h.indexer.retire(SCOPE, h.unit, old)  # already retired
    assert not await h.indexer.retire(SCOPE, h.unit, "gen_unknown")


async def test_retiring_an_active_generation_keeps_it_readable(
    store: SQLiteMetadataStore,
) -> None:
    h = harness(store)
    await h.ingestion.ingest([text_doc("d1", "a")], SCOPE)
    await h.indexer.run(SCOPE, h.unit)
    active = await pointer(h)
    assert active is not None
    assert not await h.indexer.retire(SCOPE, h.unit, active.generation_id)
    assert h.readers.try_acquire(active.generation_id)


async def test_durable_reader_leases_hold_back_retirement(store: SQLiteMetadataStore) -> None:
    clock = FakeClock()
    ttl = LifecycleConfig().lease_ttl
    backend = DictIndex(mutable=False, durable=True)
    h = harness(store, backend, clock=clock, instance_id="inst_a")
    other = harness(store, backend, clock=clock, instance_id="inst_b")
    await h.ingestion.ingest([text_doc("d1", "a")], SCOPE)
    await h.indexer.run(SCOPE, h.unit)
    first = await pointer(h)
    assert first is not None
    old = first.generation_id
    assert await h.readers.lease(store, SCOPE, old, instance_id="inst_a")
    assert await other.readers.lease(store, SCOPE, old, instance_id="inst_b")
    with pytest.MonkeyPatch.context() as patch:
        fail_transaction(patch, "put_reader_lease", lambda: AssertionError("not cached"))
        assert await other.readers.lease(store, SCOPE, old, instance_id="inst_b")

    await h.ingestion.ingest([text_doc("d1", "b")], SCOPE)
    await h.indexer.run(SCOPE, h.unit)
    assert (await generation(h, old)).state == GenerationState.RETIRING
    assert not await h.indexer.retire(SCOPE, h.unit, old)  # inst_b still reads it
    async with store.transaction(SCOPE, write=False) as transaction:
        assert await transaction.count_reader_leases(old, now=clock()) == 1  # inst_a's gone

    clock.advance(ttl / 2 + 1)
    assert await other.readers.lease(store, SCOPE, old, instance_id="inst_b")  # renewed
    clock.advance(ttl / 2 + 1)
    assert not await h.indexer.retire(SCOPE, h.unit, old)
    clock.advance(ttl)
    assert await h.indexer.retire(SCOPE, h.unit, old)  # the lease expired
    assert (SCOPE.key, old) not in backend.generations
    assert not await other.readers.lease(store, SCOPE, old, instance_id="inst_b")
    assert not await other.readers.lease(store, SCOPE, "gen_unknown", instance_id="inst_b")


async def test_changes_after_the_build_watermark_stay_pending(
    store: SQLiteMetadataStore,
) -> None:
    backend = DictIndex(mutable=False)
    h = harness(store, backend, ingestion=IngestionConfig(transient_metadata_keys=("seen",)))
    await h.ingestion.ingest(
        [text_doc("meta", "m"), text_doc("content", "c"), text_doc("gone", "g")], SCOPE
    )

    async def concurrent_changes() -> None:
        await h.ingestion.ingest(
            [text_doc("meta", "m", metadata={"seen": 1}), text_doc("content", "c2")], SCOPE
        )
        await h.ingestion.delete(["gone"], SCOPE)

    backend.hooks["seal"] = concurrent_changes
    await h.indexer.run(SCOPE, h.unit)
    # Version included but metadata changed after the watermark: still pending.
    meta = await coverage(h, "meta")
    assert (meta.state, meta.indexed_version) == (CoverageState.PENDING, None)
    # New content / deletion after the watermark: rows untouched (pending from ingestion).
    assert (await coverage(h, "content")).state == CoverageState.PENDING
    assert (await coverage(h, "gone")).state == CoverageState.PENDING
    assert await h.indexer.needs_work(SCOPE, h.unit)
    await h.indexer.run(SCOPE, h.unit)
    assert set((await active_items(h)).values()) == {"c2", "m"}
    assert (await coverage(h, "gone")).state == CoverageState.DELETED
    assert not await h.indexer.needs_work(SCOPE, h.unit)


async def test_concurrent_publication_loses_the_pointer_race(
    store: SQLiteMetadataStore,
) -> None:
    backend = DictIndex(mutable=False)
    h = harness(store, backend, instance_id="inst_a")
    rival = harness(store, backend, instance_id="inst_b")
    await h.ingestion.ingest([text_doc("d1", "a")], SCOPE)

    async def rival_publishes_first() -> None:
        await rival.indexer.run(SCOPE, rival.unit)

    backend.hooks["seal"] = rival_publishes_first
    with pytest.raises(VersionConflictError):
        await h.indexer.run(SCOPE, h.unit)
    ref = await pointer(h)
    assert ref is not None
    winner = await generation(h, ref.generation_id)
    assert (winner.owner_id, winner.state) == ("inst_b", GenerationState.ACTIVE)
    async with store.transaction(SCOPE, write=False) as transaction:
        generations = await transaction.list_generations(scope_key=SCOPE.key)
    [loser] = [g for g in generations if g.owner_id == "inst_a"]
    assert loser.state == GenerationState.FAILED
    error = loser.stats["error"]
    assert isinstance(error, dict)
    assert error["code"] == "version_conflict"
    assert (SCOPE.key, loser.generation_id) not in backend.generations
    assert "generation_build_failed" in h.logger.events("warning")


async def test_abandoned_build_cannot_publish(store: SQLiteMetadataStore) -> None:
    backend = DictIndex(mutable=False)
    h = harness(store, backend)
    await h.ingestion.ingest([text_doc("d1", "a")], SCOPE)

    async def abandoned_by_reconciler() -> None:
        async with store.transaction(SCOPE, write=True) as transaction:
            [building] = await transaction.list_generations(scope_key=SCOPE.key)
            await transaction.put_generation(
                building.model_copy(update={"state": GenerationState.ABANDONED}),
                expected_revision=building.row_revision,
            )

    backend.hooks["seal"] = abandoned_by_reconciler
    with pytest.raises(VersionConflictError):
        await h.indexer.run(SCOPE, h.unit)
    assert await pointer(h) is None
    async with store.transaction(SCOPE, write=False) as transaction:
        [built] = await transaction.list_generations(scope_key=SCOPE.key)
    assert built.state == GenerationState.ABANDONED  # not overwritten with FAILED
    assert backend.generations == {}


@pytest.mark.parametrize("error", [BackendUnavailableError("down"), asyncio.CancelledError()])
async def test_failed_build_is_marked_and_dropped(
    store: SQLiteMetadataStore, error: BaseException
) -> None:
    backend = DictIndex()
    h = harness(store, backend)
    await h.ingestion.ingest([text_doc("d1", "a")], SCOPE)
    backend.fail["write"] = error
    with pytest.raises(type(error)):
        await h.indexer.run(SCOPE, h.unit)
    assert await pointer(h) is None
    async with store.transaction(SCOPE, write=False) as transaction:
        [failed] = await transaction.list_generations(scope_key=SCOPE.key)
    assert failed.state == GenerationState.FAILED
    assert backend.generations == {}
    assert (await coverage(h, "d1")).state == CoverageState.PENDING


async def test_cleanup_failure_is_logged_and_original_error_raised(
    store: SQLiteMetadataStore,
) -> None:
    backend = DictIndex()
    h = harness(store, backend)
    await h.ingestion.ingest([text_doc("d1", "a")], SCOPE)
    backend.fail["seal"] = BackendUnavailableError("seal failed")
    backend.fail["drop_generation"] = RuntimeError("drop failed")
    with pytest.raises(BackendUnavailableError, match="seal failed"):
        await h.indexer.run(SCOPE, h.unit)
    assert ("error", "generation_cleanup_failed") in [(lvl, ev) for lvl, ev, _ in h.logger.records]
    _, _, fields = next(r for r in h.logger.records if r[1] == "generation_cleanup_failed")
    assert fields["error_type"] == "RuntimeError"


# graph unit -----------------------------------------------------------------------------
async def test_graph_unit_indexes_aggregates_by_supporting_versions(
    store: SQLiteMetadataStore,
) -> None:
    backend = DictIndex()
    unit = index_unit(backend, method="graph", channel=Channel.GRAPH)
    h = build(
        store, [unit], graph=GraphDerivation(IdentityGraphNormalizer(), CapitalizedExtractor())
    )
    await h.ingestion.ingest([text_doc("d1", "Alice met Bob"), text_doc("d2", "nothing")], SCOPE)
    await h.indexer.run(SCOPE, unit)
    assert len(await active_items(h, unit)) == 3
    assert (await coverage(h, "d1", unit)).state == CoverageState.READY
    assert (await coverage(h, "d2", unit)).state == CoverageState.NOT_APPLICABLE
    await h.ingestion.delete(["d1"], SCOPE)
    await h.indexer.run(SCOPE, unit)  # graph units always rebuild
    assert await active_items(h, unit) == {}
    assert (await coverage(h, "d1", unit)).state == CoverageState.DELETED


# reconcile / crash recovery -------------------------------------------------------------
async def test_crashed_instance_build_is_abandoned_by_a_survivor(
    store: SQLiteMetadataStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    clock = FakeClock()
    backend = DictIndex(durable=True)  # shared by both instances
    crashed = harness(store, backend, clock=clock, instance_id="inst_crashed")
    survivor = harness(store, backend, clock=clock, instance_id="inst_survivor")
    await crashed.coordinator.maintain()
    await crashed.ingestion.ingest([text_doc("d1", "a")], SCOPE)

    async def crash() -> None:
        raise asyncio.CancelledError

    async def no_cleanup(*_: object) -> None:
        """The process died: nothing runs after the failure."""

    monkeypatch.setattr(crashed.indexer, "_fail_generation", no_cleanup)
    backend.hooks["seal"] = crash
    with pytest.raises(asyncio.CancelledError):
        await crashed.indexer.run(SCOPE, crashed.unit)
    async with store.transaction(SCOPE, write=False) as transaction:
        [stuck] = await transaction.list_generations(scope_key=SCOPE.key)
    assert stuck.state == GenerationState.BUILDING
    assert (SCOPE.key, stuck.generation_id) in backend.generations

    await survivor.coordinator.maintain()  # owner still alive: left alone
    assert (await generation(survivor, stuck.generation_id)).state == GenerationState.BUILDING

    clock.advance(LifecycleConfig().instance_ttl + 1)
    await survivor.coordinator.maintain()
    assert (await generation(survivor, stuck.generation_id)).state == GenerationState.ABANDONED
    assert backend.generations == {}
    assert "generation_abandoned" in survivor.logger.events("info")
    assert "engine_instance_purged" in survivor.logger.events("info")
    # The survivor takes over and builds the unit.
    await survivor.indexer.run(SCOPE, survivor.unit)
    assert list((await active_items(survivor)).values()) == ["a"]


async def test_reconcile_drops_orphans_and_retires(store: SQLiteMetadataStore) -> None:
    clock = FakeClock()
    backend = DictIndex(mutable=False)
    h = harness(
        store,
        backend,
        clock=clock,
        lifecycle=LifecycleConfig(background=False, generation_retention=30),
    )
    await h.ingestion.ingest([text_doc("d1", "a")], SCOPE)
    await h.indexer.run(SCOPE, h.unit)
    await h.ingestion.ingest([text_doc("d1", "b")], SCOPE)
    await h.indexer.run(SCOPE, h.unit)
    async with store.transaction(SCOPE, write=False) as transaction:
        retiring = [
            g
            for g in await transaction.list_generations(scope_key=SCOPE.key)
            if g.state == GenerationState.RETIRING
        ]
    [old] = retiring
    failed_id, other_id, orphan_id = random_id("gen"), random_id("gen"), random_id("gen")
    # A generation only the backend knows (crash between create and metadata write) and
    # one whose metadata says it failed.
    backend.generations[(SCOPE.key, orphan_id)] = backend.generations[
        (SCOPE.key, old.generation_id)
    ]
    async with store.transaction(SCOPE, write=True) as transaction:
        await transaction.put_generation(
            old.model_copy(
                update={
                    "generation_id": failed_id,
                    "state": GenerationState.FAILED,
                    "row_revision": 0,
                }
            ),
            expected_revision=None,
        )
        await transaction.put_generation(
            old.model_copy(
                update={
                    "generation_id": other_id,
                    "method": "dense",
                    "state": GenerationState.FAILED,
                    "row_revision": 0,
                }
            ),
            expected_revision=None,
        )
    backend.generations[(SCOPE.key, failed_id)] = backend.generations[("x", "y")] = (
        backend.generations[(SCOPE.key, old.generation_id)]
    )
    await h.indexer.reconcile(SCOPE, h.unit)
    remaining = {gid for key, gid in backend.generations if key == SCOPE.key}
    assert orphan_id not in remaining
    assert failed_id not in remaining
    assert old.generation_id in remaining  # retention not yet elapsed
    clock.advance(31)
    await h.indexer.reconcile(SCOPE, h.unit)
    current = await pointer(h)
    assert current is not None
    assert {gid for key, gid in backend.generations if key == SCOPE.key} == {current.generation_id}
    assert ("x", "y") in backend.generations  # other scopes are not touched
    async with store.transaction(SCOPE, write=False) as transaction:
        untouched = await transaction.get_generation(other_id)
    assert untouched is not None  # another unit's generation is not ours to reconcile


async def test_reconcile_waits_for_the_unit_lock(store: SQLiteMetadataStore) -> None:
    h = harness(store)
    async with h.indexer.lock(SCOPE, h.unit):
        task = asyncio.create_task(h.indexer.reconcile(SCOPE, h.unit))
        await asyncio.sleep(0.01)
        assert not task.done()
    await task


async def test_reconcile_keeps_a_generation_registered_meanwhile(
    store: SQLiteMetadataStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    backend = DictIndex(mutable=False)
    h = harness(store, backend)
    await h.ingestion.ingest([text_doc("d1", "a")], SCOPE)
    await h.indexer.run(SCOPE, h.unit)
    current = await pointer(h)
    assert current is not None
    active = await generation(h, current.generation_id)
    late_id = random_id("gen")
    listed = backend.list_generations

    async def register_then_list(scope: Any) -> Any:
        # Another engine starts a build after reconcile read the metadata.
        async with store.transaction(SCOPE, write=True) as transaction:
            await transaction.put_generation(
                active.model_copy(
                    update={
                        "generation_id": late_id,
                        "state": GenerationState.BUILDING,
                        "row_revision": 0,
                    }
                ),
                expected_revision=None,
            )
        backend.generations[(SCOPE.key, late_id)] = backend.generations[
            (SCOPE.key, current.generation_id)
        ]
        return await listed(scope)

    monkeypatch.setattr(backend, "list_generations", register_then_list)
    await h.indexer.reconcile(SCOPE, h.unit)
    assert (SCOPE.key, late_id) in backend.generations


# helpers --------------------------------------------------------------------------------
def test_task_units_and_supporting_versions() -> None:
    now = FakeClock()()
    task = LifecycleTask(
        task_id="t",
        scope_key=SCOPE.key,
        kind=TaskKind.REBUILD,
        target={"units": "index.sparse"},
        state=TaskState.QUEUED,
        created_at=now,
        updated_at=now,
    )
    assert task_units(task) == ()
    assert task_units(task.model_copy(update={"target": {"units": ["a", "b"]}})) == ("a", "b")
    [chunk] = derive_chunks(record("d1", version=3), TextRepresentation(text="x"))
    assert _supporting_versions(chunk) == [(chunk.document_uid, 3)]
    assert _supporting_versions(chunk.model_copy(update={"document_uid": None})) == []
    assert _supporting_versions(chunk.model_copy(update={"version": None})) == []
    source = chunk.sources[0]
    entity = chunk.model_copy(
        update={
            "kind": ObjectKind.GRAPH_ENTITY,
            "document_uid": None,
            "version": None,
            "sources": (
                source,
                source.model_copy(update={"document_uid": "u2", "document_version": 1}),
            ),
        }
    )
    assert _supporting_versions(entity) == [(chunk.document_uid, 3), ("u2", 1)]
