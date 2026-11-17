"""Tenant isolation (``_guard``) and read-only enforcement for every write path."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Any

import pytest

from retrievalloom.domain.base import utc_now
from retrievalloom.domain.records import (
    CoverageState,
    LineageEdge,
    LineageRelation,
    ObjectKind,
    ObjectState,
)
from retrievalloom.errors import LifecycleError, TenantIsolationError
from retrievalloom.storage.base import StoredRefreshState, UnitKey
from tests.unit.storage.factories import (
    OTHER,
    SCOPE,
    coverage,
    document,
    event,
    generation,
    obj,
    snapshot,
    task,
    uid,
    version,
)

if TYPE_CHECKING:
    from retrievalloom.storage.sqlite import SQLiteMetadataStore

Call = Callable[[Any], Awaitable[object]]
FOREIGN_UNIT = UnitKey(scope_key=OTHER.key, channel="index", method="keyword")
FOREIGN_UID = uid(OTHER, "doc-1")
NOW = utc_now()

# Every scope-bound operation, addressed at OTHER from a transaction bound to SCOPE.
CROSS_SCOPE: dict[str, Call] = {
    "ensure_scope": lambda tx: tx.ensure_scope(OTHER),
    "get_document": lambda tx: tx.get_document(OTHER, "doc-1"),
    "get_documents_by_uid": lambda tx: tx.get_documents_by_uid(OTHER.key, [FOREIGN_UID]),
    "put_document": lambda tx: tx.put_document(document(OTHER, "d"), expected_revision=None),
    "get_documents": lambda tx: tx.get_documents(OTHER, ["doc-1"]),
    "put_documents": lambda tx: tx.put_documents([(document(OTHER, "d"), None)]),
    "list_documents": lambda tx: tx.list_documents(OTHER.key),
    "list_document_metadata": lambda tx: tx.list_document_metadata(OTHER.key),
    "count_documents": lambda tx: tx.count_documents(OTHER.key),
    "put_version": lambda tx: tx.put_version(OTHER.key, version(OTHER, "d", 1)),
    "put_versions": lambda tx: tx.put_versions(OTHER.key, [version(OTHER, "d", 1)]),
    "put_representations": lambda tx: tx.put_representations(
        OTHER.key, [snapshot(OTHER, "d", 1, "t")]
    ),
    "get_event": lambda tx: tx.get_event(OTHER.key, "e"),
    "put_event": lambda tx: tx.put_event(event(OTHER, "e")),
    "get_events": lambda tx: tx.get_events(OTHER.key, ["e"]),
    "put_events": lambda tx: tx.put_events([event(OTHER, "e")]),
    "find_events_by_fingerprints": lambda tx: tx.find_events_by_fingerprints(
        OTHER.key, {"fp": "e"}, since=NOW
    ),
    "find_event_by_fingerprint": lambda tx: tx.find_event_by_fingerprint(
        OTHER.key, "fp", since=NOW, exclude_event_id="e"
    ),
    "put_objects": lambda tx: tx.put_objects([obj(OTHER, "o")]),
    "get_objects": lambda tx: tx.get_objects(OTHER.key, ["o"]),
    "list_document_objects": lambda tx: tx.list_document_objects(OTHER.key, FOREIGN_UID),
    "list_objects_for_documents": lambda tx: tx.list_objects_for_documents(
        OTHER.key, [(FOREIGN_UID, None)]
    ),
    "list_current_objects": lambda tx: tx.list_current_objects(OTHER.key, [ObjectKind.CHUNK]),
    "set_document_objects_state": lambda tx: tx.set_document_objects_state(
        OTHER.key, FOREIGN_UID, ObjectState.LIVE
    ),
    "replace_contributions": lambda tx: tx.replace_contributions(OTHER.key, FOREIGN_UID, []),
    "get_contributions": lambda tx: tx.get_contributions(OTHER.key, ["o"]),
    "list_document_contributions": lambda tx: tx.list_document_contributions(
        OTHER.key, FOREIGN_UID
    ),
    "put_lineage": lambda tx: tx.put_lineage(
        [LineageEdge(scope_key=OTHER.key, from_id="a", to_id="b", relation=LineageRelation.PART_OF)]
    ),
    "get_lineage": lambda tx: tx.get_lineage(OTHER.key, "a"),
    "open_visibility": lambda tx: tx.open_visibility(FOREIGN_UNIT, FOREIGN_UID, 1, 1),
    "open_visibility_many": lambda tx: tx.open_visibility_many(FOREIGN_UNIT, [(FOREIGN_UID, 1)], 1),
    "close_visibility": lambda tx: tx.close_visibility(OTHER.key, FOREIGN_UID, 1),
    "visible_versions": lambda tx: tx.visible_versions(FOREIGN_UNIT, [FOREIGN_UID], 1),
    "upsert_coverage": lambda tx: tx.upsert_coverage([coverage(OTHER, "doc-1")]),
    "get_coverage": lambda tx: tx.get_coverage(FOREIGN_UNIT, [FOREIGN_UID]),
    "list_coverage": lambda tx: tx.list_coverage(FOREIGN_UNIT, [CoverageState.PENDING]),
    "get_coverage_watermark": lambda tx: tx.get_coverage_watermark(FOREIGN_UNIT),
    "set_coverage_watermark": lambda tx: tx.set_coverage_watermark(FOREIGN_UNIT, 1),
    "list_changed_documents": lambda tx: tx.list_changed_documents(OTHER.key, after_seq=0),
    "list_keyword_dictionaries": lambda tx: tx.list_keyword_dictionaries(OTHER.key),
    "create_keyword_dictionary": lambda tx: tx.create_keyword_dictionary(OTHER.key, "d", [], {}),
    "adjust_keyword_terms": lambda tx: tx.adjust_keyword_terms(OTHER.key, "d", {"t": 1}),
    "list_keyword_terms": lambda tx: tx.list_keyword_terms(OTHER.key, "d"),
    "coverage_counts": lambda tx: tx.coverage_counts(OTHER.key, [""]),
    "set_document_coverage_state": lambda tx: tx.set_document_coverage_state(
        OTHER.key, FOREIGN_UID, CoverageState.STALE, target_version=1
    ),
    "set_documents_coverage_state": lambda tx: tx.set_documents_coverage_state(
        OTHER.key, {FOREIGN_UID: 1}, CoverageState.STALE
    ),
    "put_generation": lambda tx: tx.put_generation(generation(OTHER), expected_revision=None),
    "list_generations": lambda tx: tx.list_generations(scope_key=OTHER.key),
    "get_pointer": lambda tx: tx.get_pointer(FOREIGN_UNIT),
    "set_pointer": lambda tx: tx.set_pointer(FOREIGN_UNIT, "g", expected_revision=None),
    "list_pointers": lambda tx: tx.list_pointers(OTHER.key, [""]),
    "put_task": lambda tx: tx.put_task(task(OTHER), expected_revision=None),
    "list_tasks": lambda tx: tx.list_tasks(scope_key=OTHER.key),
    "purge_documents": lambda tx: tx.purge_documents(OTHER.key, {}, purged_at=NOW),
    "list_purge_log": lambda tx: tx.list_purge_log(OTHER.key, after_seq=0),
    "prune_purge_log": lambda tx: tx.prune_purge_log(OTHER.key, before=NOW),
}


# Batch writes check the transaction mode before inspecting the items.
WRITE_CHECKED_FIRST = {"put_objects", "put_lineage", "upsert_coverage"}


@pytest.mark.parametrize("name", CROSS_SCOPE)
async def test_scope_bound_transaction_rejects_other_scope(
    store: SQLiteMetadataStore, name: str
) -> None:
    call = CROSS_SCOPE[name]
    for write in (True,) if name in WRITE_CHECKED_FIRST else (True, False):
        with pytest.raises(TenantIsolationError) as info:
            async with store.transaction(SCOPE, write=write) as tx:
                await call(tx)
        assert info.value.details == {"expected": SCOPE.key, "actual": OTHER.key}
    assert not store._write_lock.locked()


async def test_rows_of_other_scope_are_not_loadable_by_id(store: SQLiteMetadataStore) -> None:
    foreign_generation = generation(OTHER)
    foreign_task = task(OTHER)
    async with store.transaction(write=True) as tx:
        await tx.put_generation(foreign_generation, expected_revision=None)
        await tx.put_task(foreign_task, expected_revision=None)
    async with store.transaction(SCOPE, write=False) as tx:
        with pytest.raises(TenantIsolationError):
            await tx.get_generation(foreign_generation.generation_id)
        with pytest.raises(TenantIsolationError):
            await tx.get_task(foreign_task.task_id)
    async with store.transaction(OTHER, write=False) as tx:
        assert await tx.get_generation(foreign_generation.generation_id) is not None
        assert await tx.get_task(foreign_task.task_id) is not None


async def test_mixed_scope_batches_are_rejected_atomically(store: SQLiteMetadataStore) -> None:
    with pytest.raises(TenantIsolationError):
        async with store.transaction(SCOPE, write=True) as tx:
            await tx.put_objects([obj(SCOPE, "mine"), obj(OTHER, "theirs")])
    with pytest.raises(TenantIsolationError):
        async with store.transaction(SCOPE, write=True) as tx:
            await tx.upsert_coverage([coverage(SCOPE, "d"), coverage(OTHER, "d")])
    async with store.transaction(SCOPE, write=False) as tx:
        assert dict(await tx.get_objects(SCOPE.key, [obj(SCOPE, "mine").object_id])) == {}
        unit = UnitKey(scope_key=SCOPE.key, channel="index", method="keyword")
        assert dict(await tx.get_coverage(unit, [uid(SCOPE, "d")])) == {}


# read-only transactions --------------------------------------------------------------
LEASE_TIMES = {"expires_at": NOW, "now": NOW}
UNIT = UnitKey(scope_key=SCOPE.key, channel="index", method="keyword")
SCOPE_UID = uid(SCOPE, "doc-1")

WRITES: dict[str, Call] = {
    "ensure_scope": lambda tx: tx.ensure_scope(SCOPE),
    "next_commit_seq": lambda tx: tx.next_commit_seq(),
    "put_document": lambda tx: tx.put_document(document(SCOPE, "d"), expected_revision=None),
    "put_document_cas": lambda tx: tx.put_document(document(SCOPE, "d"), expected_revision=1),
    "put_documents": lambda tx: tx.put_documents([]),
    "put_version": lambda tx: tx.put_version(SCOPE.key, version(SCOPE, "d", 1)),
    "put_versions": lambda tx: tx.put_versions(SCOPE.key, []),
    "put_representations": lambda tx: tx.put_representations(SCOPE.key, []),
    "put_event": lambda tx: tx.put_event(event(SCOPE, "e")),
    "put_events": lambda tx: tx.put_events([]),
    "put_objects": lambda tx: tx.put_objects([]),
    "set_document_objects_state": lambda tx: tx.set_document_objects_state(
        SCOPE.key, SCOPE_UID, ObjectState.LIVE
    ),
    "replace_contributions": lambda tx: tx.replace_contributions(SCOPE.key, SCOPE_UID, []),
    "put_lineage": lambda tx: tx.put_lineage([]),
    "open_visibility": lambda tx: tx.open_visibility(UNIT, SCOPE_UID, 1, 1),
    "open_visibility_many": lambda tx: tx.open_visibility_many(UNIT, [], 1),
    "close_visibility": lambda tx: tx.close_visibility(SCOPE.key, SCOPE_UID, 1),
    "upsert_coverage": lambda tx: tx.upsert_coverage([]),
    "set_document_coverage_state": lambda tx: tx.set_document_coverage_state(
        SCOPE.key, SCOPE_UID, CoverageState.STALE, target_version=1
    ),
    "set_documents_coverage_state": lambda tx: tx.set_documents_coverage_state(
        SCOPE.key, {}, CoverageState.STALE
    ),
    "put_generation": lambda tx: tx.put_generation(generation(SCOPE), expected_revision=None),
    "set_pointer": lambda tx: tx.set_pointer(UNIT, "g", expected_revision=None),
    "put_task": lambda tx: tx.put_task(task(SCOPE), expected_revision=None),
    "put_reader_lease": lambda tx: tx.put_reader_lease("g", "e", **LEASE_TIMES),
    "delete_reader_lease": lambda tx: tx.delete_reader_lease("g", "e"),
    "purge_expired_leases": lambda tx: tx.purge_expired_leases(now=NOW),
    "heartbeat_instance": lambda tx: tx.heartbeat_instance(
        "i", "e", started_at=NOW, expires_at=NOW
    ),
    "purge_instance": lambda tx: tx.purge_instance("i"),
    "purge_documents": lambda tx: tx.purge_documents(SCOPE.key, {}, purged_at=NOW),
    "prune_purge_log": lambda tx: tx.prune_purge_log(SCOPE.key, before=NOW),
    "apply_refresh_states": lambda tx: tx.apply_refresh_states(
        "ns", [StoredRefreshState(resource_id="r", state={})], []
    ),
}


@pytest.mark.parametrize("name", WRITES)
async def test_read_only_transaction_rejects_writes(store: SQLiteMetadataStore, name: str) -> None:
    with pytest.raises(LifecycleError, match="read-only"):
        async with store.transaction(write=False) as tx:
            await WRITES[name](tx)
    assert not store._write_lock.locked()
