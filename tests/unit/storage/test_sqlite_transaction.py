"""Behaviour of every ``SQLiteTransaction`` method."""

from __future__ import annotations

from datetime import timedelta
from typing import TYPE_CHECKING

import pytest

from retrievalloom.domain.base import utc_now
from retrievalloom.domain.hashing import stable_id
from retrievalloom.domain.inputs import DeliveryProvenance, SourceDescriptor
from retrievalloom.domain.records import (
    CoverageState,
    DocumentState,
    ErrorInfo,
    EventStatus,
    GenerationState,
    IngestOutcome,
    LineageEdge,
    LineageRelation,
    ObjectContribution,
    ObjectKind,
    ObjectState,
    TaskState,
)
from retrievalloom.errors import TenantIsolationError, VersionConflictError
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

CHUNK = 500  # ``_chunks`` default size
UNIT = UnitKey(scope_key=SCOPE.key, channel="index", method="keyword")
ERROR = ErrorInfo(code="boom", category="internal", message="boom")


async def test_commit_sequence_starts_at_one_and_increments(store: SQLiteMetadataStore) -> None:
    async with store.transaction(write=True) as tx:
        assert await tx.current_commit_seq() == 0
        assert await tx.next_commit_seq() == 1
        assert await tx.next_commit_seq() == 2
    async with store.transaction(write=False) as tx:
        assert await tx.current_commit_seq() == 2


async def test_list_scopes_is_ordered_by_key(store: SQLiteMetadataStore) -> None:
    async with store.transaction(write=False) as tx:
        scopes = await tx.list_scopes()
    assert [scope.key for scope in scopes] == sorted([SCOPE.key, OTHER.key])


# documents ----------------------------------------------------------------------------
async def test_document_round_trip_preserves_every_field(store: SQLiteMetadataStore) -> None:
    record = document(
        SCOPE,
        "full",
        state=DocumentState.DELETED,
        deleted_at=utc_now(),
        metadata={"a": [1, 2]},
        dataset="ds",
        source=SourceDescriptor(uri="file://full", title="Full"),
        representation_digests={"text": "sha256:" + "0" * 64},
        representation_kinds={"text": "text"},
        lifecycle_revision=3,
        metadata_revision=2,
        change_seq=7,
    )
    async with store.transaction(SCOPE, write=True) as tx:
        await tx.put_document(record, expected_revision=None)
    async with store.transaction(SCOPE, write=False) as tx:
        stored = await tx.get_document(SCOPE, "full")
        assert await tx.get_document(SCOPE, "missing") is None
    assert stored is not None
    assert stored.model_dump(exclude={"row_revision"}) == record.model_dump(
        exclude={"row_revision"}
    )
    assert stored.row_revision == 1


async def test_document_cas_conflicts(store: SQLiteMetadataStore) -> None:
    record = document(SCOPE, "cas")
    async with store.transaction(SCOPE, write=True) as tx:
        await tx.put_document(record, expected_revision=None)
        await tx.put_document(record.model_copy(update={"version": 2}), expected_revision=1)
    with pytest.raises(VersionConflictError) as info:
        async with store.transaction(SCOPE, write=True) as tx:
            await tx.put_document(record, expected_revision=1)
    assert info.value.details["table"] == "rl_documents"
    assert info.value.details["document_id"] == "cas"
    # Updating a missing row is a conflict too.
    with pytest.raises(VersionConflictError):
        async with store.transaction(SCOPE, write=True) as tx:
            await tx.put_document(document(SCOPE, "ghost"), expected_revision=1)
    async with store.transaction(SCOPE, write=False) as tx:
        stored = await tx.get_document(SCOPE, "cas")
        assert await tx.get_document(SCOPE, "ghost") is None
    assert stored is not None
    assert (stored.version, stored.row_revision) == (2, 2)


async def test_list_documents_paging_and_state_filter(store: SQLiteMetadataStore) -> None:
    async with store.transaction(SCOPE, write=True) as tx:
        for index in range(7):
            state = DocumentState.DELETED if index % 3 == 0 else DocumentState.ACTIVE
            await tx.put_document(document(SCOPE, f"d{index}", state=state), expected_revision=None)
    async with store.transaction(OTHER, write=True) as tx:
        await tx.put_document(document(OTHER, "d9"), expected_revision=None)
    async with store.transaction(SCOPE, write=False) as tx:
        pages: list[list[str]] = []
        after: str | None = None
        while True:
            page = await tx.list_documents(SCOPE.key, after_id=after, limit=3)
            if not page:
                break
            pages.append([item.document_id for item in page])
            after = page[-1].document_id
        deleted = await tx.list_documents(SCOPE.key, state=DocumentState.DELETED)
        active_after = await tx.list_documents(
            SCOPE.key, state=DocumentState.ACTIVE, after_id="d2", limit=2
        )
        counts = await tx.count_documents(SCOPE.key)
    assert pages == [["d0", "d1", "d2"], ["d3", "d4", "d5"], ["d6"]]
    assert [item.document_id for item in deleted] == ["d0", "d3", "d6"]
    assert [item.document_id for item in active_after] == ["d4", "d5"]
    assert dict(counts) == {"active": 4, "deleted": 3}


async def test_get_documents_by_uid_chunks_large_lists(store: SQLiteMetadataStore) -> None:
    total = CHUNK + 3
    async with store.transaction(SCOPE, write=True) as tx:
        for index in range(total):
            await tx.put_document(document(SCOPE, f"bulk{index:04d}"), expected_revision=None)
        assert dict(await tx.get_documents_by_uid(SCOPE.key, [])) == {}
    wanted = [uid(SCOPE, f"bulk{index:04d}") for index in range(total)]
    async with store.transaction(SCOPE, write=False) as tx:
        found = await tx.get_documents_by_uid(SCOPE.key, [*wanted, wanted[0], uid(SCOPE, "nope")])
    assert set(found) == set(wanted)
    assert found[wanted[-1]].document_id == f"bulk{total - 1:04d}"


# versions and representations ---------------------------------------------------------
async def test_versions_are_ordered_and_scope_filtered(store: SQLiteMetadataStore) -> None:
    async with store.transaction(write=True) as tx:
        await tx.put_version(SCOPE.key, version(SCOPE, "v", 2, source=True))
        await tx.put_version(SCOPE.key, version(SCOPE, "v", 1))
    with pytest.raises(VersionConflictError):
        async with store.transaction(write=True) as tx:
            await tx.put_version(SCOPE.key, version(SCOPE, "v", 1))
    document_uid = uid(SCOPE, "v")
    async with store.transaction(write=False) as tx:
        unscoped = await tx.get_versions(document_uid)
    async with store.transaction(SCOPE, write=False) as tx:
        scoped = await tx.get_versions(document_uid)
    async with store.transaction(OTHER, write=False) as tx:
        foreign = await tx.get_versions(document_uid)
    assert [item.version for item in unscoped] == [1, 2]
    assert unscoped[1].source == SourceDescriptor(uri="file://x")
    assert unscoped[0].source is None
    assert scoped == unscoped
    assert list(foreign) == []


async def test_representations_round_trip_and_scope_filter(store: SQLiteMetadataStore) -> None:
    async with store.transaction(SCOPE, write=True) as tx:
        await tx.put_representations(SCOPE.key, [])
        await tx.put_representations(
            SCOPE.key,
            [snapshot(SCOPE, "r", 1, "b", transformed=True), snapshot(SCOPE, "r", 1, "a")],
        )
    document_uid = uid(SCOPE, "r")
    async with store.transaction(write=False) as tx:
        unscoped = await tx.get_representations(document_uid, 1)
        assert list(await tx.get_representations(document_uid, 2)) == []
    async with store.transaction(OTHER, write=False) as tx:
        foreign = await tx.get_representations(document_uid, 1)
    async with store.transaction(SCOPE, write=False) as tx:
        scoped = await tx.get_representations(document_uid, 1)
    assert [item.representation_id for item in unscoped] == ["a", "b"]
    assert unscoped[0].transformation is None
    assert unscoped[1].transformation is not None
    assert unscoped[1].transformation.name == "chunker"
    assert scoped == unscoped
    assert list(foreign) == []


# events -------------------------------------------------------------------------------
async def test_event_insert_then_replace(store: SQLiteMetadataStore) -> None:
    pending = event(SCOPE, "e1")
    async with store.transaction(SCOPE, write=True) as tx:
        await tx.put_event(pending)
        assert await tx.get_event(SCOPE.key, "missing") is None
    committed = pending.model_copy(
        update={
            "status": EventStatus.COMMITTED,
            "outcome": IngestOutcome.CREATED,
            "counter_after": 1,
            "version_after": 1,
            "delivery": DeliveryProvenance(source_system="s", delivery_id="d"),
            "staged_payload": {"k": "v"},
            "error": ERROR,
            "possible_redelivery_of": "e0",
        }
    )
    async with store.transaction(SCOPE, write=True) as tx:
        await tx.put_event(committed)
    async with store.transaction(SCOPE, write=False) as tx:
        stored = await tx.get_event(SCOPE.key, "e1")
    assert stored == committed


async def test_find_event_by_fingerprint(store: SQLiteMetadataStore) -> None:
    now = utc_now()
    old = event(SCOPE, "old", status=EventStatus.COMMITTED, received_at=now - timedelta(hours=2))
    recent = event(SCOPE, "recent", status=EventStatus.COMMITTED, received_at=now)
    newest_pending = event(SCOPE, "pend", received_at=now + timedelta(seconds=1))
    other_fp = event(SCOPE, "otherfp", status=EventStatus.COMMITTED, fingerprint="zz")
    async with store.transaction(SCOPE, write=True) as tx:
        for item in (old, recent, newest_pending, other_fp):
            await tx.put_event(item)
    fingerprint = recent.appearance_fingerprint
    async with store.transaction(SCOPE, write=False) as tx:
        found = await tx.find_event_by_fingerprint(
            SCOPE.key, fingerprint, since=now - timedelta(days=1), exclude_event_id="x"
        )
        excluded = await tx.find_event_by_fingerprint(
            SCOPE.key, fingerprint, since=now - timedelta(minutes=1), exclude_event_id="recent"
        )
        older = await tx.find_event_by_fingerprint(
            SCOPE.key, fingerprint, since=now - timedelta(days=1), exclude_event_id="recent"
        )
    assert found is not None
    assert found.event_id == "recent"
    assert excluded is None
    assert older is not None
    assert older.event_id == "old"


async def test_list_pending_events_scope_and_limit(store: SQLiteMetadataStore) -> None:
    now = utc_now()
    async with store.transaction(write=True) as tx:
        await tx.put_event(event(SCOPE, "p2", received_at=now + timedelta(seconds=2)))
        await tx.put_event(event(SCOPE, "p1", received_at=now))
        await tx.put_event(event(OTHER, "p3", received_at=now + timedelta(seconds=1)))
        await tx.put_event(event(SCOPE, "c", status=EventStatus.COMMITTED))
    async with store.transaction(write=False) as tx:
        everything = await tx.list_pending_events()
        limited = await tx.list_pending_events(limit=1)
    async with store.transaction(SCOPE, write=False) as tx:
        scoped = await tx.list_pending_events()
    assert [item.event_id for item in everything] == ["p1", "p3", "p2"]
    assert [item.scope for item in everything] == [SCOPE, OTHER, SCOPE]
    assert [item.event_id for item in limited] == ["p1"]
    assert [item.event_id for item in scoped] == ["p1", "p2"]


# objects ------------------------------------------------------------------------------
async def test_objects_insert_update_and_cross_scope_reuse(store: SQLiteMetadataStore) -> None:
    first = obj(SCOPE, "o1", transformed=True, metadata={"m": 1}, payload={"p": 2})
    async with store.transaction(SCOPE, write=True) as tx:
        await tx.put_objects([first, obj(SCOPE, "o2")])
        await tx.put_objects([first.model_copy(update={"text": "changed"})])
    async with store.transaction(SCOPE, write=False) as tx:
        found = await tx.get_objects(SCOPE.key, [first.object_id, stable_id("obj", "nope")])
    assert list(found) == [first.object_id]
    assert found[first.object_id].text == "changed"
    assert found[first.object_id].transformation == first.transformation
    assert found[first.object_id].sources == first.sources
    # The same object id may not be claimed by another scope, even unscoped.
    stolen = obj(OTHER, "o1")
    with pytest.raises(TenantIsolationError):
        async with store.transaction(write=True) as tx:
            await tx.put_objects([stolen])
    async with store.transaction(OTHER, write=False) as tx:
        assert dict(await tx.get_objects(OTHER.key, [first.object_id])) == {}


async def test_get_objects_chunks_large_lists(store: SQLiteMetadataStore) -> None:
    items = [obj(SCOPE, f"bulk{index}") for index in range(CHUNK + 2)]
    async with store.transaction(SCOPE, write=True) as tx:
        await tx.put_objects(items)
    async with store.transaction(SCOPE, write=False) as tx:
        found = await tx.get_objects(SCOPE.key, [item.object_id for item in items])
    assert set(found) == {item.object_id for item in items}


async def test_list_document_objects_filters(store: SQLiteMetadataStore) -> None:
    v1 = obj(SCOPE, "v1", number=1)
    v2 = obj(SCOPE, "v2", number=2)
    node = obj(SCOPE, "n2", number=2, kind=ObjectKind.STRUCTURE_NODE)
    elsewhere = obj(SCOPE, "x", document_id="doc-2")
    async with store.transaction(SCOPE, write=True) as tx:
        await tx.put_objects([v1, v2, node, elsewhere])
    document_uid = uid(SCOPE, "doc-1")
    async with store.transaction(SCOPE, write=False) as tx:
        every = await tx.list_document_objects(SCOPE.key, document_uid)
        second = await tx.list_document_objects(SCOPE.key, document_uid, 2)
        chunks = await tx.list_document_objects(SCOPE.key, document_uid, kinds=[ObjectKind.CHUNK])
        both = await tx.list_document_objects(
            SCOPE.key, document_uid, 2, [ObjectKind.STRUCTURE_NODE]
        )
    assert {item.object_id for item in every} == {v1.object_id, v2.object_id, node.object_id}
    assert [item.object_id for item in every] == sorted(item.object_id for item in every)
    assert {item.object_id for item in second} == {v2.object_id, node.object_id}
    assert {item.object_id for item in chunks} == {v1.object_id, v2.object_id}
    assert [item.object_id for item in both] == [node.object_id]


async def test_list_current_objects(store: SQLiteMetadataStore) -> None:
    async with store.transaction(SCOPE, write=True) as tx:
        await tx.put_document(document(SCOPE, "doc-1", 2), expected_revision=None)
        await tx.put_document(
            document(SCOPE, "gone", state=DocumentState.DELETED), expected_revision=None
        )
        await tx.put_objects(
            [
                obj(SCOPE, "old", number=1),
                obj(SCOPE, "cur-a", number=2),
                obj(SCOPE, "cur-b", number=2),
                obj(SCOPE, "dead", number=2, state=ObjectState.SUPERSEDED),
                obj(SCOPE, "deleted-doc", document_id="gone"),
                obj(SCOPE, "ent", document_id=None, number=None, kind=ObjectKind.GRAPH_ENTITY),
                obj(
                    SCOPE,
                    "rel",
                    document_id=None,
                    number=None,
                    kind=ObjectKind.GRAPH_RELATIONSHIP,
                ),
            ]
        )
    current = sorted([stable_id("obj", "cur-a"), stable_id("obj", "cur-b")])
    graph = sorted([stable_id("obj", "ent"), stable_id("obj", "rel")])
    async with store.transaction(SCOPE, write=False) as tx:
        chunks = await tx.list_current_objects(SCOPE.key, [ObjectKind.CHUNK])
        first = await tx.list_current_objects(SCOPE.key, [ObjectKind.CHUNK], limit=1)
        rest = await tx.list_current_objects(
            SCOPE.key, [ObjectKind.CHUNK], after_object_id=first[0].object_id
        )
        aggregates = await tx.list_current_objects(
            SCOPE.key, [ObjectKind.GRAPH_ENTITY, ObjectKind.GRAPH_RELATIONSHIP]
        )
        mixed = await tx.list_current_objects(
            SCOPE.key, [ObjectKind.CHUNK, ObjectKind.GRAPH_ENTITY]
        )
    assert [item.object_id for item in chunks] == current
    assert [item.object_id for item in [*first, *rest]] == current
    assert [item.object_id for item in aggregates] == graph
    # Mixing in non-graph kinds joins against documents, which excludes aggregates.
    assert [item.object_id for item in mixed] == current


async def test_set_document_objects_state(store: SQLiteMetadataStore) -> None:
    document_uid = uid(SCOPE, "doc-1")
    async with store.transaction(SCOPE, write=True) as tx:
        await tx.put_objects([obj(SCOPE, f"s{n}", number=n) for n in (1, 2, 3)])
        superseded = await tx.set_document_objects_state(
            SCOPE.key, document_uid, ObjectState.SUPERSEDED, exclude_version=3
        )
        revived = await tx.set_document_objects_state(
            SCOPE.key, document_uid, ObjectState.LIVE, version=2
        )
        states = {
            item.version: item.state
            for item in await tx.list_document_objects(SCOPE.key, document_uid)
        }
        deleted = await tx.set_document_objects_state(SCOPE.key, document_uid, ObjectState.DELETED)
        missing = await tx.set_document_objects_state(
            SCOPE.key, uid(SCOPE, "nope"), ObjectState.LIVE
        )
    assert (superseded, revived, deleted, missing) == (2, 1, 3, 0)
    assert states == {1: ObjectState.SUPERSEDED, 2: ObjectState.LIVE, 3: ObjectState.LIVE}


# contributions and lineage ------------------------------------------------------------
def _contribution(name: str, document_id: str, number: int, ordinal: int = 0) -> ObjectContribution:
    return ObjectContribution(
        object_id=stable_id("obj", name),
        document_uid=uid(SCOPE, document_id),
        version=number,
        field_path="entities",
        ordinal=ordinal,
        value={"n": ordinal},
    )


async def test_replace_contributions_reports_affected_objects(store: SQLiteMetadataStore) -> None:
    doc_a = uid(SCOPE, "a")
    async with store.transaction(SCOPE, write=True) as tx:
        first = await tx.replace_contributions(
            SCOPE.key, doc_a, [_contribution("x", "a", 1), _contribution("y", "a", 1, 1)]
        )
        await tx.replace_contributions(SCOPE.key, uid(SCOPE, "b"), [_contribution("x", "b", 1)])
        second = await tx.replace_contributions(SCOPE.key, doc_a, [_contribution("z", "a", 2)])
        cleared = await tx.replace_contributions(SCOPE.key, doc_a, [])
    x, y, z = (stable_id("obj", name) for name in "xyz")
    assert first == sorted([x, y])
    assert second == sorted([x, y, z])
    assert cleared == [z]
    async with store.transaction(SCOPE, write=False) as tx:
        remaining = await tx.get_contributions(SCOPE.key, [x, y, z])
        by_doc = await tx.list_document_contributions(SCOPE.key, uid(SCOPE, "b"))
        assert list(await tx.list_document_contributions(SCOPE.key, doc_a)) == []
    assert [(item.object_id, item.document_uid) for item in remaining] == [(x, uid(SCOPE, "b"))]
    assert by_doc == remaining


async def test_contributions_ordering_and_chunking(store: SQLiteMetadataStore) -> None:
    contributions = [
        _contribution("agg", "a", 1, 1),
        _contribution("agg", "a", 1, 0),
        _contribution("agg", "a", 2, 0),
    ]
    many = [_contribution(f"c{index}", "m", 1) for index in range(CHUNK + 1)]
    async with store.transaction(SCOPE, write=True) as tx:
        await tx.replace_contributions(SCOPE.key, uid(SCOPE, "a"), contributions)
        await tx.replace_contributions(SCOPE.key, uid(SCOPE, "m"), many)
    async with store.transaction(SCOPE, write=False) as tx:
        newest_first = await tx.get_contributions(SCOPE.key, [stable_id("obj", "agg")])
        by_document = await tx.list_document_contributions(SCOPE.key, uid(SCOPE, "a"))
        chunked = await tx.get_contributions(SCOPE.key, [item.object_id for item in many])
    assert [(item.version, item.ordinal) for item in newest_first] == [(2, 0), (1, 0), (1, 1)]
    assert [(item.version, item.ordinal) for item in by_document] == [(1, 0), (1, 1), (2, 0)]
    assert len(chunked) == CHUNK + 1


async def test_lineage_is_idempotent_and_bidirectional(store: SQLiteMetadataStore) -> None:
    edges = [
        LineageEdge(scope_key=SCOPE.key, from_id="b", to_id="a", relation=LineageRelation.PART_OF),
        LineageEdge(scope_key=SCOPE.key, from_id="a", to_id="c", relation=LineageRelation.CHILD_OF),
    ]
    async with store.transaction(SCOPE, write=True) as tx:
        await tx.put_lineage(edges)
        await tx.put_lineage(edges)
    async with store.transaction(SCOPE, write=False) as tx:
        touching = await tx.get_lineage(SCOPE.key, "a")
        assert list(await tx.get_lineage(SCOPE.key, "zzz")) == []
    assert touching == sorted(edges, key=lambda edge: (edge.from_id, edge.to_id))


# visibility ---------------------------------------------------------------------------
async def test_visibility_reopen_and_unit_separation(store: SQLiteMetadataStore) -> None:
    document_uid = uid(SCOPE, "vis")
    ephemeral = UNIT.model_copy(update={"instance": "inst"})
    async with store.transaction(SCOPE, write=True) as tx:
        await tx.open_visibility(UNIT, document_uid, 1, 10)
        await tx.open_visibility(UNIT, document_uid, 1, 12)  # already open: no change
        await tx.open_visibility(UNIT, document_uid, 2, 20)
        await tx.open_visibility(UNIT, document_uid, 1, 30)  # reopen a closed window
        await tx.open_visibility(ephemeral, document_uid, 5, 1)
        await tx.open_visibility(ephemeral, document_uid, 6, 40)
        await tx.close_visibility(SCOPE.key, document_uid, 40)  # 6 opened and closed at 40
        await tx.open_visibility(ephemeral, document_uid, 6, 40)  # reuses the empty window
    async with store.transaction(SCOPE, write=False) as tx:
        at = {
            seq: dict(await tx.visible_versions(UNIT, [document_uid], seq))
            for seq in (11, 25, 30, 99)
        }
        other_unit = await tx.visible_versions(ephemeral, [document_uid], 100)
    # Each window is kept: a reader pinned before the reopen still sees the first window.
    assert at[11] == {document_uid: frozenset({1})}
    assert at[25] == {document_uid: frozenset({2})}
    assert at[30] == {document_uid: frozenset({1})}
    assert at[99] == {}
    assert dict(other_unit) == {document_uid: frozenset({6})}


async def test_close_visibility_closes_every_unit(store: SQLiteMetadataStore) -> None:
    document_uid = uid(SCOPE, "closing")
    ephemeral = UNIT.model_copy(update={"instance": "inst"})
    async with store.transaction(SCOPE, write=True) as tx:
        await tx.open_visibility(UNIT, document_uid, 1, 1)
        await tx.open_visibility(UNIT, document_uid, 2, 5)
        await tx.open_visibility(ephemeral, document_uid, 2, 5)
        await tx.close_visibility(SCOPE.key, document_uid, 9)
        await tx.close_visibility(SCOPE.key, document_uid, 50)  # closed windows stay closed
    async with store.transaction(SCOPE, write=False) as tx:
        for unit in (UNIT, ephemeral):
            assert dict(await tx.visible_versions(unit, [document_uid], 8)) == {
                document_uid: frozenset({2})
            }
            assert dict(await tx.visible_versions(unit, [document_uid], 9)) == {}
        assert dict(await tx.visible_versions(UNIT, [document_uid], 3)) == {
            document_uid: frozenset({1})
        }


async def test_visible_versions_chunks_large_lists(store: SQLiteMetadataStore) -> None:
    uids = [uid(SCOPE, f"vis{index}") for index in range(CHUNK + 1)]
    async with store.transaction(SCOPE, write=True) as tx:
        for document_uid in uids:
            await tx.open_visibility(UNIT, document_uid, 1, 1)
    async with store.transaction(SCOPE, write=False) as tx:
        visible = await tx.visible_versions(UNIT, uids, 1)
    assert set(visible) == set(uids)


# coverage -----------------------------------------------------------------------------
async def test_coverage_upsert_and_queries(store: SQLiteMetadataStore) -> None:
    now = utc_now()
    async with store.transaction(SCOPE, write=True) as tx:
        for name in ("c1", "c2", "c3"):
            await tx.put_document(document(SCOPE, name), expected_revision=None)
        await tx.put_document(
            document(SCOPE, "dead", state=DocumentState.DELETED), expected_revision=None
        )
        await tx.upsert_coverage(
            [
                coverage(SCOPE, "c1"),
                coverage(SCOPE, "c2", CoverageState.FAILED, last_error=ERROR, attempts=2),
                coverage(SCOPE, "c1", method="dense"),
                coverage(SCOPE, "c1", instance="inst"),
            ]
        )
        await tx.upsert_coverage(
            [
                coverage(
                    SCOPE,
                    "c1",
                    CoverageState.READY,
                    indexed_version=1,
                    lease_owner="w",
                    lease_expires_at=now,
                )
            ]
        )
    async with store.transaction(SCOPE, write=False) as tx:
        rows = await tx.get_coverage(UNIT, [uid(SCOPE, "c1"), uid(SCOPE, "c2"), uid(SCOPE, "c3")])
        failed = await tx.list_coverage(UNIT, [CoverageState.FAILED, CoverageState.PENDING])
        ready = await tx.list_coverage(UNIT, [CoverageState.READY], limit=1)
        missing = await tx.documents_missing_coverage(UNIT)
        missing_limited = await tx.documents_missing_coverage(UNIT, limit=0)
        shared = await tx.coverage_counts(SCOPE.key, [""])
        everything = await tx.coverage_counts(SCOPE.key, ["", "inst"])
        assert dict(await tx.coverage_counts(SCOPE.key, [])) == {}
    c1 = rows[uid(SCOPE, "c1")]
    assert set(rows) == {uid(SCOPE, "c1"), uid(SCOPE, "c2")}
    assert (c1.state, c1.row_revision, c1.lease_owner) == (CoverageState.READY, 2, "w")
    assert rows[uid(SCOPE, "c2")].last_error == ERROR
    assert [item.document_uid for item in failed] == [uid(SCOPE, "c2")]
    assert [item.document_uid for item in ready] == [uid(SCOPE, "c1")]
    assert [item.document_id for item in missing] == ["c3"]
    assert list(missing_limited) == []
    assert dict(shared) == {
        "index.keyword": {"ready": 1, "failed": 1},
        "index.dense": {"pending": 1},
    }
    assert everything["index.keyword"] == {"ready": 1, "failed": 1, "pending": 1}


async def test_get_coverage_chunks_large_lists(store: SQLiteMetadataStore) -> None:
    names = [f"cov{index}" for index in range(CHUNK + 1)]
    async with store.transaction(SCOPE, write=True) as tx:
        await tx.upsert_coverage([coverage(SCOPE, name) for name in names])
    async with store.transaction(SCOPE, write=False) as tx:
        rows = await tx.get_coverage(UNIT, [uid(SCOPE, name) for name in names])
    assert len(rows) == CHUNK + 1


async def test_set_document_coverage_state_touches_every_unit(store: SQLiteMetadataStore) -> None:
    async with store.transaction(SCOPE, write=True) as tx:
        await tx.upsert_coverage(
            [coverage(SCOPE, "d"), coverage(SCOPE, "d", method="dense"), coverage(SCOPE, "e")]
        )
        await tx.set_document_coverage_state(
            SCOPE.key, uid(SCOPE, "d"), CoverageState.STALE, target_version=4
        )
    dense = UNIT.model_copy(update={"method": "dense"})
    async with store.transaction(SCOPE, write=False) as tx:
        keyword_rows = await tx.get_coverage(UNIT, [uid(SCOPE, "d"), uid(SCOPE, "e")])
        dense_rows = await tx.get_coverage(dense, [uid(SCOPE, "d")])
    stale = keyword_rows[uid(SCOPE, "d")]
    assert (stale.state, stale.target_version, stale.row_revision) == (CoverageState.STALE, 4, 2)
    assert dense_rows[uid(SCOPE, "d")].state is CoverageState.STALE
    assert keyword_rows[uid(SCOPE, "e")].state is CoverageState.PENDING


# generations and pointers -------------------------------------------------------------
async def test_generation_round_trip_and_listing(store: SQLiteMetadataStore) -> None:
    now = utc_now()
    durable = generation(SCOPE, GenerationState.ACTIVE).model_copy(
        update={"published_at": now, "stats": {"docs": 3}, "owner_id": "w", "heartbeat_at": now}
    )
    ephemeral = generation(SCOPE, GenerationState.BUILDING, instance="inst")
    foreign = generation(OTHER, GenerationState.ACTIVE)
    async with store.transaction(write=True) as tx:
        for item in (durable, ephemeral, foreign):
            await tx.put_generation(item, expected_revision=None)
        assert await tx.get_generation("gen_" + "0" * 32) is None
    async with store.transaction(write=False) as tx:
        stored = await tx.get_generation(durable.generation_id)
        unscoped = await tx.list_generations()
        active = await tx.list_generations(states=[GenerationState.ACTIVE])
        named = await tx.list_generations(instances=["inst"])
        shared = await tx.list_generations(instances=[None])
        both = await tx.list_generations(scope_key=SCOPE.key, instances=["inst", None])
        nothing = await tx.list_generations(instances=[])
    async with store.transaction(SCOPE, write=False) as tx:
        scoped = await tx.list_generations()
    assert stored is not None
    assert stored.model_dump(exclude={"row_revision"}) == durable.model_dump(
        exclude={"row_revision"}
    )
    ids = {item.generation_id for item in (durable, ephemeral, foreign)}
    assert {item.generation_id for item in unscoped} == ids
    assert {item.generation_id for item in active} == {durable.generation_id, foreign.generation_id}
    assert [item.generation_id for item in named] == [ephemeral.generation_id]
    assert {item.generation_id for item in shared} == {
        durable.generation_id,
        foreign.generation_id,
    }
    assert {item.generation_id for item in both} == {
        durable.generation_id,
        ephemeral.generation_id,
    }
    assert list(nothing) == []
    assert {item.generation_id for item in scoped} == {
        durable.generation_id,
        ephemeral.generation_id,
    }


async def test_pointer_cas_and_listing(store: SQLiteMetadataStore) -> None:
    ephemeral = UNIT.model_copy(update={"instance": "inst"})
    async with store.transaction(SCOPE, write=True) as tx:
        assert await tx.get_pointer(UNIT) is None
        await tx.set_pointer(UNIT, "gen-1", expected_revision=None)
        await tx.set_pointer(ephemeral, "gen-e", expected_revision=None)
        await tx.set_pointer(UNIT, "gen-2", expected_revision=1)
    with pytest.raises(VersionConflictError) as info:
        async with store.transaction(SCOPE, write=True) as tx:
            await tx.set_pointer(UNIT, "gen-3", expected_revision=1)
    assert info.value.details["table"] == "rl_generation_pointers"
    async with store.transaction(SCOPE, write=False) as tx:
        pointer = await tx.get_pointer(UNIT)
        durable = await tx.list_pointers(SCOPE.key, [""])
        every = await tx.list_pointers(SCOPE.key, ["", "inst"])
    assert pointer is not None
    assert (pointer.unit, pointer.generation_id, pointer.row_revision) == (UNIT, "gen-2", 2)
    assert list(durable) == [pointer]
    assert [row.unit.instance for row in every] == ["", "inst"]


# tasks --------------------------------------------------------------------------------
async def test_tasks_round_trip_cas_and_listing(store: SQLiteMetadataStore) -> None:
    queued = task(SCOPE, target={"method": "keyword"}, cursor={"after": "x"})
    failed = task(SCOPE, TaskState.FAILED, error=ERROR, attempts=3)
    foreign = task(OTHER)
    async with store.transaction(write=True) as tx:
        for item in (queued, failed, foreign):
            await tx.put_task(item, expected_revision=None)
        assert await tx.get_task("task_missing") is None
        running = queued.model_copy(update={"state": TaskState.RUNNING})
        await tx.put_task(running, expected_revision=1)
    with pytest.raises(VersionConflictError):
        async with store.transaction(write=True) as tx:
            await tx.put_task(queued, expected_revision=1)
    async with store.transaction(write=False) as tx:
        stored_failed = await tx.get_task(failed.task_id)
        every = await tx.list_tasks()
        listed_running = await tx.list_tasks(states=[TaskState.RUNNING])
        limited = await tx.list_tasks(scope_key=OTHER.key, limit=1)
    async with store.transaction(SCOPE, write=False) as tx:
        scoped = await tx.list_tasks()
    assert stored_failed is not None
    assert stored_failed.model_dump(exclude={"row_revision"}) == failed.model_dump(
        exclude={"row_revision"}
    )
    assert {item.task_id for item in every} == {queued.task_id, failed.task_id, foreign.task_id}
    assert [item.task_id for item in listed_running] == [queued.task_id]
    assert listed_running[0].target == {"method": "keyword"}
    assert [item.task_id for item in limited] == [foreign.task_id]
    assert {item.task_id for item in scoped} == {queued.task_id, failed.task_id}


# leases and instances -----------------------------------------------------------------
async def test_reader_lease_extension_and_purge(store: SQLiteMetadataStore) -> None:
    now = utc_now()
    async with store.transaction(write=True) as tx:
        await tx.put_reader_lease("gen", "e1", expires_at=now, now=now)
        await tx.put_reader_lease("gen", "e1", expires_at=now + timedelta(minutes=1), now=now)
        await tx.put_reader_lease("gen", "e2", expires_at=now, now=now)
        await tx.put_reader_lease("other", "e1", expires_at=now + timedelta(minutes=1), now=now)
    async with store.transaction(write=False) as tx:
        assert await tx.count_reader_leases("gen", now=now) == 1
        assert await tx.count_reader_leases("none", now=now) == 0
    async with store.transaction(write=True) as tx:
        assert await tx.purge_expired_leases(now=now) == 1
        await tx.delete_reader_lease("gen", "e1")
        assert await tx.count_reader_leases("gen", now=now - timedelta(days=1)) == 0
        assert await tx.count_reader_leases("other", now=now) == 1


async def test_instances_heartbeat_expiry_and_purge(store: SQLiteMetadataStore) -> None:
    now = utc_now()
    later = now + timedelta(minutes=5)
    instance = "inst"
    unit = UNIT.model_copy(update={"instance": instance})
    document_uid = uid(SCOPE, "d")
    ephemeral_generation = generation(SCOPE, instance=instance)
    durable_generation = generation(SCOPE)
    async with store.transaction(SCOPE, write=True) as tx:
        await tx.heartbeat_instance(instance, "eng", started_at=now, expires_at=now)
        await tx.heartbeat_instance("other", "eng", started_at=now, expires_at=now)
        await tx.heartbeat_instance(instance, "eng", started_at=now, expires_at=later)
        await tx.upsert_coverage([coverage(SCOPE, "d", instance=instance), coverage(SCOPE, "d")])
        await tx.open_visibility(unit, document_uid, 1, 1)
        await tx.open_visibility(UNIT, document_uid, 1, 1)
        await tx.set_pointer(unit, "g", expected_revision=None)
        await tx.set_pointer(UNIT, "g", expected_revision=None)
        await tx.put_generation(ephemeral_generation, expected_revision=None)
        await tx.put_generation(durable_generation, expected_revision=None)
        assert list(await tx.expired_instances(now=now)) == ["other"]
        assert list(await tx.expired_instances(now=later)) == [instance, "other"]
        with pytest.raises(ValueError, match="shared"):
            await tx.purge_instance("")
        await tx.purge_instance(instance)
        assert list(await tx.expired_instances(now=later)) == ["other"]
        assert dict(await tx.get_coverage(unit, [document_uid])) == {}
        assert set(await tx.get_coverage(UNIT, [document_uid])) == {document_uid}
        assert dict(await tx.visible_versions(unit, [document_uid], 1)) == {}
        assert await tx.get_pointer(unit) is None
        assert await tx.get_pointer(UNIT) is not None
        assert await tx.get_generation(ephemeral_generation.generation_id) is None
        assert await tx.get_generation(durable_generation.generation_id) is not None


# refresh state ------------------------------------------------------------------------
async def test_refresh_states_chunk_large_deletes(store: SQLiteMetadataStore) -> None:
    puts = [
        StoredRefreshState(resource_id=f"r{index:04d}", state={"i": index})
        for index in range(CHUNK + 5)
    ]
    async with store.transaction(write=True) as tx:
        await tx.apply_refresh_states("ns", puts, [])
        await tx.apply_refresh_states("other", puts[:1], [])
    async with store.transaction(write=True) as tx:
        # Re-putting an existing id replaces it; deletes span several chunks.
        await tx.apply_refresh_states(
            "ns",
            [StoredRefreshState(resource_id="r0000", state={"i": -1})],
            [item.resource_id for item in puts[1:]],
        )
        await tx.apply_refresh_states("ns", [], [])
    async with store.transaction(write=False) as tx:
        remaining = await tx.load_refresh_states("ns")
        other = await tx.load_refresh_states("other")
    assert [(item.resource_id, item.state) for item in remaining] == [("r0000", {"i": -1})]
    assert [(item.resource_id, item.state) for item in other] == [("r0000", {"i": 0})]
