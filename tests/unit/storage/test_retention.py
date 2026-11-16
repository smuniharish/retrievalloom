"""Physical erasure and retention queries of ``SQLiteTransaction``."""

from __future__ import annotations

from datetime import timedelta
from typing import TYPE_CHECKING

import pytest

from retrievalloom.domain.base import utc_now
from retrievalloom.domain.records import (
    CoverageState,
    DocumentState,
    EventStatus,
    GenerationState,
    LineageEdge,
    LineageRelation,
    ObjectContribution,
    ObjectKind,
    ObjectState,
)
from retrievalloom.errors import LifecycleError, TenantIsolationError
from retrievalloom.storage.base import UnitKey
from tests.unit.storage.factories import (
    OTHER,
    SCOPE,
    coverage,
    document,
    event,
    generation,
    obj,
    snapshot,
    uid,
    version,
)

if TYPE_CHECKING:
    from retrievalloom.storage.sqlite import SQLiteMetadataStore

UNIT = UnitKey(scope_key=SCOPE.key, channel="index", method="keyword")


async def _history(store: SQLiteMetadataStore, document_id: str, versions: int) -> None:
    async with store.transaction(SCOPE, write=True) as tx:
        await tx.put_document(document(SCOPE, document_id, versions), expected_revision=None)
        for number in range(1, versions + 1):
            await tx.put_version(SCOPE.key, version(SCOPE, document_id, number))
            await tx.put_representations(SCOPE.key, [snapshot(SCOPE, document_id, number, "r")])


async def _versions(store: SQLiteMetadataStore, document_id: str) -> list[int]:
    async with store.transaction(SCOPE, write=False) as tx:
        return [item.version for item in await tx.get_versions(uid(SCOPE, document_id))]


async def test_prune_versions_keeps_protected_versions(store: SQLiteMetadataStore) -> None:
    await _history(store, "d", 6)
    await _history(store, "small", 1)
    doc = uid(SCOPE, "d")
    live = obj(SCOPE, "live-agg", document_id=None, number=None, kind=ObjectKind.GRAPH_ENTITY)
    orphan = obj(
        SCOPE,
        "orphan-agg",
        document_id=None,
        number=None,
        kind=ObjectKind.GRAPH_ENTITY,
        state=ObjectState.ORPHANED,
    )
    active = generation(SCOPE, GenerationState.ACTIVE)
    retired = generation(SCOPE, GenerationState.RETIRED)
    async with store.transaction(SCOPE, write=True) as tx:
        # v1: a live chunk; v2: supports a live aggregate; v4: open window; v5: indexed
        # by an active generation; v6: current. v3 only backs an orphan and a retired
        # generation, so it is the one version pruned.
        await tx.put_objects([obj(SCOPE, "chunk", document_id="d", number=1), live, orphan])
        await tx.replace_contributions(
            SCOPE.key,
            doc,
            [
                ObjectContribution(
                    object_id=live.object_id, document_uid=doc, version=2, field_path="n"
                ),
                ObjectContribution(
                    object_id=orphan.object_id, document_uid=doc, version=3, field_path="n"
                ),
            ],
        )
        await tx.open_visibility(UNIT, doc, 4, 1)
        await tx.put_generation(active, expected_revision=None)
        await tx.put_generation(retired, expected_revision=None)
        await tx.upsert_coverage(
            [
                coverage(
                    SCOPE,
                    "d",
                    CoverageState.READY,
                    generation_id=active.generation_id,
                    indexed_version=5,
                ),
                coverage(
                    SCOPE,
                    "d",
                    CoverageState.READY,
                    method="dense",
                    generation_id=retired.generation_id,
                    indexed_version=3,
                ),
            ]
        )
        with pytest.raises(ValueError, match="at least 1"):
            await tx.prune_versions(SCOPE.key, keep=0)
        assert await tx.prune_versions(SCOPE.key, keep=1) == 1
        assert await tx.prune_versions(SCOPE.key, keep=1) == 0
        assert list(await tx.get_representations(doc, 3)) == []
        assert list(await tx.get_representations(doc, 2)) != []
    assert await _versions(store, "d") == [1, 2, 4, 5, 6]
    assert await _versions(store, "small") == [1]


async def test_prune_versions_honours_keep_and_limit(store: SQLiteMetadataStore) -> None:
    await _history(store, "d", 5)
    async with store.transaction(SCOPE, write=True) as tx:
        assert await tx.prune_versions(SCOPE.key, keep=2, limit=2) == 2
        assert await tx.prune_versions(SCOPE.key, keep=2, limit=2) == 1
    assert await _versions(store, "d") == [4, 5]
    async with store.transaction(OTHER, write=True) as tx:
        assert await tx.prune_versions(OTHER.key, keep=1) == 0
        with pytest.raises(TenantIsolationError):
            await tx.prune_versions(SCOPE.key, keep=1)


async def test_prune_events_deletes_only_old_terminal_events(store: SQLiteMetadataStore) -> None:
    now = utc_now()
    old = now - timedelta(days=100)
    async with store.transaction(SCOPE, write=True) as tx:
        await tx.put_event(event(SCOPE, "pending", received_at=old))
        await tx.put_event(event(SCOPE, "committed", status=EventStatus.COMMITTED, received_at=old))
        await tx.put_event(event(SCOPE, "failed", status=EventStatus.FAILED, received_at=old))
        await tx.put_event(event(SCOPE, "recent", status=EventStatus.COMMITTED, received_at=now))
        cutoff = now - timedelta(days=90)
        assert await tx.prune_events(SCOPE.key, before=cutoff, limit=1) == 1
        assert await tx.prune_events(SCOPE.key, before=cutoff) == 1
        assert await tx.prune_events(SCOPE.key, before=cutoff) == 0
        assert await tx.get_event(SCOPE.key, "pending") is not None
        assert await tx.get_event(SCOPE.key, "recent") is not None
        assert await tx.get_event(SCOPE.key, "committed") is None
        assert await tx.get_event(SCOPE.key, "failed") is None


async def test_purge_document_rules(store: SQLiteMetadataStore) -> None:
    doc = uid(SCOPE, "d")
    aggregate = obj(SCOPE, "agg", document_id=None, number=None, kind=ObjectKind.GRAPH_ENTITY)
    chunk = obj(SCOPE, "chunk", document_id="d")
    await _history(store, "d", 2)
    async with store.transaction(SCOPE, write=True) as tx:
        await tx.put_objects([chunk, aggregate])
        await tx.put_lineage(
            [
                LineageEdge(
                    scope_key=SCOPE.key,
                    from_id=chunk.object_id,
                    to_id=aggregate.object_id,
                    relation=LineageRelation.PART_OF,
                )
            ]
        )
        with pytest.raises(LifecycleError) as caught:
            await tx.purge_document(SCOPE.key, doc)
        assert caught.value.details == {"document_uid": doc, "state": "active"}
        stored = await tx.get_document(SCOPE, "d")
        assert stored is not None
        await tx.put_document(
            stored.model_copy(update={"state": DocumentState.DELETED, "deleted_at": utc_now()}),
            expected_revision=stored.row_revision,
        )
        # 2 versions + 2 representations + document + chunk + aggregate + 1 lineage edge
        assert await tx.purge_document(SCOPE.key, doc, object_ids=[aggregate.object_id]) == 8
        assert await tx.purge_document(SCOPE.key, doc) == 0
        assert await tx.purge_document(SCOPE.key, uid(SCOPE, "never")) == 0
    async with store.transaction(SCOPE, write=False) as tx:
        assert dict(await tx.get_objects(SCOPE.key, [aggregate.object_id])) == {}
        with pytest.raises(TenantIsolationError):
            await tx.list_purgeable_documents(OTHER.key, deleted_before=utc_now())
    with pytest.raises(TenantIsolationError):
        async with store.transaction(SCOPE, write=True) as tx:
            await tx.purge_document(OTHER.key, doc)
