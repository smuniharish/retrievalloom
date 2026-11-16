"""Contract kit for ``DocumentMetadataStoreABC``."""

from __future__ import annotations

from datetime import timedelta
from typing import Any

import pytest

from retrievalloom.domain.base import utc_now
from retrievalloom.domain.hashing import random_id, sha256_digest, stable_id
from retrievalloom.domain.inputs import Scope
from retrievalloom.domain.records import (
    Channel,
    DocumentRecord,
    GenerationState,
    IndexGeneration,
    LifecycleTask,
    TaskKind,
    TaskState,
)
from retrievalloom.errors import TenantIsolationError, VersionConflictError
from retrievalloom.storage.base import StoredRefreshState, UnitKey
from retrievalloom.testing._base import ComponentContract

SCOPE = Scope(tenant_id="store-a")
OTHER_SCOPE = Scope(tenant_id="store-b")


def document(scope: Scope, document_id: str, version: int = 1, **values: Any) -> DocumentRecord:
    """A document record of the kit."""
    now = utc_now()
    return DocumentRecord.model_validate(
        {
            "_id": document_id,
            "_counter": version,
            "_version": version,
            "_content_hash": sha256_digest(f"{document_id}:{version}"),
            "_created_at": now,
            "_updated_at": now,
            "document_uid": stable_id("doc", scope.key, document_id),
            "scope": scope,
            "hash_profile": "contract",
            **values,
        }
    )


def generation(scope: Scope, state: GenerationState = GenerationState.BUILDING) -> IndexGeneration:
    """An index generation of the kit."""
    return IndexGeneration(
        generation_id=random_id("gen"),
        scope_key=scope.key,
        channel=Channel.INDEX,
        method="keyword",
        backend_id="contract",
        dependency_fingerprint=sha256_digest("deps"),
        state=state,
        created_at=utc_now(),
    )


class MetadataStoreContract(ComponentContract):
    """Kit for ``DocumentMetadataStoreABC``: transactions, CAS, isolation and lifecycle rows."""

    async def prepare(self, component: Any) -> None:
        """Migrate the schema to head."""
        await component.migrate()

    async def test_migrate_is_idempotent(self, component: Any) -> None:
        """Migrating twice keeps the schema at the same revision."""
        await component.migrate()
        version = await component.schema_version()
        await component.migrate()
        assert version is not None
        assert await component.schema_version() == version

    async def test_scopes_and_commit_sequence(self, component: Any) -> None:
        """Scopes register idempotently; the commit sequence is strictly monotonic."""
        async with component.transaction(write=True) as tx:
            await tx.ensure_scope(SCOPE)
            await tx.ensure_scope(SCOPE)
            first = await tx.next_commit_seq()
            second = await tx.next_commit_seq()
        async with component.transaction(write=False) as tx:
            assert SCOPE in await tx.list_scopes()
            assert await tx.current_commit_seq() == second
        assert second > first

    async def test_document_compare_and_set(self, component: Any) -> None:
        """Inserting twice or updating with a stale revision conflicts."""
        record = document(SCOPE, "doc-1")
        async with component.transaction(SCOPE, write=True) as tx:
            await tx.ensure_scope(SCOPE)
            await tx.put_document(record, expected_revision=None)
        with pytest.raises(VersionConflictError):
            async with component.transaction(SCOPE, write=True) as tx:
                await tx.put_document(record, expected_revision=None)
        async with component.transaction(SCOPE, write=False) as tx:
            stored = await tx.get_document(SCOPE, "doc-1")
        assert stored is not None
        assert stored.document_uid == record.document_uid
        updated = stored.model_copy(update={"metadata": {"k": "v"}})
        async with component.transaction(SCOPE, write=True) as tx:
            await tx.put_document(updated, expected_revision=stored.row_revision)
        with pytest.raises(VersionConflictError):
            async with component.transaction(SCOPE, write=True) as tx:
                await tx.put_document(updated, expected_revision=stored.row_revision)
        async with component.transaction(SCOPE, write=False) as tx:
            final = await tx.get_document(SCOPE, "doc-1")
            by_uid = await tx.get_documents_by_uid(SCOPE.key, [record.document_uid])
            listed = await tx.list_documents(SCOPE.key)
        assert final is not None
        assert final.metadata == {"k": "v"}
        assert final.row_revision > stored.row_revision
        assert set(by_uid) == {record.document_uid}
        assert [item.document_id for item in listed] == ["doc-1"]

    async def test_rollback_on_exception(self, component: Any) -> None:
        """A failing transaction leaves no trace."""
        message = "boom"
        with pytest.raises(RuntimeError, match=message):  # noqa: PT012
            async with component.transaction(SCOPE, write=True) as tx:
                await tx.ensure_scope(SCOPE)
                await tx.put_document(document(SCOPE, "doc-r"), expected_revision=None)
                raise RuntimeError(message)
        async with component.transaction(SCOPE, write=False) as tx:
            assert await tx.get_document(SCOPE, "doc-r") is None

    async def test_tenant_isolation(self, component: Any) -> None:
        """A scope-bound transaction rejects another scope and never sees its rows."""
        async with component.transaction(write=True) as tx:
            await tx.ensure_scope(SCOPE)
            await tx.ensure_scope(OTHER_SCOPE)
            await tx.put_document(document(OTHER_SCOPE, "doc-x"), expected_revision=None)
        with pytest.raises(TenantIsolationError):
            async with component.transaction(SCOPE, write=True) as tx:
                await tx.put_document(document(OTHER_SCOPE, "doc-y"), expected_revision=None)
        async with component.transaction(SCOPE, write=False) as tx:
            assert await tx.get_document(SCOPE, "doc-x") is None
            assert list(await tx.list_documents(SCOPE.key)) == []

    async def test_visibility_windows(self, component: Any) -> None:
        """Opening a version closes the previous one at that sequence; closing hides all."""
        unit = UnitKey(scope_key=SCOPE.key, channel="index", method="keyword")
        uid = stable_id("doc", "visibility")
        async with component.transaction(SCOPE, write=True) as tx:
            await tx.ensure_scope(SCOPE)
            await tx.open_visibility(unit, uid, 1, 10)
            await tx.open_visibility(unit, uid, 2, 20)
            await tx.close_visibility(SCOPE.key, uid, 30)
        async with component.transaction(SCOPE, write=False) as tx:
            assert dict(await tx.visible_versions(unit, [uid], 5)) == {}
            assert dict(await tx.visible_versions(unit, [uid], 15)) == {uid: frozenset({1})}
            assert dict(await tx.visible_versions(unit, [uid], 25)) == {uid: frozenset({2})}
            assert dict(await tx.visible_versions(unit, [uid], 30)) == {}

    async def test_generations_and_pointer_cas(self, component: Any) -> None:
        """Generations CAS by revision; pointers publish with compare-and-set."""
        built = generation(SCOPE)
        unit = UnitKey(scope_key=SCOPE.key, channel="index", method="keyword")
        async with component.transaction(SCOPE, write=True) as tx:
            await tx.ensure_scope(SCOPE)
            await tx.put_generation(built, expected_revision=None)
            await tx.set_pointer(unit, built.generation_id, expected_revision=None)
        async with component.transaction(SCOPE, write=False) as tx:
            stored = await tx.get_generation(built.generation_id)
            pointer = await tx.get_pointer(unit)
        assert stored is not None
        assert pointer is not None
        assert pointer.generation_id == built.generation_id
        with pytest.raises(VersionConflictError):
            async with component.transaction(SCOPE, write=True) as tx:
                await tx.set_pointer(unit, built.generation_id, expected_revision=None)
        active = stored.model_copy(update={"state": GenerationState.ACTIVE})
        async with component.transaction(SCOPE, write=True) as tx:
            await tx.put_generation(active, expected_revision=stored.row_revision)
            await tx.set_pointer(unit, built.generation_id, expected_revision=pointer.row_revision)
        with pytest.raises(VersionConflictError):
            async with component.transaction(SCOPE, write=True) as tx:
                await tx.put_generation(active, expected_revision=stored.row_revision)
        async with component.transaction(SCOPE, write=False) as tx:
            listed = await tx.list_generations(scope_key=SCOPE.key, states=[GenerationState.ACTIVE])
            pointers = await tx.list_pointers(SCOPE.key, [""])
        assert [item.generation_id for item in listed] == [built.generation_id]
        assert [row.generation_id for row in pointers] == [built.generation_id]

    async def test_task_fencing(self, component: Any) -> None:
        """Task updates with a stale revision are rejected."""
        now = utc_now()
        task = LifecycleTask(
            task_id=random_id("task"),
            scope_key=SCOPE.key,
            kind=TaskKind.REBUILD,
            state=TaskState.QUEUED,
            created_at=now,
            updated_at=now,
        )
        async with component.transaction(SCOPE, write=True) as tx:
            await tx.ensure_scope(SCOPE)
            await tx.put_task(task, expected_revision=None)
        async with component.transaction(SCOPE, write=False) as tx:
            stored = await tx.get_task(task.task_id)
        assert stored is not None
        running = stored.model_copy(update={"state": TaskState.RUNNING})
        async with component.transaction(SCOPE, write=True) as tx:
            await tx.put_task(running, expected_revision=stored.row_revision)
        with pytest.raises(VersionConflictError):
            async with component.transaction(SCOPE, write=True) as tx:
                await tx.put_task(running, expected_revision=stored.row_revision)
        async with component.transaction(SCOPE, write=False) as tx:
            listed = await tx.list_tasks(scope_key=SCOPE.key, states=[TaskState.RUNNING])
        assert [item.task_id for item in listed] == [task.task_id]

    async def test_reader_leases_and_instances(self, component: Any) -> None:
        """Leases count until expiry; expired instances are reported and purgeable."""
        now = utc_now()
        later = now + timedelta(minutes=5)
        async with component.transaction(write=True) as tx:
            await tx.put_reader_lease("gen", "engine-1", expires_at=later, now=now)
            await tx.put_reader_lease("gen", "engine-2", expires_at=now, now=now)
            await tx.put_reader_lease("gen", "inst-1", expires_at=later, now=now)
            await tx.heartbeat_instance("inst-1", "engine-1", started_at=now, expires_at=now)
        async with component.transaction(write=False) as tx:
            assert await tx.count_reader_leases("gen", now=now) == 2
            assert "inst-1" in await tx.expired_instances(now=later)
        async with component.transaction(write=True) as tx:
            assert await tx.purge_expired_leases(now=now) == 1
            await tx.delete_reader_lease("gen", "engine-1")
            await tx.purge_instance("inst-1")
        async with component.transaction(write=False) as tx:
            assert await tx.count_reader_leases("gen", now=now) == 0
            assert "inst-1" not in await tx.expired_instances(now=later)

    async def test_refresh_state_persistence(self, component: Any) -> None:
        """Refresh-engine state round-trips per namespace with puts and deletes."""
        a = StoredRefreshState(resource_id="a", state={"n": 1})
        b = StoredRefreshState(resource_id="b", state={"n": 2})
        async with component.transaction(write=True) as tx:
            await tx.apply_refresh_states("ns", [a, b], [])
        async with component.transaction(write=True) as tx:
            await tx.apply_refresh_states("ns", [a.model_copy(update={"state": {"n": 3}})], ["b"])
        async with component.transaction(write=False) as tx:
            states = await tx.load_refresh_states("ns")
            assert list(await tx.load_refresh_states("other")) == []
        assert [(item.resource_id, item.state) for item in states] == [("a", {"n": 3})]
