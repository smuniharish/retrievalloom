"""Store lifecycle, pragmas, migrations and transaction mechanics of the SQLite store."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any

import pytest
import sqlalchemy as sa
from alembic import command
from alembic.autogenerate import compare_metadata
from alembic.runtime.migration import MigrationContext
from alembic.script import ScriptDirectory
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import create_async_engine

from retrievalloom.domain.reports import HealthStatus
from retrievalloom.errors import (
    BackendUnavailableError,
    LifecycleError,
    MigrationError,
    TenantIsolationError,
)
from retrievalloom.storage import schema
from retrievalloom.storage import sqlite as sqlite_module
from retrievalloom.storage.base import MetadataTransactionABC, MigrationPolicy
from retrievalloom.storage.sqlite import (
    VERSION_TABLE,
    SQLiteMetadataStore,
    SQLiteTransaction,
    alembic_config,
)
from tests.unit.storage.factories import SCOPE, document

if TYPE_CHECKING:
    from pathlib import Path

    from sqlalchemy.engine import Connection


def _head() -> str | None:
    return ScriptDirectory.from_config(alembic_config()).get_current_head()


async def _pragma(store: SQLiteMetadataStore, name: str) -> Any:
    async with store.engine.connect() as connection:
        return (await connection.exec_driver_sql(f"PRAGMA {name}")).scalar()


async def _tables(store: SQLiteMetadataStore) -> set[str]:
    async with store.engine.connect() as connection:
        return set(await connection.run_sync(lambda sync: sa.inspect(sync).get_table_names()))


# construction and lifecycle -----------------------------------------------------------
@pytest.mark.parametrize(
    ("kwargs", "error"),
    [
        ({"synchronous": "OFF"}, ValueError),
        ({"busy_timeout_ms": True}, TypeError),
        ({"busy_timeout_ms": 1.5}, TypeError),
        ({"busy_timeout_ms": -1}, ValueError),
    ],
)
def test_constructor_validates_options(kwargs: dict[str, Any], error: type[Exception]) -> None:
    with pytest.raises(error):
        SQLiteMetadataStore(**kwargs)


async def test_uninitialised_store_is_unhealthy_and_rejects_use() -> None:
    store = SQLiteMetadataStore()
    assert store.info.implementation == "sqlite"
    with pytest.raises(LifecycleError):
        _ = store.engine
    report = await store.health()
    assert report.status is HealthStatus.UNHEALTHY
    assert report.message == "LifecycleError"
    with pytest.raises(LifecycleError):
        async with store.transaction(write=True):
            pass
    # The writer lock was released by the failed enter.
    assert not store._write_lock.locked()
    await store.close()


async def test_memory_store_lifecycle_and_pragmas() -> None:
    store = SQLiteMetadataStore(synchronous="FULL", busy_timeout_ms=1234)
    await store.initialize()
    engine = store.engine
    await store.initialize()
    assert store.engine is engine
    assert store.serialises_reads
    assert (await store.health()).status is HealthStatus.HEALTHY
    assert await _pragma(store, "busy_timeout") == 1234
    assert await _pragma(store, "synchronous") == 2
    assert await _pragma(store, "foreign_keys") == 1
    assert await _pragma(store, "journal_mode") == "memory"
    await store.close()
    await store.close()
    with pytest.raises(LifecycleError):
        _ = store.engine


async def test_file_store_uses_wal_and_persists(tmp_path: Path) -> None:
    path = tmp_path / "deep" / "dir" / "meta.db"
    store = SQLiteMetadataStore(path, busy_timeout_ms=500)
    await store.initialize()
    assert path.parent.is_dir()
    assert not store.serialises_reads
    assert await _pragma(store, "journal_mode") == "wal"
    assert await _pragma(store, "synchronous") == 1
    assert await _pragma(store, "busy_timeout") == 500
    await store.migrate()
    async with store.transaction(SCOPE, write=True) as tx:
        await tx.ensure_scope(SCOPE)
        await tx.put_document(document(SCOPE, "persisted"), expected_revision=None)
    await store.close()

    reopened = SQLiteMetadataStore(str(path))
    await reopened.initialize()
    await reopened.migrate(MigrationPolicy.VERIFY)
    async with reopened.transaction(write=False) as tx:
        listed = await tx.list_documents(SCOPE.key)
        # Scope came from the database, then from the cache.
        again = await tx.list_documents(SCOPE.key)
    assert [item.document_id for item in listed] == ["persisted"]
    assert listed[0].scope == SCOPE
    assert again == listed
    await reopened.close()


async def test_application_owned_engine_is_never_disposed(tmp_path: Path) -> None:
    engine = create_async_engine(f"sqlite+aiosqlite:///{(tmp_path / 'app.db').as_posix()}")
    try:
        store = SQLiteMetadataStore(engine=engine, busy_timeout_ms=4321)
        await store.initialize()
        await store.initialize()  # hooks installed once
        assert store.engine is engine
        assert not store.serialises_reads
        assert await _pragma(store, "busy_timeout") == 4321
        await store.migrate()
        await store.close()
        await store.close()
        # Still usable by the application after the store closed.
        async with engine.connect() as connection:
            assert (await connection.execute(sa.text("SELECT 1"))).scalar() == 1
        assert store.engine is engine
    finally:
        await engine.dispose()


async def test_unknown_scope_key_is_rejected(store: SQLiteMetadataStore) -> None:
    async with store.transaction(write=False) as tx:
        with pytest.raises(TenantIsolationError) as info:
            await tx.list_documents("sk_00000000000000000000000000000000")
    assert info.value.details["scope_key"] == "sk_00000000000000000000000000000000"


async def test_ensure_scope_registers_rows_known_only_to_the_database(
    store: SQLiteMetadataStore,
) -> None:
    store._scopes.clear()
    async with store.transaction(SCOPE, write=True) as tx:
        await tx.ensure_scope(SCOPE)
    assert store._scopes[SCOPE.key] == SCOPE
    async with store.transaction(SCOPE, write=True) as tx:
        await tx.ensure_scope(SCOPE)  # cached: no query, no duplicate
    async with store.transaction(write=False) as tx:
        assert [scope for scope in await tx.list_scopes() if scope == SCOPE] == [SCOPE]


# transaction mechanics ----------------------------------------------------------------
async def test_transaction_requires_enter() -> None:
    store = SQLiteMetadataStore()
    tx = SQLiteTransaction(store, None, write=True)
    with pytest.raises(LifecycleError, match="not active"):
        await tx.list_scopes()
    # Exiting a never-entered transaction is a no-op.
    await tx.__aexit__(None, None, None)


async def test_abstract_transaction_enter_returns_itself() -> None:
    marker: Any = object()
    assert await MetadataTransactionABC.__aenter__(marker) is marker


async def test_rollback_on_exception(store: SQLiteMetadataStore) -> None:
    with pytest.raises(KeyError):  # noqa: PT012
        async with store.transaction(SCOPE, write=True) as tx:
            await tx.next_commit_seq()
            await tx.put_document(document(SCOPE, "rolled"), expected_revision=None)
            raise KeyError("boom")
    async with store.transaction(SCOPE, write=False) as tx:
        assert await tx.get_document(SCOPE, "rolled") is None
        assert await tx.current_commit_seq() == 0
    assert not store._write_lock.locked()


async def test_settled_lets_cancelled_operations_finish_and_releases_results() -> None:
    released: list[str] = []
    finished: list[str] = []

    async def release(value: str) -> None:
        released.append(value)

    async def connect() -> str:
        await asyncio.sleep(0.05)
        finished.append("conn")
        return "conn"

    outer = asyncio.create_task(sqlite_module._settled(connect(), release))
    await asyncio.sleep(0.01)
    outer.cancel()
    await asyncio.sleep(0.01)
    outer.cancel()  # cancelled again while waiting for the operation
    with pytest.raises(asyncio.CancelledError):
        await outer
    assert finished == released == ["conn"]

    async def failing() -> str:
        await asyncio.sleep(0.02)
        raise RuntimeError("driver error")

    outer = asyncio.create_task(sqlite_module._settled(failing(), release))
    await asyncio.sleep(0.005)
    outer.cancel()
    with pytest.raises(asyncio.CancelledError):
        await outer
    assert released == ["conn"]  # nothing to release for a failed operation


async def test_cancelled_memory_transactions_keep_the_database(
    store: SQLiteMetadataStore,
) -> None:
    async def write(index: int) -> None:
        async with store.transaction(SCOPE, write=True) as tx:
            await tx.put_document(document(SCOPE, f"doc-{index}"), expected_revision=None)

    for index in range(30):
        task = asyncio.create_task(write(index))
        for _ in range(index % 6):
            await asyncio.sleep(0)
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
    assert not store._write_lock.locked()
    assert (await store.health()).status is HealthStatus.HEALTHY
    async with store.transaction(SCOPE, write=False) as tx:
        assert await tx.current_commit_seq() == 0  # schema intact: tables still exist


async def test_memory_reads_are_serialised_with_the_writer(store: SQLiteMetadataStore) -> None:
    async with store.transaction(write=True):
        with pytest.raises(TimeoutError):
            async with asyncio.timeout(0.05), store.transaction(write=False):
                pass
    async with store.transaction(write=False) as tx:
        assert await tx.current_commit_seq() == 0


async def test_file_reads_do_not_wait_for_the_writer(file_store: SQLiteMetadataStore) -> None:
    async with file_store.transaction(write=True) as writer:
        await writer.ensure_scope(SCOPE)
        async with asyncio.timeout(5), file_store.transaction(write=False) as reader:
            # Uncommitted writes are invisible to the concurrent reader.
            assert list(await reader.list_scopes()) == []
    async with file_store.transaction(write=False) as reader:
        assert list(await reader.list_scopes()) == [SCOPE]


async def test_locked_database_maps_to_backend_unavailable(
    file_store: SQLiteMetadataStore,
) -> None:
    contender = SQLiteMetadataStore(file_store._path, busy_timeout_ms=0)
    await contender.initialize()
    try:
        assert await contender.schema_version() == _head()  # pools a connection
        async with file_store.transaction(write=True) as tx:
            await tx.next_commit_seq()
            with pytest.raises(BackendUnavailableError):
                async with contender.transaction(write=True):
                    pass
        assert not contender._write_lock.locked()
        async with contender.transaction(write=True) as tx:
            assert await tx.next_commit_seq() == 2
    finally:
        await contender.close()


async def test_unopenable_database_maps_to_backend_unavailable(tmp_path: Path) -> None:
    directory = tmp_path / "is-a-directory"
    directory.mkdir()
    store = SQLiteMetadataStore(directory)
    await store.initialize()
    try:
        with pytest.raises(BackendUnavailableError):
            async with store.transaction(write=False):
                pass
        assert (await store.health()).status is HealthStatus.UNHEALTHY
    finally:
        await store.close()


async def test_operational_error_inside_transaction_maps_to_backend_unavailable() -> None:
    store = SQLiteMetadataStore()
    await store.initialize()
    try:
        with pytest.raises(BackendUnavailableError):
            async with store.transaction(write=False) as tx:
                await tx.list_scopes()  # tables do not exist yet
    finally:
        await store.close()


# migrations ---------------------------------------------------------------------------
async def test_migrate_auto_is_idempotent_and_verify_passes_at_head() -> None:
    store = SQLiteMetadataStore()
    await store.initialize()
    try:
        assert await store.schema_version() is None
        await store.migrate(MigrationPolicy.AUTO)
        assert await store.schema_version() == _head()
        await store.migrate(MigrationPolicy.AUTO)
        await store.migrate(MigrationPolicy.VERIFY)
        assert await store.schema_version() == _head()
    finally:
        await store.close()


async def test_verify_rejects_unmigrated_schema() -> None:
    store = SQLiteMetadataStore()
    await store.initialize()
    try:
        with pytest.raises(MigrationError) as info:
            await store.migrate(MigrationPolicy.VERIFY)
        assert info.value.details == {"current": None, "head": _head()}
        assert await store.schema_version() is None
    finally:
        await store.close()


async def test_verify_rejects_old_revision(store: SQLiteMetadataStore) -> None:
    async with store.engine.begin() as connection:
        await connection.execute(
            sa.update(sa.table(VERSION_TABLE, sa.column("version_num"))).values(
                version_num="0000_old"
            )
        )
    with pytest.raises(MigrationError) as info:
        await store.migrate(MigrationPolicy.VERIFY)
    assert info.value.details["current"] == "0000_old"
    assert info.value.details["head"] == _head()


async def test_failed_upgrade_raises_migration_error(monkeypatch: pytest.MonkeyPatch) -> None:
    def broken(connection: Connection) -> None:
        raise OperationalError("upgrade", None, Exception("disk full"))

    monkeypatch.setattr(sqlite_module, "_upgrade", broken)
    store = SQLiteMetadataStore()
    await store.initialize()
    try:
        with pytest.raises(MigrationError):
            await store.migrate()
        assert not store._write_lock.locked()
        assert await store.schema_version() is None
    finally:
        await store.close()


async def test_upgrade_downgrade_upgrade_round_trip(tmp_path: Path) -> None:
    store = SQLiteMetadataStore(tmp_path / "rt.db")
    await store.initialize()
    try:
        revisions = list(ScriptDirectory.from_config(alembic_config()).walk_revisions())
        assert revisions
        await store.migrate()
        at_head = await _tables(store)
        assert set(schema.metadata.tables) <= at_head
        # Step down through every revision to base.
        for revision in revisions:
            assert await store.schema_version() == revision.revision
            down = revision.down_revision
            assert down is None or isinstance(down, str)  # linear history
            await store.downgrade(down or "base")
        assert await store.schema_version() is None
        assert not (await _tables(store) & set(schema.metadata.tables))
        await store.migrate()
        assert await store.schema_version() == _head()
        assert await _tables(store) == at_head
        async with store.transaction(SCOPE, write=True) as tx:
            await tx.ensure_scope(SCOPE)
            await tx.put_document(document(SCOPE, "after"), expected_revision=None)
    finally:
        await store.close()


async def test_schema_metadata_has_no_drift_from_migrations(store: SQLiteMetadataStore) -> None:
    def diff(connection: Connection, metadata: sa.MetaData) -> list[Any]:
        context = MigrationContext.configure(
            connection,
            opts={"version_table": VERSION_TABLE, "compare_type": True},
        )
        return compare_metadata(context, metadata)

    async with store.engine.connect() as connection:
        assert await connection.run_sync(diff, schema.metadata) == []
        # Sanity check: the comparison does see the migrated tables.
        assert await connection.run_sync(diff, sa.MetaData())


def test_migrations_require_a_supplied_connection() -> None:
    with pytest.raises(RuntimeError, match="supplied connection"):
        command.upgrade(alembic_config(), "head")
