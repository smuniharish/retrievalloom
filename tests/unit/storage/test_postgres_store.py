"""Offline tests of the PostgreSQL store: configuration, dialect hooks, error mapping.

The behaviour against a live server is covered by ``tests/live/test_postgres.py``.
"""

from __future__ import annotations

import contextlib
import sys
from datetime import UTC, datetime, timedelta, timezone
from typing import Any

import pytest
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql, sqlite
from sqlalchemy.exc import DBAPIError, IntegrityError, OperationalError, SQLAlchemyError
from sqlalchemy.exc import TimeoutError as PoolTimeoutError
from sqlalchemy.ext.asyncio import AsyncConnection, create_async_engine

import retrievalloom
import retrievalloom.storage
from retrievalloom.domain.inputs import Scope
from retrievalloom.errors import (
    BackendTimeoutError,
    BackendUnavailableError,
    ConfigurationError,
    ValidationError,
    VersionConflictError,
)
from retrievalloom.storage import postgres as pg
from retrievalloom.storage import schema as tables
from retrievalloom.storage.postgres import PostgresMetadataStore, PostgresTransaction
from retrievalloom.storage.sqlite import SQLiteMetadataStore
from tests.unit.storage.factories import SCOPE

DSN = "postgresql://rl:secret@db.example:5432/rl"


class _DriverError(Exception):
    """Stand-in for an asyncpg error carrying a SQLSTATE."""

    def __init__(self, sqlstate: str) -> None:
        super().__init__(sqlstate)
        self.sqlstate = sqlstate


def _wrapped(sqlstate: str) -> DBAPIError:
    return DBAPIError("SELECT 1", {}, _DriverError(sqlstate))


class _Recorder:
    """Fake async connection/transaction recording the SQL it executes."""

    def __init__(self) -> None:
        self.statements: list[str] = []

    async def execute(self, statement: Any) -> None:
        self.statements.append(str(statement.compile(dialect=postgresql.dialect())))

    def begin(self) -> contextlib.nullcontext[None]:
        return contextlib.nullcontext()


class _FakeEngine:
    def __init__(self, connection: _Recorder) -> None:
        self._connection = connection

    def connect(self) -> contextlib.nullcontext[_Recorder]:
        return contextlib.nullcontext(self._connection)


# construction -----------------------------------------------------------------------
def test_dsn_and_engine_are_mutually_exclusive() -> None:
    with pytest.raises(ValueError, match="exactly one"):
        PostgresMetadataStore()
    engine = create_async_engine("postgresql+asyncpg://u@h/db")
    with pytest.raises(ValueError, match="exactly one"):
        PostgresMetadataStore(DSN, engine=engine)


@pytest.mark.parametrize(
    ("dsn", "driver"),
    [
        ("postgres://u@h/db", "postgresql+asyncpg"),
        ("postgresql://u@h/db", "postgresql+asyncpg"),
        ("postgresql+asyncpg://u@h/db", "postgresql+asyncpg"),
    ],
)
def test_dsn_is_normalised_to_asyncpg(dsn: str, driver: str) -> None:
    assert pg._async_url(dsn).drivername == driver


@pytest.mark.parametrize("dsn", ["mysql://u@h/db", "postgresql+psycopg://u@h/db", "not a url"])
def test_unsupported_dsn_is_rejected(dsn: str) -> None:
    with pytest.raises(ValueError, match="dsn"):
        PostgresMetadataStore(dsn)


@pytest.mark.parametrize(
    ("options", "error"),
    [
        ({"schema": "bad-name"}, ValueError),
        ({"schema": "x" * 64}, ValueError),
        ({"pool_size": 0}, ValueError),
        ({"pool_size": True}, TypeError),
        ({"max_overflow": 1.5}, TypeError),
        ({"pool_recycle": -2}, ValueError),
        ({"lock_timeout_ms": -1}, ValueError),
        ({"statement_timeout_ms": -1}, ValueError),
        ({"pool_timeout": 0}, ValueError),
        ({"pool_timeout": True}, TypeError),
        ({"pool_timeout": "30"}, TypeError),
    ],
)
def test_invalid_options_are_rejected(options: dict[str, Any], error: type[Exception]) -> None:
    with pytest.raises(error):
        PostgresMetadataStore(DSN, **options)


def test_missing_asyncpg_is_a_configuration_error(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setitem(sys.modules, "asyncpg", None)
    with pytest.raises(ConfigurationError, match=r"retrievalloom\[postgres\]"):
        PostgresMetadataStore(DSN)


def test_engine_must_be_postgres() -> None:
    with pytest.raises(ValueError, match="PostgreSQL engine"):
        PostgresMetadataStore(engine=create_async_engine("sqlite+aiosqlite://"))


def test_connection_settings_and_info() -> None:
    store = PostgresMetadataStore(
        DSN,
        schema="tenant_a",
        statement_timeout_ms=5000,
        connect_args={"ssl": "require", "server_settings": {"application_name": "worker"}},
    )
    assert store.schema == "tenant_a"
    assert store._connect_args == {
        "ssl": "require",
        "server_settings": {
            "application_name": "worker",
            "lock_timeout": "30000",
            "statement_timeout": "5000",
            "search_path": '"tenant_a"',
        },
    }
    assert store.info.implementation == "postgres"
    # The DSN (and its credentials) is not part of the fingerprint; the settings are.
    elsewhere = "postgresql://x:y@elsewhere/rl"
    same = PostgresMetadataStore(elsewhere, schema="tenant_a", statement_timeout_ms=5000)
    other = PostgresMetadataStore(elsewhere, schema="tenant_b", statement_timeout_ms=5000)
    assert same.info.fingerprint == store.info.fingerprint
    assert other.info.fingerprint != store.info.fingerprint
    default = PostgresMetadataStore(DSN)
    assert default.schema is None
    assert default._connect_args == {
        "server_settings": {"application_name": "retrievalloom", "lock_timeout": "30000"}
    }


def test_lock_keys_are_namespaced_signed_64_bit() -> None:
    first = PostgresMetadataStore(DSN, schema="a")
    second = PostgresMetadataStore(DSN, schema="b")
    assert first.scope_lock_key("k") != second.scope_lock_key("k")
    assert first.scope_lock_key("k") == PostgresMetadataStore(DSN, schema="a").scope_lock_key("k")
    assert first.scope_lock_key("k") != first.scope_lock_key("j")
    assert all(-(2**63) <= key < 2**63 for key in (first._global_key, first.scope_lock_key("k")))


# lifecycle ---------------------------------------------------------------------------
async def test_initialize_creates_the_pool_lazily_and_close_disposes_it() -> None:
    store = PostgresMetadataStore(DSN, pool_size=2, pool_timeout=1)
    await store.initialize()  # creating the engine does not connect
    engine = store.engine
    assert engine.dialect.name == "postgresql"
    assert engine.dialect.driver == "asyncpg"
    await store.initialize()
    assert store.engine is engine
    await store.close()
    assert store._engine is None


async def test_application_owned_engine_is_kept() -> None:
    engine = create_async_engine("postgresql+asyncpg://u@h/db")
    store = PostgresMetadataStore(engine=engine, schema="s")
    await store.initialize()
    await store.close()
    assert store.engine is engine
    await engine.dispose()


async def test_maintenance_locks_and_creates_the_schema() -> None:
    connection = _Recorder()
    store = PostgresMetadataStore(DSN, schema="tenant_a")
    store._engine = _FakeEngine(connection)  # type: ignore[assignment]
    async with store._maintenance() as yielded:
        assert yielded is connection
    assert connection.statements[0].startswith("SELECT pg_advisory_xact_lock(")
    assert connection.statements[1] == 'CREATE SCHEMA IF NOT EXISTS "tenant_a"'

    connection = _Recorder()
    store = PostgresMetadataStore(DSN)
    store._engine = _FakeEngine(connection)  # type: ignore[assignment]
    async with store._maintenance():
        pass
    assert len(connection.statements) == 1


def test_upgrade_runs_alembic_to_head(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[Any, str]] = []
    monkeypatch.setattr(pg.command, "upgrade", lambda config, rev: calls.append((config, rev)))
    marker: Any = object()
    PostgresMetadataStore(DSN)._upgrade(marker)
    ((config, revision),) = calls
    assert revision == "head"
    assert config.attributes["connection"] is marker


# locking and isolation -----------------------------------------------------------------
async def test_process_locks_queue_writers_per_scope() -> None:
    store = PostgresMetadataStore(DSN)
    other = Scope(tenant_id="t", namespace="other")
    assert store._process_lock(SCOPE, write=False) is None
    lock = store._process_lock(SCOPE, write=True)
    assert store._process_lock(SCOPE, write=True) is lock  # reused while referenced
    assert store._process_lock(other, write=True) is not lock
    assert store._process_lock(None, write=True) is not lock
    assert store._begin_options(write=True) == {"isolation_level": "READ COMMITTED"}
    assert store._begin_options(write=False) == {
        "isolation_level": "REPEATABLE READ",
        "postgresql_readonly": True,
    }
    assert isinstance(store.transaction(SCOPE, write=True), PostgresTransaction)


async def _locks_taken(
    store: PostgresMetadataStore, scope: Scope | None, *, write: bool
) -> list[str]:
    transaction = PostgresTransaction(store, scope, write=write)
    recorder = _Recorder()
    transaction._exec = recorder.execute  # type: ignore[method-assign]
    await store._after_begin(transaction)
    return recorder.statements


async def test_writers_take_advisory_locks() -> None:
    store = PostgresMetadataStore(DSN, schema="s")
    assert await _locks_taken(store, SCOPE, write=False) == []
    unscoped = await _locks_taken(store, None, write=True)
    assert len(unscoped) == 1
    assert unscoped[0].startswith("SELECT pg_advisory_xact_lock(")
    scoped = await _locks_taken(store, SCOPE, write=True)
    assert len(scoped) == 2
    assert "pg_advisory_xact_lock_shared(" in scoped[0]
    assert "pg_advisory_xact_lock(" in scoped[1]


def test_upserts_use_the_postgres_insert() -> None:
    store = PostgresMetadataStore(DSN)
    table = tables.commit_seq
    statement = (
        store._insert(table)
        .values(id=1, seq=1)
        .on_conflict_do_update(index_elements=[table.c.id], set_={"seq": table.c.seq + 1})
    )
    assert "ON CONFLICT (id) DO UPDATE" in str(statement.compile(dialect=postgresql.dialect()))


# error translation ---------------------------------------------------------------------
@pytest.mark.parametrize(
    ("error", "expected", "retryable"),
    [
        (PoolTimeoutError("pool"), BackendUnavailableError, True),
        (_wrapped("40001"), VersionConflictError, True),
        (_wrapped("40P01"), VersionConflictError, True),
        (_wrapped("23505"), VersionConflictError, True),
        (IntegrityError("INSERT", {}, Exception("dup")), VersionConflictError, True),
        (_wrapped("55P03"), BackendTimeoutError, True),
        (_wrapped("57014"), BackendTimeoutError, True),
        (_wrapped("28P01"), BackendUnavailableError, False),
        (_wrapped("3D000"), BackendUnavailableError, False),
        (_wrapped("22P05"), ValidationError, False),
        (_wrapped("08006"), BackendUnavailableError, True),
        (_wrapped("53300"), BackendUnavailableError, True),
        (ConnectionRefusedError("refused"), BackendUnavailableError, True),
        (
            DBAPIError("SELECT 1", {}, Exception("gone"), connection_invalidated=True),
            BackendUnavailableError,
            True,
        ),
    ],
)
def test_driver_errors_are_classified(
    error: Exception, expected: type[Exception], retryable: bool
) -> None:
    translated = pg.classify_error(error)
    assert type(translated) is expected
    assert translated.retryable is retryable
    state = pg._sqlstate(error)
    assert translated.details == ({} if state is None else {"sqlstate": state})


@pytest.mark.parametrize(
    "error", [_wrapped("42P01"), DBAPIError("SELECT 1", {}, Exception("x")), RuntimeError("x")]
)
def test_other_errors_are_not_translated(error: Exception) -> None:
    assert PostgresMetadataStore(DSN)._translate(error) is None


def test_sqlstate_follows_wrapped_and_chained_errors() -> None:
    chained = RuntimeError("outer")
    chained.__cause__ = _DriverError("40001")
    assert pg._sqlstate(chained) == "40001"
    assert pg._sqlstate(_wrapped("55P03")) == "55P03"
    assert pg._sqlstate(RuntimeError("plain")) is None


# shared base -----------------------------------------------------------------------------
def test_untranslated_errors_are_reraised_unchanged() -> None:
    store = SQLiteMetadataStore()
    error = SQLAlchemyError("unexpected")
    with pytest.raises(SQLAlchemyError) as raised:
        store._reraise(error)
    assert raised.value is error


async def test_commit_failures_are_translated(
    store: SQLiteMetadataStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def fail(self: AsyncConnection) -> None:
        raise OperationalError("COMMIT", {}, Exception("disk I/O error"))

    transaction = store.transaction(SCOPE, write=True)
    await transaction.__aenter__()
    assert transaction.scope == SCOPE
    assert transaction.write
    monkeypatch.setattr(AsyncConnection, "commit", fail)
    with pytest.raises(BackendUnavailableError):
        await transaction.__aexit__(None, None, None)
    monkeypatch.undo()
    async with store.transaction(SCOPE, write=True):  # the write lock was released
        pass


# schema types ------------------------------------------------------------------------------
def test_timestamps_are_native_on_postgres() -> None:
    column = tables.UTCDateTime()
    dialect = postgresql.dialect()
    impl = column.load_dialect_impl(dialect)
    assert impl.compile(dialect=dialect) == "TIMESTAMP WITH TIME ZONE"
    assert isinstance(column.load_dialect_impl(sqlite.dialect()), sa.String)
    local = datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone(timedelta(hours=2)))
    bound = column.process_bind_param(local, dialect)
    assert bound == local
    assert isinstance(bound, datetime)
    assert bound.tzinfo is UTC
    assert column.process_result_value(local, dialect) == local
    assert column.process_result_value(local, dialect).tzinfo is UTC  # type: ignore[union-attr]
    assert isinstance(column.process_bind_param(local, sqlite.dialect()), str)


# exports -------------------------------------------------------------------------------
def test_store_classes_are_exported_lazily() -> None:
    assert retrievalloom.PostgresMetadataStore is PostgresMetadataStore
    assert retrievalloom.storage.PostgresMetadataStore is PostgresMetadataStore
    assert retrievalloom.storage.SQLiteMetadataStore is SQLiteMetadataStore
    with pytest.raises(AttributeError):
        _ = retrievalloom.storage.MissingStore
    with pytest.raises(AttributeError):
        _ = retrievalloom.MissingStore
