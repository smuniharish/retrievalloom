"""PostgreSQL metadata store (SQLAlchemy async Core + asyncpg + Alembic).

Several engines, on one host or many, can share one PostgreSQL metadata store. Write
transactions run at ``READ COMMITTED`` and are serialised per scope with
transaction-level advisory locks:

* a scoped write takes the global lock in shared mode, then its scope's lock
  exclusively, so writers of one scope run one at a time (the guarantee the SQLite
  store gives with its single writer) while different scopes write concurrently;
* an unscoped (maintenance) write takes the global lock exclusively;
* the single commit-sequence row is bumped with an atomic upsert, whose row lock
  orders commit sequences by commit time.

Read transactions are ``REPEATABLE READ READ ONLY`` snapshots and never block writers.
Lost lock races (deadlock, serialization failure, unique violation) surface as the
retryable :class:`~retrievalloom.errors.VersionConflictError`, which the engine already
retries; lock and statement timeouts surface as
:class:`~retrievalloom.errors.BackendTimeoutError`.
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import importlib
import re
import weakref
from typing import TYPE_CHECKING, Any

import sqlalchemy as sa
from alembic import command
from sqlalchemy.dialects.postgresql import insert as postgresql_insert
from sqlalchemy.engine import make_url
from sqlalchemy.exc import ArgumentError, DBAPIError, IntegrityError
from sqlalchemy.exc import TimeoutError as PoolTimeoutError
from sqlalchemy.ext.asyncio import create_async_engine

from retrievalloom.contracts.component import make_info
from retrievalloom.errors import (
    BackendTimeoutError,
    BackendUnavailableError,
    ConfigurationError,
    RetrievalLoomError,
    ValidationError,
    VersionConflictError,
)
from retrievalloom.storage.sql import SQLAlchemyMetadataStore, SQLAlchemyTransaction, alembic_config

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Mapping

    from sqlalchemy.engine import URL, Connection
    from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

    from retrievalloom.contracts.component import ComponentContext
    from retrievalloom.domain.inputs import Scope

__all__ = ["PostgresMetadataStore", "PostgresTransaction"]

_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]{0,62}")
_DRIVERS = {"postgres": "postgresql+asyncpg", "postgresql": "postgresql+asyncpg"}
_CONFLICT_STATES = frozenset({"40001", "40P01"})  # serialization failure, deadlock
_TIMEOUT_STATES = frozenset({"55P03", "57014"})  # lock not available, query cancelled
_UNAVAILABLE_CLASSES = frozenset({"08", "53", "57", "58"})  # connection, resources, admin
_REJECTED_CLASSES = frozenset({"28", "3D"})  # authentication, unknown database


def _require_asyncpg() -> None:
    try:
        importlib.import_module("asyncpg")
    except ImportError as exc:
        raise ConfigurationError(
            "PostgresMetadataStore needs the 'postgres' extra: "
            'pip install "retrievalloom[postgres]"'
        ) from exc


def _async_url(dsn: str) -> URL:
    try:
        url = make_url(dsn)
    except ArgumentError as exc:
        raise ValueError("dsn is not a valid database URL") from exc
    driver = _DRIVERS.get(url.drivername, url.drivername)
    if driver != "postgresql+asyncpg":
        raise ValueError("dsn must be a postgresql:// or postgresql+asyncpg:// URL")
    return url.set(drivername=driver)


def _lock_key(namespace: str, name: str) -> int:
    """Signed 64-bit advisory lock key of ``name`` within ``namespace`` (a schema)."""
    digest = hashlib.blake2b(f"retrievalloom\0{namespace}\0{name}".encode(), digest_size=8).digest()
    return int.from_bytes(digest, "big", signed=True)


def _sqlstate(exc: BaseException) -> str | None:
    """SQLSTATE of a driver error, searching the wrapped and chained exceptions."""
    current: BaseException | None = exc
    while current is not None:
        state = getattr(current, "sqlstate", None)
        if isinstance(state, str):
            return state
        current = getattr(current, "orig", None) or current.__cause__
    return None


def classify_error(exc: Exception) -> RetrievalLoomError | None:
    """Map a PostgreSQL driver error to a RetrievalLoom error (``None``: not a driver error).

    Args:
        exc: Exception raised by SQLAlchemy or the driver.
    """
    if isinstance(exc, PoolTimeoutError):
        return BackendUnavailableError("metadata connection pool exhausted")
    state = _sqlstate(exc)
    details = {} if state is None else {"sqlstate": state}
    category = (state or "")[:2]
    if state in _CONFLICT_STATES or category == "23" or isinstance(exc, IntegrityError):
        return VersionConflictError("concurrent modification detected", details=details)
    if state in _TIMEOUT_STATES:
        return BackendTimeoutError("metadata database lock or statement timed out", details=details)
    if category in _REJECTED_CLASSES:
        return BackendUnavailableError(
            "metadata database rejected the connection", retryable=False, details=details
        )
    if category == "22":
        return ValidationError("value rejected by the metadata database", details=details)
    if (
        category in _UNAVAILABLE_CLASSES
        or isinstance(exc, OSError)
        or (isinstance(exc, DBAPIError) and exc.connection_invalidated)
    ):
        return BackendUnavailableError("metadata database operation failed", details=details)
    return None


def _check_int(name: str, value: object, *, minimum: int) -> None:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{name} must be an int")
    if value < minimum:
        raise ValueError(f"{name} must be at least {minimum}")


class PostgresMetadataStore(SQLAlchemyMetadataStore):
    """Metadata store on PostgreSQL, shareable by several engines on several hosts.

    Install the ``postgres`` extra (``pip install "retrievalloom[postgres]"``) and pass the
    store as ``Components(metadata_store=PostgresMetadataStore(dsn, ...))``. Every engine
    sharing the store must use the same DSN and ``schema``.

    Args:
        dsn: ``postgresql://``, ``postgres://`` or ``postgresql+asyncpg://`` URL. Exactly
            one of ``dsn`` and ``engine`` is required.
        schema: Schema holding the ``rl_`` tables (created by ``migrate``); ``None`` uses the
            role's default ``search_path``.
        pool_size: Connections kept open per engine.
        max_overflow: Extra connections opened under load.
        pool_timeout: Seconds to wait for a pooled connection.
        pool_recycle: Seconds after which a pooled connection is replaced (``-1``: never).
        pool_pre_ping: Test pooled connections before use (survives server restarts).
        lock_timeout_ms: Server ``lock_timeout`` (``0``: wait forever).
        statement_timeout_ms: Server ``statement_timeout``, or ``None`` for the server default.
        application_name: Reported in ``pg_stat_activity``.
        connect_args: Extra ``asyncpg.connect`` arguments (for example ``ssl``); its
            ``server_settings`` are merged over the store's.
        engine: Application-owned SQLAlchemy async engine (not disposed on close). Its own
            connection settings apply; ``schema`` only namespaces the advisory locks and
            names the schema ``migrate`` creates.

    Raises:
        ConfigurationError: If ``dsn`` is given and asyncpg is not installed.
    """

    def __init__(
        self,
        dsn: str | None = None,
        *,
        schema: str | None = None,
        pool_size: int = 5,
        max_overflow: int = 10,
        pool_timeout: float = 30.0,
        pool_recycle: int = 1800,
        pool_pre_ping: bool = True,
        lock_timeout_ms: int = 30_000,
        statement_timeout_ms: int | None = None,
        application_name: str = "retrievalloom",
        connect_args: Mapping[str, Any] | None = None,
        engine: AsyncEngine | None = None,
    ) -> None:
        if (dsn is None) == (engine is None):
            raise ValueError("pass exactly one of dsn and engine")
        if schema is not None and not _IDENTIFIER.fullmatch(schema):
            raise ValueError("schema must be a plain SQL identifier of at most 63 characters")
        _check_int("pool_size", pool_size, minimum=1)
        _check_int("max_overflow", max_overflow, minimum=0)
        _check_int("pool_recycle", pool_recycle, minimum=-1)
        _check_int("lock_timeout_ms", lock_timeout_ms, minimum=0)
        if statement_timeout_ms is not None:
            _check_int("statement_timeout_ms", statement_timeout_ms, minimum=0)
        if isinstance(pool_timeout, bool) or not isinstance(pool_timeout, int | float):
            raise TypeError("pool_timeout must be a number")
        if pool_timeout <= 0:
            raise ValueError("pool_timeout must be positive")
        if engine is not None and engine.dialect.name != "postgresql":
            raise ValueError("engine must be a PostgreSQL engine")
        self._url: URL | None = None
        if dsn is not None:
            _require_asyncpg()
            self._url = _async_url(dsn)
        super().__init__(
            engine=engine,
            info=make_info(
                component="metadata_store",
                implementation="postgres",
                version="1",
                config={
                    "schema": schema,
                    "pool_size": pool_size,
                    "max_overflow": max_overflow,
                    "lock_timeout_ms": lock_timeout_ms,
                    "statement_timeout_ms": statement_timeout_ms,
                },
            ),
        )
        self._schema = schema
        self._pool = {
            "pool_size": pool_size,
            "max_overflow": max_overflow,
            "pool_timeout": float(pool_timeout),
            "pool_recycle": pool_recycle,
            "pool_pre_ping": pool_pre_ping,
        }
        settings = {"application_name": application_name, "lock_timeout": str(lock_timeout_ms)}
        if statement_timeout_ms is not None:
            settings["statement_timeout"] = str(statement_timeout_ms)
        if schema is not None:
            settings["search_path"] = f'"{schema}"'
        extra = dict(connect_args or {})
        settings.update(extra.pop("server_settings", None) or {})
        self._connect_args = {**extra, "server_settings": settings}
        namespace = schema or ""
        self._global_key = _lock_key(namespace, "global")
        self._namespace = namespace
        # Weak: a lock lives only while some transaction holds or awaits it.
        self._locks: weakref.WeakValueDictionary[str, asyncio.Lock] = weakref.WeakValueDictionary()

    @property
    def schema(self) -> str | None:
        """Schema holding the tables, or ``None`` for the default ``search_path``."""
        return self._schema

    async def initialize(self, context: ComponentContext | None = None) -> None:
        """Create the connection pool (idempotent; connects lazily)."""
        del context  # configured at construction; the context carries nothing it needs
        if self._engine is not None or self._url is None:
            return
        self._engine = create_async_engine(self._url, connect_args=self._connect_args, **self._pool)

    def transaction(self, scope: Scope | None = None, *, write: bool) -> PostgresTransaction:
        """Open a transaction bound to ``scope`` (if given)."""
        return PostgresTransaction(self, scope, write=write)

    def scope_lock_key(self, scope_key: str) -> int:
        """Advisory lock key serialising the writers of ``scope_key``."""
        return _lock_key(self._namespace, f"scope\0{scope_key}")

    # dialect hooks ---------------------------------------------------------------------
    @contextlib.asynccontextmanager
    async def _maintenance(self) -> AsyncGenerator[AsyncConnection]:
        async with self.engine.connect() as connection, connection.begin():
            # Exclusive against every writer and every other migrating engine.
            await connection.execute(sa.select(sa.func.pg_advisory_xact_lock(self._global_key)))
            if self._schema is not None:
                await connection.execute(sa.text(f'CREATE SCHEMA IF NOT EXISTS "{self._schema}"'))
            yield connection

    def _upgrade(self, connection: Connection) -> None:
        command.upgrade(alembic_config(connection), "head")

    def _process_lock(self, scope: Scope | None, *, write: bool) -> asyncio.Lock | None:
        # Not needed for correctness (advisory locks serialise writers across engines);
        # queueing same-scope writers in-process keeps them from holding pooled
        # connections while they wait for the database lock.
        if not write:
            return None
        key = "" if scope is None else scope.key
        lock = self._locks.get(key)
        if lock is None:
            lock = asyncio.Lock()
            self._locks[key] = lock
        return lock

    def _begin_options(self, *, write: bool) -> dict[str, Any]:
        if write:
            return {"isolation_level": "READ COMMITTED"}
        return {"isolation_level": "REPEATABLE READ", "postgresql_readonly": True}

    async def _after_begin(self, transaction: SQLAlchemyTransaction) -> None:
        if not transaction.write:
            return
        scope = transaction.scope
        if scope is None:
            await transaction._exec(sa.select(sa.func.pg_advisory_xact_lock(self._global_key)))
            return
        await transaction._exec(sa.select(sa.func.pg_advisory_xact_lock_shared(self._global_key)))
        await transaction._exec(
            sa.select(sa.func.pg_advisory_xact_lock(self.scope_lock_key(scope.key)))
        )

    def _insert(self, table: sa.Table) -> Any:
        return postgresql_insert(table)

    def _translate(self, exc: Exception) -> RetrievalLoomError | None:
        return classify_error(exc)


class PostgresTransaction(SQLAlchemyTransaction):
    """Transaction on ``PostgresMetadataStore``."""
