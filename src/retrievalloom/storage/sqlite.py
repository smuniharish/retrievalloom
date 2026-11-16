"""SQLite metadata store (SQLAlchemy async Core + aiosqlite + Alembic)."""

from __future__ import annotations

import asyncio
import contextlib
from pathlib import Path
from typing import TYPE_CHECKING, Any

from alembic import command
from sqlalchemy import event
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.exc import IntegrityError, OperationalError
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import StaticPool

from retrievalloom.contracts.component import make_info
from retrievalloom.errors import (
    BackendUnavailableError,
    RetrievalLoomError,
    VersionConflictError,
)
from retrievalloom.storage.sql import (
    MIGRATIONS_DIR,
    VERSION_TABLE,
    SQLAlchemyMetadataStore,
    SQLAlchemyTransaction,
    alembic_config,
)
from retrievalloom.storage.sql import (
    _settled as _settled,
)

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    import sqlalchemy as sa
    from sqlalchemy.engine import Connection
    from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

    from retrievalloom.contracts.component import ComponentContext
    from retrievalloom.domain.inputs import Scope

__all__ = [
    "MIGRATIONS_DIR",
    "VERSION_TABLE",
    "SQLiteMetadataStore",
    "SQLiteTransaction",
    "alembic_config",
]


class SQLiteMetadataStore(SQLAlchemyMetadataStore):
    """Metadata store on SQLite.

    Uses WAL journaling, one serialised writer (``BEGIN IMMEDIATE``) and concurrent
    readers. ``path=None`` keeps the database in memory (tests and ephemeral use only;
    reads are then serialised through the writer lock).

    Args:
        path: Database file, or ``None`` for an in-memory database.
        synchronous: SQLite ``synchronous`` pragma (``NORMAL`` or ``FULL``).
        busy_timeout_ms: SQLite busy timeout.
        engine: Application-owned SQLAlchemy async engine (not disposed on close).
    """

    def __init__(
        self,
        path: str | Path | None = None,
        *,
        synchronous: str = "NORMAL",
        busy_timeout_ms: int = 30_000,
        engine: AsyncEngine | None = None,
    ) -> None:
        if synchronous not in {"NORMAL", "FULL"}:
            raise ValueError("synchronous must be NORMAL or FULL")
        if isinstance(busy_timeout_ms, bool) or not isinstance(busy_timeout_ms, int):
            raise TypeError("busy_timeout_ms must be an int")
        if busy_timeout_ms < 0:
            raise ValueError("busy_timeout_ms must be non-negative")
        super().__init__(
            engine=engine,
            info=make_info(
                component="metadata_store",
                implementation="sqlite",
                version="1",
                config={"synchronous": synchronous},
            ),
        )
        self._path = Path(path) if path is not None else None
        self._synchronous = synchronous
        self._busy_timeout_ms = busy_timeout_ms
        self._memory = path is None and engine is None
        self._write_lock = asyncio.Lock()

    @property
    def serialises_reads(self) -> bool:
        """Whether read transactions share the writer lock (in-memory databases)."""
        return self._memory

    async def initialize(self, context: ComponentContext | None = None) -> None:
        """Create the engine and install connection hooks (idempotent)."""
        del context  # configured at construction; the context carries nothing it needs
        if self._engine is not None and not self._owns_engine:
            self._install_hooks(self._engine)
            return
        if self._engine is not None:
            return
        if self._path is None:
            engine = create_async_engine(
                "sqlite+aiosqlite:///:memory:",
                poolclass=StaticPool,
                connect_args={"check_same_thread": False},
            )
        else:
            self._path.parent.mkdir(parents=True, exist_ok=True)
            engine = create_async_engine(f"sqlite+aiosqlite:///{self._path.as_posix()}")
        self._install_hooks(engine)
        self._engine = engine

    def _install_hooks(self, engine: AsyncEngine) -> None:
        sync_engine = engine.sync_engine
        if event.contains(sync_engine, "connect", self._on_connect):
            return
        event.listen(sync_engine, "connect", self._on_connect)
        event.listen(sync_engine, "begin", self._on_begin)

    def _on_connect(self, dbapi_connection: Any, _record: Any) -> None:
        # Disable the driver's implicit BEGIN so the "begin" hook controls locking.
        dbapi_connection.isolation_level = None
        cursor = dbapi_connection.cursor()
        if not self._memory:
            cursor.execute("PRAGMA journal_mode=WAL")
        cursor.execute(f"PRAGMA synchronous={self._synchronous}")
        cursor.execute(f"PRAGMA busy_timeout={self._busy_timeout_ms}")
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()

    @staticmethod
    def _on_begin(connection: Any) -> None:
        mode = connection.get_execution_options().get("rl_begin", "DEFERRED")
        connection.exec_driver_sql(f"BEGIN {mode}")

    def transaction(self, scope: Scope | None = None, *, write: bool) -> SQLiteTransaction:
        """Open a transaction bound to ``scope`` (if given)."""
        return SQLiteTransaction(self, scope, write=write)

    # dialect hooks ---------------------------------------------------------------------
    @contextlib.asynccontextmanager
    async def _maintenance(self) -> AsyncGenerator[AsyncConnection]:
        async with self._write_lock, self.engine.connect() as connection:
            connection = await connection.execution_options(rl_begin="IMMEDIATE")
            async with connection.begin():
                yield connection

    def _upgrade(self, connection: Connection) -> None:
        _upgrade(connection)

    def _process_lock(self, scope: Scope | None, *, write: bool) -> asyncio.Lock | None:
        del scope  # SQLite has a single writer for the whole database
        return self._write_lock if write or self._memory else None

    def _begin_options(self, *, write: bool) -> dict[str, Any]:
        return {"rl_begin": "IMMEDIATE" if write else "DEFERRED"}

    def _insert(self, table: sa.Table) -> Any:
        return sqlite_insert(table)

    def _translate(self, exc: Exception) -> RetrievalLoomError | None:
        if isinstance(exc, IntegrityError):
            return VersionConflictError("concurrent modification detected")
        if isinstance(exc, OperationalError):
            return BackendUnavailableError("metadata database operation failed")
        return None


def _upgrade(connection: Connection) -> None:
    command.upgrade(alembic_config(connection), "head")


class SQLiteTransaction(SQLAlchemyTransaction):
    """Transaction on ``SQLiteMetadataStore``."""
