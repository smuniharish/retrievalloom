"""SQLite metadata store (SQLAlchemy async Core + aiosqlite + Alembic)."""

from __future__ import annotations

import asyncio
import contextlib
from pathlib import Path
from typing import TYPE_CHECKING, Any, Self

import sqlalchemy as sa
from alembic import command
from alembic.config import Config
from alembic.runtime.migration import MigrationContext
from alembic.script import ScriptDirectory
from sqlalchemy import event
from sqlalchemy.exc import IntegrityError, OperationalError, SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, create_async_engine
from sqlalchemy.pool import StaticPool

from retrievalloom._logging import get_logger
from retrievalloom.contracts.component import ComponentContext, ComponentInfo, make_info
from retrievalloom.domain.inputs import Scope
from retrievalloom.domain.records import (
    CoverageRecord,
    CoverageState,
    DerivedObject,
    DocumentRecord,
    DocumentState,
    DocumentVersion,
    GenerationState,
    IndexGeneration,
    IngestionEvent,
    LifecycleTask,
    LineageEdge,
    ObjectContribution,
    ObjectKind,
    ObjectState,
    RepresentationSnapshot,
    TaskState,
)
from retrievalloom.domain.reports import HealthReport, HealthStatus
from retrievalloom.errors import (
    BackendUnavailableError,
    LifecycleError,
    MigrationError,
    TenantIsolationError,
    VersionConflictError,
)
from retrievalloom.storage import schema as t
from retrievalloom.storage.base import (
    DocumentMetadataStoreABC,
    MetadataTransactionABC,
    MigrationPolicy,
    PointerRow,
    StoredRefreshState,
    UnitKey,
)

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Mapping, Sequence
    from datetime import datetime
    from types import TracebackType

    from sqlalchemy.engine import Connection

_log = get_logger("storage.sqlite")

MIGRATIONS_DIR = Path(__file__).parent / "migrations"
VERSION_TABLE = "rl_alembic_version"
_GRAPH_KINDS = frozenset({ObjectKind.GRAPH_ENTITY, ObjectKind.GRAPH_RELATIONSHIP})


def alembic_config(connection: Connection | None = None) -> Config:
    """Alembic configuration pointing at the packaged migrations."""
    config = Config()
    config.set_main_option("script_location", str(MIGRATIONS_DIR))
    config.attributes["connection"] = connection
    return config


async def _settled[T](
    operation: Awaitable[T], release: Callable[[T], Awaitable[object]] | None = None
) -> T:
    """Await a driver operation, letting it finish before propagating a cancellation.

    Cancelling an aiosqlite call mid-statement makes SQLAlchemy invalidate the connection,
    and an abandoned checked-out connection is closed by the garbage collector; for an
    in-memory database either one replaces the database with an empty one. The operation
    therefore always runs to completion and ``release`` disposes of a result the cancelled
    caller will never see; the cancellation is re-raised afterwards.
    """
    task = asyncio.ensure_future(operation)
    try:
        return await asyncio.shield(task)
    except asyncio.CancelledError:
        while not task.done():
            with contextlib.suppress(asyncio.CancelledError):
                await asyncio.wait((task,))
        if release is not None and task.exception() is None:
            await asyncio.shield(release(task.result()))
        raise


async def _close(connection: AsyncConnection) -> None:
    await connection.close()


class SQLiteMetadataStore(DocumentMetadataStoreABC):
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
        self._path = Path(path) if path is not None else None
        self._synchronous = synchronous
        self._busy_timeout_ms = busy_timeout_ms
        self._engine = engine
        self._owns_engine = engine is None
        self._memory = path is None and engine is None
        self._write_lock = asyncio.Lock()
        self._scopes: dict[str, Scope] = {}
        self._info = make_info(
            component="metadata_store",
            implementation="sqlite",
            version="1",
            config={"synchronous": synchronous},
        )

    @property
    def info(self) -> ComponentInfo:
        """Component identity."""
        return self._info

    @property
    def engine(self) -> AsyncEngine:
        """Underlying SQLAlchemy engine (after ``initialize``)."""
        if self._engine is None:
            raise LifecycleError("metadata store is not initialised")
        return self._engine

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

    async def close(self) -> None:
        """Dispose the engine when engine-owned (idempotent)."""
        if self._engine is not None and self._owns_engine:
            await self._engine.dispose()
            self._engine = None

    async def health(self) -> HealthReport:
        """Run a trivial query."""
        try:
            async with self.transaction(write=False) as transaction:
                await transaction._exec(sa.text("SELECT 1"))
        except (SQLAlchemyError, LifecycleError, BackendUnavailableError) as exc:
            return HealthReport(
                component=self.info.name,
                status=HealthStatus.UNHEALTHY,
                message=type(exc).__name__,
            )
        return HealthReport(component=self.info.name, status=HealthStatus.HEALTHY)

    async def migrate(self, policy: MigrationPolicy = MigrationPolicy.AUTO) -> None:
        """Upgrade to head (``auto``) or verify the schema is at head (``verify``).

        Raises:
            MigrationError: If verification fails or a migration cannot be applied.
        """
        head = ScriptDirectory.from_config(alembic_config()).get_current_head()
        current = await self.schema_version()
        if current == head:
            return
        if policy is MigrationPolicy.VERIFY:
            raise MigrationError(
                "metadata schema is not at head", details={"current": current, "head": head}
            )
        async with self._write_lock:
            try:
                async with self.engine.connect() as connection:
                    connection = await connection.execution_options(rl_begin="IMMEDIATE")
                    async with connection.begin():
                        await connection.run_sync(_upgrade)
            except SQLAlchemyError as exc:
                raise MigrationError("metadata migration failed") from exc
        _log.info("metadata.migrated", revision=head)

    async def downgrade(self, revision: str) -> None:
        """Downgrade the schema (maintenance and tests)."""
        async with self._write_lock, self.engine.connect() as connection:
            connection = await connection.execution_options(rl_begin="IMMEDIATE")
            async with connection.begin():
                await connection.run_sync(_downgrade, revision)

    async def schema_version(self) -> str | None:
        """Current Alembic revision, or ``None`` for an empty database."""
        async with self.engine.connect() as connection:
            return await connection.run_sync(_current_revision)

    def transaction(self, scope: Scope | None = None, *, write: bool) -> SQLiteTransaction:
        """Open a transaction bound to ``scope`` (if given)."""
        return SQLiteTransaction(self, scope, write=write)

    async def _lookup_scope(self, connection: AsyncConnection, scope_key: str) -> Scope:
        scope = self._scopes.get(scope_key)
        if scope is not None:
            return scope
        row = (
            await _settled(
                connection.execute(sa.select(t.scopes).where(t.scopes.c.scope_key == scope_key))
            )
        ).first()
        if row is None:
            raise TenantIsolationError("unknown scope key", details={"scope_key": scope_key})
        scope = Scope(tenant_id=row.tenant_id, namespace=row.namespace, collection=row.collection)
        self._scopes[scope_key] = scope
        return scope


def _upgrade(connection: Connection) -> None:
    command.upgrade(alembic_config(connection), "head")


def _downgrade(connection: Connection, revision: str) -> None:
    command.downgrade(alembic_config(connection), revision)


def _current_revision(connection: Connection) -> str | None:
    context = MigrationContext.configure(connection, opts={"version_table": VERSION_TABLE})
    return context.get_current_revision()


class SQLiteTransaction(MetadataTransactionABC):
    """Transaction on ``SQLiteMetadataStore``."""

    def __init__(self, store: SQLiteMetadataStore, scope: Scope | None, *, write: bool) -> None:
        self._store = store
        self._scope = scope
        self._write = write
        self._connection: AsyncConnection | None = None
        self._locked = False

    async def __aenter__(self) -> Self:
        lock = self._write or self._store.serialises_reads
        if lock:
            await self._store._write_lock.acquire()
            self._locked = True
        try:
            connection = await _settled(self._store.engine.connect(), _close)
            try:
                connection = await _settled(
                    connection.execution_options(
                        rl_begin="IMMEDIATE" if self._write else "DEFERRED"
                    )
                )
                await _settled(connection.begin())
            except BaseException:
                await _settled(connection.close())
                raise
        except OperationalError as exc:
            self._unlock()
            raise BackendUnavailableError("metadata database unavailable") from exc
        except BaseException:
            self._unlock()
            raise
        self._connection = connection
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Commit on success, roll back otherwise."""
        connection = self._connection
        self._connection = None
        try:
            if connection is not None:
                try:
                    if exc_type is None:
                        await _settled(connection.commit())
                    else:
                        await _settled(connection.rollback())
                finally:
                    await _settled(connection.close())
        finally:
            self._unlock()

    def _unlock(self) -> None:
        if self._locked:
            self._locked = False
            self._store._write_lock.release()

    @property
    def _conn(self) -> AsyncConnection:
        if self._connection is None:
            raise LifecycleError("transaction is not active")
        return self._connection

    def _guard(self, scope_key: str) -> None:
        if self._scope is not None and self._scope.key != scope_key:
            _log.error(
                "metadata.tenant_isolation_violation",
                expected=self._scope.key,
                actual=scope_key,
            )
            raise TenantIsolationError(
                "operation crosses scope boundary",
                details={"expected": self._scope.key, "actual": scope_key},
            )

    def _require_write(self) -> None:
        if not self._write:
            raise LifecycleError("write attempted in a read-only transaction")

    async def _exec(self, statement: Any) -> Any:
        try:
            return await _settled(self._conn.execute(statement))
        except IntegrityError as exc:
            raise VersionConflictError("concurrent modification detected") from exc
        except OperationalError as exc:
            raise BackendUnavailableError("metadata database operation failed") from exc

    async def _scope_of(self, scope_key: str) -> Scope:
        return await self._store._lookup_scope(self._conn, scope_key)

    async def _cas(
        self,
        table: sa.Table,
        keys: dict[str, Any],
        values: dict[str, Any],
        expected_revision: int | None,
    ) -> None:
        self._require_write()
        if expected_revision is None:
            await self._exec(sa.insert(table).values(**values, row_revision=1))
            return
        condition = sa.and_(
            *(table.c[name] == value for name, value in keys.items()),
            table.c.row_revision == expected_revision,
        )
        result = await self._exec(
            sa.update(table).where(condition).values(**values, row_revision=expected_revision + 1)
        )
        if result.rowcount != 1:
            raise VersionConflictError(
                "row revision mismatch", details={"table": table.name, **keys}
            )

    # scopes -----------------------------------------------------------------------
    async def ensure_scope(self, scope: Scope) -> None:
        """Register ``scope``."""
        self._guard(scope.key)
        self._require_write()
        if scope.key in self._store._scopes:
            return
        exists = (
            await self._exec(
                sa.select(t.scopes.c.scope_key).where(t.scopes.c.scope_key == scope.key)
            )
        ).first()
        if exists is None:
            await self._exec(
                sa.insert(t.scopes).values(
                    scope_key=scope.key,
                    tenant_id=scope.tenant_id,
                    namespace=scope.namespace,
                    collection=scope.collection,
                )
            )
        self._store._scopes[scope.key] = scope

    async def list_scopes(self) -> Sequence[Scope]:
        """Every registered scope."""
        rows = (await self._exec(sa.select(t.scopes).order_by(t.scopes.c.scope_key))).all()
        return [
            Scope(tenant_id=row.tenant_id, namespace=row.namespace, collection=row.collection)
            for row in rows
        ]

    # commit sequence --------------------------------------------------------------
    async def next_commit_seq(self) -> int:
        """Increment the commit sequence."""
        self._require_write()
        current = await self.current_commit_seq()
        if current == 0:
            await self._exec(sa.insert(t.commit_seq).values(id=1, seq=1))
            return 1
        await self._exec(
            sa.update(t.commit_seq).where(t.commit_seq.c.id == 1).values(seq=current + 1)
        )
        return current + 1

    async def current_commit_seq(self) -> int:
        """Current commit sequence."""
        value = (
            await self._exec(sa.select(t.commit_seq.c.seq).where(t.commit_seq.c.id == 1))
        ).scalar()
        return int(value or 0)

    # documents --------------------------------------------------------------------
    def _document(self, row: Any, scope: Scope) -> DocumentRecord:
        return DocumentRecord(
            document_id=row.document_id,
            counter=row.counter,
            version=row.version,
            content_hash=row.content_hash,
            created_at=row.created_at,
            updated_at=row.updated_at,
            document_uid=row.document_uid,
            scope=scope,
            state=DocumentState(row.state),
            deleted_at=row.deleted_at,
            metadata=row.metadata,
            dataset=row.dataset,
            source=row.source,
            representation_digests=row.representation_digests,
            representation_kinds=row.representation_kinds,
            hash_profile=row.hash_profile,
            row_revision=row.row_revision,
            lifecycle_revision=row.lifecycle_revision,
            metadata_revision=row.metadata_revision,
            change_seq=row.change_seq,
        )

    async def get_document(self, scope: Scope, document_id: str) -> DocumentRecord | None:
        """Load a document by key."""
        self._guard(scope.key)
        row = (
            await self._exec(
                sa.select(t.documents).where(
                    t.documents.c.scope_key == scope.key,
                    t.documents.c.document_id == document_id,
                )
            )
        ).first()
        return None if row is None else self._document(row, scope)

    async def get_documents_by_uid(
        self, scope_key: str, document_uids: Sequence[str]
    ) -> Mapping[str, DocumentRecord]:
        """Load documents by uid."""
        self._guard(scope_key)
        if not document_uids:
            return {}
        scope = await self._scope_of(scope_key)
        result: dict[str, DocumentRecord] = {}
        for chunk in _chunks(sorted(set(document_uids))):
            rows = (
                await self._exec(
                    sa.select(t.documents).where(
                        t.documents.c.scope_key == scope_key,
                        t.documents.c.document_uid.in_(chunk),
                    )
                )
            ).all()
            for row in rows:
                result[row.document_uid] = self._document(row, scope)
        return result

    async def put_document(self, record: DocumentRecord, *, expected_revision: int | None) -> None:
        """Insert or CAS-update a document."""
        self._guard(record.scope.key)
        values = {
            "scope_key": record.scope.key,
            "document_id": record.document_id,
            "document_uid": record.document_uid,
            "counter": record.counter,
            "version": record.version,
            "lifecycle_revision": record.lifecycle_revision,
            "metadata_revision": record.metadata_revision,
            "content_hash": record.content_hash,
            "hash_profile": record.hash_profile,
            "state": record.state.value,
            "created_at": record.created_at,
            "updated_at": record.updated_at,
            "deleted_at": record.deleted_at,
            "change_seq": record.change_seq,
            "metadata": record.metadata,
            "dataset": record.dataset,
            "source": None if record.source is None else record.source.model_dump(mode="json"),
            "representation_digests": record.representation_digests,
            "representation_kinds": record.representation_kinds,
        }
        await self._cas(
            t.documents,
            {"scope_key": record.scope.key, "document_id": record.document_id},
            values,
            expected_revision,
        )

    async def list_documents(
        self,
        scope_key: str,
        *,
        state: DocumentState | None = None,
        after_id: str | None = None,
        limit: int = 1000,
    ) -> Sequence[DocumentRecord]:
        """Page through documents."""
        self._guard(scope_key)
        scope = await self._scope_of(scope_key)
        query = sa.select(t.documents).where(t.documents.c.scope_key == scope_key)
        if state is not None:
            query = query.where(t.documents.c.state == state.value)
        if after_id is not None:
            query = query.where(t.documents.c.document_id > after_id)
        rows = (await self._exec(query.order_by(t.documents.c.document_id).limit(limit))).all()
        return [self._document(row, scope) for row in rows]

    async def count_documents(self, scope_key: str) -> Mapping[str, int]:
        """Count documents per state."""
        self._guard(scope_key)
        rows = (
            await self._exec(
                sa.select(t.documents.c.state, sa.func.count())
                .where(t.documents.c.scope_key == scope_key)
                .group_by(t.documents.c.state)
            )
        ).all()
        return {row[0]: int(row[1]) for row in rows}

    # versions and representations -------------------------------------------------
    async def put_version(self, scope_key: str, version: DocumentVersion) -> None:
        """Append a version."""
        self._guard(scope_key)
        self._require_write()
        await self._exec(
            sa.insert(t.document_versions).values(
                document_uid=version.document_uid,
                version=version.version,
                scope_key=scope_key,
                content_hash=version.content_hash,
                hash_profile=version.hash_profile,
                metadata=version.metadata,
                dataset=version.dataset,
                source=None if version.source is None else version.source.model_dump(mode="json"),
                representation_digests=version.representation_digests,
                event_id=version.event_id,
                created_at=version.created_at,
            )
        )

    async def get_versions(self, document_uid: str) -> Sequence[DocumentVersion]:
        """Version history."""
        query = sa.select(t.document_versions).where(
            t.document_versions.c.document_uid == document_uid
        )
        if self._scope is not None:
            query = query.where(t.document_versions.c.scope_key == self._scope.key)
        rows = (await self._exec(query.order_by(t.document_versions.c.version))).all()
        return [
            DocumentVersion(
                document_uid=row.document_uid,
                version=row.version,
                content_hash=row.content_hash,
                hash_profile=row.hash_profile,
                representation_digests=row.representation_digests,
                metadata=row.metadata,
                dataset=row.dataset,
                source=row.source,
                created_at=row.created_at,
                event_id=row.event_id,
            )
            for row in rows
        ]

    async def put_representations(
        self, scope_key: str, snapshots: Sequence[RepresentationSnapshot]
    ) -> None:
        """Store representation payloads."""
        self._guard(scope_key)
        self._require_write()
        if not snapshots:
            return
        for snapshot in snapshots:
            await self._exec(
                sa.insert(t.representations).values(
                    document_uid=snapshot.document_uid,
                    version=snapshot.version,
                    representation_id=snapshot.representation_id,
                    scope_key=scope_key,
                    kind=snapshot.kind,
                    payload=snapshot.payload,
                    transformation=None
                    if snapshot.transformation is None
                    else snapshot.transformation.model_dump(mode="json"),
                    hash_profile=snapshot.hash_profile,
                )
            )

    async def get_representations(
        self, document_uid: str, version: int
    ) -> Sequence[RepresentationSnapshot]:
        """Representation payloads of one version."""
        query = sa.select(t.representations).where(
            t.representations.c.document_uid == document_uid,
            t.representations.c.version == version,
        )
        if self._scope is not None:
            query = query.where(t.representations.c.scope_key == self._scope.key)
        rows = (await self._exec(query.order_by(t.representations.c.representation_id))).all()
        return [
            RepresentationSnapshot(
                document_uid=row.document_uid,
                version=row.version,
                representation_id=row.representation_id,
                kind=row.kind,
                payload=row.payload,
                transformation=row.transformation,
                hash_profile=row.hash_profile,
            )
            for row in rows
        ]

    # events -----------------------------------------------------------------------
    async def _event(self, row: Any) -> IngestionEvent:
        return IngestionEvent(
            event_id=row.event_id,
            scope=await self._scope_of(row.scope_key),
            document_id=row.document_id,
            document_uid=row.document_uid,
            received_at=row.received_at,
            status=row.status,
            outcome=row.outcome,
            counter_after=row.counter_after,
            version_after=row.version_after,
            content_hash=row.content_hash,
            delivery=row.delivery,
            appearance_fingerprint=row.appearance_fingerprint,
            possible_redelivery_of=row.possible_redelivery_of,
            staged_payload=row.staged_payload,
            error=row.error,
        )

    async def get_event(self, scope_key: str, event_id: str) -> IngestionEvent | None:
        """Load an event."""
        self._guard(scope_key)
        row = (
            await self._exec(
                sa.select(t.ingestion_events).where(
                    t.ingestion_events.c.scope_key == scope_key,
                    t.ingestion_events.c.event_id == event_id,
                )
            )
        ).first()
        return None if row is None else await self._event(row)

    async def put_event(self, event: IngestionEvent) -> None:
        """Insert or replace an event."""
        scope_key = event.scope.key
        self._guard(scope_key)
        self._require_write()
        values = {
            "document_id": event.document_id,
            "document_uid": event.document_uid,
            "status": event.status.value,
            "outcome": None if event.outcome is None else event.outcome.value,
            "counter_after": event.counter_after,
            "version_after": event.version_after,
            "content_hash": event.content_hash,
            "appearance_fingerprint": event.appearance_fingerprint,
            "possible_redelivery_of": event.possible_redelivery_of,
            "delivery": None if event.delivery is None else event.delivery.model_dump(mode="json"),
            "staged_payload": event.staged_payload,
            "error": None if event.error is None else event.error.model_dump(mode="json"),
            "received_at": event.received_at,
        }
        updated = await self._exec(
            sa.update(t.ingestion_events)
            .where(
                t.ingestion_events.c.scope_key == scope_key,
                t.ingestion_events.c.event_id == event.event_id,
            )
            .values(**values)
        )
        if updated.rowcount == 0:
            await self._exec(
                sa.insert(t.ingestion_events).values(
                    scope_key=scope_key, event_id=event.event_id, **values
                )
            )

    async def find_event_by_fingerprint(
        self, scope_key: str, fingerprint: str, *, since: datetime, exclude_event_id: str
    ) -> IngestionEvent | None:
        """Most recent committed event with the same fingerprint."""
        self._guard(scope_key)
        events = t.ingestion_events
        row = (
            await self._exec(
                sa.select(events)
                .where(
                    events.c.scope_key == scope_key,
                    events.c.appearance_fingerprint == fingerprint,
                    events.c.status == "committed",
                    events.c.event_id != exclude_event_id,
                    events.c.received_at >= since,
                )
                .order_by(events.c.received_at.desc())
                .limit(1)
            )
        ).first()
        return None if row is None else await self._event(row)

    async def list_pending_events(self, *, limit: int = 100) -> Sequence[IngestionEvent]:
        """Pending events."""
        query = sa.select(t.ingestion_events).where(t.ingestion_events.c.status == "pending")
        if self._scope is not None:
            query = query.where(t.ingestion_events.c.scope_key == self._scope.key)
        rows = (
            await self._exec(query.order_by(t.ingestion_events.c.received_at).limit(limit))
        ).all()
        return [await self._event(row) for row in rows]

    # objects ----------------------------------------------------------------------
    async def _object(self, row: Any) -> DerivedObject:
        return DerivedObject(
            object_id=row.object_id,
            scope=await self._scope_of(row.scope_key),
            kind=ObjectKind(row.kind),
            document_uid=row.document_uid,
            version=row.version,
            representation_id=row.representation_id,
            text=row.text,
            sources=row.sources,
            transformation=row.transformation,
            metadata=row.metadata,
            payload=row.payload,
            content_hash=row.content_hash,
            state=ObjectState(row.state),
        )

    async def put_objects(self, objects: Sequence[DerivedObject]) -> None:
        """Insert or replace objects."""
        self._require_write()
        for item in objects:
            self._guard(item.scope.key)
            values = {
                "scope_key": item.scope.key,
                "kind": item.kind.value,
                "document_uid": item.document_uid,
                "version": item.version,
                "representation_id": item.representation_id,
                "state": item.state.value,
                "text": item.text,
                "content_hash": item.content_hash,
                "metadata": item.metadata,
                "payload": item.payload,
                "sources": [source.model_dump(mode="json") for source in item.sources],
                "transformation": None
                if item.transformation is None
                else item.transformation.model_dump(mode="json"),
            }
            existing = (
                await self._exec(
                    sa.select(t.objects.c.scope_key).where(t.objects.c.object_id == item.object_id)
                )
            ).first()
            if existing is None:
                await self._exec(sa.insert(t.objects).values(object_id=item.object_id, **values))
                continue
            if existing.scope_key != item.scope.key:
                raise TenantIsolationError("object id belongs to another scope")
            await self._exec(
                sa.update(t.objects).where(t.objects.c.object_id == item.object_id).values(**values)
            )

    async def get_objects(
        self, scope_key: str, object_ids: Sequence[str]
    ) -> Mapping[str, DerivedObject]:
        """Load objects of one scope by id."""
        self._guard(scope_key)
        result: dict[str, DerivedObject] = {}
        for chunk in _chunks(sorted(set(object_ids))):
            rows = (
                await self._exec(
                    sa.select(t.objects).where(
                        t.objects.c.scope_key == scope_key, t.objects.c.object_id.in_(chunk)
                    )
                )
            ).all()
            for row in rows:
                result[row.object_id] = await self._object(row)
        return result

    async def list_document_objects(
        self,
        scope_key: str,
        document_uid: str,
        version: int | None = None,
        kinds: Sequence[ObjectKind] | None = None,
    ) -> Sequence[DerivedObject]:
        """Objects of one document."""
        self._guard(scope_key)
        query = sa.select(t.objects).where(
            t.objects.c.scope_key == scope_key, t.objects.c.document_uid == document_uid
        )
        if version is not None:
            query = query.where(t.objects.c.version == version)
        if kinds is not None:
            query = query.where(t.objects.c.kind.in_([kind.value for kind in kinds]))
        rows = (await self._exec(query.order_by(t.objects.c.object_id))).all()
        return [await self._object(row) for row in rows]

    async def list_current_objects(
        self,
        scope_key: str,
        kinds: Sequence[ObjectKind],
        *,
        after_object_id: str | None = None,
        limit: int = 1000,
    ) -> Sequence[DerivedObject]:
        """Live objects of current versions of active documents (or live aggregates)."""
        self._guard(scope_key)
        objects = t.objects
        query = sa.select(objects).where(
            objects.c.scope_key == scope_key,
            objects.c.kind.in_([kind.value for kind in kinds]),
            objects.c.state == ObjectState.LIVE.value,
        )
        if not set(kinds) <= _GRAPH_KINDS:
            documents = t.documents
            query = query.join(
                documents,
                sa.and_(
                    documents.c.document_uid == objects.c.document_uid,
                    documents.c.version == objects.c.version,
                    documents.c.state == DocumentState.ACTIVE.value,
                ),
            )
        if after_object_id is not None:
            query = query.where(objects.c.object_id > after_object_id)
        rows = (await self._exec(query.order_by(objects.c.object_id).limit(limit))).all()
        return [await self._object(row) for row in rows]

    async def set_document_objects_state(
        self,
        scope_key: str,
        document_uid: str,
        state: ObjectState,
        *,
        version: int | None = None,
        exclude_version: int | None = None,
    ) -> int:
        """Change object states of a document."""
        self._guard(scope_key)
        self._require_write()
        statement = sa.update(t.objects).where(
            t.objects.c.scope_key == scope_key, t.objects.c.document_uid == document_uid
        )
        if version is not None:
            statement = statement.where(t.objects.c.version == version)
        if exclude_version is not None:
            statement = statement.where(t.objects.c.version != exclude_version)
        result = await self._exec(statement.values(state=state.value))
        return int(result.rowcount)

    async def replace_contributions(
        self,
        scope_key: str,
        document_uid: str,
        contributions: Sequence[ObjectContribution],
    ) -> Sequence[str]:
        """Replace the contributions of a document."""
        self._guard(scope_key)
        self._require_write()
        table = t.object_contributions
        previous = (
            await self._exec(
                sa.select(table.c.object_id)
                .where(table.c.scope_key == scope_key, table.c.document_uid == document_uid)
                .distinct()
            )
        ).scalars()
        affected = set(previous)
        await self._exec(
            sa.delete(table).where(
                table.c.scope_key == scope_key, table.c.document_uid == document_uid
            )
        )
        for contribution in contributions:
            affected.add(contribution.object_id)
            await self._exec(
                sa.insert(table).values(
                    object_id=contribution.object_id,
                    document_uid=contribution.document_uid,
                    version=contribution.version,
                    field_path=contribution.field_path,
                    ordinal=contribution.ordinal,
                    scope_key=scope_key,
                    value=contribution.value,
                )
            )
        return sorted(affected)

    async def get_contributions(
        self, scope_key: str, object_ids: Sequence[str]
    ) -> Sequence[ObjectContribution]:
        """Contributions of aggregate objects."""
        self._guard(scope_key)
        table = t.object_contributions
        result: list[ObjectContribution] = []
        for chunk in _chunks(sorted(set(object_ids))):
            rows = (
                await self._exec(
                    sa.select(table)
                    .where(table.c.scope_key == scope_key, table.c.object_id.in_(chunk))
                    .order_by(
                        table.c.object_id,
                        table.c.version.desc(),
                        table.c.document_uid,
                        table.c.field_path,
                        table.c.ordinal,
                    )
                )
            ).all()
            result.extend(_contribution(row) for row in rows)
        return result

    async def list_document_contributions(
        self, scope_key: str, document_uid: str
    ) -> Sequence[ObjectContribution]:
        """Every contribution of one document."""
        self._guard(scope_key)
        table = t.object_contributions
        rows = (
            await self._exec(
                sa.select(table)
                .where(table.c.scope_key == scope_key, table.c.document_uid == document_uid)
                .order_by(table.c.object_id, table.c.version, table.c.field_path, table.c.ordinal)
            )
        ).all()
        return [_contribution(row) for row in rows]

    async def put_lineage(self, edges: Sequence[LineageEdge]) -> None:
        """Insert lineage edges."""
        self._require_write()
        for edge in edges:
            self._guard(edge.scope_key)
            exists = (
                await self._exec(
                    sa.select(t.lineage.c.relation).where(
                        t.lineage.c.scope_key == edge.scope_key,
                        t.lineage.c.from_id == edge.from_id,
                        t.lineage.c.to_id == edge.to_id,
                        t.lineage.c.relation == edge.relation.value,
                    )
                )
            ).first()
            if exists is None:
                await self._exec(
                    sa.insert(t.lineage).values(
                        scope_key=edge.scope_key,
                        from_id=edge.from_id,
                        to_id=edge.to_id,
                        relation=edge.relation.value,
                    )
                )

    async def get_lineage(self, scope_key: str, object_id: str) -> Sequence[LineageEdge]:
        """Edges touching ``object_id``."""
        self._guard(scope_key)
        rows = (
            await self._exec(
                sa.select(t.lineage)
                .where(
                    t.lineage.c.scope_key == scope_key,
                    sa.or_(t.lineage.c.from_id == object_id, t.lineage.c.to_id == object_id),
                )
                .order_by(t.lineage.c.from_id, t.lineage.c.to_id, t.lineage.c.relation)
            )
        ).all()
        return [
            LineageEdge(
                scope_key=row.scope_key, from_id=row.from_id, to_id=row.to_id, relation=row.relation
            )
            for row in rows
        ]

    # visibility -------------------------------------------------------------------
    def _unit_filter(self, table: sa.Table, unit: UnitKey) -> Any:
        return sa.and_(
            table.c.scope_key == unit.scope_key,
            table.c.channel == unit.channel,
            table.c.method == unit.method,
            table.c.instance == unit.instance,
        )

    async def open_visibility(
        self, unit: UnitKey, document_uid: str, version: int, seq: int
    ) -> None:
        """Open a window of ``version`` at ``seq`` and close other open windows.

        A version keeps one row per window, so reopening a closed version never rewrites
        history that readers pinned to an earlier sequence still depend on.
        """
        self._guard(unit.scope_key)
        self._require_write()
        table = t.visibility
        base = sa.and_(self._unit_filter(table, unit), table.c.document_uid == document_uid)
        await self._exec(
            sa.update(table)
            .where(base, table.c.version != version, table.c.visible_until_seq.is_(None))
            .values(visible_until_seq=seq)
        )
        rows = (
            await self._exec(
                sa.select(table.c.visible_from_seq, table.c.visible_until_seq).where(
                    base, table.c.version == version
                )
            )
        ).all()
        if any(row.visible_until_seq is None for row in rows):
            return
        if any(row.visible_from_seq == seq for row in rows):
            # Closed within the same commit it opened in: the window was empty, reuse it.
            await self._exec(
                sa.update(table)
                .where(base, table.c.version == version, table.c.visible_from_seq == seq)
                .values(visible_until_seq=None)
            )
            return
        await self._exec(
            sa.insert(table).values(
                scope_key=unit.scope_key,
                document_uid=document_uid,
                version=version,
                channel=unit.channel,
                method=unit.method,
                instance=unit.instance,
                visible_from_seq=seq,
                visible_until_seq=None,
            )
        )

    async def close_visibility(self, scope_key: str, document_uid: str, seq: int) -> None:
        """Close every open window of a document."""
        self._guard(scope_key)
        self._require_write()
        table = t.visibility
        await self._exec(
            sa.update(table)
            .where(
                table.c.scope_key == scope_key,
                table.c.document_uid == document_uid,
                table.c.visible_until_seq.is_(None),
            )
            .values(visible_until_seq=seq)
        )

    async def visible_versions(
        self, unit: UnitKey, document_uids: Sequence[str], seq: int
    ) -> Mapping[str, frozenset[int]]:
        """Visible versions per document at ``seq``."""
        self._guard(unit.scope_key)
        table = t.visibility
        found: dict[str, set[int]] = {}
        for chunk in _chunks(sorted(set(document_uids))):
            rows = (
                await self._exec(
                    sa.select(table.c.document_uid, table.c.version).where(
                        self._unit_filter(table, unit),
                        table.c.document_uid.in_(chunk),
                        table.c.visible_from_seq <= seq,
                        sa.or_(
                            table.c.visible_until_seq.is_(None), table.c.visible_until_seq > seq
                        ),
                    )
                )
            ).all()
            for row in rows:
                found.setdefault(row.document_uid, set()).add(row.version)
        return {uid: frozenset(versions) for uid, versions in found.items()}

    # coverage ---------------------------------------------------------------------
    def _coverage(self, row: Any) -> CoverageRecord:
        return CoverageRecord(
            scope_key=row.scope_key,
            document_uid=row.document_uid,
            channel=row.channel,
            method=row.method,
            backend_instance_id=row.instance,
            generation_id=row.generation_id,
            target_version=row.target_version,
            indexed_version=row.indexed_version,
            indexed_metadata_revision=row.indexed_metadata_revision,
            dependency_fingerprint=row.dependency_fingerprint,
            state=CoverageState(row.state),
            attempts=row.attempts,
            last_error=row.last_error,
            lease_owner=row.lease_owner,
            lease_epoch=row.lease_epoch,
            lease_expires_at=row.lease_expires_at,
            updated_at=row.updated_at,
            row_revision=row.row_revision,
        )

    async def upsert_coverage(self, records: Sequence[CoverageRecord]) -> None:
        """Insert or replace coverage rows."""
        self._require_write()
        table = t.coverage
        for record in records:
            self._guard(record.scope_key)
            keys = {
                "scope_key": record.scope_key,
                "document_uid": record.document_uid,
                "channel": record.channel.value,
                "method": record.method,
                "instance": record.backend_instance_id,
            }
            values = {
                "generation_id": record.generation_id,
                "target_version": record.target_version,
                "indexed_version": record.indexed_version,
                "indexed_metadata_revision": record.indexed_metadata_revision,
                "dependency_fingerprint": record.dependency_fingerprint,
                "state": record.state.value,
                "attempts": record.attempts,
                "last_error": None
                if record.last_error is None
                else record.last_error.model_dump(mode="json"),
                "lease_owner": record.lease_owner,
                "lease_epoch": record.lease_epoch,
                "lease_expires_at": record.lease_expires_at,
                "updated_at": record.updated_at,
            }
            condition = sa.and_(*(table.c[name] == value for name, value in keys.items()))
            result = await self._exec(
                sa.update(table)
                .where(condition)
                .values(**values, row_revision=table.c.row_revision + 1)
            )
            if result.rowcount == 0:
                await self._exec(sa.insert(table).values(**keys, **values, row_revision=1))

    async def get_coverage(
        self, unit: UnitKey, document_uids: Sequence[str]
    ) -> Mapping[str, CoverageRecord]:
        """Coverage rows of a unit."""
        self._guard(unit.scope_key)
        table = t.coverage
        result: dict[str, CoverageRecord] = {}
        for chunk in _chunks(sorted(set(document_uids))):
            rows = (
                await self._exec(
                    sa.select(table).where(
                        self._unit_filter(table, unit), table.c.document_uid.in_(chunk)
                    )
                )
            ).all()
            for row in rows:
                result[row.document_uid] = self._coverage(row)
        return result

    async def list_coverage(
        self,
        unit: UnitKey,
        states: Sequence[CoverageState],
        *,
        limit: int = 1000,
    ) -> Sequence[CoverageRecord]:
        """Coverage rows in the given states."""
        self._guard(unit.scope_key)
        table = t.coverage
        rows = (
            await self._exec(
                sa.select(table)
                .where(
                    self._unit_filter(table, unit),
                    table.c.state.in_([state.value for state in states]),
                )
                .order_by(table.c.document_uid)
                .limit(limit)
            )
        ).all()
        return [self._coverage(row) for row in rows]

    async def documents_missing_coverage(
        self, unit: UnitKey, *, limit: int = 1000
    ) -> Sequence[DocumentRecord]:
        """Active documents without coverage for ``unit``."""
        self._guard(unit.scope_key)
        scope = await self._scope_of(unit.scope_key)
        documents = t.documents
        coverage = t.coverage
        covered = sa.exists().where(
            self._unit_filter(coverage, unit),
            coverage.c.document_uid == documents.c.document_uid,
        )
        rows = (
            await self._exec(
                sa.select(documents)
                .where(
                    documents.c.scope_key == unit.scope_key,
                    documents.c.state == DocumentState.ACTIVE.value,
                    ~covered,
                )
                .order_by(documents.c.document_uid)
                .limit(limit)
            )
        ).all()
        return [self._document(row, scope) for row in rows]

    async def coverage_counts(
        self, scope_key: str, instances: Sequence[str]
    ) -> Mapping[str, Mapping[str, int]]:
        """Counts per method key and state."""
        self._guard(scope_key)
        table = t.coverage
        rows = (
            await self._exec(
                sa.select(table.c.channel, table.c.method, table.c.state, sa.func.count())
                .where(table.c.scope_key == scope_key, table.c.instance.in_(list(instances)))
                .group_by(table.c.channel, table.c.method, table.c.state)
            )
        ).all()
        counts: dict[str, dict[str, int]] = {}
        for channel, method, state, count in rows:
            counts.setdefault(f"{channel}.{method}", {})[state] = int(count)
        return counts

    async def set_document_coverage_state(
        self, scope_key: str, document_uid: str, state: CoverageState, *, target_version: int
    ) -> None:
        """Set every coverage row of a document."""
        self._guard(scope_key)
        self._require_write()
        table = t.coverage
        await self._exec(
            sa.update(table)
            .where(table.c.scope_key == scope_key, table.c.document_uid == document_uid)
            .values(
                state=state.value,
                target_version=target_version,
                row_revision=table.c.row_revision + 1,
            )
        )

    # generations and pointers -----------------------------------------------------
    @staticmethod
    def _generation(row: Any) -> IndexGeneration:
        return IndexGeneration(
            generation_id=row.generation_id,
            scope_key=row.scope_key,
            channel=row.channel,
            method=row.method,
            backend_id=row.backend_id,
            backend_instance_id=row.backend_instance_id,
            dependency_fingerprint=row.dependency_fingerprint,
            build_watermark_seq=row.build_watermark_seq,
            state=GenerationState(row.state),
            owner_id=row.owner_id,
            lease_epoch=row.lease_epoch,
            heartbeat_at=row.heartbeat_at,
            created_at=row.created_at,
            published_at=row.published_at,
            retired_at=row.retired_at,
            predecessor_id=row.predecessor_id,
            stats=row.stats,
            row_revision=row.row_revision,
        )

    async def put_generation(
        self, generation: IndexGeneration, *, expected_revision: int | None
    ) -> None:
        """Insert or CAS-update a generation."""
        self._guard(generation.scope_key)
        values = {
            "generation_id": generation.generation_id,
            "scope_key": generation.scope_key,
            "channel": generation.channel.value,
            "method": generation.method,
            "backend_id": generation.backend_id,
            "backend_instance_id": generation.backend_instance_id,
            "dependency_fingerprint": generation.dependency_fingerprint,
            "build_watermark_seq": generation.build_watermark_seq,
            "state": generation.state.value,
            "owner_id": generation.owner_id,
            "lease_epoch": generation.lease_epoch,
            "heartbeat_at": generation.heartbeat_at,
            "created_at": generation.created_at,
            "published_at": generation.published_at,
            "retired_at": generation.retired_at,
            "predecessor_id": generation.predecessor_id,
            "stats": generation.stats,
        }
        await self._cas(
            t.generations,
            {"generation_id": generation.generation_id},
            values,
            expected_revision,
        )

    async def get_generation(self, generation_id: str) -> IndexGeneration | None:
        """Load a generation."""
        row = (
            await self._exec(
                sa.select(t.generations).where(t.generations.c.generation_id == generation_id)
            )
        ).first()
        if row is None:
            return None
        self._guard(row.scope_key)
        return self._generation(row)

    async def list_generations(
        self,
        *,
        scope_key: str | None = None,
        states: Sequence[GenerationState] | None = None,
        instances: Sequence[str | None] | None = None,
    ) -> Sequence[IndexGeneration]:
        """List generations."""
        table = t.generations
        query = sa.select(table)
        if scope_key is None and self._scope is not None:
            scope_key = self._scope.key
        if scope_key is not None:
            self._guard(scope_key)
            query = query.where(table.c.scope_key == scope_key)
        if states is not None:
            query = query.where(table.c.state.in_([state.value for state in states]))
        if instances is not None:
            named = [instance for instance in instances if instance is not None]
            clauses = [table.c.backend_instance_id.in_(named)] if named else []
            if None in instances:
                clauses.append(table.c.backend_instance_id.is_(None))
            query = query.where(sa.or_(*clauses)) if clauses else query.where(sa.false())
        rows = (await self._exec(query.order_by(table.c.created_at, table.c.generation_id))).all()
        return [self._generation(row) for row in rows]

    async def get_pointer(self, unit: UnitKey) -> PointerRow | None:
        """Active pointer of a unit."""
        self._guard(unit.scope_key)
        table = t.generation_pointers
        row = (await self._exec(sa.select(table).where(self._unit_filter(table, unit)))).first()
        if row is None:
            return None
        return PointerRow(
            unit=unit, generation_id=row.active_generation_id, row_revision=row.row_revision
        )

    async def set_pointer(
        self, unit: UnitKey, generation_id: str, *, expected_revision: int | None
    ) -> None:
        """Pointer CAS."""
        self._guard(unit.scope_key)
        keys = {
            "scope_key": unit.scope_key,
            "channel": unit.channel,
            "method": unit.method,
            "instance": unit.instance,
        }
        values = {**keys, "active_generation_id": generation_id}
        await self._cas(t.generation_pointers, keys, values, expected_revision)

    async def list_pointers(self, scope_key: str, instances: Sequence[str]) -> Sequence[PointerRow]:
        """Pointers of a scope."""
        self._guard(scope_key)
        table = t.generation_pointers
        rows = (
            await self._exec(
                sa.select(table)
                .where(table.c.scope_key == scope_key, table.c.instance.in_(list(instances)))
                .order_by(table.c.channel, table.c.method, table.c.instance)
            )
        ).all()
        return [
            PointerRow(
                unit=UnitKey(
                    scope_key=row.scope_key,
                    channel=row.channel,
                    method=row.method,
                    instance=row.instance,
                ),
                generation_id=row.active_generation_id,
                row_revision=row.row_revision,
            )
            for row in rows
        ]

    # tasks ------------------------------------------------------------------------
    @staticmethod
    def _task(row: Any) -> LifecycleTask:
        return LifecycleTask(
            task_id=row.task_id,
            scope_key=row.scope_key,
            kind=row.kind,
            target=row.target,
            state=TaskState(row.state),
            cursor=row.cursor,
            attempts=row.attempts,
            owner_id=row.owner_id,
            lease_epoch=row.lease_epoch,
            heartbeat_at=row.heartbeat_at,
            refresh_correlation_id=row.refresh_correlation_id,
            created_at=row.created_at,
            updated_at=row.updated_at,
            error=row.error,
            row_revision=row.row_revision,
        )

    async def put_task(self, task: LifecycleTask, *, expected_revision: int | None) -> None:
        """Insert or CAS-update a task."""
        self._guard(task.scope_key)
        values = {
            "task_id": task.task_id,
            "scope_key": task.scope_key,
            "kind": task.kind.value,
            "target": task.target,
            "state": task.state.value,
            "cursor": task.cursor,
            "attempts": task.attempts,
            "owner_id": task.owner_id,
            "lease_epoch": task.lease_epoch,
            "heartbeat_at": task.heartbeat_at,
            "refresh_correlation_id": task.refresh_correlation_id,
            "created_at": task.created_at,
            "updated_at": task.updated_at,
            "error": None if task.error is None else task.error.model_dump(mode="json"),
        }
        await self._cas(t.tasks, {"task_id": task.task_id}, values, expected_revision)

    async def get_task(self, task_id: str) -> LifecycleTask | None:
        """Load a task."""
        row = (await self._exec(sa.select(t.tasks).where(t.tasks.c.task_id == task_id))).first()
        if row is None:
            return None
        self._guard(row.scope_key)
        return self._task(row)

    async def list_tasks(
        self,
        *,
        scope_key: str | None = None,
        states: Sequence[TaskState] | None = None,
        limit: int = 1000,
    ) -> Sequence[LifecycleTask]:
        """List tasks."""
        table = t.tasks
        query = sa.select(table)
        if scope_key is None and self._scope is not None:
            scope_key = self._scope.key
        if scope_key is not None:
            self._guard(scope_key)
            query = query.where(table.c.scope_key == scope_key)
        if states is not None:
            query = query.where(table.c.state.in_([state.value for state in states]))
        rows = (
            await self._exec(query.order_by(table.c.created_at, table.c.task_id).limit(limit))
        ).all()
        return [self._task(row) for row in rows]

    # reader leases and engine instances -------------------------------------------
    async def put_reader_lease(
        self, generation_id: str, engine_id: str, *, expires_at: datetime, now: datetime
    ) -> None:
        """Acquire or extend a reader lease."""
        self._require_write()
        table = t.reader_leases
        result = await self._exec(
            sa.update(table)
            .where(table.c.generation_id == generation_id, table.c.engine_id == engine_id)
            .values(expires_at=expires_at, heartbeat_at=now)
        )
        if result.rowcount == 0:
            await self._exec(
                sa.insert(table).values(
                    generation_id=generation_id,
                    engine_id=engine_id,
                    expires_at=expires_at,
                    heartbeat_at=now,
                )
            )

    async def delete_reader_lease(self, generation_id: str, engine_id: str) -> None:
        """Release a reader lease."""
        self._require_write()
        table = t.reader_leases
        await self._exec(
            sa.delete(table).where(
                table.c.generation_id == generation_id, table.c.engine_id == engine_id
            )
        )

    async def count_reader_leases(self, generation_id: str, *, now: datetime) -> int:
        """Unexpired leases on a generation."""
        table = t.reader_leases
        value = (
            await self._exec(
                sa.select(sa.func.count()).where(
                    table.c.generation_id == generation_id,
                    table.c.expires_at > now,
                )
            )
        ).scalar()
        return int(value or 0)

    async def purge_expired_leases(self, *, now: datetime) -> int:
        """Delete expired leases."""
        self._require_write()
        table = t.reader_leases
        result = await self._exec(sa.delete(table).where(table.c.expires_at <= now))
        return int(result.rowcount)

    async def heartbeat_instance(
        self, instance_id: str, engine_id: str, *, started_at: datetime, expires_at: datetime
    ) -> None:
        """Register or extend an engine instance."""
        self._require_write()
        table = t.engine_instances
        result = await self._exec(
            sa.update(table).where(table.c.instance_id == instance_id).values(expires_at=expires_at)
        )
        if result.rowcount == 0:
            await self._exec(
                sa.insert(table).values(
                    instance_id=instance_id,
                    engine_id=engine_id,
                    started_at=started_at,
                    expires_at=expires_at,
                )
            )

    async def expired_instances(self, *, now: datetime) -> Sequence[str]:
        """Expired engine instances."""
        table = t.engine_instances
        rows = (
            await self._exec(
                sa.select(table.c.instance_id)
                .where(table.c.expires_at <= now)
                .order_by(table.c.instance_id)
            )
        ).scalars()
        return list(rows)

    async def purge_instance(self, instance_id: str) -> None:
        """Delete rows owned by an engine instance, including its reader leases."""
        self._require_write()
        if not instance_id:
            raise ValueError("the shared (durable) instance cannot be purged")
        for table in (t.coverage, t.visibility, t.generation_pointers):
            await self._exec(sa.delete(table).where(table.c.instance == instance_id))
        await self._exec(
            sa.delete(t.generations).where(t.generations.c.backend_instance_id == instance_id)
        )
        await self._exec(
            sa.delete(t.reader_leases).where(t.reader_leases.c.engine_id == instance_id)
        )
        await self._exec(
            sa.delete(t.engine_instances).where(t.engine_instances.c.instance_id == instance_id)
        )

    # refresh-engine state ---------------------------------------------------------
    async def load_refresh_states(self, namespace: str) -> Sequence[StoredRefreshState]:
        """Load refresh-engine state."""
        table = t.refresh_state
        rows = (
            await self._exec(
                sa.select(table).where(table.c.namespace == namespace).order_by(table.c.resource_id)
            )
        ).all()
        return [StoredRefreshState(resource_id=row.resource_id, state=row.state) for row in rows]

    async def apply_refresh_states(
        self,
        namespace: str,
        puts: Sequence[StoredRefreshState],
        deletes: Sequence[str],
    ) -> None:
        """Persist refresh-engine state changes."""
        self._require_write()
        table = t.refresh_state
        ids = [*deletes, *(item.resource_id for item in puts)]
        for chunk in _chunks(ids):
            await self._exec(
                sa.delete(table).where(
                    table.c.namespace == namespace, table.c.resource_id.in_(chunk)
                )
            )
        for item in puts:
            await self._exec(
                sa.insert(table).values(
                    namespace=namespace, resource_id=item.resource_id, state=item.state
                )
            )


def _contribution(row: Any) -> ObjectContribution:
    return ObjectContribution(
        object_id=row.object_id,
        document_uid=row.document_uid,
        version=row.version,
        field_path=row.field_path,
        ordinal=row.ordinal,
        value=row.value,
    )


def _chunks(values: Sequence[str], size: int = 500) -> list[Sequence[str]]:
    return [values[index : index + size] for index in range(0, len(values), size)]
