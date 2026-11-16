"""Dialect-neutral metadata store on SQLAlchemy async Core.

``SQLAlchemyMetadataStore`` and ``SQLAlchemyTransaction`` hold every query of the
metadata store. Dialect subclasses (``SQLiteMetadataStore``, ``PostgresMetadataStore``)
supply engine creation, transaction isolation and locking, the dialect ``INSERT``
construct (for upserts) and the translation of driver errors.
"""

from __future__ import annotations

import asyncio
import contextlib
from abc import abstractmethod
from pathlib import Path
from typing import TYPE_CHECKING, Any, NoReturn, Self

import sqlalchemy as sa
from alembic import command
from alembic.config import Config
from alembic.runtime.migration import MigrationContext
from alembic.script import ScriptDirectory
from sqlalchemy.exc import SQLAlchemyError

from retrievalloom._logging import get_logger
from retrievalloom.domain.inputs import Scope
from retrievalloom.domain.records import (
    CoverageRecord,
    CoverageState,
    DerivedObject,
    DocumentRecord,
    DocumentState,
    DocumentVersion,
    EventStatus,
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
    LifecycleError,
    MigrationError,
    RetrievalLoomError,
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
    from contextlib import AbstractAsyncContextManager
    from datetime import datetime
    from types import TracebackType

    from sqlalchemy.engine import Connection
    from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

    from retrievalloom.contracts.component import ComponentContext, ComponentInfo

_log = get_logger("storage.sql")

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

    Cancelling a driver call mid-statement makes SQLAlchemy invalidate the connection,
    and an abandoned checked-out connection is closed by the garbage collector; for an
    in-memory SQLite database either one replaces the database with an empty one. The
    operation therefore always runs to completion and ``release`` disposes of a result
    the cancelled caller will never see; the cancellation is re-raised afterwards.
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


def _downgrade(connection: Connection, revision: str) -> None:
    command.downgrade(alembic_config(connection), revision)


def _current_revision(connection: Connection) -> str | None:
    context = MigrationContext.configure(connection, opts={"version_table": VERSION_TABLE})
    return context.get_current_revision()


class SQLAlchemyMetadataStore(DocumentMetadataStoreABC):
    """Shared implementation of the SQL metadata stores.

    Args:
        engine: Application-owned SQLAlchemy async engine (not disposed on close), or
            ``None`` to let the subclass create and own one in ``initialize``.
        info: Component identity of the concrete store.
    """

    def __init__(self, *, engine: AsyncEngine | None, info: ComponentInfo) -> None:
        self._engine = engine
        self._owns_engine = engine is None
        self._scopes: dict[str, Scope] = {}
        self._info = info

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

    @abstractmethod
    async def initialize(self, context: ComponentContext | None = None) -> None:
        """Create the engine (idempotent)."""

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
        except (SQLAlchemyError, OSError, RetrievalLoomError) as exc:
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
        try:
            async with self._maintenance() as connection:
                await connection.run_sync(self._upgrade)
        except SQLAlchemyError as exc:
            raise MigrationError("metadata migration failed") from exc
        _log.info("metadata.migrated", revision=head)

    async def downgrade(self, revision: str) -> None:
        """Downgrade the schema (maintenance and tests)."""
        async with self._maintenance() as connection:
            await connection.run_sync(_downgrade, revision)

    async def schema_version(self) -> str | None:
        """Current Alembic revision, or ``None`` for an empty database."""
        async with self.engine.connect() as connection:
            return await connection.run_sync(_current_revision)

    @abstractmethod
    def transaction(self, scope: Scope | None = None, *, write: bool) -> SQLAlchemyTransaction:
        """Open a transaction bound to ``scope`` (if given)."""

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

    # dialect hooks ---------------------------------------------------------------------
    @abstractmethod
    def _maintenance(self) -> AbstractAsyncContextManager[AsyncConnection]:
        """Exclusive connection, inside a transaction, for schema migrations."""

    @abstractmethod
    def _upgrade(self, connection: Connection) -> None:
        """Upgrade the schema to head on a synchronous migration connection."""

    @abstractmethod
    def _process_lock(self, scope: Scope | None, *, write: bool) -> asyncio.Lock | None:
        """In-process lock a transaction holds while open, or ``None``."""

    @abstractmethod
    def _begin_options(self, *, write: bool) -> dict[str, Any]:
        """Connection execution options applied before ``BEGIN``."""

    async def _after_begin(self, transaction: SQLAlchemyTransaction) -> None:
        """Take database locks right after ``BEGIN`` (no-op by default)."""

    @abstractmethod
    def _insert(self, table: sa.Table) -> Any:
        """Dialect ``INSERT`` supporting ``on_conflict_do_update`` and ``excluded``."""

    @abstractmethod
    def _translate(self, exc: Exception) -> RetrievalLoomError | None:
        """Map a driver error to a RetrievalLoom error, or ``None`` to re-raise it."""

    def _reraise(self, exc: Exception) -> NoReturn:
        error = self._translate(exc)
        if error is None:
            raise exc
        raise error from exc


class SQLAlchemyTransaction(MetadataTransactionABC):
    """Transaction on a ``SQLAlchemyMetadataStore``."""

    def __init__(self, store: SQLAlchemyMetadataStore, scope: Scope | None, *, write: bool) -> None:
        self._store = store
        self._scope = scope
        self._write = write
        self._connection: AsyncConnection | None = None
        self._lock: asyncio.Lock | None = None

    @property
    def scope(self) -> Scope | None:
        """Scope the transaction is bound to."""
        return self._scope

    @property
    def write(self) -> bool:
        """Whether this is a write transaction."""
        return self._write

    async def __aenter__(self) -> Self:
        lock = self._store._process_lock(self._scope, write=self._write)
        if lock is not None:
            await lock.acquire()
            self._lock = lock
        try:
            connection = await _settled(self._store.engine.connect(), _close)
            try:
                connection = await _settled(
                    connection.execution_options(**self._store._begin_options(write=self._write))
                )
                await _settled(connection.begin())
                self._connection = connection
                await self._store._after_begin(self)
            except BaseException:
                self._connection = None
                await _settled(connection.close())
                raise
        except (SQLAlchemyError, OSError) as exc:
            self._unlock()
            self._store._reraise(exc)
        except BaseException:
            self._unlock()
            raise
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
                        try:
                            await _settled(connection.commit())
                        except (SQLAlchemyError, OSError) as error:
                            self._store._reraise(error)
                    else:
                        await _settled(connection.rollback())
                finally:
                    await _settled(connection.close())
        finally:
            self._unlock()

    def _unlock(self) -> None:
        lock = self._lock
        if lock is not None:
            self._lock = None
            lock.release()

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
        except (SQLAlchemyError, OSError) as exc:
            self._store._reraise(exc)

    async def _exec_many(self, statement: Any, rows: Sequence[dict[str, Any]]) -> None:
        """Execute ``statement`` once per parameter set in one driver round trip."""
        if not rows:
            return
        try:
            await _settled(self._conn.execute(statement, list(rows)))
        except (SQLAlchemyError, OSError) as exc:
            self._store._reraise(exc)

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
        table = t.commit_seq
        # One atomic upsert: concurrent writers can never read the same value, and the row
        # lock it takes orders commit sequences by commit time on multi-writer databases.
        statement = self._store._insert(table).values(id=1, seq=1)
        statement = statement.on_conflict_do_update(
            index_elements=[table.c.id], set_={"seq": table.c.seq + 1}
        ).returning(table.c.seq)
        return int((await self._exec(statement)).scalar_one())

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
            # ``document_uid`` is unique, so filtering the scope here keeps SQLite on the
            # uid index instead of scanning the scope.
            rows = (
                await self._exec(
                    sa.select(t.documents).where(t.documents.c.document_uid.in_(chunk))
                )
            ).all()
            for row in rows:
                if row.scope_key == scope_key:
                    result[row.document_uid] = self._document(row, scope)
        return result

    async def get_documents(
        self, scope: Scope, document_ids: Sequence[str]
    ) -> Mapping[str, DocumentRecord]:
        """Load documents by key."""
        self._guard(scope.key)
        result: dict[str, DocumentRecord] = {}
        for chunk in _chunks(sorted(set(document_ids))):
            rows = (
                await self._exec(
                    sa.select(t.documents).where(
                        t.documents.c.scope_key == scope.key,
                        t.documents.c.document_id.in_(chunk),
                    )
                )
            ).all()
            for row in rows:
                result[row.document_id] = self._document(row, scope)
        return result

    async def put_document(self, record: DocumentRecord, *, expected_revision: int | None) -> None:
        """Insert or CAS-update a document."""
        await self.put_documents([(record, expected_revision)])

    async def put_documents(self, entries: Sequence[tuple[DocumentRecord, int | None]]) -> None:
        """Insert or CAS-update many documents."""
        for record, _ in entries:
            self._guard(record.scope.key)
        self._require_write()
        inserts = [_document_values(record) for record, expected in entries if expected is None]
        updates = [(record, expected) for record, expected in entries if expected is not None]
        await self._exec_many(
            sa.insert(t.documents), [{**values, "row_revision": 1} for values in inserts]
        )
        if len(updates) == 1:
            record, expected = updates[0]
            await self._cas(
                t.documents,
                {"scope_key": record.scope.key, "document_id": record.document_id},
                _document_values(record),
                expected,
            )
        elif updates:
            await self._put_document_updates(updates)

    async def _put_document_updates(self, updates: Sequence[tuple[DocumentRecord, int]]) -> None:
        table = t.documents
        current: dict[tuple[str, str], int] = {}
        by_scope: dict[str, set[str]] = {}
        for record, _ in updates:
            by_scope.setdefault(record.scope.key, set()).add(record.document_id)
        for scope_key, document_ids in by_scope.items():
            for chunk in _chunks(sorted(document_ids)):
                rows = (
                    await self._exec(
                        sa.select(table.c.document_id, table.c.row_revision).where(
                            table.c.scope_key == scope_key, table.c.document_id.in_(chunk)
                        )
                    )
                ).all()
                current.update({(scope_key, row.document_id): row.row_revision for row in rows})
        for record, expected in updates:
            key = (record.scope.key, record.document_id)
            if current.get(key) != expected:
                raise VersionConflictError(
                    "row revision mismatch",
                    details={"table": table.name, "document_id": record.document_id},
                )
            current[key] = expected + 1
        # Write transactions are serialised per scope, so no other writer can move a
        # revision between the check above and the guarded update below.
        await self._exec_many(
            sa.update(table).where(
                table.c.scope_key == sa.bindparam("b_scope_key"),
                table.c.document_id == sa.bindparam("b_document_id"),
                table.c.row_revision == sa.bindparam("b_row_revision"),
            ),
            [
                {
                    **_document_values(record),
                    "b_scope_key": record.scope.key,
                    "b_document_id": record.document_id,
                    "b_row_revision": expected,
                    "row_revision": expected + 1,
                }
                for record, expected in updates
            ],
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

    async def list_document_metadata(
        self,
        scope_key: str,
        *,
        state: DocumentState | None = None,
        after_id: str | None = None,
        limit: int = 1000,
    ) -> Sequence[tuple[str, Mapping[str, object]]]:
        """Page through ``(document_id, metadata)`` pairs."""
        self._guard(scope_key)
        columns = t.documents.c
        query = sa.select(columns.document_id, columns.metadata).where(
            columns.scope_key == scope_key
        )
        if state is not None:
            query = query.where(columns.state == state.value)
        if after_id is not None:
            query = query.where(columns.document_id > after_id)
        rows = (await self._exec(query.order_by(columns.document_id).limit(limit))).all()
        return [(row.document_id, row.metadata) for row in rows]

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
        await self.put_versions(scope_key, [version])

    async def put_versions(self, scope_key: str, versions: Sequence[DocumentVersion]) -> None:
        """Append versions."""
        self._guard(scope_key)
        self._require_write()
        await self._exec_many(
            sa.insert(t.document_versions),
            [
                {
                    "document_uid": version.document_uid,
                    "version": version.version,
                    "scope_key": scope_key,
                    "content_hash": version.content_hash,
                    "hash_profile": version.hash_profile,
                    "metadata": version.metadata,
                    "dataset": version.dataset,
                    "source": None
                    if version.source is None
                    else version.source.model_dump(mode="json"),
                    "representation_digests": version.representation_digests,
                    "event_id": version.event_id,
                    "created_at": version.created_at,
                }
                for version in versions
            ],
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
        await self._exec_many(
            sa.insert(t.representations),
            [
                {
                    "document_uid": snapshot.document_uid,
                    "version": snapshot.version,
                    "representation_id": snapshot.representation_id,
                    "scope_key": scope_key,
                    "kind": snapshot.kind,
                    "payload": snapshot.payload,
                    "transformation": None
                    if snapshot.transformation is None
                    else snapshot.transformation.model_dump(mode="json"),
                    "hash_profile": snapshot.hash_profile,
                }
                for snapshot in snapshots
            ],
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

    async def get_events(
        self, scope_key: str, event_ids: Sequence[str]
    ) -> Mapping[str, IngestionEvent]:
        """Load events by id."""
        self._guard(scope_key)
        events = t.ingestion_events
        result: dict[str, IngestionEvent] = {}
        for chunk in _chunks(sorted(set(event_ids))):
            rows = (
                await self._exec(
                    sa.select(events).where(
                        events.c.scope_key == scope_key, events.c.event_id.in_(chunk)
                    )
                )
            ).all()
            for row in rows:
                result[row.event_id] = await self._event(row)
        return result

    async def put_event(self, event: IngestionEvent) -> None:
        """Insert or replace an event."""
        await self.put_events([event])

    async def put_events(self, events: Sequence[IngestionEvent]) -> None:
        """Insert or replace events."""
        rows: dict[tuple[str, str], dict[str, Any]] = {}
        for event in events:
            self._guard(event.scope.key)
            rows[event.scope.key, event.event_id] = _event_values(event)
        self._require_write()
        if not rows:
            return
        table = t.ingestion_events
        keys = ["scope_key", "event_id"]
        statement = self._store._insert(table)
        statement = statement.on_conflict_do_update(
            index_elements=keys,
            set_={
                column.name: statement.excluded[column.name]
                for column in table.columns
                if column.name not in keys
            },
        )
        await self._exec_many(statement, list(rows.values()))

    async def find_events_by_fingerprints(
        self, scope_key: str, probes: Mapping[str, str], *, since: datetime
    ) -> Mapping[str, IngestionEvent]:
        """Most recent committed event per fingerprint."""
        self._guard(scope_key)
        events = t.ingestion_events
        found: dict[str, Any] = {}
        for chunk in _chunks(sorted(probes)):
            rows = (
                await self._exec(
                    sa.select(events)
                    .where(
                        events.c.scope_key == scope_key,
                        events.c.appearance_fingerprint.in_(chunk),
                        events.c.status == "committed",
                        events.c.received_at >= since,
                    )
                    .order_by(events.c.received_at.desc())
                )
            ).all()
            for row in rows:
                fingerprint = row.appearance_fingerprint
                if fingerprint not in found and row.event_id != probes[fingerprint]:
                    found[fingerprint] = row
        return {fingerprint: await self._event(row) for fingerprint, row in found.items()}

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
        rows: dict[str, dict[str, Any]] = {}
        for item in objects:
            self._guard(item.scope.key)
            rows[item.object_id] = _object_values(item)
        existing: dict[str, str] = {}
        for chunk in _chunks(sorted(rows)):
            found = (
                await self._exec(
                    sa.select(t.objects.c.object_id, t.objects.c.scope_key).where(
                        t.objects.c.object_id.in_(chunk)
                    )
                )
            ).all()
            existing.update({row.object_id: row.scope_key for row in found})
        if any(rows[object_id]["scope_key"] != scope for object_id, scope in existing.items()):
            raise TenantIsolationError("object id belongs to another scope")
        await self._exec_many(
            sa.insert(t.objects),
            [values for object_id, values in rows.items() if object_id not in existing],
        )
        await self._exec_many(
            sa.update(t.objects).where(t.objects.c.object_id == sa.bindparam("b_object_id")),
            [
                {**values, "b_object_id": object_id}
                for object_id, values in rows.items()
                if object_id in existing
            ],
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

    async def list_objects_for_documents(
        self,
        scope_key: str,
        documents: Sequence[tuple[str, int | None]],
        kinds: Sequence[ObjectKind] | None = None,
    ) -> Mapping[str, Sequence[DerivedObject]]:
        """Objects of many documents."""
        self._guard(scope_key)
        objects = t.objects
        wanted: dict[str, set[int | None]] = {}
        for document_uid, version in documents:
            wanted.setdefault(document_uid, set()).add(version)
        collected: dict[str, dict[str, Any]] = {document_uid: {} for document_uid in wanted}
        any_version = sorted(uid for uid, versions in wanted.items() if None in versions)
        versioned = sorted(uid for uid, versions in wanted.items() if None not in versions)
        # Kinds are filtered here: a ``kind`` predicate makes SQLite pick the kind index
        # and scan every object of that kind.
        kept = None if kinds is None else {kind.value for kind in kinds}
        for uids, pinned in ((any_version, False), (versioned, True)):
            for chunk in _chunks(uids):
                query = sa.select(objects).where(
                    objects.c.scope_key == scope_key, objects.c.document_uid.in_(chunk)
                )
                if pinned:
                    versions = sorted({v for uid in chunk for v in wanted[uid] if v is not None})
                    query = query.where(objects.c.version.in_(versions))
                for row in (await self._exec(query)).all():
                    if (kept is None or row.kind in kept) and (
                        not pinned or row.version in wanted[row.document_uid]
                    ):
                        collected[row.document_uid][row.object_id] = row
        return {
            document_uid: [await self._object(rows[key]) for key in sorted(rows)]
            for document_uid, rows in collected.items()
        }

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
        await self._exec_many(
            sa.insert(table),
            [
                {
                    "object_id": contribution.object_id,
                    "document_uid": contribution.document_uid,
                    "version": contribution.version,
                    "field_path": contribution.field_path,
                    "ordinal": contribution.ordinal,
                    "scope_key": scope_key,
                    "value": contribution.value,
                }
                for contribution in contributions
            ],
        )
        affected.update(contribution.object_id for contribution in contributions)
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
        rows: dict[tuple[str, str, str, str], dict[str, Any]] = {}
        for edge in edges:
            self._guard(edge.scope_key)
            key = (edge.scope_key, edge.from_id, edge.to_id, edge.relation.value)
            rows[key] = {
                "scope_key": edge.scope_key,
                "from_id": edge.from_id,
                "to_id": edge.to_id,
                "relation": edge.relation.value,
            }
        await self._exec_many(
            self._store._insert(t.lineage).on_conflict_do_nothing(), list(rows.values())
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
        await self.open_visibility_many(unit, [(document_uid, version)], seq)

    async def open_visibility_many(
        self, unit: UnitKey, documents: Sequence[tuple[str, int]], seq: int
    ) -> None:
        """Open windows of many ``(document_uid, version)`` pairs at ``seq``."""
        self._guard(unit.scope_key)
        self._require_write()
        table = t.visibility
        targets = dict(documents)
        base = sa.and_(
            self._unit_filter(table, unit), table.c.document_uid == sa.bindparam("b_uid")
        )
        await self._exec_many(
            sa.update(table)
            .where(
                base,
                table.c.version != sa.bindparam("b_version"),
                table.c.visible_until_seq.is_(None),
            )
            .values(visible_until_seq=seq),
            [{"b_uid": uid, "b_version": version} for uid, version in targets.items()],
        )
        open_now: set[str] = set()
        empty: set[str] = set()
        for chunk in _chunks(sorted(targets)):
            rows = (
                await self._exec(
                    sa.select(
                        table.c.document_uid,
                        table.c.version,
                        table.c.visible_from_seq,
                        table.c.visible_until_seq,
                    ).where(self._unit_filter(table, unit), table.c.document_uid.in_(chunk))
                )
            ).all()
            for row in rows:
                if row.version != targets[row.document_uid]:
                    continue
                if row.visible_until_seq is None:
                    open_now.add(row.document_uid)
                elif row.visible_from_seq == seq:
                    empty.add(row.document_uid)
        # Closed within the same commit it opened in: the window was empty, reuse it.
        await self._exec_many(
            sa.update(table)
            .where(
                base,
                table.c.version == sa.bindparam("b_version"),
                table.c.visible_from_seq == seq,
            )
            .values(visible_until_seq=None),
            [{"b_uid": uid, "b_version": targets[uid]} for uid in sorted(empty - open_now)],
        )
        await self._exec_many(
            sa.insert(table),
            [
                {
                    "scope_key": unit.scope_key,
                    "document_uid": uid,
                    "version": version,
                    "channel": unit.channel,
                    "method": unit.method,
                    "instance": unit.instance,
                    "visible_from_seq": seq,
                    "visible_until_seq": None,
                }
                for uid, version in sorted(targets.items())
                if uid not in open_now and uid not in empty
            ],
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
        rows: list[dict[str, Any]] = []
        for record in records:
            self._guard(record.scope_key)
            rows.append(
                {
                    "scope_key": record.scope_key,
                    "document_uid": record.document_uid,
                    "channel": record.channel.value,
                    "method": record.method,
                    "instance": record.backend_instance_id,
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
                    "row_revision": 1,
                }
            )
        keys = [column.name for column in table.primary_key.columns]
        if not rows:
            return
        statement = self._store._insert(table)
        statement = statement.on_conflict_do_update(
            index_elements=keys,
            set_={
                **{
                    name: statement.excluded[name]
                    for name in rows[0]
                    if name not in keys and name != "row_revision"
                },
                "row_revision": table.c.row_revision + 1,
            },
        )
        await self._exec_many(statement, rows)

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
                # Same order as ``document_uid``, but keeps SQLite from walking the primary
                # key over the whole scope to avoid a sort instead of using the state index.
                .order_by(table.c.document_uid.concat(""))
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
        await self.set_documents_coverage_state(scope_key, {document_uid: target_version}, state)

    async def set_documents_coverage_state(
        self, scope_key: str, targets: Mapping[str, int], state: CoverageState
    ) -> None:
        """Set every coverage row of many documents."""
        self._guard(scope_key)
        self._require_write()
        table = t.coverage
        await self._exec_many(
            sa.update(table)
            .where(table.c.scope_key == scope_key, table.c.document_uid == sa.bindparam("b_uid"))
            .values(
                state=state.value,
                target_version=sa.bindparam("b_target"),
                row_revision=table.c.row_revision + 1,
            ),
            [{"b_uid": uid, "b_target": target} for uid, target in targets.items()],
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

    # erasure and retention --------------------------------------------------------
    async def list_purgeable_documents(
        self, scope_key: str, *, deleted_before: datetime, limit: int = 1000
    ) -> Sequence[DocumentRecord]:
        """Tombstoned documents whose coverage is fully deleted."""
        self._guard(scope_key)
        scope = await self._scope_of(scope_key)
        documents = t.documents
        coverage = t.coverage
        pending = sa.exists().where(
            coverage.c.scope_key == documents.c.scope_key,
            coverage.c.document_uid == documents.c.document_uid,
            coverage.c.state != CoverageState.DELETED.value,
        )
        rows = (
            await self._exec(
                sa.select(documents)
                .where(
                    documents.c.scope_key == scope_key,
                    documents.c.state == DocumentState.DELETED.value,
                    documents.c.deleted_at < deleted_before,
                    ~pending,
                )
                .order_by(documents.c.document_uid)
                .limit(limit)
            )
        ).all()
        return [self._document(row, scope) for row in rows]

    async def purge_document(
        self, scope_key: str, document_uid: str, *, object_ids: Sequence[str] = ()
    ) -> int:
        """Physically delete a tombstoned document and everything derived from it."""
        self._guard(scope_key)
        self._require_write()
        state = (
            await self._exec(
                sa.select(t.documents.c.state).where(
                    t.documents.c.scope_key == scope_key,
                    t.documents.c.document_uid == document_uid,
                )
            )
        ).scalar()
        if state is not None and state != DocumentState.DELETED.value:
            raise LifecycleError(
                "only deleted documents can be purged",
                details={"document_uid": document_uid, "state": state},
            )
        objects = t.objects
        owned = (
            await self._exec(
                sa.select(objects.c.object_id).where(
                    objects.c.scope_key == scope_key, objects.c.document_uid == document_uid
                )
            )
        ).scalars()
        removed = sorted({*owned, *object_ids})
        deleted = 0
        lineage = t.lineage
        deleted += await self._delete(
            lineage,
            sa.or_(lineage.c.from_id == document_uid, lineage.c.to_id == document_uid),
            scope_key,
        )
        contributions = t.object_contributions
        deleted += await self._delete(
            contributions, contributions.c.document_uid == document_uid, scope_key
        )
        for chunk in _chunks(removed):
            deleted += await self._delete(
                lineage,
                sa.or_(lineage.c.from_id.in_(chunk), lineage.c.to_id.in_(chunk)),
                scope_key,
            )
            deleted += await self._delete(
                contributions, contributions.c.object_id.in_(chunk), scope_key
            )
            deleted += await self._delete(objects, objects.c.object_id.in_(chunk), scope_key)
        for table in (
            t.visibility,
            t.coverage,
            t.ingestion_events,
            t.representations,
            t.document_versions,
            t.documents,
        ):
            deleted += await self._delete(table, table.c.document_uid == document_uid, scope_key)
        return deleted

    async def _delete(self, table: sa.Table, condition: Any, scope_key: str) -> int:
        result = await self._exec(sa.delete(table).where(table.c.scope_key == scope_key, condition))
        return int(result.rowcount)

    async def prune_versions(self, scope_key: str, *, keep: int, limit: int = 1000) -> int:
        """Delete version history beyond the ``keep`` newest unprotected versions."""
        self._guard(scope_key)
        self._require_write()
        if keep < 1:
            raise ValueError("keep must be at least 1")
        versions = t.document_versions
        crowded = (
            sa.select(versions.c.document_uid)
            .where(versions.c.scope_key == scope_key)
            .group_by(versions.c.document_uid)
            .having(sa.func.count() > keep)
        )
        ranked = (
            sa.select(
                versions.c.document_uid,
                versions.c.version,
                sa.func.row_number()
                .over(partition_by=versions.c.document_uid, order_by=versions.c.version.desc())
                .label("rank"),
            )
            .where(versions.c.scope_key == scope_key, versions.c.document_uid.in_(crowded))
            .subquery()
        )
        uid = ranked.c.document_uid
        number = ranked.c.version
        documents = t.documents
        objects = t.objects
        aggregates = objects.alias("aggregates")
        contributions = t.object_contributions
        visibility = t.visibility
        coverage = t.coverage
        generations = t.generations
        current = sa.exists().where(
            documents.c.scope_key == scope_key,
            documents.c.document_uid == uid,
            documents.c.version == number,
        )
        live_object = sa.exists().where(
            objects.c.scope_key == scope_key,
            objects.c.document_uid == uid,
            objects.c.version == number,
            objects.c.state == ObjectState.LIVE.value,
        )
        live_contribution = sa.exists().where(
            contributions.c.scope_key == scope_key,
            contributions.c.document_uid == uid,
            contributions.c.version == number,
            aggregates.c.object_id == contributions.c.object_id,
            aggregates.c.state == ObjectState.LIVE.value,
        )
        open_window = sa.exists().where(
            visibility.c.scope_key == scope_key,
            visibility.c.document_uid == uid,
            visibility.c.version == number,
            visibility.c.visible_until_seq.is_(None),
        )
        indexed = sa.exists().where(
            coverage.c.scope_key == scope_key,
            coverage.c.document_uid == uid,
            coverage.c.indexed_version == number,
            generations.c.generation_id == coverage.c.generation_id,
            generations.c.state.in_([GenerationState.ACTIVE.value, GenerationState.RETIRING.value]),
        )
        rows = (
            await self._exec(
                sa.select(uid, number)
                .where(
                    ranked.c.rank > keep,
                    ~current,
                    ~live_object,
                    ~live_contribution,
                    ~open_window,
                    ~indexed,
                )
                .order_by(uid, number)
                .limit(limit)
            )
        ).all()
        for document_uid, version in rows:
            for table in (t.representations, versions):
                await self._delete(
                    table,
                    sa.and_(table.c.document_uid == document_uid, table.c.version == version),
                    scope_key,
                )
        return len(rows)

    async def prune_events(self, scope_key: str, *, before: datetime, limit: int = 1000) -> int:
        """Delete terminal events received before ``before``."""
        self._guard(scope_key)
        self._require_write()
        events = t.ingestion_events
        expired = (
            await self._exec(
                sa.select(events.c.event_id)
                .where(
                    events.c.scope_key == scope_key,
                    events.c.status.in_([EventStatus.COMMITTED.value, EventStatus.FAILED.value]),
                    events.c.received_at < before,
                )
                .order_by(events.c.received_at, events.c.event_id)
                .limit(limit)
            )
        ).scalars()
        ids = list(expired)
        for chunk in _chunks(ids):
            await self._delete(events, events.c.event_id.in_(chunk), scope_key)
        return len(ids)

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


def _chunks[T](values: Sequence[T], size: int = 500) -> list[Sequence[T]]:
    return [values[index : index + size] for index in range(0, len(values), size)]


def _document_values(record: DocumentRecord) -> dict[str, Any]:
    return {
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


def _object_values(item: DerivedObject) -> dict[str, Any]:
    return {
        "object_id": item.object_id,
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


def _event_values(event: IngestionEvent) -> dict[str, Any]:
    return {
        "scope_key": event.scope.key,
        "event_id": event.event_id,
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
