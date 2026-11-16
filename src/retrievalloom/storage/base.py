"""Metadata store contracts.

The metadata store is the single authority for document identity, versions, events,
derived objects, lineage, coverage, visibility, generations, pointers, lifecycle tasks
and refresh-engine state. All lifecycle logic lives in the engine and talks to the
store only through these fine-grained transactional methods.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from enum import StrEnum
from typing import TYPE_CHECKING, Self

from retrievalloom.contracts.component import ComponentABC
from retrievalloom.domain.base import FrozenModel

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence
    from datetime import datetime
    from types import TracebackType

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


class MigrationPolicy(StrEnum):
    """How the store treats a schema that is not at head."""

    AUTO = "auto"
    VERIFY = "verify"


class UnitKey(FrozenModel):
    """Address of one index unit: ``(scope_key, channel, method, instance)``.

    ``instance`` is the engine-local backend instance id for ephemeral backends and
    ``""`` for durable shared backends.
    """

    scope_key: str
    channel: str
    method: str
    instance: str = ""


class PointerRow(FrozenModel):
    """Generation pointer of one unit."""

    unit: UnitKey
    generation_id: str
    row_revision: int


class StoredRefreshState(FrozenModel):
    """Serialised refresh-engine resource state."""

    resource_id: str
    state: dict[str, object]


class MetadataTransactionABC(ABC):
    """One metadata transaction.

    Write transactions are serialisable. Every compare-and-set method raises
    ``VersionConflictError`` when the expected revision does not match. When the
    transaction is bound to a scope, writes of another scope raise
    ``TenantIsolationError``.
    """

    async def __aenter__(self) -> Self:
        return self

    @abstractmethod
    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Commit on success, roll back otherwise."""

    # scopes -----------------------------------------------------------------------
    @abstractmethod
    async def ensure_scope(self, scope: Scope) -> None:
        """Register ``scope`` (idempotent)."""

    @abstractmethod
    async def list_scopes(self) -> Sequence[Scope]:
        """Return every registered scope."""

    # commit sequence --------------------------------------------------------------
    @abstractmethod
    async def next_commit_seq(self) -> int:
        """Increment and return the monotonic commit sequence."""

    @abstractmethod
    async def current_commit_seq(self) -> int:
        """Return the current commit sequence."""

    # documents --------------------------------------------------------------------
    @abstractmethod
    async def get_document(self, scope: Scope, document_id: str) -> DocumentRecord | None:
        """Load a document by key."""

    @abstractmethod
    async def get_documents_by_uid(
        self, scope_key: str, document_uids: Sequence[str]
    ) -> Mapping[str, DocumentRecord]:
        """Load documents by uid."""

    @abstractmethod
    async def put_document(self, record: DocumentRecord, *, expected_revision: int | None) -> None:
        """Insert (``expected_revision=None``) or CAS-update a document."""

    @abstractmethod
    async def list_documents(
        self,
        scope_key: str,
        *,
        state: DocumentState | None = None,
        after_id: str | None = None,
        limit: int = 1000,
    ) -> Sequence[DocumentRecord]:
        """Page through documents ordered by ``document_id``."""

    @abstractmethod
    async def count_documents(self, scope_key: str) -> Mapping[str, int]:
        """Count documents per state."""

    # versions and representations -------------------------------------------------
    @abstractmethod
    async def put_version(self, scope_key: str, version: DocumentVersion) -> None:
        """Append a version history entry."""

    @abstractmethod
    async def get_versions(self, document_uid: str) -> Sequence[DocumentVersion]:
        """Return the version history of a document."""

    @abstractmethod
    async def put_representations(
        self, scope_key: str, snapshots: Sequence[RepresentationSnapshot]
    ) -> None:
        """Store canonical representation payloads of one version."""

    @abstractmethod
    async def get_representations(
        self, document_uid: str, version: int
    ) -> Sequence[RepresentationSnapshot]:
        """Return the representation payloads of one version."""

    # events -----------------------------------------------------------------------
    @abstractmethod
    async def get_event(self, scope_key: str, event_id: str) -> IngestionEvent | None:
        """Load an ingestion event."""

    @abstractmethod
    async def put_event(self, event: IngestionEvent) -> None:
        """Insert or replace an ingestion event."""

    @abstractmethod
    async def find_event_by_fingerprint(
        self, scope_key: str, fingerprint: str, *, since: datetime, exclude_event_id: str
    ) -> IngestionEvent | None:
        """Most recent committed event with the same appearance fingerprint since ``since``."""

    @abstractmethod
    async def list_pending_events(self, *, limit: int = 100) -> Sequence[IngestionEvent]:
        """Events stuck in ``pending`` (all scopes)."""

    # derived objects, contributions and lineage -----------------------------------
    @abstractmethod
    async def put_objects(self, objects: Sequence[DerivedObject]) -> None:
        """Insert or replace derived objects."""

    @abstractmethod
    async def get_objects(
        self, scope_key: str, object_ids: Sequence[str]
    ) -> Mapping[str, DerivedObject]:
        """Load objects by id (missing ids are absent from the result)."""

    @abstractmethod
    async def list_document_objects(
        self,
        scope_key: str,
        document_uid: str,
        version: int | None = None,
        kinds: Sequence[ObjectKind] | None = None,
    ) -> Sequence[DerivedObject]:
        """Objects of one document (optionally one version and kinds)."""

    @abstractmethod
    async def list_current_objects(
        self,
        scope_key: str,
        kinds: Sequence[ObjectKind],
        *,
        after_object_id: str | None = None,
        limit: int = 1000,
    ) -> Sequence[DerivedObject]:
        """Live objects of the current version of active documents (paged by object id)."""

    @abstractmethod
    async def set_document_objects_state(
        self,
        scope_key: str,
        document_uid: str,
        state: ObjectState,
        *,
        version: int | None = None,
        exclude_version: int | None = None,
    ) -> int:
        """Change the state of a document's objects."""

    @abstractmethod
    async def replace_contributions(
        self,
        scope_key: str,
        document_uid: str,
        contributions: Sequence[ObjectContribution],
    ) -> Sequence[str]:
        """Replace every contribution of ``document_uid``; return affected object ids."""

    @abstractmethod
    async def get_contributions(
        self, scope_key: str, object_ids: Sequence[str]
    ) -> Sequence[ObjectContribution]:
        """Contributions of the given aggregate objects."""

    @abstractmethod
    async def list_document_contributions(
        self, scope_key: str, document_uid: str
    ) -> Sequence[ObjectContribution]:
        """Every contribution made by one document (all versions)."""

    @abstractmethod
    async def put_lineage(self, edges: Sequence[LineageEdge]) -> None:
        """Insert lineage edges (idempotent)."""

    @abstractmethod
    async def get_lineage(self, scope_key: str, object_id: str) -> Sequence[LineageEdge]:
        """Edges leaving or entering ``object_id``."""

    # visibility -------------------------------------------------------------------
    @abstractmethod
    async def open_visibility(
        self, unit: UnitKey, document_uid: str, version: int, seq: int
    ) -> None:
        """Open the window of ``version`` and close other open windows of the document."""

    @abstractmethod
    async def close_visibility(self, scope_key: str, document_uid: str, seq: int) -> None:
        """Close every open window of a document (all units)."""

    @abstractmethod
    async def visible_versions(
        self, unit: UnitKey, document_uids: Sequence[str], seq: int
    ) -> Mapping[str, frozenset[int]]:
        """Versions of each document visible to ``unit`` at ``seq``."""

    # coverage ---------------------------------------------------------------------
    @abstractmethod
    async def upsert_coverage(self, records: Sequence[CoverageRecord]) -> None:
        """Insert or replace coverage rows."""

    @abstractmethod
    async def get_coverage(
        self, unit: UnitKey, document_uids: Sequence[str]
    ) -> Mapping[str, CoverageRecord]:
        """Coverage rows of a unit for the given documents."""

    @abstractmethod
    async def list_coverage(
        self,
        unit: UnitKey,
        states: Sequence[CoverageState],
        *,
        limit: int = 1000,
    ) -> Sequence[CoverageRecord]:
        """Coverage rows of a unit in the given states (ordered by document uid)."""

    @abstractmethod
    async def documents_missing_coverage(
        self, unit: UnitKey, *, limit: int = 1000
    ) -> Sequence[DocumentRecord]:
        """Active documents without a coverage row for ``unit``."""

    @abstractmethod
    async def coverage_counts(
        self, scope_key: str, instances: Sequence[str]
    ) -> Mapping[str, Mapping[str, int]]:
        """Counts per ``channel.method`` and state for the given instances."""

    @abstractmethod
    async def set_document_coverage_state(
        self, scope_key: str, document_uid: str, state: CoverageState, *, target_version: int
    ) -> None:
        """Set every coverage row of a document to ``state`` with ``target_version``."""

    # generations and pointers -----------------------------------------------------
    @abstractmethod
    async def put_generation(
        self, generation: IndexGeneration, *, expected_revision: int | None
    ) -> None:
        """Insert or CAS-update a generation."""

    @abstractmethod
    async def get_generation(self, generation_id: str) -> IndexGeneration | None:
        """Load a generation."""

    @abstractmethod
    async def list_generations(
        self,
        *,
        scope_key: str | None = None,
        states: Sequence[GenerationState] | None = None,
        instances: Sequence[str | None] | None = None,
    ) -> Sequence[IndexGeneration]:
        """List generations."""

    @abstractmethod
    async def get_pointer(self, unit: UnitKey) -> PointerRow | None:
        """Active generation pointer of a unit."""

    @abstractmethod
    async def set_pointer(
        self, unit: UnitKey, generation_id: str, *, expected_revision: int | None
    ) -> None:
        """Publish ``generation_id`` (pointer CAS)."""

    @abstractmethod
    async def list_pointers(self, scope_key: str, instances: Sequence[str]) -> Sequence[PointerRow]:
        """Pointers of a scope for the given instances."""

    # tasks ------------------------------------------------------------------------
    @abstractmethod
    async def put_task(self, task: LifecycleTask, *, expected_revision: int | None) -> None:
        """Insert or CAS-update a task."""

    @abstractmethod
    async def get_task(self, task_id: str) -> LifecycleTask | None:
        """Load a task."""

    @abstractmethod
    async def list_tasks(
        self,
        *,
        scope_key: str | None = None,
        states: Sequence[TaskState] | None = None,
        limit: int = 1000,
    ) -> Sequence[LifecycleTask]:
        """List tasks ordered by creation time."""

    # reader leases and engine instances -------------------------------------------
    @abstractmethod
    async def put_reader_lease(
        self, generation_id: str, engine_id: str, *, expires_at: datetime, now: datetime
    ) -> None:
        """Acquire or extend a reader lease."""

    @abstractmethod
    async def delete_reader_lease(self, generation_id: str, engine_id: str) -> None:
        """Release a reader lease."""

    @abstractmethod
    async def count_reader_leases(self, generation_id: str, *, now: datetime) -> int:
        """Number of unexpired leases on a generation."""

    @abstractmethod
    async def purge_expired_leases(self, *, now: datetime) -> int:
        """Delete expired reader leases."""

    @abstractmethod
    async def heartbeat_instance(
        self, instance_id: str, engine_id: str, *, started_at: datetime, expires_at: datetime
    ) -> None:
        """Register or extend an engine instance."""

    @abstractmethod
    async def expired_instances(self, *, now: datetime) -> Sequence[str]:
        """Instances whose registration expired."""

    @abstractmethod
    async def purge_instance(self, instance_id: str) -> None:
        """Delete every row owned by an engine instance.

        Covers its coverage, visibility, pointers and generations of non-durable units,
        its reader leases and its registration.
        """

    # refresh-engine state ---------------------------------------------------------
    @abstractmethod
    async def load_refresh_states(self, namespace: str) -> Sequence[StoredRefreshState]:
        """Load refresh-engine state."""

    @abstractmethod
    async def apply_refresh_states(
        self,
        namespace: str,
        puts: Sequence[StoredRefreshState],
        deletes: Sequence[str],
    ) -> None:
        """Persist refresh-engine state changes."""


class DocumentMetadataStoreABC(ComponentABC):
    """Authoritative metadata store."""

    @abstractmethod
    async def migrate(self, policy: MigrationPolicy = MigrationPolicy.AUTO) -> None:
        """Bring the schema to head (``auto``) or verify it is at head (``verify``)."""

    @abstractmethod
    async def schema_version(self) -> str | None:
        """Current schema revision."""

    @abstractmethod
    def transaction(self, scope: Scope | None = None, *, write: bool) -> MetadataTransactionABC:
        """Open a transaction (use as ``async with``)."""
