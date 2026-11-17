"""Metadata store contracts.

The metadata store is the single authority for document identity, versions, events,
derived objects, lineage, coverage, visibility, generations, pointers, lifecycle tasks
and refresh-engine state. All lifecycle logic lives in the engine and talks to the
store only through these fine-grained transactional methods.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime
from enum import StrEnum
from typing import TYPE_CHECKING, Self

from retrievalloom.contracts.component import ComponentABC
from retrievalloom.domain.base import FrozenModel, utc_now

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence
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


class KeywordDictionaryState(FrozenModel):
    """A metadata keyword dictionary maintained by the store for one scope.

    ``fields`` are the metadata fields whose string values are terms. ``revision``
    increases whenever a term appears in or disappears from the dictionary.
    """

    dictionary_id: str
    fields: tuple[str, ...]
    revision: int


class PurgeLogEntry(FrozenModel):
    """One physically erased document, for engines evicting it from in-memory indexes.

    ``seq`` is dense and increasing per scope in commit order.
    """

    scope_key: str
    seq: int
    document_uid: str
    object_ids: tuple[str, ...]
    purged_at: datetime


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

    async def snapshot_marker(self, scope_key: str) -> tuple[int, str | None]:
        """The current commit sequence and the snapshot token of ``scope_key``.

        The token must change whenever a committed write changes the scope's documents,
        coverage, generations or pointers, so that two reads returning the same token
        see the same document counts, coverage counts, pointers and generations; readers
        may then reuse what they read before. ``None`` (the default) disables that reuse.
        """
        del scope_key
        return await self.current_commit_seq(), None

    # documents --------------------------------------------------------------------
    @abstractmethod
    async def get_document(self, scope: Scope, document_id: str) -> DocumentRecord | None:
        """Load a document by key."""

    @abstractmethod
    async def get_documents_by_uid(
        self, scope_key: str, document_uids: Sequence[str]
    ) -> Mapping[str, DocumentRecord]:
        """Load documents by uid."""

    async def get_change_seqs(
        self, scope_key: str, document_uids: Sequence[str]
    ) -> Mapping[str, int]:
        """``change_seq`` of the documents in ``document_uids`` that exist (a narrow load)."""
        documents = await self.get_documents_by_uid(scope_key, document_uids)
        return {uid: document.change_seq for uid, document in documents.items()}

    @abstractmethod
    async def get_documents(
        self, scope: Scope, document_ids: Sequence[str]
    ) -> Mapping[str, DocumentRecord]:
        """Load documents by key (missing ids are absent from the result)."""

    @abstractmethod
    async def put_document(self, record: DocumentRecord, *, expected_revision: int | None) -> None:
        """Insert (``expected_revision=None``) or CAS-update a document."""

    @abstractmethod
    async def put_documents(self, entries: Sequence[tuple[DocumentRecord, int | None]]) -> None:
        """``put_document`` for many ``(record, expected_revision)`` pairs, all or nothing.

        Raises ``VersionConflictError`` when any insert or compare-and-set fails; the
        caller's transaction must then be rolled back.
        """

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
    async def list_document_metadata(
        self,
        scope_key: str,
        *,
        state: DocumentState | None = None,
        after_id: str | None = None,
        limit: int = 1000,
    ) -> Sequence[tuple[str, Mapping[str, object]]]:
        """Page through ``(document_id, metadata)`` pairs ordered by ``document_id``.

        A lighter ``list_documents`` for callers that only read metadata.
        """

    @abstractmethod
    async def list_changed_documents(
        self, scope_key: str, *, after_seq: int, limit: int = 1000
    ) -> Sequence[DocumentRecord]:
        """Documents (any state) whose ``change_seq`` exceeds ``after_seq``.

        Ordered by ``(change_seq, document_uid)``. A page never splits the documents
        sharing a ``change_seq``: it ends with every document of its last
        ``change_seq``, so it may hold more than ``limit`` documents.
        """

    @abstractmethod
    async def count_documents(self, scope_key: str) -> Mapping[str, int]:
        """Count documents per state."""

    # versions and representations -------------------------------------------------
    @abstractmethod
    async def put_version(self, scope_key: str, version: DocumentVersion) -> None:
        """Append a version history entry."""

    @abstractmethod
    async def put_versions(self, scope_key: str, versions: Sequence[DocumentVersion]) -> None:
        """Append many version history entries."""

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
    async def get_events(
        self, scope_key: str, event_ids: Sequence[str]
    ) -> Mapping[str, IngestionEvent]:
        """Load ingestion events by id (missing ids are absent from the result)."""

    @abstractmethod
    async def put_event(self, event: IngestionEvent) -> None:
        """Insert or replace an ingestion event."""

    @abstractmethod
    async def put_events(self, events: Sequence[IngestionEvent]) -> None:
        """Insert or replace many ingestion events (the last of duplicate ids wins)."""

    @abstractmethod
    async def find_event_by_fingerprint(
        self, scope_key: str, fingerprint: str, *, since: datetime, exclude_event_id: str
    ) -> IngestionEvent | None:
        """Most recent committed event with the same appearance fingerprint since ``since``."""

    @abstractmethod
    async def find_events_by_fingerprints(
        self, scope_key: str, probes: Mapping[str, str], *, since: datetime
    ) -> Mapping[str, IngestionEvent]:
        """``find_event_by_fingerprint`` for many fingerprints at once.

        ``probes`` maps each appearance fingerprint to the event id it excludes. The
        result maps each fingerprint with a match to its most recent committed event.
        """

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
    async def list_objects_for_documents(
        self,
        scope_key: str,
        documents: Sequence[tuple[str, int | None]],
        kinds: Sequence[ObjectKind] | None = None,
    ) -> Mapping[str, Sequence[DerivedObject]]:
        """``list_document_objects`` for many ``(document_uid, version or None)`` pairs.

        The result has one entry per requested uid, its objects ordered by object id
        (an empty sequence when it has none); a uid requested twice gets the union.
        """

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

    async def set_documents_objects_state(
        self, scope_key: str, targets: Mapping[str, int | None], state: ObjectState
    ) -> None:
        """``set_document_objects_state`` for many ``document_uid -> exclude_version``.

        ``None`` changes every object of the document. The default calls the singular
        method once per document; stores should override it with a batched form.
        """
        for document_uid, exclude_version in targets.items():
            await self.set_document_objects_state(
                scope_key, document_uid, state, exclude_version=exclude_version
            )

    @abstractmethod
    async def replace_contributions(
        self,
        scope_key: str,
        document_uid: str,
        contributions: Sequence[ObjectContribution],
    ) -> Sequence[str]:
        """Replace every contribution of ``document_uid``; return affected object ids."""

    async def retract_contributions(
        self, scope_key: str, document_uids: Sequence[str]
    ) -> Sequence[str]:
        """Delete every contribution of the documents; return the affected object ids (sorted).

        Equivalent to ``replace_contributions(scope_key, uid, [])`` for each document.
        """
        affected: set[str] = set()
        for document_uid in dict.fromkeys(document_uids):
            affected.update(await self.replace_contributions(scope_key, document_uid, []))
        return sorted(affected)

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

    async def get_lineage_many(
        self, scope_key: str, object_ids: Sequence[str]
    ) -> Sequence[LineageEdge]:
        """Edges leaving or entering any of ``object_ids``, each edge once.

        Ordered by ``(from_id, to_id, relation)``.
        """
        edges: dict[tuple[str, str, str], LineageEdge] = {}
        for object_id in dict.fromkeys(object_ids):
            for edge in await self.get_lineage(scope_key, object_id):
                edges[(edge.from_id, edge.to_id, edge.relation.value)] = edge
        return [edges[key] for key in sorted(edges)]

    # visibility -------------------------------------------------------------------
    @abstractmethod
    async def open_visibility(
        self, unit: UnitKey, document_uid: str, version: int, seq: int
    ) -> None:
        """Open the window of ``version`` and close other open windows of the document."""

    @abstractmethod
    async def open_visibility_many(
        self, unit: UnitKey, documents: Sequence[tuple[str, int]], seq: int
    ) -> None:
        """``open_visibility`` for many ``(document_uid, version)`` pairs at ``seq``.

        Equivalent to opening them one by one in order (the last version of a uid listed
        twice wins).
        """

    @abstractmethod
    async def close_visibility(self, scope_key: str, document_uid: str, seq: int) -> None:
        """Close every open window of a document (all units)."""

    async def close_visibility_many(
        self, scope_key: str, document_uids: Sequence[str], seq: int
    ) -> None:
        """``close_visibility`` for many documents."""
        for document_uid in dict.fromkeys(document_uids):
            await self.close_visibility(scope_key, document_uid, seq)

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
        after_document_uid: str | None = None,
    ) -> Sequence[CoverageRecord]:
        """Coverage rows of a unit in the given states (ordered by document uid).

        ``after_document_uid`` pages: only rows with a greater document uid are returned.
        """

    @abstractmethod
    async def get_coverage_watermark(self, unit: UnitKey) -> int | None:
        """The ``change_seq`` up to which ``unit`` has coverage rows; ``None`` before any."""

    @abstractmethod
    async def set_coverage_watermark(self, unit: UnitKey, change_seq: int) -> None:
        """Record that every document with ``change_seq`` up to this has a coverage row."""

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

    @abstractmethod
    async def set_documents_coverage_state(
        self, scope_key: str, targets: Mapping[str, int], state: CoverageState
    ) -> None:
        """``set_document_coverage_state`` for many ``document_uid -> target_version``."""

    # keyword dictionaries ---------------------------------------------------------
    @abstractmethod
    async def list_keyword_dictionaries(self, scope_key: str) -> Sequence[KeywordDictionaryState]:
        """Keyword dictionaries maintained in ``scope_key``."""

    @abstractmethod
    async def create_keyword_dictionary(
        self,
        scope_key: str,
        dictionary_id: str,
        fields: Sequence[str],
        terms: Mapping[str, int],
    ) -> None:
        """Register a dictionary at revision 1 with its ``term -> document count`` rows.

        Raises ``VersionConflictError`` when the dictionary already exists.
        """

    @abstractmethod
    async def adjust_keyword_terms(
        self, scope_key: str, dictionary_id: str, deltas: Mapping[str, int]
    ) -> None:
        """Add ``deltas`` to the document counts of terms.

        A term whose count drops to zero or below is removed. The revision increases
        when a term is added or removed. Unknown dictionaries are ignored.
        """

    @abstractmethod
    async def list_keyword_terms(self, scope_key: str, dictionary_id: str) -> Sequence[str]:
        """Terms of a dictionary, sorted."""

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

        Covers its coverage, coverage watermarks, visibility, pointers and generations of
        non-durable units, its reader leases and its registration.
        """

    # erasure and retention --------------------------------------------------------
    @abstractmethod
    async def list_purgeable_documents(
        self,
        scope_key: str,
        *,
        deleted_before: datetime,
        limit: int = 1000,
        methods: Sequence[tuple[str, str]] | None = None,
    ) -> Sequence[DocumentRecord]:
        """Tombstoned documents ready for physical erasure (ordered by document uid).

        A document qualifies when it was deleted strictly before ``deleted_before`` and
        every coverage row it has (any unit, any instance) is ``deleted``. With
        ``methods``, only coverage rows of those ``(channel, method)`` pairs count, so
        rows left behind by a unit that is no longer configured do not block erasure.
        """

    async def purge_document(
        self, scope_key: str, document_uid: str, *, object_ids: Sequence[str] = ()
    ) -> int:
        """Physically delete every metadata row of one tombstoned document.

        Same as :meth:`purge_documents` with one document, logged at the current time.
        """
        return await self.purge_documents(
            scope_key, {document_uid: tuple(object_ids)}, purged_at=utc_now()
        )

    @abstractmethod
    async def purge_documents(
        self,
        scope_key: str,
        documents: Mapping[str, Sequence[str]],
        *,
        purged_at: datetime,
    ) -> int:
        """Physically delete every metadata row of tombstoned documents.

        ``documents`` maps each document uid to further objects of the scope to delete
        with it and their lineage (graph aggregates the document supported). For each
        document this removes the document, its versions, representations, ingestion
        events, objects, contributions, visibility windows and coverage rows, plus
        lineage edges touching the document or any removed object. Every document that
        still had its document row gets a purge-log entry (with the ids of the objects
        removed for it) in the same transaction. Idempotent; returns the number of rows
        deleted, purge-log entries excluded.

        Raises:
            LifecycleError: A document exists and is not deleted (nothing is deleted).
        """

    @abstractmethod
    async def purge_log_heads(self) -> Mapping[str, int]:
        """Newest purge-log sequence per scope that has purge-log entries."""

    @abstractmethod
    async def list_purge_log(
        self, scope_key: str, *, after_seq: int, limit: int = 1000
    ) -> Sequence[PurgeLogEntry]:
        """Purge-log entries of a scope with ``seq > after_seq``, in sequence order."""

    @abstractmethod
    async def prune_purge_log(self, scope_key: str, *, before: datetime, limit: int = 1000) -> int:
        """Delete up to ``limit`` purge-log entries logged before ``before``.

        The newest entry of the scope is always kept, so sequences never restart and a
        reader can tell that entries it has not seen were pruned.
        """

    @abstractmethod
    async def prune_versions(self, scope_key: str, *, keep: int, limit: int = 1000) -> int:
        """Delete old version history entries and their representations.

        For each document only the ``keep`` newest versions are kept. A version is never
        pruned when it is the document's current version, has a ``live`` derived object
        or a contribution to a ``live`` aggregate, has an open visibility window, is
        the indexed version of a coverage row of an ``active`` or ``retiring``
        generation, or is the ``version_after`` of a retained ingestion event of the
        document. Deletes at most ``limit`` versions; returns how many were deleted.
        """

    @abstractmethod
    async def prune_events(self, scope_key: str, *, before: datetime, limit: int = 1000) -> int:
        """Delete up to ``limit`` committed or failed events received before ``before``."""

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
