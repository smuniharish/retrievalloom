"""Authoritative records: documents, versions, events, derived objects, lineage, lifecycle."""

from __future__ import annotations

from enum import StrEnum
from typing import Any, Literal

from pydantic import AwareDatetime, Field

from retrievalloom.domain.base import (
    Digest,
    EngineId,
    FrozenModel,
    Identifier,
    Metadata,
    NonNegativeInt,
    PositiveInt,
    VersionedModel,
)
from retrievalloom.domain.inputs import (
    DeliveryProvenance,
    LineRange,
    PageRange,
    Scope,
    SourceDescriptor,
    TextSpan,
    TransformationRef,
)
from retrievalloom.errors import RetrievalLoomError


class ErrorInfo(FrozenModel):
    """Persistable, content-free error description."""

    code: str
    category: str
    retryable: bool = False
    message: str
    details: Metadata = Field(default_factory=dict)

    @classmethod
    def from_exception(cls, exc: BaseException) -> ErrorInfo:
        """Build an ``ErrorInfo`` from any exception without leaking its arguments."""
        if isinstance(exc, RetrievalLoomError):
            return cls(
                code=exc.code,
                category=exc.category,
                retryable=exc.retryable,
                message=exc.message,
                details=_json_safe(exc.details),
            )
        return cls(
            code="unexpected_error",
            category="internal",
            retryable=False,
            message=type(exc).__name__,
        )


def _json_safe(details: dict[str, Any]) -> Metadata:
    safe: Metadata = {}
    for key, value in details.items():
        if value is None or isinstance(value, str | int | float | bool):
            safe[key] = value
        elif isinstance(value, list | tuple):
            safe[key] = [str(item) for item in value]
        else:
            safe[key] = str(value)
    return safe


class DocumentState(StrEnum):
    """Document lifecycle state."""

    ACTIVE = "active"
    DELETED = "deleted"


class IngestOutcome(StrEnum):
    """Outcome of one appearance."""

    CREATED = "created"
    UPDATED = "updated"
    UNCHANGED = "unchanged"
    REVIVED = "revived"
    REPLAYED = "replayed"
    SUSPECTED_REDELIVERY = "suspected_redelivery"
    REJECTED = "rejected"
    FAILED = "failed"


class EventStatus(StrEnum):
    """Ingestion event status."""

    PENDING = "pending"
    COMMITTED = "committed"
    FAILED = "failed"


class DocumentRecord(VersionedModel):
    """Authoritative document record owned by the metadata store."""

    document_id: Identifier = Field(alias="_id")
    counter: PositiveInt = Field(alias="_counter")
    version: PositiveInt = Field(alias="_version")
    content_hash: Digest = Field(alias="_content_hash")
    created_at: AwareDatetime = Field(alias="_created_at")
    updated_at: AwareDatetime = Field(alias="_updated_at")
    document_uid: EngineId
    scope: Scope
    state: DocumentState = DocumentState.ACTIVE
    deleted_at: AwareDatetime | None = None
    metadata: Metadata = Field(default_factory=dict)
    dataset: Identifier | None = None
    source: SourceDescriptor | None = None
    representation_digests: dict[str, Digest] = Field(default_factory=dict)
    representation_kinds: dict[str, str] = Field(default_factory=dict)
    hash_profile: str
    row_revision: NonNegativeInt = 0
    lifecycle_revision: NonNegativeInt = 0
    metadata_revision: NonNegativeInt = 0
    change_seq: NonNegativeInt = 0


class DocumentVersion(VersionedModel):
    """Append-only version history entry."""

    document_uid: EngineId
    version: PositiveInt
    content_hash: Digest
    hash_profile: str
    representation_digests: dict[str, Digest]
    metadata: Metadata
    dataset: Identifier | None = None
    source: SourceDescriptor | None = None
    created_at: AwareDatetime
    event_id: str


class RepresentationSnapshot(VersionedModel):
    """Stored canonical payload of one representation of one version."""

    document_uid: EngineId
    version: PositiveInt
    representation_id: Identifier
    kind: Literal["text", "chunks", "structure", "graph"]
    payload: dict[str, Any]
    transformation: TransformationRef | None = None
    hash_profile: str


class IngestionEvent(VersionedModel):
    """Append-only record of one appearance."""

    event_id: str
    scope: Scope
    document_id: Identifier
    document_uid: EngineId
    received_at: AwareDatetime
    status: EventStatus
    outcome: IngestOutcome | None = None
    counter_after: NonNegativeInt | None = None
    version_after: NonNegativeInt | None = None
    content_hash: Digest
    delivery: DeliveryProvenance | None = None
    appearance_fingerprint: Digest
    possible_redelivery_of: str | None = None
    staged_payload: dict[str, Any] | None = None
    error: ErrorInfo | None = None


class ObjectKind(StrEnum):
    """Kind of derived object."""

    CHUNK = "chunk"
    STRUCTURE_NODE = "structure_node"
    GRAPH_ENTITY = "graph_entity"
    GRAPH_RELATIONSHIP = "graph_relationship"


class ObjectState(StrEnum):
    """Lifecycle bookkeeping state of a derived object."""

    LIVE = "live"
    SUPERSEDED = "superseded"
    DELETED = "deleted"
    ORPHANED = "orphaned"


class SourceReference(FrozenModel):
    """Where a piece of evidence came from."""

    scope: Scope
    document_id: Identifier
    document_uid: EngineId
    document_version: PositiveInt
    representation_id: Identifier
    object_id: EngineId | None = None
    span: TextSpan | None = None
    pages: PageRange | None = None
    lines: LineRange | None = None
    section_path: tuple[str, ...] = ()
    source_uri: str | None = None
    title: str | None = None
    transformation: TransformationRef | None = None


class DerivedObject(VersionedModel):
    """Indexed unit derived from accepted representations.

    ``payload`` carries the kind-specific fields (``ordinal``, ``span``, ``entity_type``,
    ``source_entity_id`` ...); typed accessors live on the engine side.
    """

    object_id: EngineId
    scope: Scope
    kind: ObjectKind
    document_uid: EngineId | None = None
    version: PositiveInt | None = None
    representation_id: Identifier | None = None
    text: str | None = None
    sources: tuple[SourceReference, ...] = Field(min_length=1)
    transformation: TransformationRef | None = None
    metadata: Metadata = Field(default_factory=dict)
    payload: dict[str, Any] = Field(default_factory=dict)
    content_hash: Digest
    state: ObjectState = ObjectState.LIVE


class LineageRelation(StrEnum):
    """Lineage edge relation."""

    DERIVED_FROM = "derived_from"
    PART_OF = "part_of"
    CHILD_OF = "child_of"
    SUPPORTED_BY = "supported_by"
    SUPERSEDES = "supersedes"


class LineageEdge(FrozenModel):
    """Directed lineage edge between engine ids."""

    scope_key: str
    from_id: str
    to_id: str
    relation: LineageRelation


class ObjectContribution(FrozenModel):
    """Source-attributed contribution of one document version to a graph aggregate."""

    object_id: EngineId
    document_uid: EngineId
    version: PositiveInt
    field_path: str
    ordinal: NonNegativeInt = 0
    value: Any = None


class Channel(StrEnum):
    """Retrieval channel."""

    INDEX = "index"
    GRAPH = "graph"
    STRUCTURE = "structure"


class IndexMethod(StrEnum):
    """Index channel method."""

    KEYWORD = "keyword"
    FUZZY = "fuzzy"
    SPARSE = "sparse"
    DENSE = "dense"


class GenerationState(StrEnum):
    """Index generation state."""

    BUILDING = "building"
    VALIDATING = "validating"
    ACTIVE = "active"
    RETIRING = "retiring"
    RETIRED = "retired"
    FAILED = "failed"
    ABANDONED = "abandoned"


class CoverageState(StrEnum):
    """Per-document, per-method coverage state."""

    PENDING = "pending"
    PROCESSING = "processing"
    READY = "ready"
    STALE = "stale"
    FAILED = "failed"
    DELETED = "deleted"
    NOT_APPLICABLE = "not_applicable"


class TaskKind(StrEnum):
    """Durable lifecycle task kind."""

    INGEST_INDEX = "ingest_index"
    BACKFILL = "backfill"
    REBUILD = "rebuild"
    REEMBED = "reembed"
    REEXTRACT = "reextract"
    PROJECTION_REFRESH = "projection_refresh"
    PURGE = "purge"
    RECONCILE = "reconcile"


class TaskState(StrEnum):
    """Durable lifecycle task state."""

    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"


class IndexingMode(StrEnum):
    """How document changes reach backends."""

    LOCKED = "locked"
    INCREMENTAL = "incremental"
    REBUILD = "rebuild"


class MissingIndexPolicy(StrEnum):
    """What retrieval does when a requested method lacks coverage."""

    SKIP = "skip"
    FAIL = "fail"
    SCHEDULE_BACKFILL = "schedule_backfill"


class IndexGeneration(VersionedModel):
    """One build of one (scope, channel, method) index."""

    generation_id: EngineId
    scope_key: str
    channel: Channel
    method: str
    backend_id: str
    backend_instance_id: str | None = None
    dependency_fingerprint: Digest
    build_watermark_seq: NonNegativeInt = 0
    state: GenerationState
    owner_id: str | None = None
    lease_epoch: NonNegativeInt = 0
    heartbeat_at: AwareDatetime | None = None
    created_at: AwareDatetime
    published_at: AwareDatetime | None = None
    retired_at: AwareDatetime | None = None
    predecessor_id: str | None = None
    stats: Metadata = Field(default_factory=dict)
    row_revision: NonNegativeInt = 0


class CoverageRecord(VersionedModel):
    """Coverage of one document by one (channel, method)."""

    scope_key: str
    document_uid: EngineId
    channel: Channel
    method: str
    backend_instance_id: str = ""
    generation_id: str | None = None
    target_version: NonNegativeInt = 0
    indexed_version: NonNegativeInt | None = None
    indexed_metadata_revision: NonNegativeInt | None = None
    dependency_fingerprint: Digest | None = None
    state: CoverageState
    attempts: NonNegativeInt = 0
    last_error: ErrorInfo | None = None
    lease_owner: str | None = None
    lease_epoch: NonNegativeInt = 0
    lease_expires_at: AwareDatetime | None = None
    updated_at: AwareDatetime
    row_revision: NonNegativeInt = 0


class LifecycleTask(VersionedModel):
    """Durable outbox task."""

    task_id: str
    scope_key: str
    kind: TaskKind
    target: Metadata = Field(default_factory=dict)
    state: TaskState
    cursor: Metadata = Field(default_factory=dict)
    attempts: NonNegativeInt = 0
    owner_id: str | None = None
    lease_epoch: NonNegativeInt = 0
    heartbeat_at: AwareDatetime | None = None
    refresh_correlation_id: str | None = None
    created_at: AwareDatetime
    updated_at: AwareDatetime
    error: ErrorInfo | None = None
    row_revision: NonNegativeInt = 0
