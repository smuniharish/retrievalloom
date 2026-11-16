"""Operation reports: ingestion, deletion, lifecycle tasks, health and status."""

from __future__ import annotations

from enum import StrEnum

from pydantic import Field

from retrievalloom.domain.base import FrozenModel, Identifier, Metadata, VersionedModel
from retrievalloom.domain.inputs import Scope
from retrievalloom.domain.records import (
    CoverageState,
    ErrorInfo,
    GenerationState,
    IngestOutcome,
    TaskKind,
    TaskState,
)
from retrievalloom.domain.retrieval import RetrievalWarning


class DocumentIngestResult(FrozenModel):
    """Outcome of one document appearance."""

    document_id: str
    event_id: str | None = None
    outcome: IngestOutcome
    counter: int | None = None
    version: int | None = None
    possible_redelivery_of: str | None = None
    coverage: dict[str, CoverageState] = Field(default_factory=dict)
    error: ErrorInfo | None = None


class IngestReport(VersionedModel):
    """Result of one ``ingest`` call."""

    scope: Scope
    documents: tuple[DocumentIngestResult, ...]
    warnings: tuple[RetrievalWarning, ...] = ()
    task_ids: tuple[str, ...] = ()

    @property
    def outcomes(self) -> dict[IngestOutcome, int]:
        """Count of documents per outcome."""
        counts: dict[IngestOutcome, int] = {}
        for document in self.documents:
            counts[document.outcome] = counts.get(document.outcome, 0) + 1
        return counts

    @property
    def failed(self) -> tuple[DocumentIngestResult, ...]:
        """Documents that were rejected or failed."""
        return tuple(
            document
            for document in self.documents
            if document.outcome in {IngestOutcome.FAILED, IngestOutcome.REJECTED}
        )


class DeleteReport(VersionedModel):
    """Result of one ``delete`` call."""

    scope: Scope
    deleted: tuple[Identifier, ...] = ()
    already_deleted: tuple[Identifier, ...] = ()
    not_found: tuple[Identifier, ...] = ()
    task_ids: tuple[str, ...] = ()


class LifecycleTaskRef(FrozenModel):
    """Handle to a durable lifecycle task."""

    task_id: str
    scope: Scope
    kind: TaskKind


class LifecycleTaskResult(FrozenModel):
    """Final or current state of a lifecycle task."""

    task_id: str
    kind: TaskKind
    state: TaskState
    attempts: int = 0
    error: ErrorInfo | None = None
    details: Metadata = Field(default_factory=dict)


class HealthStatus(StrEnum):
    """Health status."""

    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"


class HealthReport(FrozenModel):
    """Health of one component or of the whole engine."""

    component: str
    status: HealthStatus = HealthStatus.HEALTHY
    message: str | None = None
    details: Metadata = Field(default_factory=dict)
    components: tuple[HealthReport, ...] = ()


class GenerationStatus(FrozenModel):
    """One generation of one method and its state (active, building, retired, ...)."""

    method: str
    generation_id: str
    state: GenerationState
    backend_id: str


class ScopeStatus(VersionedModel):
    """Lifecycle status of one scope."""

    scope: Scope
    documents_active: int = 0
    documents_deleted: int = 0
    coverage: dict[str, dict[str, int]] = Field(default_factory=dict)
    generations: tuple[GenerationStatus, ...] = ()
    tasks: dict[str, int] = Field(default_factory=dict)
