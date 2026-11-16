"""Structured exception hierarchy.

Every exception carries a stable ``code``, a ``retryable`` flag and safe
``details``. Messages never contain document content or secrets.
"""

from __future__ import annotations

from typing import Any, ClassVar

__all__ = [
    "AccessDeniedError",
    "BackendError",
    "BackendTimeoutError",
    "BackendUnavailableError",
    "ComponentContractError",
    "ConfigurationError",
    "CoverageError",
    "DocumentIdentityConflictError",
    "EmbeddingValidationError",
    "GenerationError",
    "IdentityError",
    "IngestionError",
    "LifecycleError",
    "MigrationError",
    "OperationCancelledError",
    "OperationTimeoutError",
    "ProvenanceError",
    "RecoveryError",
    "ResourceAdmissionError",
    "RetrievalError",
    "RetrievalLoomError",
    "TenantIsolationError",
    "ValidationError",
    "VersionConflictError",
]


class RetrievalLoomError(Exception):
    """Base class of every error raised by RetrievalLoom.

    Args:
        message: Safe, content-free description of the failure.
        code: Stable machine-readable code; defaults to the class default code.
        retryable: Whether retrying the operation may succeed; defaults to the class default.
        details: JSON-safe diagnostic details without document content or secrets.
    """

    default_code: ClassVar[str] = "retrievalloom_error"
    default_retryable: ClassVar[bool] = False
    category: ClassVar[str] = "internal"

    def __init__(
        self,
        message: str,
        *,
        code: str | None = None,
        retryable: bool | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.message = message
        self.code = code or self.default_code
        self.retryable = self.default_retryable if retryable is None else retryable
        self.details: dict[str, Any] = dict(details or {})

    def __repr__(self) -> str:
        return f"{type(self).__name__}(code={self.code!r}, message={self.message!r})"


class ConfigurationError(RetrievalLoomError):
    """Invalid or contradictory configuration."""

    default_code = "configuration_error"
    category = "configuration"


class ValidationError(RetrievalLoomError):
    """Invalid input. Pydantic errors are wrapped without echoing the input."""

    default_code = "validation_error"
    category = "validation"


class EmbeddingValidationError(ValidationError):
    """An embedding model returned output violating its contract."""

    default_code = "embedding_validation_error"


class ComponentContractError(ValidationError):
    """A pluggable component violated its ABC contract."""

    default_code = "component_contract_error"


class IdentityError(RetrievalLoomError):
    """Document identity or versioning failure."""

    default_code = "identity_error"
    category = "identity"


class DocumentIdentityConflictError(IdentityError):
    """Two inputs claim one identity inconsistently."""

    default_code = "document_identity_conflict"


class VersionConflictError(IdentityError):
    """An optimistic concurrency check was lost; retrying is safe."""

    default_code = "version_conflict"
    default_retryable = True


class IngestionError(RetrievalLoomError):
    """Ingestion failed."""

    default_code = "ingestion_error"
    category = "ingestion"


class RetrievalError(RetrievalLoomError):
    """Retrieval failed."""

    default_code = "retrieval_error"
    category = "retrieval"


class CoverageError(RetrievalError):
    """Required index coverage is missing and the policy is ``fail``."""

    default_code = "coverage_error"


class BackendError(RetrievalLoomError):
    """A retrieval or storage backend failed."""

    default_code = "backend_error"
    category = "backend"


class BackendUnavailableError(BackendError):
    """A backend is unreachable or not ready."""

    default_code = "backend_unavailable"
    default_retryable = True


class BackendTimeoutError(BackendError):
    """A backend call exceeded its deadline."""

    default_code = "backend_timeout"
    default_retryable = True


class LifecycleError(RetrievalLoomError):
    """An operation was used in the wrong lifecycle state or a lifecycle task failed."""

    default_code = "lifecycle_error"
    category = "lifecycle"


class GenerationError(LifecycleError):
    """Index generation build, validation or publication failed."""

    default_code = "generation_error"


class RecoveryError(LifecycleError):
    """Crash recovery or reconciliation could not repair a state."""

    default_code = "recovery_error"


class ResourceAdmissionError(RetrievalLoomError):
    """Work was rejected by resource admission control."""

    default_code = "resource_admission_rejected"
    default_retryable = True
    category = "resources"


class OperationTimeoutError(RetrievalLoomError):
    """An operation exceeded its deadline."""

    default_code = "operation_timeout"
    default_retryable = True
    category = "timeout"


class OperationCancelledError(RetrievalLoomError):
    """A durable lifecycle task was cancelled.

    ``asyncio.CancelledError`` always propagates unchanged; this error only
    describes persisted task outcomes.
    """

    default_code = "operation_cancelled"
    category = "cancellation"


class ProvenanceError(RetrievalLoomError):
    """Evidence provenance is incomplete or unresolvable."""

    default_code = "provenance_error"
    category = "provenance"


class MigrationError(RetrievalLoomError):
    """The metadata schema is not at the expected revision or a migration failed."""

    default_code = "migration_error"
    category = "storage"


class TenantIsolationError(RetrievalLoomError):
    """An object or relation crossed a scope boundary. Never retryable."""

    default_code = "tenant_isolation_violation"
    category = "isolation"


class AccessDeniedError(RetrievalLoomError):
    """The access policy denied the operation."""

    default_code = "access_denied"
    category = "authorization"
