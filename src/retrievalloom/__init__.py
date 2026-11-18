"""RetrievalLoom: async-first, provider-agnostic, multi-channel retrieval and evidence engine.

Extension contracts (ABCs) live in :mod:`retrievalloom.contracts`; default adapters live in
:mod:`retrievalloom.index`, :mod:`retrievalloom.evidence`, :mod:`retrievalloom.infra`,
:mod:`retrievalloom.graph` (``graph`` extra) and :mod:`retrievalloom.structure`
(``structure`` extra).
"""

from importlib.metadata import PackageNotFoundError, version
from typing import TYPE_CHECKING, Any

from retrievalloom.config import (
    ChannelsConfig,
    Components,
    ConsolidationConfig,
    EngineConfig,
    EngineLimits,
    EngineResources,
    FailurePolicy,
    FusionConfig,
    FuzzyConfig,
    GraphChannelConfig,
    IndexChannelConfig,
    IndexWait,
    IngestionConfig,
    IngestionLimits,
    LifecycleConfig,
    MethodConfig,
    NLIConfig,
    RerankingConfig,
    RetrievalConfig,
    StructureChannelConfig,
    TenantPolicy,
)
from retrievalloom.domain.filters import And, Eq, Exists, FilterExpression, In, Not, Or, Range
from retrievalloom.domain.inputs import (
    ChunkInput,
    ChunksRepresentation,
    DocumentInput,
    GraphEntityInput,
    GraphRelationshipInput,
    GraphRepresentation,
    Scope,
    SourceDescriptor,
    StructureNodeInput,
    StructureRepresentation,
    TextRepresentation,
    TextSpan,
)
from retrievalloom.domain.records import Channel, MissingIndexPolicy
from retrievalloom.domain.reports import (
    DeleteReport,
    HealthReport,
    HealthStatus,
    IngestReport,
    LifecycleTaskRef,
    LifecycleTaskResult,
    PurgeReport,
    ScopeStatus,
)
from retrievalloom.domain.retrieval import (
    Candidate,
    Evidence,
    ResultStatus,
    RetrievalExecutionConfig,
    RetrievalResult,
    RetrievalWarning,
    Severity,
    WarningCode,
)
from retrievalloom.engine import EngineState, RetrievalEngine, create_retrieval_engine
from retrievalloom.errors import (
    AccessDeniedError,
    BackendError,
    BackendTimeoutError,
    BackendUnavailableError,
    ComponentContractError,
    ConfigurationError,
    CoverageError,
    EmbeddingValidationError,
    IdentityError,
    IngestionError,
    LifecycleError,
    MigrationError,
    OperationCancelledError,
    OperationTimeoutError,
    ProvenanceError,
    ResourceAdmissionError,
    RetrievalError,
    RetrievalLoomError,
    TenantIsolationError,
    ValidationError,
)
from retrievalloom.runtime.concurrency import AutoConcurrency
from retrievalloom.runtime.offloader import OffloaderConfig

if TYPE_CHECKING:
    from retrievalloom.storage.postgres import PostgresMetadataStore

try:
    __version__ = version("retrievalloom")
except PackageNotFoundError:  # pragma: no cover - only when running from a raw checkout
    __version__ = "0.0.0"

__all__ = [
    "AccessDeniedError",
    "And",
    "AutoConcurrency",
    "BackendError",
    "BackendTimeoutError",
    "BackendUnavailableError",
    "Candidate",
    "Channel",
    "ChannelsConfig",
    "ChunkInput",
    "ChunksRepresentation",
    "ComponentContractError",
    "Components",
    "ConfigurationError",
    "ConsolidationConfig",
    "CoverageError",
    "DeleteReport",
    "DocumentInput",
    "EmbeddingValidationError",
    "EngineConfig",
    "EngineLimits",
    "EngineResources",
    "EngineState",
    "Eq",
    "Evidence",
    "Exists",
    "FailurePolicy",
    "FilterExpression",
    "FusionConfig",
    "FuzzyConfig",
    "GraphChannelConfig",
    "GraphEntityInput",
    "GraphRelationshipInput",
    "GraphRepresentation",
    "HealthReport",
    "HealthStatus",
    "IdentityError",
    "In",
    "IndexChannelConfig",
    "IndexWait",
    "IngestReport",
    "IngestionConfig",
    "IngestionError",
    "IngestionLimits",
    "LifecycleConfig",
    "LifecycleError",
    "LifecycleTaskRef",
    "LifecycleTaskResult",
    "MethodConfig",
    "MigrationError",
    "MissingIndexPolicy",
    "NLIConfig",
    "Not",
    "OffloaderConfig",
    "OperationCancelledError",
    "OperationTimeoutError",
    "Or",
    "PostgresMetadataStore",
    "ProvenanceError",
    "PurgeReport",
    "Range",
    "RerankingConfig",
    "ResourceAdmissionError",
    "ResultStatus",
    "RetrievalConfig",
    "RetrievalEngine",
    "RetrievalError",
    "RetrievalExecutionConfig",
    "RetrievalLoomError",
    "RetrievalResult",
    "RetrievalWarning",
    "Scope",
    "ScopeStatus",
    "Severity",
    "SourceDescriptor",
    "StructureChannelConfig",
    "StructureNodeInput",
    "StructureRepresentation",
    "TenantIsolationError",
    "TenantPolicy",
    "TextRepresentation",
    "TextSpan",
    "ValidationError",
    "WarningCode",
    "__version__",
    "create_retrieval_engine",
]


def __getattr__(name: str) -> Any:
    # Optional backends are imported on first use so ``import retrievalloom`` stays light.
    if name == "PostgresMetadataStore":
        from retrievalloom.storage.postgres import PostgresMetadataStore

        return PostgresMetadataStore
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
