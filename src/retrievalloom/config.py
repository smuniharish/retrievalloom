"""Engine configuration (frozen Pydantic models) and component/resource wiring."""

from __future__ import annotations

import warnings
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal

from pydantic import AliasChoices, Field, model_validator

from retrievalloom.domain.base import FrozenModel, Identifier
from retrievalloom.domain.hashing import HASH_PROFILE, SUPPORTED_HASH_PROFILES
from retrievalloom.domain.records import Channel, IndexMethod, MissingIndexPolicy
from retrievalloom.runtime.concurrency import AutoConcurrency, Concurrency
from retrievalloom.runtime.offloader import DEFAULT_MAX_WORKERS, OffloaderConfig

if TYPE_CHECKING:
    from concurrent.futures import Executor

    from resourcera import ResourceManager

    from retrievalloom.contracts.evidence import (
        DeduplicatorABC,
        EvidenceConsolidatorABC,
        FusionABC,
    )
    from retrievalloom.contracts.indexes import (
        FuzzyIndexABC,
        GraphRetrieverABC,
        GraphStoreABC,
        KeywordIndexABC,
        SparseIndexABC,
        StructureEngineABC,
        VectorStoreABC,
    )
    from retrievalloom.contracts.infra import AccessPolicyABC, CacheABC, ProvenanceRecorderABC
    from retrievalloom.contracts.models import (
        NLIABC,
        EmbeddingModelABC,
        GraphExtractorABC,
        GraphNormalizerABC,
        KeywordDictionaryABC,
        RerankerABC,
        TokenizerABC,
    )
    from retrievalloom.storage.base import DocumentMetadataStoreABC

FusionMethod = Literal["rrf", "weighted_rrf", "weighted_score", "combsum", "combmnz"]
NormalizerName = Literal["minmax", "zscore", "rank", "sigmoid"]
OnError = Literal["degrade", "fail"]


class FailurePolicy(StrEnum):
    """How one invalid document affects its batch."""

    PER_DOCUMENT = "per_document"
    FAIL_BATCH = "fail_batch"


class IndexWait(StrEnum):
    """What ``ingest`` waits for after the allocation commit.

    ``none`` returns immediately and lets background lifecycle index the documents;
    ``all`` runs one lifecycle round for the scope and reports the coverage reached.
    """

    NONE = "none"
    ALL = "all"


class EngineLimits(FrozenModel):
    """Upper bounds the configuration is validated against.

    The defaults protect a typical deployment from settings that would exhaust memory,
    threads or a backend. Raise any of them when your hardware and backends allow
    more; every field accepts any positive integer.

    Attributes:
        max_candidate_k: Highest ``candidate_k`` of a retrieval method.
        max_rerank_k: Highest ``reranking.rerank_k``.
        max_nli_k: Highest ``nli.nli_k``.
        max_fuzzy_ngram: Highest fuzzy ``max_ngram``.
        max_fuzzy_matches_per_ngram: Highest fuzzy ``matches_per_ngram``.
        max_graph_depth: Highest graph traversal ``depth`` (a graph store may
            support less; ``initialize`` checks it).
        max_ingestion_concurrency: Highest ingestion concurrency.
        max_lifecycle_batch: Highest lifecycle ``batch_size``.
        max_lifecycle_concurrency: Highest lifecycle concurrency.
        max_lifecycle_attempts: Highest lifecycle ``max_attempts``.
        max_offloader_workers: Highest offloader ``max_workers``.
        max_claims: Most claims one retrieval request may carry.
        max_window_multiplier: Highest consolidation ``window_multiplier``.
    """

    max_candidate_k: int = Field(default=10_000, ge=1)
    max_rerank_k: int = Field(default=1000, ge=1)
    max_nli_k: int = Field(default=200, ge=1)
    max_fuzzy_ngram: int = Field(default=6, ge=1)
    max_fuzzy_matches_per_ngram: int = Field(default=100, ge=1)
    max_graph_depth: int = Field(default=4, ge=1)
    max_ingestion_concurrency: int = Field(default=256, ge=1)
    max_lifecycle_batch: int = Field(default=10_000, ge=1)
    max_lifecycle_concurrency: int = Field(default=64, ge=1)
    max_lifecycle_attempts: int = Field(default=20, ge=1)
    max_offloader_workers: int = Field(default=DEFAULT_MAX_WORKERS, ge=1)
    max_claims: int = Field(default=32, ge=1)
    max_window_multiplier: int = Field(default=20, ge=1)


class IngestionLimits(FrozenModel):
    """Input size limits."""

    max_documents_per_call: int = Field(default=1000, ge=1)
    max_representations: int = Field(default=16, ge=1)
    max_chunks: int = Field(default=10_000, ge=1)
    max_text_chars: int = Field(default=10_000_000, ge=1)
    max_tree_depth: int = Field(default=64, ge=1)
    max_tree_nodes: int = Field(default=50_000, ge=1)
    max_graph_items: int = Field(default=50_000, ge=1)


class IngestionConfig(FrozenModel):
    """Ingestion behaviour.

    Attributes:
        hash_profile: Content hash profile.
        transient_metadata_keys: Metadata keys that never create a new version (a change
            only bumps ``metadata_revision``).
        redelivery_window_seconds: Window of the advisory redelivery check (0 disables).
        concurrency: Graph extractions running at once across the engine.
            ``AutoConcurrency`` (the default, also accepted as ``"auto"``) starts at 8,
            grows while work is queued and capacity allows, and halves when the
            extractor times out or is unavailable; an integer fixes the limit.
        failure_policy: ``per_document`` reports failures per document; ``fail_batch``
            validates the whole batch first and rejects it entirely on any invalid input.
        index_wait: Whether ``ingest`` waits for indexing.
        index_wait_timeout: Seconds ``ingest`` waits when ``index_wait=all``.
        limits: Input size limits.
    """

    hash_profile: str = HASH_PROFILE
    transient_metadata_keys: tuple[str, ...] = ()
    redelivery_window_seconds: float = Field(default=300.0, ge=0)
    concurrency: Concurrency = Field(default_factory=AutoConcurrency)
    failure_policy: FailurePolicy = FailurePolicy.PER_DOCUMENT
    index_wait: IndexWait = IndexWait.ALL
    index_wait_timeout: float = Field(default=120.0, gt=0)
    limits: IngestionLimits = Field(default_factory=IngestionLimits)

    @model_validator(mode="after")
    def _check(self) -> IngestionConfig:
        if self.hash_profile not in SUPPORTED_HASH_PROFILES:
            raise ValueError(f"unsupported hash profile {self.hash_profile!r}")
        return self


class MethodConfig(FrozenModel):
    """One retrieval method."""

    enabled: bool = True
    candidate_k: int = Field(default=50, ge=1)
    weight: float = Field(default=1.0, ge=0.0, allow_inf_nan=False)
    on_error: OnError = "degrade"


class FuzzyConfig(MethodConfig):
    """Fuzzy method settings (RapidFuzz).

    Attributes:
        matches_per_ngram: Vocabulary matches kept per query n-gram; raise it when
            near-identical terms crowd out the ones you need.
    """

    score_cutoff: float = Field(default=85.0, ge=0.0, le=100.0)
    max_ngram: int = Field(default=3, ge=1)
    matches_per_ngram: int = Field(default=5, ge=1)
    corpus_vocabulary: bool = True
    min_token_length: int = Field(default=3, ge=1)


class IndexChannelConfig(FrozenModel):
    """Index channel: keyword, fuzzy, sparse and dense methods."""

    enabled: bool = True
    keyword: MethodConfig = Field(default_factory=MethodConfig)
    fuzzy: FuzzyConfig = Field(default_factory=FuzzyConfig)
    sparse: MethodConfig = Field(default_factory=MethodConfig)
    dense: MethodConfig = Field(default_factory=MethodConfig)
    weight: float = Field(default=1.0, ge=0.0, allow_inf_nan=False)

    def method(self, name: str) -> MethodConfig:
        """Return the configuration of an index method."""
        return getattr(self, IndexMethod(name).value)


class GraphChannelConfig(MethodConfig):
    """Graph channel (disabled by default)."""

    enabled: bool = False
    depth: int = Field(default=1, ge=1)


class StructureChannelConfig(MethodConfig):
    """Structure channel (disabled by default)."""

    enabled: bool = False


class ChannelsConfig(FrozenModel):
    """All channels.

    At least one channel must be enabled; an enabled index channel needs at least one
    enabled method. An engine with nothing to index or query is rejected.
    """

    index: IndexChannelConfig = Field(default_factory=IndexChannelConfig)
    graph: GraphChannelConfig = Field(default_factory=GraphChannelConfig)
    structure: StructureChannelConfig = Field(default_factory=StructureChannelConfig)

    @model_validator(mode="after")
    def _check(self) -> ChannelsConfig:
        index = self.index.enabled and any(self.index.method(name).enabled for name in IndexMethod)
        if not (index or self.graph.enabled or self.structure.enabled):
            raise ValueError("at least one channel (and index method) must be enabled")
        return self


class FusionConfig(FrozenModel):
    """Fusion settings of one level."""

    method: FusionMethod = "rrf"
    k: int = Field(default=60, ge=1)
    normalizer: NormalizerName = "minmax"
    sigmoid_center: float = 0.0
    sigmoid_temperature: float = Field(default=1.0, gt=0.0)


class RerankingConfig(FrozenModel):
    """Reranking of the fused (RRF by default) candidate order.

    When no reranker component is supplied, the engine wires the built-in FlashRank
    cross-encoder, loading ``model_name`` from ``model_dir`` (default ``<data_dir>/models``,
    or ``~/.cache/retrievalloom/models`` for in-memory engines). Models are never downloaded
    unless ``allow_download`` is set; when the model is missing the default reranker is
    dropped at startup (one ``reranker_unavailable`` log) and results keep the fused order.
    A supplied reranker always replaces the default.
    """

    enabled: bool = True
    rerank_k: int = Field(default=50, ge=1)
    on_error: OnError = "degrade"
    model_dir: Path | None = None
    model_name: str = "ms-marco-TinyBERT-L-2-v2"
    allow_download: bool = False


class NLIConfig(FrozenModel):
    """NLI judgements (disabled by default)."""

    enabled: bool = False
    nli_k: int = Field(default=10, ge=1)
    on_error: OnError = "degrade"


class ConsolidationConfig(FrozenModel):
    """Evidence consolidation (always executed)."""

    window_multiplier: int = Field(default=3, ge=1)
    group_structural: bool = True
    merge_adjacent: bool = True
    span_iou_threshold: float = Field(default=0.5, gt=0.0, le=1.0)
    semantic_relations: bool = False
    semantic_threshold: float = Field(default=0.9, gt=0.0, le=1.0)
    contradiction_overflow: int | None = Field(default=None, ge=0)


class TenantPolicy(FrozenModel):
    """Per-tenant limits and capabilities.

    Requests asking for more than ``max_candidate_k`` (or ``rerank_k``/``nli_k`` above
    ``max_candidate_k``) are reduced to it and report a ``limit_clamped`` warning.

    Attributes:
        max_concurrent_offloaded_jobs: Offloaded jobs of the tenant running at once;
            ``None`` uses ``OffloaderConfig.max_concurrent_jobs_per_tenant``. Also
            accepted under its 0.1 name ``max_concurrent_cpu_jobs`` (deprecated).
    """

    max_concurrent_requests: int = Field(default=16, ge=1)
    max_concurrent_offloaded_jobs: int | None = Field(
        default=None,
        ge=1,
        validation_alias=AliasChoices("max_concurrent_offloaded_jobs", "max_concurrent_cpu_jobs"),
    )
    max_top_k: int = Field(default=100, ge=1)
    max_candidate_k: int = Field(default=500, ge=1)
    max_ingest_batch: int = Field(default=1000, ge=1)
    allowed_channels: tuple[Channel, ...] = (Channel.INDEX, Channel.GRAPH, Channel.STRUCTURE)

    @model_validator(mode="before")
    @classmethod
    def _renamed(cls, data: Any) -> Any:
        if isinstance(data, dict) and "max_concurrent_cpu_jobs" in data:
            warnings.warn(
                "TenantPolicy.max_concurrent_cpu_jobs is deprecated; "
                "use max_concurrent_offloaded_jobs",
                DeprecationWarning,
                stacklevel=2,
            )
        return data


class RetrievalConfig(FrozenModel):
    """Retrieval behaviour.

    Attributes:
        filter_overfetch: Methods that cannot push filters down to their backend fetch
            ``candidate_k * filter_overfetch`` hits so enough remain after filtering;
            a ``filter_underfetch`` warning reports when filtering still left fewer
            than ``candidate_k``. Raise it for selective filters.
    """

    channels: ChannelsConfig = Field(default_factory=ChannelsConfig)
    method_fusion: FusionConfig = Field(default_factory=FusionConfig)
    channel_fusion: FusionConfig = Field(default_factory=FusionConfig)
    reranking: RerankingConfig = Field(default_factory=RerankingConfig)
    nli: NLIConfig = Field(default_factory=NLIConfig)
    consolidation: ConsolidationConfig = Field(default_factory=ConsolidationConfig)
    missing_index_policy: MissingIndexPolicy = MissingIndexPolicy.SKIP
    default_top_k: int = Field(default=10, ge=1)
    max_query_chars: int = Field(default=4096, ge=1)
    max_filter_nodes: int = Field(default=64, ge=1)
    filter_overfetch: int = Field(default=4, ge=1)
    request_timeout: float = Field(default=30.0, gt=0)
    method_timeout: float = Field(default=10.0, gt=0)
    allow_partial_on_deadline: bool = True
    allow_stale_versions: bool = False
    fail_on_recording_error: bool = False
    embedding_cache_size: int = Field(default=1024, ge=0)
    embedding_cache_ttl: float = Field(default=600.0, gt=0)
    default_tenant_policy: TenantPolicy = Field(default_factory=TenantPolicy)
    tenant_policies: dict[Identifier, TenantPolicy] = Field(default_factory=dict)

    def tenant_policy(self, tenant_id: str) -> TenantPolicy:
        """Return the policy of ``tenant_id`` (or the default policy)."""
        return self.tenant_policies.get(tenant_id, self.default_tenant_policy)


class LifecycleConfig(FrozenModel):
    """Background lifecycle (refresh-engine) settings.

    Attributes:
        background: Run a periodic lifecycle scheduler while the engine is open.
        poll_interval: Seconds between scheduled lifecycle rounds.
        batch_size: Documents per backend write batch (also capped by each backend's
            ``capabilities.max_batch``).
        max_concurrency: Index units processed at once. ``AutoConcurrency`` (the
            default, also accepted as ``"auto"``) starts at 4 and follows measured CPU
            and memory capacity; an integer fixes the limit.
        max_attempts: Attempts per unit round (refresh-engine retry), and attempts per
            document before its coverage is marked ``failed``.
        retry_failed_after: Seconds before coverage that failed with a retryable error
            (for example a backend outage) is queued again by the background lifecycle;
            the delay doubles with every further failure up to
            ``retry_failed_max_delay``. Non-retryable failures stay ``failed`` until a
            backfill. ``None`` disables automatic retries.
        retry_failed_max_delay: Upper bound, in seconds, of the retry delay of failed
            coverage.
        unit_timeout: Seconds one unit round may take.
        instance_ttl: Seconds an engine-instance registration lives without heartbeat.
        lease_ttl: Seconds a durable reader lease lives.
        generation_retention: Seconds a retired generation is kept before dropping.
        auto_rebuild_ephemeral: Rebuild ephemeral backends from metadata at
            startup. Without ``background`` (or with ``wait_for_rebuild``) the
            rebuild completes before the engine opens; otherwise it runs in the
            background and retrievals report ``index_missing`` until it finishes.
        wait_for_rebuild: With ``background``, still finish the startup rebuild
            before the engine opens (bounded by ``rebuild_timeout``).
        rebuild_timeout: Seconds a startup rebuild may block ``initialize`` when
            ``background`` is on; on timeout the rebuild continues in the
            background.
        purge_after: Seconds a deleted document stays tombstoned before it is
            physically erased by the background lifecycle; ``None`` disables.
        max_versions: Version-history entries kept per document; older
            unreferenced versions and their representations are pruned.
            ``None`` disables.
        event_retention: Seconds committed or failed ingestion events are kept;
            ``None`` disables.
        purge_log_retention: Seconds purge-log entries are kept. Engines evict purged
            objects from their in-memory indexes by reading this log; an engine that
            missed pruned entries rebuilds its in-memory indexes instead. Keep it well
            above ``instance_ttl``.
        migration: Metadata schema policy (``auto`` or ``verify``).
        shutdown_timeout: Seconds ``close`` waits for in-flight work.
        housekeeping_interval: Seconds between housekeeping passes (retention, purge
            and generation cleanup) of the background lifecycle.
    """

    background: bool = True
    poll_interval: float = Field(default=2.0, gt=0)
    batch_size: int = Field(default=128, ge=1)
    max_concurrency: Concurrency = Field(default_factory=AutoConcurrency)
    max_attempts: int = Field(default=3, ge=1)
    retry_failed_after: float | None = Field(default=30.0, gt=0)
    retry_failed_max_delay: float = Field(default=900.0, gt=0)
    unit_timeout: float = Field(default=600.0, gt=0)
    instance_ttl: float = Field(default=120.0, gt=0)
    lease_ttl: float = Field(default=120.0, gt=0)
    generation_retention: float = Field(default=0.0, ge=0)
    auto_rebuild_ephemeral: bool = True
    wait_for_rebuild: bool = False
    rebuild_timeout: float = Field(default=300.0, gt=0)
    purge_after: float | None = Field(default=7 * 86_400.0, ge=0)
    max_versions: int | None = Field(default=10, ge=1)
    event_retention: float | None = Field(default=90 * 86_400.0, ge=0)
    purge_log_retention: float = Field(default=86_400.0, gt=0)
    migration: Literal["auto", "verify"] = "auto"
    shutdown_timeout: float = Field(default=30.0, gt=0)
    housekeeping_interval: float = Field(default=300.0, gt=0)


class EngineConfig(FrozenModel):
    """Complete engine configuration.

    Attributes:
        limits: Upper bounds the other settings are checked against; raise them to
            allow larger values.
        health_timeout: Seconds one component health check may take before it is
            reported unhealthy.
    """

    ingestion: IngestionConfig = Field(default_factory=IngestionConfig)
    retrieval: RetrievalConfig = Field(default_factory=RetrievalConfig)
    lifecycle: LifecycleConfig = Field(default_factory=LifecycleConfig)
    offloader: OffloaderConfig = Field(default_factory=OffloaderConfig)
    limits: EngineLimits = Field(default_factory=EngineLimits)
    health_timeout: float = Field(default=5.0, gt=0)
    log_content: bool = False

    @model_validator(mode="after")
    def _within_limits(self) -> EngineConfig:
        limits = self.limits
        retrieval = self.retrieval
        index = retrieval.channels.index
        checks: list[tuple[str, int, str, int]] = [
            *(
                (
                    f"retrieval.channels.index.{name}.candidate_k",
                    index.method(name).candidate_k,
                    "max_candidate_k",
                    limits.max_candidate_k,
                )
                for name in IndexMethod
            ),
            (
                "retrieval.channels.graph.candidate_k",
                retrieval.channels.graph.candidate_k,
                "max_candidate_k",
                limits.max_candidate_k,
            ),
            (
                "retrieval.channels.structure.candidate_k",
                retrieval.channels.structure.candidate_k,
                "max_candidate_k",
                limits.max_candidate_k,
            ),
            (
                "retrieval.reranking.rerank_k",
                retrieval.reranking.rerank_k,
                "max_rerank_k",
                limits.max_rerank_k,
            ),
            ("retrieval.nli.nli_k", retrieval.nli.nli_k, "max_nli_k", limits.max_nli_k),
            (
                "retrieval.channels.index.fuzzy.max_ngram",
                index.fuzzy.max_ngram,
                "max_fuzzy_ngram",
                limits.max_fuzzy_ngram,
            ),
            (
                "retrieval.channels.index.fuzzy.matches_per_ngram",
                index.fuzzy.matches_per_ngram,
                "max_fuzzy_matches_per_ngram",
                limits.max_fuzzy_matches_per_ngram,
            ),
            (
                "retrieval.channels.graph.depth",
                retrieval.channels.graph.depth,
                "max_graph_depth",
                limits.max_graph_depth,
            ),
            (
                "retrieval.consolidation.window_multiplier",
                retrieval.consolidation.window_multiplier,
                "max_window_multiplier",
                limits.max_window_multiplier,
            ),
            (
                "lifecycle.batch_size",
                self.lifecycle.batch_size,
                "max_lifecycle_batch",
                limits.max_lifecycle_batch,
            ),
            (
                "lifecycle.max_attempts",
                self.lifecycle.max_attempts,
                "max_lifecycle_attempts",
                limits.max_lifecycle_attempts,
            ),
        ]
        for path, setting, limit_name, ceiling in (
            (
                "ingestion.concurrency",
                self.ingestion.concurrency,
                "max_ingestion_concurrency",
                limits.max_ingestion_concurrency,
            ),
            (
                "lifecycle.max_concurrency",
                self.lifecycle.max_concurrency,
                "max_lifecycle_concurrency",
                limits.max_lifecycle_concurrency,
            ),
            (
                "offloader.max_workers",
                self.offloader.max_workers,
                "max_offloader_workers",
                limits.max_offloader_workers,
            ),
        ):
            if isinstance(setting, AutoConcurrency):
                if setting.maximum is not None:
                    checks.append((f"{path}.maximum", setting.maximum, limit_name, ceiling))
                if setting.minimum is not None:
                    checks.append((f"{path}.minimum", setting.minimum, limit_name, ceiling))
            else:
                checks.append((path, setting, limit_name, ceiling))
        for path, value, limit_name, ceiling in checks:
            if value > ceiling:
                raise ValueError(
                    f"{path}={value} exceeds limits.{limit_name}={ceiling}; raise "
                    f"EngineLimits.{limit_name} (EngineConfig.limits) to allow it"
                )
        return self


@dataclass(frozen=True, slots=True, kw_only=True)
class Components:
    """Pluggable components; ``None`` selects the default (or disables the capability).

    Application-supplied components stay application-owned unless the engine created them.
    """

    metadata_store: DocumentMetadataStoreABC | None = None
    embedding: EmbeddingModelABC | None = None
    reranker: RerankerABC | None = None
    nli: NLIABC | None = None
    tokenizer: TokenizerABC | None = None
    keyword_dictionary: KeywordDictionaryABC | None = None
    keyword_index: KeywordIndexABC | None = None
    fuzzy_index: FuzzyIndexABC | None = None
    sparse_index: SparseIndexABC | None = None
    vector_store: VectorStoreABC | None = None
    graph_extractor: GraphExtractorABC | None = None
    graph_normalizer: GraphNormalizerABC | None = None
    graph_store: GraphStoreABC | None = None
    graph_retriever: GraphRetrieverABC | None = None
    structure_engine: StructureEngineABC | None = None
    method_fusion: FusionABC | None = None
    channel_fusion: FusionABC | None = None
    deduplicator: DeduplicatorABC | None = None
    consolidator: EvidenceConsolidatorABC | None = None
    cache: CacheABC | None = None
    access_policy: AccessPolicyABC | None = None
    provenance_recorder: ProvenanceRecorderABC | None = None


@dataclass(frozen=True, slots=True, kw_only=True)
class EngineResources:
    """Application-owned infrastructure the engine uses but never closes.

    Attributes:
        resource_manager: resourcera manager for admission control.
        xai_runtime: agxai ``XAIRuntime``; enables the agxai provenance recorder.
        executor: Executor for offloaded blocking work.
        data_dir: Directory for durable component files (graph, structure, metadata).
        metrics: refresh-engine ``MetricsSink``.
    """

    resource_manager: ResourceManager | None = None
    xai_runtime: Any = None
    executor: Executor | None = None
    data_dir: Path | None = None
    metrics: Any = None
