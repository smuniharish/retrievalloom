"""RetrievalEngine: the async public entry point that owns wiring and lifecycle."""

from __future__ import annotations

import asyncio
import contextlib
from dataclasses import dataclass, field
from enum import StrEnum
from typing import TYPE_CHECKING, Any, Self

import pydantic
import structlog

from retrievalloom._logging import get_logger
from retrievalloom.config import (
    Components,
    EngineConfig,
    EngineLimits,
    EngineResources,
    IndexWait,
    IngestionConfig,
    LifecycleConfig,
    RerankingConfig,
    RetrievalConfig,
)
from retrievalloom.contracts.component import ComponentContext
from retrievalloom.contracts.infra import Action
from retrievalloom.domain.base import utc_now
from retrievalloom.domain.hashing import random_id
from retrievalloom.domain.records import Channel, TaskKind, TaskState
from retrievalloom.domain.reports import (
    DeleteReport,
    GenerationStatus,
    HealthReport,
    HealthStatus,
    IngestReport,
    PurgeReport,
    ScopeStatus,
)
from retrievalloom.domain.retrieval import (
    RetrievalExecutionConfig,
    RetrievalRequest,
    RetrievalWarning,
    WarningCode,
)
from retrievalloom.errors import (
    AccessDeniedError,
    ConfigurationError,
    LifecycleError,
    OperationTimeoutError,
    ProvenanceError,
    TenantIsolationError,
    ValidationError,
)
from retrievalloom.evidence.consolidation import HierarchicalConsolidator
from retrievalloom.evidence.dedup import IdentityAndHashDeduplicator
from retrievalloom.index.embedding import EmbeddingValidator
from retrievalloom.index.fuzzy import RapidFuzzIndex
from retrievalloom.index.keyword import AhoCorasickKeywordIndex
from retrievalloom.index.sparse import BM25sIndex
from retrievalloom.index.text import MetadataKeywordDictionary, SimpleTokenizer
from retrievalloom.infra.access import AllowAllAccessPolicy
from retrievalloom.infra.cache import InMemoryLRUCache, NoCache
from retrievalloom.infra.metrics import NullMetrics
from retrievalloom.infra.recorders import NoOpProvenanceRecorder
from retrievalloom.lifecycle.coordinator import (
    LIFECYCLE_FLOOR,
    LIFECYCLE_REQUIREMENTS,
    LifecycleCoordinator,
)
from retrievalloom.lifecycle.indexer import ReaderTracker, UnitIndexer, task_units
from retrievalloom.lifecycle.ingestion import (
    INGESTION_FLOOR,
    INGESTION_REQUIREMENTS,
    GraphDerivation,
    IngestionService,
)
from retrievalloom.lifecycle.purge import DocumentEraser
from retrievalloom.lifecycle.units import ItemPreparer, make_unit
from retrievalloom.retrieval.pipeline import PipelineComponents, RetrievalPipeline
from retrievalloom.runtime.offloader import Offloader, OffloaderConfig
from retrievalloom.storage.base import MigrationPolicy
from retrievalloom.storage.sqlite import SQLiteMetadataStore

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Iterable, Sequence
    from pathlib import Path
    from types import TracebackType

    from refresh_engine import RefreshResult

    from retrievalloom.contracts.component import ComponentABC
    from retrievalloom.contracts.indexes import (
        GenerationalIndexABC,
        GraphRetrieverABC,
        GraphStoreABC,
        StructureEngineABC,
        VectorStoreABC,
    )
    from retrievalloom.contracts.infra import AccessPolicyABC, ProvenanceRecorderABC
    from retrievalloom.contracts.models import GraphNormalizerABC, RerankerABC
    from retrievalloom.domain.filters import FilterExpression
    from retrievalloom.domain.inputs import DocumentInput, Scope
    from retrievalloom.domain.reports import LifecycleTaskRef, LifecycleTaskResult
    from retrievalloom.domain.retrieval import RetrievalResult
    from retrievalloom.lifecycle.units import IndexUnit
    from retrievalloom.storage.base import DocumentMetadataStoreABC

_ACTIVE_TASKS = (TaskState.QUEUED, TaskState.RUNNING)


class EngineState(StrEnum):
    """Engine lifecycle state."""

    NEW = "new"
    OPEN = "open"
    CLOSING = "closing"
    CLOSED = "closed"


@dataclass(slots=True)
class _Runtime:
    """Everything built by ``initialize``; dropped by ``close``."""

    offloader: Offloader
    store: DocumentMetadataStoreABC
    units: dict[str, IndexUnit]
    coordinator: LifecycleCoordinator
    ingestion: IngestionService
    eraser: DocumentEraser
    pipeline: RetrievalPipeline
    access: AccessPolicyABC
    recorder: ProvenanceRecorderABC
    initialized: list[ComponentABC]
    owned: set[int]
    semaphores: dict[str, asyncio.Semaphore] = field(default_factory=dict)


class _Wiring:
    """Resolves components to defaults and records which ones the engine owns."""

    def __init__(self) -> None:
        self.ordered: list[ComponentABC] = []
        self.owned: set[int] = set()

    def use[T: ComponentABC](self, supplied: T | None, default: Callable[[], T] | None) -> T | None:
        component = supplied
        if component is None and default is not None:
            component = default()
            self.owned.add(id(component))
        if component is not None and all(item is not component for item in self.ordered):
            self.ordered.append(component)
        return component


def _first(*values: int | None, default: int) -> int:
    return next((value for value in values if value is not None), default)


def _check_graph_depth(store: GraphStoreABC | None, retriever: GraphRetrieverABC | None) -> None:
    """Reject a traversal depth the graph store cannot execute, before anything starts."""
    depth = getattr(retriever, "depth", None)
    if store is None or not isinstance(depth, int):
        return
    maximum = store.capabilities.max_depth
    if maximum is not None and depth > maximum:
        raise ConfigurationError(
            "graph traversal depth exceeds what the graph store supports",
            details={"depth": depth, "max_depth": maximum, "store": store.info.name},
        )


def _error_locations(exc: pydantic.ValidationError) -> list[dict[str, Any]]:
    return [
        {"loc": ".".join(str(part) for part in error["loc"]), "type": error["type"]}
        for error in exc.errors(include_url=False, include_input=False, include_context=False)
    ]


class RetrievalEngine:
    """Async-only retrieval and evidence orchestration engine.

    Holds all state (no module-level registries), so several engines can coexist. Use
    ``async with engine:`` or call :meth:`initialize` and :meth:`close` explicitly.
    Components passed in ``components`` and infrastructure in ``resources`` stay
    application-owned: the engine initialises them but never closes them.

    Args:
        config: Complete engine configuration.
        components: Pluggable components (``None`` fields select defaults).
        resources: Application-owned infrastructure.
        engine_id: Identifier used in logs, leases and instance registration.
    """

    def __init__(
        self,
        config: EngineConfig | None = None,
        components: Components | None = None,
        resources: EngineResources | None = None,
        *,
        engine_id: str | None = None,
    ) -> None:
        self._config = config or EngineConfig()
        self._components = components or Components()
        self._resources = resources or EngineResources()
        self._engine_id = engine_id or random_id("engine")
        self._instance_id = random_id("inst")
        self._logger = get_logger("engine", engine_id=self._engine_id)
        self._state = EngineState.NEW
        self._state_lock = asyncio.Lock()
        self._runtime: _Runtime | None = None
        self._inflight = 0
        self._drained = asyncio.Event()
        self._drained.set()
        self._background: set[asyncio.Task[Any]] = set()

    # properties -------------------------------------------------------------------------
    @property
    def engine_id(self) -> str:
        """Engine identifier."""
        return self._engine_id

    @property
    def instance_id(self) -> str:
        """Engine-instance id owning ephemeral backends."""
        return self._instance_id

    @property
    def config(self) -> EngineConfig:
        """Engine configuration."""
        return self._config

    @property
    def state(self) -> EngineState:
        """Lifecycle state."""
        return self._state

    # lifecycle --------------------------------------------------------------------------
    async def __aenter__(self) -> Self:
        await self.initialize()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        await self.close()

    async def initialize(self) -> None:
        """Build defaults, initialise components, migrate metadata and start lifecycle.

        Idempotent while open.

        Raises:
            LifecycleError: The engine was closed.
            ConfigurationError: The components cannot serve the configuration.
            MigrationError: The metadata schema is not usable under the migration policy.
        """
        async with self._state_lock:
            if self._state == EngineState.OPEN:
                return
            if self._state != EngineState.NEW:
                raise LifecycleError("engine is closed", details={"state": self._state.value})
            offloader = self._make_offloader()
            await offloader.start()
            wiring = _Wiring()
            try:
                self._runtime = await self._build(offloader, wiring)
            except BaseException:
                await self._close_components(wiring.ordered, wiring.owned)
                await offloader.close()
                raise
            self._state = EngineState.OPEN
        self._logger.info(
            "engine_initialized",
            instance_id=self._instance_id,
            units=sorted(self._runtime.units),
            background=self._config.lifecycle.background,
        )

    async def close(self) -> None:
        """Drain in-flight operations, stop lifecycle and close engine-owned components.

        Idempotent. Waits up to ``lifecycle.shutdown_timeout`` for in-flight operations
        and background indexing rounds, then cancels what is left.
        """
        async with self._state_lock:
            if self._state in {EngineState.CLOSED, EngineState.NEW}:
                self._state = EngineState.CLOSED
                return
            self._state = EngineState.CLOSING
            runtime = self._runtime
            assert runtime is not None
            timeout = self._config.lifecycle.shutdown_timeout
            drained = True
            try:
                async with asyncio.timeout(timeout):
                    await self._drained.wait()
                    if self._background:
                        await asyncio.gather(*self._background, return_exceptions=True)
            except TimeoutError:
                drained = False
            for task in list(self._background):
                task.cancel()
            if self._background:
                await asyncio.gather(*self._background, return_exceptions=True)
            try:
                await runtime.coordinator.stop()
            finally:
                await self._close_components(runtime.initialized, runtime.owned)
                await runtime.offloader.close()
                self._runtime = None
                self._state = EngineState.CLOSED
        self._logger.info("engine_closed", drained=drained)

    # operations -------------------------------------------------------------------------
    async def ingest(
        self, documents: Sequence[DocumentInput], *, scope: Scope, principal: str | None = None
    ) -> IngestReport:
        """Ingest ``documents`` into ``scope`` and (by default) wait for indexing.

        Raises:
            AccessDeniedError: The access policy denied the operation.
            ValidationError: The batch exceeds the tenant limit or is invalid.
        """
        async with self._operation() as runtime:
            with structlog.contextvars.bound_contextvars(scope_key=scope.key):
                await self._authorize(runtime, principal, scope, Action.INGEST)
                limit = self._config.retrieval.tenant_policy(scope.tenant_id).max_ingest_batch
                if len(documents) > limit:
                    raise ValidationError(
                        "ingest batch exceeds the tenant limit",
                        details={"count": len(documents), "max": limit},
                    )
                report = await runtime.ingestion.ingest(documents, scope)
                warnings = await self._index_after_write(runtime, scope)
                if warnings:
                    report = report.model_copy(update={"warnings": (*report.warnings, *warnings)})
                return await self._record_write(runtime, report)

    async def delete(
        self, document_ids: Sequence[str], *, scope: Scope, principal: str | None = None
    ) -> DeleteReport:
        """Delete documents; they leave retrieval results immediately.

        Raises:
            AccessDeniedError: The access policy denied the operation.
        """
        async with self._operation() as runtime:
            with structlog.contextvars.bound_contextvars(scope_key=scope.key):
                await self._authorize(runtime, principal, scope, Action.DELETE)
                report = await runtime.ingestion.delete(document_ids, scope)
                await self._index_after_write(runtime, scope)
                return await self._record_write(runtime, report)

    async def purge(
        self, document_ids: Sequence[str], *, scope: Scope, principal: str | None = None
    ) -> PurgeReport:
        """Physically erase documents (right to erasure), tombstoning them first if needed.

        The documents' objects are removed from every index generation of this engine
        and every metadata row of the documents (versions, representations, events,
        objects, lineage, visibility, coverage) is deleted. Purging is idempotent:
        unknown or already purged ids are reported as `not_found`.

        Raises:
            AccessDeniedError: The access policy denied the operation.
            BackendError: A backend failed to remove objects; nothing was purged for
                that document and the call can be retried.
        """
        async with self._operation() as runtime:
            with structlog.contextvars.bound_contextvars(scope_key=scope.key):
                await self._authorize(runtime, principal, scope, Action.DELETE)
                report = await runtime.eraser.purge(document_ids, scope)
                await self._index_after_write(runtime, scope)
                return report

    async def retrieve(
        self,
        query: str,
        *,
        scope: Scope,
        filters: FilterExpression | None = None,
        top_k: int | None = None,
        claims: Sequence[str] = (),
        principal: str | None = None,
        config: RetrievalExecutionConfig | None = None,
        request_id: str | None = None,
    ) -> RetrievalResult:
        """Retrieve evidence for ``query`` in ``scope``.

        ``top_k`` defaults to ``config.top_k`` and then to ``default_top_k``.

        Raises:
            AccessDeniedError: The access policy denied the operation.
            ValidationError: The request is invalid or exceeds a bound.
            OperationTimeoutError: The tenant's concurrency slot or the request timed out.
        """
        async with self._operation() as runtime:
            execution = config or RetrievalExecutionConfig()
            try:
                request = RetrievalRequest(
                    request_id=request_id or random_id("req"),
                    scope=scope,
                    query=query,
                    filters=filters,
                    top_k=_first(
                        top_k, execution.top_k, default=self._config.retrieval.default_top_k
                    ),
                    claims=tuple(claims),
                    principal=principal,
                    execution=execution,
                )
            except pydantic.ValidationError as exc:
                raise ValidationError(
                    "invalid retrieval request", details={"errors": _error_locations(exc)}
                ) from None
            with structlog.contextvars.bound_contextvars(
                request_id=request.request_id, scope_key=scope.key
            ):
                await self._authorize(runtime, principal, scope, Action.RETRIEVE)
                row_filter = await runtime.access.row_filter(principal, scope)
                semaphore = self._tenant_semaphore(runtime, scope.tenant_id)
                timeout = execution.timeout or self._config.retrieval.request_timeout
                try:
                    async with asyncio.timeout(timeout):
                        await semaphore.acquire()
                except TimeoutError as exc:
                    raise OperationTimeoutError(
                        "tenant request slot not available in time",
                        retryable=True,
                        details={"tenant": scope.tenant_id},
                    ) from exc
                try:
                    return await runtime.pipeline.retrieve(request, row_filter=row_filter)
                finally:
                    semaphore.release()

    async def backfill(
        self,
        *,
        scope: Scope,
        channels: Sequence[Channel | str] | None = None,
        methods: Sequence[str] | None = None,
        principal: str | None = None,
    ) -> LifecycleTaskRef:
        """Queue a task indexing every document the selected units do not cover yet.

        ``channels``/``methods`` select units (all by default); failed coverage is reset.

        Raises:
            ConfigurationError: No configured unit matches the selection.
        """
        async with self._operation() as runtime:
            await self._authorize(runtime, principal, scope, Action.BACKFILL)
            units = self._select(runtime, channels, methods)
            return await runtime.coordinator.request(scope, TaskKind.BACKFILL, units)

    async def rebuild(
        self,
        *,
        scope: Scope,
        channel: Channel | str,
        method: str | None = None,
        principal: str | None = None,
    ) -> LifecycleTaskRef:
        """Queue a full rebuild (new generation, atomic switch) of a channel or one method.

        Raises:
            ConfigurationError: No configured unit matches the selection.
        """
        async with self._operation() as runtime:
            await self._authorize(runtime, principal, scope, Action.REBUILD)
            units = self._select(runtime, [channel], None if method is None else [method])
            return await runtime.coordinator.request(scope, TaskKind.REBUILD, units)

    async def wait_for(
        self,
        task: LifecycleTaskRef,
        *,
        timeout: float | None = None,  # noqa: ASYNC109 - documented public API bound
    ) -> LifecycleTaskResult:
        """Wait until ``task`` is terminal.

        Without a background scheduler this runs lifecycle rounds itself.

        Raises:
            OperationTimeoutError: ``timeout`` elapsed first.
            LifecycleError: The task is unknown in its scope.
        """
        async with self._operation() as runtime:
            return await runtime.coordinator.wait_for(task, timeout=timeout)

    async def run_lifecycle_once(self, scope: Scope | None = None) -> RefreshResult:
        """Run one maintenance and indexing round now (for externally scheduled deployments).

        Returns:
            The refresh-engine round result; failed units are retried by later rounds.
        """
        async with self._operation() as runtime:
            await runtime.coordinator.maintain()
            return await runtime.coordinator.run_once(scope)

    async def status(self, *, scope: Scope, principal: str | None = None) -> ScopeStatus:
        """Documents, coverage, generations and lifecycle tasks of ``scope``."""
        async with self._operation() as runtime:
            await self._authorize(runtime, principal, scope, Action.STATUS)
            instances = ["", self._instance_id]
            async with runtime.store.transaction(scope, write=False) as transaction:
                counts = await transaction.count_documents(scope.key)
                coverage = await transaction.coverage_counts(scope.key, instances)
                generations = await transaction.list_generations(
                    scope_key=scope.key, instances=[None, self._instance_id]
                )
                tasks = await transaction.list_tasks(scope_key=scope.key)
            task_counts: dict[str, int] = {}
            for task in tasks:
                task_counts[task.state.value] = task_counts.get(task.state.value, 0) + 1
            return ScopeStatus(
                scope=scope,
                documents_active=counts.get("active", 0),
                documents_deleted=counts.get("deleted", 0),
                coverage={name: dict(states) for name, states in coverage.items()},
                generations=tuple(
                    GenerationStatus(
                        method=f"{generation.channel.value}.{generation.method}",
                        generation_id=generation.generation_id,
                        state=generation.state,
                        backend_id=generation.backend_id,
                    )
                    for generation in generations
                ),
                tasks=task_counts,
            )

    async def health(self) -> HealthReport:
        """Aggregate health of the engine and its components (never raises when open).

        ``details["concurrency"]`` reports every concurrency limiter: its mode, current
        limit, bounds, active and waiting work, and the resource limiting it.
        """
        async with self._operation() as runtime:
            reports = [await self._component_health(item) for item in runtime.initialized]
            order = [HealthStatus.HEALTHY, HealthStatus.DEGRADED, HealthStatus.UNHEALTHY]
            worst = max((report.status for report in reports), key=order.index)
            return HealthReport(
                component="retrieval_engine",
                status=worst,
                details={
                    "engine_id": self._engine_id,
                    "units": sorted(runtime.units),
                    "concurrency": runtime.offloader.concurrency(),
                },
                components=tuple(reports),
            )

    # internals --------------------------------------------------------------------------
    @contextlib.asynccontextmanager
    async def _operation(self) -> Any:
        if self._state != EngineState.OPEN or self._runtime is None:
            raise LifecycleError("engine is not open", details={"state": self._state.value})
        self._inflight += 1
        self._drained.clear()
        try:
            yield self._runtime
        finally:
            self._inflight -= 1
            if self._inflight == 0:
                self._drained.set()

    def _make_offloader(self) -> Offloader:
        policy = self._config.retrieval

        def tenant_limit(tenant: str) -> int | None:
            return policy.tenant_policy(tenant).max_concurrent_offloaded_jobs

        return Offloader(
            self._config.offloader,
            executor=self._resources.executor,
            resource_manager=self._resources.resource_manager,
            tenant_limit=tenant_limit,
            max_workers_ceiling=self._config.limits.max_offloader_workers,
        )

    async def _build(self, offloader: Offloader, wiring: _Wiring) -> _Runtime:
        config = self._config
        supplied = self._components
        data_dir = self._resources.data_dir
        retrieval = config.retrieval
        channels = retrieval.channels
        index = channels.index
        metrics = self._resources.metrics or NullMetrics()

        store = wiring.use(
            supplied.metadata_store,
            lambda: SQLiteMetadataStore(None if data_dir is None else data_dir / "metadata.db"),
        )
        assert store is not None
        context = ComponentContext(
            engine_id=self._engine_id,
            offloader=offloader,
            clock=utc_now,
            logger=self._logger,
            metrics=metrics,
            data_dir=data_dir,
            metadata_store=store,
        )
        await store.initialize(context)
        await store.migrate(MigrationPolicy(config.lifecycle.migration))

        def method_on(name: str) -> bool:
            return index.enabled and index.method(name).enabled

        tokenizer = wiring.use(supplied.tokenizer, SimpleTokenizer)
        dictionary = wiring.use(supplied.keyword_dictionary, MetadataKeywordDictionary)
        assert tokenizer is not None
        assert dictionary is not None
        fuzzy = index.fuzzy
        keyword_index = wiring.use(
            supplied.keyword_index, AhoCorasickKeywordIndex if method_on("keyword") else None
        )
        fuzzy_index = wiring.use(
            supplied.fuzzy_index,
            (
                lambda: RapidFuzzIndex(
                    score_cutoff=fuzzy.score_cutoff,
                    max_ngram=fuzzy.max_ngram,
                    matches_per_ngram=fuzzy.matches_per_ngram,
                    corpus_vocabulary=fuzzy.corpus_vocabulary,
                    min_token_length=fuzzy.min_token_length,
                )
            )
            if method_on("fuzzy")
            else None,
        )
        sparse_index = wiring.use(
            supplied.sparse_index, BM25sIndex if method_on("sparse") else None
        )
        embedding = wiring.use(supplied.embedding, None)
        if supplied.vector_store is not None and embedding is None:
            raise ConfigurationError("a vector store needs an embedding model")
        vector_store = None
        if embedding is not None and method_on("dense"):
            vector_store = wiring.use(
                supplied.vector_store, lambda: _default_vector_store(data_dir)
            )

        graph_store = graph_retriever = normalizer = extractor = None
        if channels.graph.enabled:
            graph_store = wiring.use(supplied.graph_store, lambda: _default_graph_store(data_dir))
            graph_retriever = wiring.use(
                supplied.graph_retriever, lambda: _default_graph_retriever(channels.graph.depth)
            )
            normalizer = wiring.use(supplied.graph_normalizer, _default_normalizer)
            extractor = wiring.use(supplied.graph_extractor, None)
            _check_graph_depth(graph_store, graph_retriever)
        structure_engine = None
        if channels.structure.enabled:
            structure_engine = wiring.use(
                supplied.structure_engine, lambda: _default_structure_engine(data_dir)
            )

        deduplicator = wiring.use(supplied.deduplicator, IdentityAndHashDeduplicator)
        consolidator = wiring.use(supplied.consolidator, HierarchicalConsolidator)
        method_fusion = wiring.use(supplied.method_fusion, None)
        channel_fusion = wiring.use(supplied.channel_fusion, None)
        reranking = retrieval.reranking
        reranker = wiring.use(
            supplied.reranker,
            (lambda: _default_reranker(reranking, data_dir)) if reranking.enabled else None,
        )
        nli = wiring.use(supplied.nli, None)
        cache = wiring.use(supplied.cache, NoCache)
        embedding_cache = None
        if embedding is not None and retrieval.embedding_cache_size > 0:
            embedding_cache = wiring.use(
                None,
                lambda: InMemoryLRUCache(
                    retrieval.embedding_cache_size, retrieval.embedding_cache_ttl
                ),
            )
        access = wiring.use(supplied.access_policy, AllowAllAccessPolicy)
        recorder = wiring.use(supplied.provenance_recorder, self._default_recorder)
        assert deduplicator is not None
        assert consolidator is not None
        assert cache is not None
        assert access is not None
        assert recorder is not None

        for component in wiring.ordered[1:]:
            await component.initialize(context)
        reranker = await self._usable_reranker(reranker, wiring)

        embedder = None if embedding is None else EmbeddingValidator(embedding, offloader)
        units = self._units(
            keyword=(keyword_index, [dictionary]),
            fuzzy=(fuzzy_index, [tokenizer, dictionary]),
            sparse=(sparse_index, [tokenizer]),
            dense=(vector_store, [] if embedding is None else [embedding]),
            graph=(graph_store, []),
            structure=(structure_engine, []),
        )
        if not units:
            raise ConfigurationError(
                "no retrieval method has a backend; enable a channel or supply its component",
                details={"hint": "dense needs an embedding model"},
            )
        readers = ReaderTracker(lease_ttl=config.lifecycle.lease_ttl)
        lifecycle = config.lifecycle
        limits = config.limits
        ingestion = IngestionService(
            store,
            config.ingestion,
            clock=utc_now,
            logger=self._logger,
            graph=None if normalizer is None else GraphDerivation(normalizer, extractor),
            limiter=await offloader.limiter(
                "ingestion",
                config.ingestion.concurrency,
                floor=INGESTION_FLOOR,
                ceiling=limits.max_ingestion_concurrency,
                requirements=INGESTION_REQUIREMENTS,
            ),
        )
        indexer = UnitIndexer(
            store,
            ItemPreparer(tokenizer=tokenizer, embedder=embedder, dictionary=dictionary),
            lifecycle,
            instance_id=self._instance_id,
            clock=utc_now,
            logger=self._logger,
            readers=readers,
            derive_graph=None if normalizer is None else ingestion.derive_graph,
        )
        eraser = DocumentEraser(
            store, ingestion, indexer, list(units.values()), clock=utc_now, logger=self._logger
        )
        coordinator = LifecycleCoordinator(
            store,
            indexer,
            list(units.values()),
            lifecycle,
            instance_id=self._instance_id,
            engine_id=self._engine_id,
            clock=utc_now,
            logger=self._logger,
            eraser=eraser,
            limiter=await offloader.limiter(
                "lifecycle",
                lifecycle.max_concurrency,
                floor=LIFECYCLE_FLOOR,
                ceiling=limits.max_lifecycle_concurrency,
                requirements=LIFECYCLE_REQUIREMENTS,
            ),
        )
        pipeline = RetrievalPipeline(
            PipelineComponents(
                store=store,
                units=units,
                readers=readers,
                config=retrieval,
                instance_id=self._instance_id,
                deduplicator=deduplicator,
                consolidator=consolidator,
                cache=cache,
                recorder=recorder,
                metrics=metrics,
                logger=self._logger,
                method_fusion=method_fusion,
                channel_fusion=channel_fusion,
                tokenizer=tokenizer,
                embedder=embedder,
                graph_retriever=graph_retriever,
                reranker=reranker,
                nli=nli,
                embedding_cache=embedding_cache,
                schedule_backfill=self._schedule_backfill,
                log_content=config.log_content,
                max_claims=limits.max_claims,
            )
        )
        runtime = _Runtime(
            offloader=offloader,
            store=store,
            units=units,
            coordinator=coordinator,
            ingestion=ingestion,
            eraser=eraser,
            pipeline=pipeline,
            access=access,
            recorder=recorder,
            initialized=wiring.ordered,
            owned=wiring.owned,
        )
        await coordinator.start(
            rebuild=lifecycle.auto_rebuild_ephemeral
            and (not lifecycle.background or lifecycle.wait_for_rebuild),
            rebuild_timeout=lifecycle.rebuild_timeout if lifecycle.background else None,
        )
        return runtime

    def _units(
        self, **backends: tuple[GenerationalIndexABC | None, list[Any]]
    ) -> dict[str, IndexUnit]:
        units: dict[str, IndexUnit] = {}
        for method, (backend, dependencies) in backends.items():
            if backend is None:
                continue
            if method in {Channel.GRAPH.value, Channel.STRUCTURE.value}:
                channel = Channel(method)
            else:
                channel = Channel.INDEX
            unit = make_unit(
                channel,
                method,
                backend,
                instance_id=self._instance_id,
                dependencies=[item for item in dependencies if item is not None],
            )
            units[unit.name] = unit
        return units

    def _default_recorder(self) -> ProvenanceRecorderABC:
        runtime = self._resources.xai_runtime
        if runtime is None:
            return NoOpProvenanceRecorder()
        from retrievalloom.integrations.agxai import AgxaiProvenanceRecorder

        return AgxaiProvenanceRecorder(runtime)

    async def _usable_reranker(
        self, reranker: RerankerABC | None, wiring: _Wiring
    ) -> RerankerABC | None:
        """Drop the engine-owned default reranker when its model is unavailable.

        Results then keep the fused order without a per-request warning; a supplied
        reranker is always kept so its failures surface as ``reranker_unavailable``.
        """
        if reranker is None or id(reranker) not in wiring.owned:
            return reranker
        report = await self._component_health(reranker)
        if report.status is HealthStatus.HEALTHY:
            return reranker
        wiring.ordered.remove(reranker)
        wiring.owned.discard(id(reranker))
        await self._close_components([reranker], {id(reranker)})
        return None

    async def _close_components(self, components: Iterable[ComponentABC], owned: set[int]) -> None:
        for component in reversed(list(components)):
            if id(component) not in owned:
                continue
            try:
                await component.close()
            except Exception as exc:
                self._logger.error(
                    "component_close_failed",
                    component=component.info.name,
                    error_type=type(exc).__name__,
                )

    async def _authorize(
        self, runtime: _Runtime, principal: str | None, scope: Scope, action: Action
    ) -> None:
        decision = await runtime.access.authorize(principal, scope, action)
        if not decision.allowed:
            raise AccessDeniedError(
                "operation denied by the access policy",
                details={"action": action.value, "reason": decision.reason},
            )

    def _tenant_semaphore(self, runtime: _Runtime, tenant: str) -> asyncio.Semaphore:
        semaphore = runtime.semaphores.get(tenant)
        if semaphore is None:
            limit = self._config.retrieval.tenant_policy(tenant).max_concurrent_requests
            semaphore = runtime.semaphores[tenant] = asyncio.Semaphore(limit)
        return semaphore

    def _select(
        self,
        runtime: _Runtime,
        channels: Sequence[Channel | str] | None,
        methods: Sequence[str] | None,
    ) -> list[IndexUnit]:
        try:
            wanted_channels = None if channels is None else {Channel(item) for item in channels}
        except ValueError as exc:
            raise ConfigurationError("unknown channel") from exc
        selected = [
            unit
            for unit in runtime.units.values()
            if (wanted_channels is None or unit.channel in wanted_channels)
            and (methods is None or unit.method in methods)
        ]
        if not selected:
            raise ConfigurationError(
                "no configured index unit matches the selection",
                details={
                    "channels": None if channels is None else [str(item) for item in channels],
                    "methods": None if methods is None else list(methods),
                },
            )
        return selected

    def _spawn(self, coroutine: Awaitable[Any]) -> asyncio.Task[Any]:
        task = asyncio.ensure_future(coroutine)
        self._background.add(task)
        task.add_done_callback(self._background.discard)
        return task

    async def _index_after_write(self, runtime: _Runtime, scope: Scope) -> list[RetrievalWarning]:
        """Index the scope after a write as ``index_wait`` asks; never fails the write."""
        ingestion: IngestionConfig = self._config.ingestion
        if ingestion.index_wait == IndexWait.NONE:
            runtime.coordinator.kick(scope)
            return []
        # The round is not cancelled on timeout: it keeps indexing and close() awaits it.
        round_task = self._spawn(runtime.coordinator.run_once(scope))
        done, _ = await asyncio.wait({round_task}, timeout=ingestion.index_wait_timeout)
        if not done:
            self._logger.warning("index_wait_timeout", timeout=ingestion.index_wait_timeout)
            return [
                RetrievalWarning(
                    code=WarningCode.INDEX_WAIT_TIMEOUT,
                    message="indexing did not finish within index_wait_timeout",
                    details={"timeout": ingestion.index_wait_timeout},
                )
            ]
        exc = round_task.exception()
        if exc is not None:
            self._logger.error("index_round_failed", error_type=type(exc).__name__)
            return [
                RetrievalWarning(
                    code=WarningCode.STAGE_FAILED_DEGRADED,
                    message="indexing after the write failed; it is retried by later rounds",
                    details={"stage": "indexing", "error_type": type(exc).__name__},
                )
            ]
        failed = sorted(
            {
                execution.resource_id
                for execution in round_task.result().executions
                if not execution.succeeded
            }
        )
        if failed:
            return [
                RetrievalWarning(
                    code=WarningCode.STAGE_FAILED_DEGRADED,
                    message="some index units failed; they are retried by later rounds",
                    details={"stage": "indexing", "failed": len(failed)},
                )
            ]
        return []

    async def _record_write[R: (IngestReport, DeleteReport)](
        self, runtime: _Runtime, report: R
    ) -> R:
        try:
            await runtime.recorder.record_ingestion(report)
        except TenantIsolationError:
            raise
        except Exception as exc:
            if self._config.retrieval.fail_on_recording_error:
                raise ProvenanceError("provenance recording failed") from exc
            self._logger.warning("provenance_recording_failed", error_type=type(exc).__name__)
            if isinstance(report, IngestReport):
                warning = RetrievalWarning(
                    code=WarningCode.PROVENANCE_RECORDING_FAILED,
                    message="provenance recording failed",
                    details={"error_type": type(exc).__name__},
                )
                return report.model_copy(update={"warnings": (*report.warnings, warning)})
        return report

    async def _schedule_backfill(self, scope: Scope, units: Sequence[IndexUnit]) -> str | None:
        """Queue one BACKFILL task for ``units`` unless an active task already covers them."""
        runtime = self._runtime
        if runtime is None or self._state != EngineState.OPEN:
            return None
        wanted = {unit.name for unit in units}
        async with runtime.store.transaction(scope, write=False) as transaction:
            tasks = await transaction.list_tasks(scope_key=scope.key, states=_ACTIVE_TASKS)
        for task in tasks:
            if task.kind == TaskKind.BACKFILL and wanted <= set(task_units(task)):
                return task.task_id
        ref = await runtime.coordinator.request(scope, TaskKind.BACKFILL, list(units))
        return ref.task_id

    async def _component_health(self, component: ComponentABC) -> HealthReport:
        name = component.info.name
        try:
            async with asyncio.timeout(self._config.health_timeout):
                return await component.health()
        except Exception as exc:
            return HealthReport(
                component=name,
                status=HealthStatus.UNHEALTHY,
                message="health check failed",
                details={"error_type": type(exc).__name__},
            )


# Optional backends are imported lazily so their extras stay optional.
def _default_vector_store(data_dir: Path | None) -> VectorStoreABC:
    from retrievalloom.index.vector import QdrantVectorStore

    if data_dir is None:
        return QdrantVectorStore()
    return QdrantVectorStore(path=str(data_dir / "qdrant"))


def _default_graph_store(data_dir: Path | None) -> GraphStoreABC:
    from retrievalloom.graph.ladybug_store import LadybugGraphStore

    return LadybugGraphStore(None if data_dir is None else data_dir / "graph" / "graph.lbug")


def _default_graph_retriever(depth: int) -> GraphRetrieverABC:
    from retrievalloom.graph.retriever import NeighborhoodGraphRetriever

    return NeighborhoodGraphRetriever(depth=depth)


def _default_normalizer() -> GraphNormalizerABC:
    from retrievalloom.graph.normalizer import IdentityGraphNormalizer

    return IdentityGraphNormalizer()


def _default_structure_engine(data_dir: Path | None) -> StructureEngineABC:
    from retrievalloom.structure.treesearch_engine import TreeSearchStructureEngine

    return TreeSearchStructureEngine(None if data_dir is None else data_dir / "structure")


def _default_reranker(config: RerankingConfig, data_dir: Path | None) -> RerankerABC:
    from pathlib import Path

    from retrievalloom.evidence.rerank import FlashRankReranker

    model_dir = config.model_dir
    if model_dir is None:
        base = Path.home() / ".cache" / "retrievalloom" if data_dir is None else data_dir
        model_dir = base / "models"
    return FlashRankReranker(
        model_dir, model_name=config.model_name, allow_download=config.allow_download
    )


def create_retrieval_engine(
    *,
    ingestion: IngestionConfig | None = None,
    retrieval: RetrievalConfig | None = None,
    lifecycle: LifecycleConfig | None = None,
    offloader: OffloaderConfig | None = None,
    limits: EngineLimits | None = None,
    health_timeout: float = 5.0,
    components: Components | None = None,
    resources: EngineResources | None = None,
    log_content: bool = False,
    engine_id: str | None = None,
) -> RetrievalEngine:
    """Create an engine (not yet initialised) from configuration layers.

    ``limits`` raises the upper bounds the other settings are validated against.

    Example:
        ```python
        engine = create_retrieval_engine(retrieval=RetrievalConfig(default_top_k=5))
        async with engine:
            await engine.ingest(documents, scope=Scope(tenant_id="acme"))
            result = await engine.retrieve("question", scope=Scope(tenant_id="acme"))
        ```
    """
    config = EngineConfig(
        ingestion=ingestion or IngestionConfig(),
        retrieval=retrieval or RetrievalConfig(),
        lifecycle=lifecycle or LifecycleConfig(),
        offloader=offloader or OffloaderConfig(),
        limits=limits or EngineLimits(),
        health_timeout=health_timeout,
        log_content=log_content,
    )
    return RetrievalEngine(config, components, resources, engine_id=engine_id)


__all__ = ["EngineState", "RetrievalEngine", "create_retrieval_engine"]
