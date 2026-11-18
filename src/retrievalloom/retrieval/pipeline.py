"""Retrieval pipeline: methods, fusion, candidates, dedup, rerank, NLI and consolidation."""

from __future__ import annotations

import asyncio
import math
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, cast

from retrievalloom.contracts.evidence import ConsolidationContext, RankedEntry
from retrievalloom.contracts.indexes import FuzzyQuery, KeywordQuery, SparseQuery
from retrievalloom.contracts.models import NLIPair, RerankItem
from retrievalloom.domain.filters import combine
from retrievalloom.domain.hashing import digest_json
from retrievalloom.domain.records import Channel, IndexMethod
from retrievalloom.domain.retrieval import (
    ResultStatus,
    RetrievalResult,
    RetrievalWarning,
    ScoreSemantics,
    Severity,
    StageRecord,
    WarningCode,
)
from retrievalloom.errors import (
    BackendUnavailableError,
    ComponentContractError,
    ConfigurationError,
    OperationTimeoutError,
    ProvenanceError,
    TenantIsolationError,
)
from retrievalloom.evidence.normalizers import RankNormalizer, make_normalizer
from retrievalloom.evidence.validation import EvidenceValidator
from retrievalloom.retrieval.candidates import CandidateBuilder
from retrievalloom.retrieval.planning import GRAPH_UNIT, resolve_plan, validate_request
from retrievalloom.retrieval.snapshot import SnapshotReader

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from refresh_engine import MetricsSink

    from retrievalloom.config import RetrievalConfig
    from retrievalloom.contracts.component import ComponentABC
    from retrievalloom.contracts.evidence import (
        DeduplicatorABC,
        EvidenceConsolidatorABC,
        FusedEntry,
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
    from retrievalloom.contracts.infra import CacheABC, ProvenanceRecorderABC
    from retrievalloom.contracts.models import NLIABC, RerankerABC, RerankScore, TokenizerABC
    from retrievalloom.domain.filters import FilterExpression
    from retrievalloom.domain.retrieval import Candidate, Evidence, RetrievalHit, RetrievalRequest
    from retrievalloom.index.embedding import EmbeddingValidator
    from retrievalloom.lifecycle.indexer import ReaderTracker
    from retrievalloom.lifecycle.units import IndexUnit
    from retrievalloom.retrieval.planning import ExecutionPlan
    from retrievalloom.retrieval.snapshot import BackfillScheduler, ReadyMethod, Snapshot
    from retrievalloom.storage.base import DocumentMetadataStoreABC

_TOKEN_METHODS = {IndexMethod.FUZZY.value, IndexMethod.SPARSE.value}
_CACHE_NAMESPACE = "retrieval"
_EMBEDDING_NAMESPACE = "query_embedding"

ChannelPositions = dict[str, tuple[int, float | None, ScoreSemantics]]


@dataclass(frozen=True, slots=True, kw_only=True)
class PipelineComponents:
    """Everything the retrieval pipeline reads or calls."""

    store: DocumentMetadataStoreABC
    units: Mapping[str, IndexUnit]
    readers: ReaderTracker
    config: RetrievalConfig
    instance_id: str
    deduplicator: DeduplicatorABC
    consolidator: EvidenceConsolidatorABC
    cache: CacheABC
    recorder: ProvenanceRecorderABC
    metrics: MetricsSink
    logger: Any
    method_fusion: FusionABC | None = None
    channel_fusion: FusionABC | None = None
    tokenizer: TokenizerABC | None = None
    embedder: EmbeddingValidator | None = None
    graph_retriever: GraphRetrieverABC | None = None
    reranker: RerankerABC | None = None
    nli: NLIABC | None = None
    embedding_cache: CacheABC | None = None
    schedule_backfill: BackfillScheduler | None = None
    log_content: bool = False
    max_claims: int = 32


@dataclass(slots=True)
class _Run:
    request: RetrievalRequest
    plan: ExecutionPlan
    filters: FilterExpression | None
    deadline: float
    warnings: list[RetrievalWarning] = field(default_factory=list)
    timings: dict[str, float] = field(default_factory=dict)
    stages: list[StageRecord] = field(default_factory=list)

    def warn(
        self,
        code: WarningCode,
        message: str,
        *,
        severity: Severity = Severity.WARNING,
        channel: Channel | None = None,
        method: str | None = None,
        **details: Any,
    ) -> None:
        self.warnings.append(
            RetrievalWarning(
                code=code,
                severity=severity,
                channel=channel,
                method=method,
                message=message,
                details=details,
            )
        )


def _stage(stage: str, component: ComponentABC) -> StageRecord:
    info = component.info
    return StageRecord(
        stage=stage,
        component_id=info.name,
        component_version=info.version,
        config_fingerprint=info.fingerprint.config_digest,
    )


def _ms(started: float) -> float:
    return round((time.perf_counter() - started) * 1000.0, 3)


def result_status(
    evidence: Sequence[Evidence], warnings: Sequence[RetrievalWarning]
) -> ResultStatus:
    """``empty`` without evidence, ``partial`` with any warning of severity >= warning."""
    if not evidence:
        return ResultStatus.EMPTY
    if any(warning.severity != Severity.INFO for warning in warnings):
        return ResultStatus.PARTIAL
    return ResultStatus.COMPLETE


def _first_leaf(group: BaseExceptionGroup[BaseException]) -> BaseException:
    first = group.exceptions[0]
    return _first_leaf(first) if isinstance(first, BaseExceptionGroup) else first


def _with_scores(candidate: Candidate, **scores: float | None) -> Candidate:
    return candidate.model_copy(update={"scores": candidate.scores.model_copy(update=scores)})


class RetrievalPipeline:
    """Executes one retrieval request against a consistent metadata snapshot.

    Raises from :meth:`retrieve` are typed ``RetrievalLoomError`` subclasses; method,
    reranker and NLI failures degrade into warnings unless their policy is ``fail``.
    """

    def __init__(self, components: PipelineComponents) -> None:
        self._c = components
        methods = {unit.method for unit in components.units.values()}
        if methods & _TOKEN_METHODS and components.tokenizer is None:
            raise ConfigurationError("fuzzy and sparse methods need a tokenizer")
        if IndexMethod.DENSE.value in methods and components.embedder is None:
            raise ConfigurationError("the dense method needs an embedding model")
        if GRAPH_UNIT in components.units and components.graph_retriever is None:
            raise ConfigurationError("the graph channel needs a graph retriever")
        self._snapshots = SnapshotReader(
            components.store,
            components.readers,
            instance_id=components.instance_id,
            schedule_backfill=components.schedule_backfill,
        )
        self._candidates = CandidateBuilder(components.store)
        self._validator = EvidenceValidator()
        # Normalisers are stateless; built once instead of per unit per request.
        self._rank_normalizer = RankNormalizer()
        self._normalizer = make_normalizer(components.config.method_fusion)
        parts: list[ComponentABC | None] = [
            components.method_fusion,
            components.channel_fusion,
            components.deduplicator,
            components.consolidator,
            components.reranker,
            components.nli,
            components.graph_retriever,
            components.embedder.model if components.embedder is not None else None,
        ]
        self._fingerprint = digest_json(
            [
                components.config.model_dump(mode="json"),
                [part.info.fingerprint.digest if part is not None else None for part in parts],
            ]
        )

    # entry point ----------------------------------------------------------------------
    async def retrieve(
        self, request: RetrievalRequest, *, row_filter: FilterExpression | None = None
    ) -> RetrievalResult:
        """Run ``request``; ``row_filter`` is the access policy's mandatory filter."""
        started = time.perf_counter()
        config = self._c.config
        validate_request(request, config, max_claims=self._c.max_claims)
        plan = resolve_plan(
            request,
            config,
            self._c.units,
            method_fusion=self._c.method_fusion,
            channel_fusion=self._c.channel_fusion,
            has_reranker=self._c.reranker is not None,
            has_nli=self._c.nli is not None,
        )
        run = _Run(
            request=request,
            plan=plan,
            filters=combine(request.filters, row_filter),
            deadline=asyncio.get_running_loop().time() + plan.timeout,
            warnings=list(plan.warnings),
        )
        stage_started = time.perf_counter()
        snapshot = await self._snapshots.capture(request.scope, plan)
        run.timings["snapshot"] = _ms(stage_started)
        try:
            if snapshot.active_documents == 0:
                result = RetrievalResult(
                    request_id=request.request_id,
                    scope=request.scope,
                    coverage=snapshot.coverage,
                    snapshot=snapshot.model(),
                    status=ResultStatus.EMPTY,
                )
            else:
                result = await self._cached(run, snapshot)
        finally:
            self._snapshots.release(snapshot)
        result = await self._record(request, result)
        total = _ms(started)
        result = result.model_copy(update={"timings": {**result.timings, "total": total}})
        await self._c.metrics.increment(
            "retrievalloom.retrieval.requests", 1, {"status": result.status.value}
        )
        await self._c.metrics.observe(
            "retrievalloom.retrieval.duration_ms", total, {"status": result.status.value}
        )
        log = {"query": request.query} if self._c.log_content else {}
        self._c.logger.info(
            "retrieval_completed",
            request_id=request.request_id,
            scope=request.scope.key,
            status=result.status.value,
            evidence=len(result.evidence),
            warnings=[warning.code.value for warning in result.warnings],
            duration_ms=total,
            **log,
        )
        return result

    async def _cached(self, run: _Run, snapshot: Snapshot) -> RetrievalResult:
        request = run.request
        key = digest_json(
            [
                request.model_dump(mode="json", exclude={"request_id", "principal"}),
                None if run.filters is None else run.filters.model_dump(mode="json"),
                snapshot.seq,
                snapshot.model().model_dump(mode="json"),
                [warning.model_dump(mode="json") for warning in run.warnings],
                self._fingerprint,
            ]
        )
        namespace = f"{_CACHE_NAMESPACE}:{request.scope.key}"
        cached = await self._c.cache.get(namespace, key)
        if isinstance(cached, RetrievalResult):
            return cached.model_copy(update={"request_id": request.request_id})
        result = await self._execute(run, snapshot)
        if result.status == ResultStatus.COMPLETE:
            await self._c.cache.set(namespace, key, result)
        return result

    async def _record(self, request: RetrievalRequest, result: RetrievalResult) -> RetrievalResult:
        try:
            await self._c.recorder.record_retrieval(request, result)
        except TenantIsolationError:
            raise
        except Exception as exc:
            if self._c.config.fail_on_recording_error:
                raise ProvenanceError("provenance recording failed") from exc
            self._c.logger.warning(
                "provenance_recording_failed",
                request_id=request.request_id,
                error_type=type(exc).__name__,
            )
            warnings = (
                *result.warnings,
                RetrievalWarning(
                    code=WarningCode.PROVENANCE_RECORDING_FAILED,
                    message="provenance recording failed",
                    details={"error_type": type(exc).__name__},
                ),
            )
            return result.model_copy(
                update={"warnings": warnings, "status": result_status(result.evidence, warnings)}
            )
        return result

    # execution ------------------------------------------------------------------------
    async def _execute(self, run: _Run, snapshot: Snapshot) -> RetrievalResult:
        request, plan = run.request, run.plan
        run.warnings.extend(snapshot.warnings)
        hits = await self._run_methods(run, snapshot)

        started = time.perf_counter()
        normalized = {name: await self._normalize(unit_hits) for name, unit_hits in hits.items()}
        index_lists = {
            name: _ranked(unit_hits)
            for name, unit_hits in hits.items()
            if self._c.units[name].channel == Channel.INDEX
        }
        fused_index: Sequence[FusedEntry] = []
        if index_lists:
            fused_index = await plan.method_fusion.fuse(
                index_lists,
                {
                    name: method.weight
                    for name in index_lists
                    if (method := plan.method(name)) is not None
                },
            )
            run.stages.append(_stage("method_fusion", plan.method_fusion))
        run.timings["method_fusion"] = _ms(started)

        started = time.perf_counter()
        batch = await self._candidates.build(
            request.scope,
            hits,
            self._c.units,
            snapshot_seq=snapshot.seq,
            filters=run.filters,
            allow_stale=plan.allow_stale,
            normalized=normalized,
        )
        run.timings["candidates"] = _ms(started)
        self._candidate_warnings(run, snapshot, batch.stale_kept, batch.stale_dropped)
        fused_scores = {entry.item_id: entry.score for entry in fused_index}
        candidates = [
            _with_scores(candidate, method_fusion=fused_scores[candidate.object_id])
            if candidate.object_id in fused_scores
            else candidate
            for candidate in self._post_filter(run, snapshot, hits, batch.candidates)
        ]
        positions = _channel_positions(hits, fused_index, self._c.units)

        started = time.perf_counter()
        ordered = await self._channel_order(run, candidates, positions)
        deduplicated = await self._c.deduplicator.deduplicate(ordered)
        run.stages.append(_stage("dedup", self._c.deduplicator))
        ordered = await self._channel_order(run, list(deduplicated.kept), positions)
        run.stages.append(_stage("channel_fusion", plan.channel_fusion))
        run.timings["fusion"] = _ms(started)

        started = time.perf_counter()
        ordered = await self._rerank(run, ordered)
        run.timings["rerank"] = _ms(started)
        started = time.perf_counter()
        ordered = await self._judge(run, ordered)
        run.timings["nli"] = _ms(started)

        started = time.perf_counter()
        evidence = await self._consolidate(run, ordered)
        run.timings["consolidation"] = _ms(started)
        return RetrievalResult(
            request_id=request.request_id,
            scope=request.scope,
            evidence=tuple(evidence),
            candidates=tuple(ordered) if request.execution.include_candidates else None,
            warnings=tuple(run.warnings),
            coverage=snapshot.coverage,
            snapshot=snapshot.model(),
            timings=run.timings,
            status=result_status(evidence, run.warnings),
        )

    def _candidate_warnings(
        self, run: _Run, snapshot: Snapshot, stale_kept: int, stale_dropped: int
    ) -> None:
        if stale_kept:
            run.warn(
                WarningCode.STALE_DOCUMENT_VERSION,
                "some candidates come from a superseded document version",
                count=stale_kept,
            )
        if stale_dropped:
            run.warn(
                WarningCode.STALE_DOCUMENT_VERSION,
                "hits of superseded document versions were dropped",
                severity=Severity.INFO,
                count=stale_dropped,
            )
        if run.filters is not None:
            post = sorted(
                ready.planned.name
                for ready in snapshot.methods
                if not ready.planned.unit.backend.capabilities.filter_pushdown
            )
            if post:
                run.warn(
                    WarningCode.FILTER_POST_APPLIED,
                    "filters were applied after retrieval for some methods",
                    severity=Severity.INFO,
                    methods=post,
                )

    # methods --------------------------------------------------------------------------
    async def _run_methods(self, run: _Run, snapshot: Snapshot) -> dict[str, list[RetrievalHit]]:
        results: dict[str, list[RetrievalHit]] = {}
        pending = {ready.planned.name for ready in snapshot.methods}
        started = time.perf_counter()
        try:
            async with asyncio.timeout_at(run.deadline):
                tokens = await self._tokens(run, snapshot)
                async with asyncio.TaskGroup() as group:
                    for ready in snapshot.methods:
                        group.create_task(self._method(run, ready, tokens, results, pending))
        except TimeoutError as exc:
            if not self._c.config.allow_partial_on_deadline:
                raise OperationTimeoutError(
                    "retrieval exceeded its deadline", details={"pending": sorted(pending)}
                ) from exc
            for name in sorted(pending):
                unit = self._c.units[name]
                run.warn(
                    WarningCode.TIMEOUT_PARTIAL,
                    "method did not finish before the request deadline",
                    channel=unit.channel,
                    method=unit.method,
                )
        except BaseExceptionGroup as group:
            raise _first_leaf(group) from None
        run.timings["methods"] = _ms(started)
        ordered = {name: results[name] for name in self._c.units if name in results}
        run.stages.extend(_stage(f"method:{name}", self._c.units[name].backend) for name in ordered)
        return ordered

    async def _tokens(self, run: _Run, snapshot: Snapshot) -> tuple[str, ...]:
        if not any(ready.planned.unit.method in _TOKEN_METHODS for ready in snapshot.methods):
            return ()
        tokenizer = self._c.tokenizer
        assert tokenizer is not None
        tokens = await tokenizer.tokenize([run.request.query])
        if len(tokens) != 1:
            raise ComponentContractError("tokenizer must return one token list per text")
        return tuple(tokens[0])

    async def _method(
        self,
        run: _Run,
        ready: ReadyMethod,
        tokens: tuple[str, ...],
        results: dict[str, list[RetrievalHit]],
        pending: set[str],
    ) -> None:
        planned = ready.planned
        unit = planned.unit
        started = time.perf_counter()
        try:
            async with asyncio.timeout(self._c.config.method_timeout):
                hits = await self._search(run, ready, tokens)
            results[planned.name] = self._check_hits(run, ready, hits)
            pending.discard(planned.name)
        except TenantIsolationError:
            raise
        except Exception as exc:
            if planned.fail:
                if isinstance(exc, TimeoutError):
                    raise OperationTimeoutError(
                        "retrieval method timed out", details={"method": planned.name}
                    ) from exc
                raise
            timeout = isinstance(exc, TimeoutError)
            self._c.logger.warning(
                "retrieval_method_failed",
                method=planned.name,
                error_type=type(exc).__name__,
            )
            run.warn(
                WarningCode.TIMEOUT_PARTIAL if timeout else WarningCode.STAGE_FAILED_DEGRADED,
                "method timed out" if timeout else "method failed and was skipped",
                channel=unit.channel,
                method=unit.method,
                error_type=type(exc).__name__,
            )
            pending.discard(planned.name)
        finally:
            # A method cancelled by the request deadline stays pending so it is reported.
            run.timings[f"method:{planned.name}"] = _ms(started)
            await self._c.metrics.observe(
                "retrievalloom.retrieval.method_ms",
                run.timings[f"method:{planned.name}"],
                {"method": planned.name},
            )

    async def _search(
        self, run: _Run, ready: ReadyMethod, tokens: tuple[str, ...]
    ) -> Sequence[RetrievalHit]:
        request = run.request
        planned = ready.planned
        unit = planned.unit
        scope, generation = request.scope, ready.generation_id
        limit = self._fetch_limit(run, ready)
        filters = run.filters if unit.backend.capabilities.filter_pushdown else None
        match unit.method:
            case IndexMethod.KEYWORD.value:
                backend = cast("KeywordIndexABC", unit.backend)
                return await backend.search(
                    scope, generation, KeywordQuery(text=request.query), limit, filters
                )
            case IndexMethod.FUZZY.value:
                backend = cast("FuzzyIndexABC", unit.backend)
                return await backend.search(
                    scope, generation, FuzzyQuery(tokens=tokens), limit, filters
                )
            case IndexMethod.SPARSE.value:
                backend = cast("SparseIndexABC", unit.backend)
                return await backend.search(
                    scope,
                    generation,
                    SparseQuery(tokens=tokens, text=request.query),
                    limit,
                    filters,
                )
            case IndexMethod.DENSE.value:
                backend = cast("VectorStoreABC", unit.backend)
                vector = await self._query_vector(request)
                return await backend.search(scope, generation, vector, limit, filters)
            case Channel.GRAPH.value:
                retriever = self._c.graph_retriever
                assert retriever is not None
                store = cast("GraphStoreABC", unit.backend)
                return await retriever.retrieve(scope, generation, request.query, store, limit)
            case _:
                backend = cast("StructureEngineABC", unit.backend)
                return await backend.search(scope, generation, request.query, limit, filters)

    async def _query_vector(self, request: RetrievalRequest) -> tuple[float, ...]:
        embedder = self._c.embedder
        assert embedder is not None
        cache = self._c.embedding_cache
        namespace = f"{_EMBEDDING_NAMESPACE}:{request.scope.tenant_id}"
        key = digest_json([embedder.model.info.fingerprint.digest, request.query])
        if cache is not None:
            cached = await cache.get(namespace, key)
            if isinstance(cached, tuple):
                return cast("tuple[float, ...]", cached)
        vector = await embedder.embed_query(request.query, tenant=request.scope.tenant_id)
        if cache is not None:
            await cache.set(namespace, key, vector)
        return vector

    def _check_hits(
        self, run: _Run, ready: ReadyMethod, hits: Sequence[RetrievalHit]
    ) -> list[RetrievalHit]:
        unit = ready.planned.unit
        kept: list[RetrievalHit] = []
        seen: set[str] = set()
        for hit in sorted(hits, key=lambda item: item.rank):
            if hit.object_ref.scope_key != run.request.scope.key:
                raise TenantIsolationError(
                    "backend returned a hit of another scope", details={"method": unit.name}
                )
            if (
                hit.object_ref.generation_id != ready.generation_id
                or hit.method.channel != unit.channel
            ):
                raise ComponentContractError(
                    "backend returned a hit of another generation or channel",
                    details={"method": unit.name},
                )
            if hit.object_ref.object_id not in seen:
                seen.add(hit.object_ref.object_id)
                kept.append(hit)
        return kept[: self._fetch_limit(run, ready)]

    def _fetch_limit(self, run: _Run, ready: ReadyMethod) -> int:
        """Hits to request: ``candidate_k``, over-fetched when filters apply afterwards."""
        planned = ready.planned
        if run.filters is not None and not planned.unit.backend.capabilities.filter_pushdown:
            return planned.candidate_k * self._c.config.filter_overfetch
        return planned.candidate_k

    def _post_filter(
        self,
        run: _Run,
        snapshot: Snapshot,
        hits: Mapping[str, Sequence[RetrievalHit]],
        candidates: list[Candidate],
    ) -> list[Candidate]:
        """Keep each over-fetching method's best ``candidate_k`` survivors.

        Warns with ``FILTER_UNDERFETCH`` when a method returned its full over-fetched
        window yet fewer than ``candidate_k`` of its hits passed the filter.
        """
        if run.filters is None:
            return candidates
        survivors = {candidate.object_id for candidate in candidates}
        allowed: set[str] = set()
        short: list[str] = []
        for ready in snapshot.methods:
            name = ready.planned.name
            unit_hits = hits.get(name)
            if unit_hits is None:
                continue
            kept = [
                hit.object_ref.object_id
                for hit in unit_hits
                if hit.object_ref.object_id in survivors
            ]
            k = ready.planned.candidate_k
            allowed.update(kept[:k])
            limit = self._fetch_limit(run, ready)
            if limit > k and len(unit_hits) >= limit and len(kept) < k:
                short.append(name)
        if short:
            run.warn(
                WarningCode.FILTER_UNDERFETCH,
                "a selective filter left fewer than candidate_k candidates for some methods",
                severity=Severity.INFO,
                methods=sorted(short),
                filter_overfetch=self._c.config.filter_overfetch,
                hint="raise RetrievalConfig.filter_overfetch or use a backend with filter pushdown",
            )
        return [candidate for candidate in candidates if candidate.object_id in allowed]

    async def _normalize(self, hits: Sequence[RetrievalHit]) -> dict[str, float]:
        entries = _ranked(hits)
        if any(entry.score is None or not math.isfinite(entry.score) for entry in entries):
            return dict(await self._rank_normalizer.normalize(entries))
        return dict(await self._normalizer.normalize(entries))

    # fusion ---------------------------------------------------------------------------
    async def _channel_order(
        self,
        run: _Run,
        candidates: Sequence[Candidate],
        positions: Mapping[str, ChannelPositions],
    ) -> list[Candidate]:
        """Order candidates by channel fusion of their best per-channel ranks."""
        lists: dict[str, list[RankedEntry]] = {}
        for channel, channel_positions in positions.items():
            best: list[tuple[int, float | None, ScoreSemantics, str]] = []
            for candidate in candidates:
                found = [
                    channel_positions[object_id]
                    for object_id in (candidate.object_id, *candidate.merged_object_ids)
                    if object_id in channel_positions
                ]
                if found:
                    rank, score, semantics = min(found, key=lambda item: item[0])
                    best.append((rank, score, semantics, candidate.candidate_id))
            best.sort(key=lambda item: (item[0], item[3]))
            lists[channel] = [
                RankedEntry(item_id=item_id, rank=position, score=score, semantics=semantics)
                for position, (_, score, semantics, item_id) in enumerate(best, start=1)
            ]
        fused = await run.plan.channel_fusion.fuse(lists, run.plan.channel_weights)
        by_id = {candidate.candidate_id: candidate for candidate in candidates}
        ordered = [
            _with_scores(by_id[entry.item_id], channel_fusion=entry.score)
            for entry in fused
            if entry.item_id in by_id
        ]
        if len(ordered) != len(candidates):
            raise ComponentContractError("channel fusion must return every candidate once")
        return ordered

    # rerank and NLI -------------------------------------------------------------------
    async def _rerank(self, run: _Run, ordered: list[Candidate]) -> list[Candidate]:
        def unranked() -> list[Candidate]:
            return [_with_scores(c, final=c.scores.channel_fusion) for c in ordered]

        reranker = self._c.reranker
        if not run.plan.reranking or reranker is None:
            return unranked()
        targets = [c for c in ordered if c.text][: self._c.config.reranking.rerank_k]
        if not targets:
            return unranked()
        try:
            async with asyncio.timeout_at(run.deadline):
                scores = await reranker.rerank(
                    run.request.query,
                    [RerankItem(item_id=c.candidate_id, text=c.text or "") for c in targets],
                )
            mapping = _rerank_scores(scores, targets)
        except Exception as exc:
            self._degrade(run, "rerank", exc, fail=run.plan.reranking_fail)
            return unranked()
        run.stages.append(_stage("rerank", reranker))
        position = {c.candidate_id: index for index, c in enumerate(ordered)}
        reranked = sorted(
            targets, key=lambda c: (-mapping[c.candidate_id], position[c.candidate_id])
        )
        floor = min(mapping.values())
        rest = [c for c in ordered if c.candidate_id not in mapping]
        return [
            _with_scores(c, rerank=mapping[c.candidate_id], final=mapping[c.candidate_id])
            for c in reranked
        ] + [_with_scores(c, final=floor - (index + 1)) for index, c in enumerate(rest)]

    async def _judge(self, run: _Run, ordered: list[Candidate]) -> list[Candidate]:
        model = self._c.nli
        if not run.plan.nli or model is None:
            return ordered
        targets = [c for c in ordered if c.text][: self._c.config.nli.nli_k]
        if not targets:
            return ordered
        request = run.request
        hypotheses = [("query", request.query)] + [
            (f"claim:{index}", claim) for index, claim in enumerate(request.claims)
        ]
        pairs = [
            NLIPair(
                pair_id=f"{c.candidate_id}:{ref}",
                premise=c.text or "",
                hypothesis=text,
                hypothesis_ref=ref,
            )
            for c in targets
            for ref, text in hypotheses
        ]
        try:
            async with asyncio.timeout_at(run.deadline):
                judgements = list(await model.judge(pairs))
            if len(judgements) != len(pairs) or any(
                judgement.hypothesis_ref != pair.hypothesis_ref
                for judgement, pair in zip(judgements, pairs, strict=False)
            ):
                raise ComponentContractError("NLI must return one aligned judgement per pair")
        except Exception as exc:
            self._degrade(run, "nli", exc, fail=run.plan.nli_fail)
            return ordered
        run.stages.append(_stage("nli", model))
        size = len(hypotheses)
        judged = {
            c.candidate_id: tuple(judgements[index * size : (index + 1) * size])
            for index, c in enumerate(targets)
        }
        return [
            c.model_copy(update={"nli": judged[c.candidate_id]}) if c.candidate_id in judged else c
            for c in ordered
        ]

    def _degrade(self, run: _Run, stage: str, exc: Exception, *, fail: bool) -> None:
        if isinstance(exc, TenantIsolationError):
            raise exc
        if fail:
            if isinstance(exc, TimeoutError):
                raise OperationTimeoutError(
                    f"{stage} exceeded the request deadline", details={"stage": stage}
                ) from exc
            raise exc
        self._c.logger.warning(
            "retrieval_stage_degraded", stage=stage, error_type=type(exc).__name__
        )
        code = WarningCode.STAGE_FAILED_DEGRADED
        if stage == "rerank" and isinstance(exc, BackendUnavailableError):
            code = WarningCode.RERANKER_UNAVAILABLE
        elif isinstance(exc, TimeoutError):
            code = WarningCode.TIMEOUT_PARTIAL
        run.warn(
            code, f"{stage} failed and was skipped", stage=stage, error_type=type(exc).__name__
        )

    # consolidation --------------------------------------------------------------------
    async def _consolidate(self, run: _Run, ordered: list[Candidate]) -> list[Evidence]:
        config = self._c.config.consolidation
        top_k = run.plan.top_k
        window = ordered[: top_k * config.window_multiplier]
        if not window:
            return []
        embeddings: dict[str, tuple[float, ...]] | None = None
        if config.semantic_relations and self._c.embedder is not None:
            texted = [c for c in window if c.text]
            vectors = await self._c.embedder.embed_documents(
                [c.text or "" for c in texted], tenant=run.request.scope.tenant_id
            )
            embeddings = {c.candidate_id: v for c, v in zip(texted, vectors, strict=True)}
        context = ConsolidationContext(
            top_k=top_k,
            contradiction_overflow=(
                config.contradiction_overflow
                if config.contradiction_overflow is not None
                else math.ceil(top_k / 2)
            ),
            group_structural=config.group_structural,
            merge_adjacent=config.merge_adjacent,
            span_iou_threshold=config.span_iou_threshold,
            semantic_relations=config.semantic_relations,
            semantic_threshold=config.semantic_threshold,
            embeddings=embeddings,
        )
        evidence, truncated = await self._c.consolidator.consolidate(window, context)
        if truncated:
            run.warn(
                WarningCode.EVIDENCE_TRUNCATED_CONTRADICTIONS,
                "contradicting evidence beyond the overflow budget was omitted",
                severity=Severity.INFO,
                evidence_ids=list(truncated),
            )
        stages = tuple(run.stages)
        evidence = [
            item.model_copy(
                update={
                    "provenance": item.provenance.model_copy(
                        update={"stage_trail": stages + item.provenance.stage_trail}
                    )
                }
            )
            for item in evidence
        ]
        self._validator.validate(run.request.scope, evidence, window)
        return evidence


def _ranked(hits: Sequence[RetrievalHit]) -> list[RankedEntry]:
    return [
        RankedEntry(
            item_id=hit.object_ref.object_id,
            rank=position,
            score=hit.raw_score,
            semantics=hit.score_semantics,
        )
        for position, hit in enumerate(hits, start=1)
    ]


def _channel_positions(
    hits: Mapping[str, Sequence[RetrievalHit]],
    fused_index: Sequence[FusedEntry],
    units: Mapping[str, IndexUnit],
) -> dict[str, ChannelPositions]:
    """Per channel: object id to ``(rank, score, semantics)``."""
    positions: dict[str, ChannelPositions] = {}
    if fused_index:
        positions[Channel.INDEX.value] = {
            entry.item_id: (rank, entry.score, ScoreSemantics())
            for rank, entry in enumerate(fused_index, start=1)
        }
    for name, unit_hits in hits.items():
        channel = units[name].channel
        if channel == Channel.INDEX or not unit_hits:
            continue
        positions[channel.value] = {
            hit.object_ref.object_id: (rank, hit.raw_score, hit.score_semantics)
            for rank, hit in enumerate(unit_hits, start=1)
        }
    return positions


def _rerank_scores(scores: Sequence[RerankScore], targets: Sequence[Candidate]) -> dict[str, float]:
    expected = {c.candidate_id for c in targets}
    mapping: dict[str, float] = {}
    for score in scores:
        if score.item_id not in expected or score.item_id in mapping:
            raise ComponentContractError("reranker returned an unknown or repeated item")
        if not math.isfinite(score.score):
            raise ComponentContractError("reranker returned a non-finite score")
        mapping[score.item_id] = score.score
    if len(mapping) != len(expected):
        raise ComponentContractError("reranker must score every item")
    return mapping


__all__ = ["PipelineComponents", "RetrievalPipeline", "result_status"]
