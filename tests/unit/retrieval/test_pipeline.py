"""Retrieval pipeline stages: method execution, fusion, rerank, NLI, consolidation, caching."""

from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import functools
import math
from typing import TYPE_CHECKING, Any

import pytest

from retrievalloom import (
    BackendUnavailableError,
    ChannelsConfig,
    ComponentContractError,
    Components,
    ConfigurationError,
    ConsolidationConfig,
    DocumentInput,
    Eq,
    FuzzyConfig,
    GraphChannelConfig,
    IndexChannelConfig,
    IndexWait,
    IngestionConfig,
    MethodConfig,
    NLIConfig,
    OperationTimeoutError,
    ProvenanceError,
    RerankingConfig,
    ResultStatus,
    RetrievalConfig,
    RetrievalExecutionConfig,
    Scope,
    Severity,
    StructureChannelConfig,
    StructureNodeInput,
    StructureRepresentation,
    TenantIsolationError,
    TextRepresentation,
    WarningCode,
)
from retrievalloom.domain.records import Channel
from retrievalloom.evidence.consolidation import HierarchicalConsolidator
from retrievalloom.evidence.fusion import RRFFusion
from retrievalloom.retrieval.pipeline import RetrievalPipeline
from tests.fakes import CapitalizedExtractor, HashEmbedding, OverlapReranker, RuleNLI
from tests.unit.retrieval.helpers import (
    ACME,
    GLOBEX,
    DictCache,
    RecordingRecorder,
    chunks_doc,
    opened,
    runtime,
    text_doc,
    unit,
)

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Callable, Mapping, Sequence

    from retrievalloom import RetrievalEngine, RetrievalResult
    from retrievalloom.contracts.evidence import FusedEntry, RankedEntry
    from retrievalloom.contracts.models import NLIPair, RerankItem, RerankScore
    from retrievalloom.domain.retrieval import NLIJudgement, RetrievalHit

KEYWORD = "index.keyword"
FUZZY = "index.fuzzy"
SPARSE = "index.sparse"

DOCS = [
    text_doc(
        "d1", "Python asyncio event loops schedule coroutines.", lang="en", keywords=["asyncio"]
    ),
    chunks_doc(
        "d2",
        "Qdrant stores dense vectors for asyncio services.",
        "BM25 ranks sparse tokens for asyncio.",
        lang="de",
        keywords=["asyncio", "qdrant"],
    ),
    text_doc("d3", "Rust tokio runtime drives asyncio style futures.", lang="en"),
]


@contextlib.asynccontextmanager
async def ingested(**kwargs: Any) -> AsyncGenerator[RetrievalEngine]:
    async with opened(**kwargs) as engine:
        await engine.ingest(DOCS, scope=ACME)
        yield engine


def _codes(result: RetrievalResult) -> set[WarningCode]:
    return {warning.code for warning in result.warnings}


def _warning(result: RetrievalResult, code: WarningCode) -> Any:
    return next(warning for warning in result.warnings if warning.code == code)


def _patch_search(
    monkeypatch: pytest.MonkeyPatch,
    engine: RetrievalEngine,
    name: str,
    transform: Callable[[Sequence[RetrievalHit]], Sequence[RetrievalHit]] | None = None,
    *,
    error: BaseException | None = None,
    sleep: float = 0.0,
) -> None:
    backend = unit(engine, name).backend
    original = functools.partial(type(backend).search, backend)

    async def search(*args: Any, **kwargs: Any) -> Sequence[RetrievalHit]:
        if sleep:
            await asyncio.sleep(sleep)
        if error is not None:
            raise error
        hits = await original(*args, **kwargs)
        return hits if transform is None else transform(hits)

    monkeypatch.setattr(backend, "search", search)


def _patch_candidates(
    monkeypatch: pytest.MonkeyPatch, engine: RetrievalEngine, **updates: Any
) -> None:
    """Rewrite every built candidate batch (text removal, stale counters)."""
    builder = runtime(engine).pipeline._candidates
    original = builder.build
    text = updates.pop("text", ...)

    async def build(*args: Any, **kwargs: Any) -> Any:
        batch = await original(*args, **kwargs)
        if text is not ...:
            batch.candidates = [c.model_copy(update={"text": text}) for c in batch.candidates]
        for key, value in updates.items():
            setattr(batch, key, value)
        return batch

    monkeypatch.setattr(builder, "build", build)


# construction ---------------------------------------------------------------------------
async def test_constructor_rejects_missing_components() -> None:
    async with opened(components=Components(embedding=HashEmbedding())) as engine:
        parts = runtime(engine).pipeline._c
        with pytest.raises(ConfigurationError, match="tokenizer"):
            RetrievalPipeline(dataclasses.replace(parts, tokenizer=None))
        with pytest.raises(ConfigurationError, match="embedding"):
            RetrievalPipeline(dataclasses.replace(parts, embedder=None))
        dense_only = {"index.dense": unit(engine, "index.dense")}
        RetrievalPipeline(dataclasses.replace(parts, units=dense_only, tokenizer=None))


# entry point, caching, provenance ----------------------------------------------------------
async def test_empty_scope_is_empty_result() -> None:
    async with ingested() as engine:
        result = await engine.retrieve("asyncio", scope=Scope(tenant_id="x"))
    assert result.status == ResultStatus.EMPTY
    assert result.evidence == ()


async def test_no_matching_candidates_is_empty() -> None:
    async with ingested() as engine:
        result = await engine.retrieve("zzzqqq", scope=ACME)
    assert result.status == ResultStatus.EMPTY


async def test_complete_results_are_cached_and_partial_ones_are_not() -> None:
    cache = DictCache()
    async with ingested(components=Components(cache=cache)) as engine:
        first = await engine.retrieve("asyncio", scope=ACME, request_id="a")
        second = await engine.retrieve("asyncio", scope=ACME, request_id="b")
        assert cache.hits == 1
        partial_config = RetrievalExecutionConfig(dense=True)
        partial = await engine.retrieve("asyncio", scope=ACME, config=partial_config)
        await engine.retrieve("asyncio", scope=ACME, config=partial_config)
    assert first.status == ResultStatus.COMPLETE
    assert second.request_id == "b"
    assert second.evidence == first.evidence
    assert partial.status == ResultStatus.PARTIAL
    assert cache.hits == 1


async def test_recorder_receives_every_retrieval() -> None:
    recorder = RecordingRecorder()
    async with ingested(components=Components(provenance_recorder=recorder)) as engine:
        result = await engine.retrieve("asyncio", scope=ACME, request_id="r1")
    [(request, recorded)] = recorder.retrievals
    assert request.request_id == "r1"
    assert recorded.evidence == result.evidence
    assert len(recorder.ingestions) == 1


async def test_recorder_failure_degrades_or_fails() -> None:
    recorder = RecordingRecorder()
    async with ingested(components=Components(provenance_recorder=recorder)) as engine:
        recorder.error = RuntimeError("down")
        result = await engine.retrieve("asyncio", scope=ACME)
        recorder.error = TenantIsolationError("leak")
        with pytest.raises(TenantIsolationError):
            await engine.retrieve("asyncio", scope=ACME)
    assert result.status == ResultStatus.PARTIAL
    assert _warning(result, WarningCode.PROVENANCE_RECORDING_FAILED).details == {
        "error_type": "RuntimeError"
    }

    strict = RetrievalConfig(fail_on_recording_error=True)
    recorder = RecordingRecorder()
    async with ingested(retrieval=strict, components=Components(provenance_recorder=recorder)) as e:
        recorder.error = RuntimeError("down")
        with pytest.raises(ProvenanceError):
            await e.retrieve("asyncio", scope=ACME)


# method execution --------------------------------------------------------------------------
async def test_request_deadline_reports_pending_methods(monkeypatch: pytest.MonkeyPatch) -> None:
    config = RetrievalExecutionConfig(timeout=0.3)
    async with ingested() as engine:
        _patch_search(monkeypatch, engine, KEYWORD, sleep=5)
        result = await engine.retrieve("asyncio", scope=ACME, config=config)
    timeout = _warning(result, WarningCode.TIMEOUT_PARTIAL)
    assert (timeout.channel, timeout.method) == (Channel.INDEX, "keyword")
    assert result.status == ResultStatus.PARTIAL
    assert KEYWORD not in {h.method.key for e in result.evidence for h in e.provenance.hits}

    strict = RetrievalConfig(allow_partial_on_deadline=False)
    async with ingested(retrieval=strict) as engine:
        _patch_search(monkeypatch, engine, KEYWORD, sleep=5)
        with pytest.raises(OperationTimeoutError) as caught:
            await engine.retrieve("asyncio", scope=ACME, config=config)
    assert caught.value.details == {"pending": [KEYWORD]}


async def test_method_timeout_degrades_or_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    config = RetrievalConfig(method_timeout=0.05)
    async with ingested(retrieval=config) as engine:
        _patch_search(monkeypatch, engine, KEYWORD, sleep=5)
        result = await engine.retrieve("asyncio", scope=ACME)
    warning = _warning(result, WarningCode.TIMEOUT_PARTIAL)
    assert warning.details["error_type"] == "TimeoutError"
    assert result.evidence

    failing = config.model_copy(
        update={
            "channels": ChannelsConfig(
                index=IndexChannelConfig(keyword=MethodConfig(on_error="fail"))
            )
        }
    )
    async with ingested(retrieval=failing) as engine:
        _patch_search(monkeypatch, engine, KEYWORD, sleep=5)
        with pytest.raises(OperationTimeoutError) as caught:
            await engine.retrieve("asyncio", scope=ACME)
    assert caught.value.details == {"method": KEYWORD}


async def test_method_errors_degrade_or_propagate(monkeypatch: pytest.MonkeyPatch) -> None:
    async with ingested() as engine:
        _patch_search(monkeypatch, engine, SPARSE, error=RuntimeError("boom"))
        result = await engine.retrieve("asyncio", scope=ACME)
        degraded = _warning(result, WarningCode.STAGE_FAILED_DEGRADED)
        assert (degraded.method, degraded.details) == ("sparse", {"error_type": "RuntimeError"})
        assert result.status == ResultStatus.PARTIAL

        _patch_search(monkeypatch, engine, SPARSE, error=TenantIsolationError("leak"))
        with pytest.raises(TenantIsolationError):
            await engine.retrieve("asyncio", scope=ACME)

    failing = RetrievalConfig(
        channels=ChannelsConfig(index=IndexChannelConfig(fuzzy=FuzzyConfig(on_error="fail")))
    )
    async with ingested(retrieval=failing) as engine:
        _patch_search(monkeypatch, engine, FUZZY, error=RuntimeError("boom"))
        with pytest.raises(RuntimeError, match="boom"):
            await engine.retrieve("asyncio", scope=ACME)
        nested = ExceptionGroup("outer", [ExceptionGroup("inner", [KeyError("leaf")])])
        _patch_search(monkeypatch, engine, FUZZY, error=nested)
        with pytest.raises(KeyError, match="leaf"):
            await engine.retrieve("asyncio", scope=ACME)


async def test_tokenizer_must_return_one_list(monkeypatch: pytest.MonkeyPatch) -> None:
    async with ingested() as engine:
        tokenizer = runtime(engine).pipeline._c.tokenizer

        async def tokenize(texts: Sequence[str]) -> list[list[str]]:
            return [list(texts), list(texts)]

        monkeypatch.setattr(tokenizer, "tokenize", tokenize)
        with pytest.raises(ComponentContractError):
            await engine.retrieve("asyncio", scope=ACME)
        # keyword alone needs no tokens
        only_keyword = RetrievalExecutionConfig(fuzzy=False, sparse=False)
        assert (await engine.retrieve("asyncio", scope=ACME, config=only_keyword)).evidence


async def test_hit_contract_checks(monkeypatch: pytest.MonkeyPatch) -> None:
    async with ingested() as engine:

        def other_scope(hits: Sequence[RetrievalHit]) -> list[RetrievalHit]:
            return [
                h.model_copy(
                    update={"object_ref": h.object_ref.model_copy(update={"scope_key": "x"})}
                )
                for h in hits
            ]

        _patch_search(monkeypatch, engine, KEYWORD, other_scope)
        with pytest.raises(TenantIsolationError):
            await engine.retrieve("asyncio", scope=ACME)

        def other_generation(hits: Sequence[RetrievalHit]) -> list[RetrievalHit]:
            ref = {"generation_id": "gen-x"}
            return [
                h.model_copy(update={"object_ref": h.object_ref.model_copy(update=ref)})
                for h in hits
            ]

        _patch_search(monkeypatch, engine, KEYWORD, other_generation)
        result = await engine.retrieve("asyncio", scope=ACME)
        assert _warning(result, WarningCode.STAGE_FAILED_DEGRADED).details == {
            "error_type": "ComponentContractError"
        }

        def other_channel(hits: Sequence[RetrievalHit]) -> list[RetrievalHit]:
            method = {"channel": Channel.GRAPH}
            return [
                h.model_copy(update={"method": h.method.model_copy(update=method)}) for h in hits
            ]

        _patch_search(monkeypatch, engine, KEYWORD, other_channel)
        result = await engine.retrieve("asyncio", scope=ACME)
        assert WarningCode.STAGE_FAILED_DEGRADED in _codes(result)


async def test_duplicate_hits_are_dropped_and_lists_truncated(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    limited = RetrievalConfig(
        channels=ChannelsConfig(index=IndexChannelConfig(keyword=MethodConfig(candidate_k=2)))
    )
    async with ingested(retrieval=limited) as engine:

        def duplicated(hits: Sequence[RetrievalHit]) -> list[RetrievalHit]:
            # the first hit again (rank 1 ties) ahead of everything else
            return [hits[0], *hits]

        _patch_search(monkeypatch, engine, KEYWORD, duplicated)
        result = await engine.retrieve(
            "asyncio", scope=ACME, config=RetrievalExecutionConfig(include_candidates=True)
        )
    assert result.candidates is not None
    keyword_ids = [
        h.object_ref.object_id for c in result.candidates for h in c.hits if h.method.key == KEYWORD
    ]
    assert len(keyword_ids) == len(set(keyword_ids)) == 2


async def test_missing_scores_fall_back_to_ranks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with ingested() as engine:

        def unscored(hits: Sequence[RetrievalHit]) -> list[RetrievalHit]:
            return [h.model_copy(update={"raw_score": None}) for h in hits]

        _patch_search(monkeypatch, engine, KEYWORD, unscored)
        result = await engine.retrieve(
            "asyncio", scope=ACME, config=RetrievalExecutionConfig(include_candidates=True)
        )
    assert result.candidates is not None
    keyword = [
        c.scores.normalized[KEYWORD] for c in result.candidates if KEYWORD in c.scores.normalized
    ]
    assert keyword
    assert all(0.0 <= score <= 1.0 for score in keyword)


# fusion -------------------------------------------------------------------------------------
class _DroppingFusion(RRFFusion):
    async def fuse(
        self, lists: Mapping[str, Sequence[RankedEntry]], weights: Mapping[str, float]
    ) -> Sequence[FusedEntry]:
        return list(await super().fuse(lists, weights))[:-1]


async def test_channel_fusion_must_return_every_candidate() -> None:
    async with ingested(components=Components(channel_fusion=_DroppingFusion())) as engine:
        with pytest.raises(ComponentContractError):
            await engine.retrieve("asyncio", scope=ACME)


# rerank -------------------------------------------------------------------------------------
class _ScriptedReranker(OverlapReranker):
    def __init__(self, script: Callable[[list[RerankScore]], Sequence[RerankScore]]) -> None:
        self.script = script

    async def rerank(self, query: str, items: Sequence[RerankItem]) -> Sequence[RerankScore]:
        return self.script(list(await super().rerank(query, items)))


def _raise(error: BaseException) -> Callable[[list[RerankScore]], Sequence[RerankScore]]:
    def script(scores: list[RerankScore]) -> Sequence[RerankScore]:
        raise error

    return script


async def test_rerank_orders_targets_and_floors_the_rest() -> None:
    config = RetrievalConfig(reranking=RerankingConfig(rerank_k=1))
    components = Components(reranker=OverlapReranker())
    async with ingested(retrieval=config, components=components) as engine:
        result = await engine.retrieve(
            "asyncio services", scope=ACME, config=RetrievalExecutionConfig(include_candidates=True)
        )
    assert result.candidates is not None
    [top, *rest] = result.candidates
    assert top.scores.rerank is not None
    assert rest
    assert all(c.scores.rerank is None for c in rest)
    finals = [c.scores.final or 0.0 for c in result.candidates]
    assert all(c.scores.final is not None for c in result.candidates)
    assert finals == sorted(finals, reverse=True)
    assert "rerank" in [s.stage for s in result.evidence[0].provenance.stage_trail]


async def test_rerank_skipped_without_text(monkeypatch: pytest.MonkeyPatch) -> None:
    components = Components(reranker=OverlapReranker(), nli=RuleNLI())
    config = RetrievalConfig(nli=NLIConfig(enabled=True))
    async with ingested(retrieval=config, components=components) as engine:
        _patch_candidates(monkeypatch, engine, text=None)
        result = await engine.retrieve(
            "asyncio", scope=ACME, config=RetrievalExecutionConfig(include_candidates=True)
        )
    assert result.candidates
    assert all(c.scores.rerank is None and c.nli == () for c in result.candidates)
    assert all(c.scores.final == c.scores.channel_fusion for c in result.candidates)


@pytest.mark.parametrize(
    ("error", "code"),
    [
        (RuntimeError("down"), WarningCode.STAGE_FAILED_DEGRADED),
        (BackendUnavailableError("down"), WarningCode.RERANKER_UNAVAILABLE),
        (TimeoutError(), WarningCode.TIMEOUT_PARTIAL),
    ],
)
async def test_rerank_errors_degrade(error: BaseException, code: WarningCode) -> None:
    components = Components(reranker=_ScriptedReranker(_raise(error)))
    async with ingested(components=components) as engine:
        result = await engine.retrieve("asyncio", scope=ACME)
    warning = _warning(result, code)
    assert warning.details == {"stage": "rerank", "error_type": type(error).__name__}
    assert result.evidence


@pytest.mark.parametrize(
    "script",
    [
        lambda scores: [*scores, scores[0]],
        lambda scores: [s.model_copy(update={"item_id": "unknown"}) for s in scores],
        lambda scores: [s.model_copy(update={"score": math.nan}) for s in scores],
        lambda scores: scores[1:],
    ],
    ids=["repeated", "unknown", "nan", "missing"],
)
async def test_rerank_contract_violations_degrade(
    script: Callable[[list[RerankScore]], Sequence[RerankScore]],
) -> None:
    async with ingested(components=Components(reranker=_ScriptedReranker(script))) as engine:
        result = await engine.retrieve("asyncio", scope=ACME)
    assert _warning(result, WarningCode.STAGE_FAILED_DEGRADED).details == {
        "stage": "rerank",
        "error_type": "ComponentContractError",
    }


async def test_rerank_fail_policy_raises() -> None:
    config = RetrievalConfig(reranking=RerankingConfig(on_error="fail"))
    reranker = _ScriptedReranker(_raise(TimeoutError()))
    async with ingested(retrieval=config, components=Components(reranker=reranker)) as engine:
        with pytest.raises(OperationTimeoutError, match="rerank"):
            await engine.retrieve("asyncio", scope=ACME)
        reranker.script = _raise(RuntimeError("down"))
        with pytest.raises(RuntimeError, match="down"):
            await engine.retrieve("asyncio", scope=ACME)
    async with ingested(components=Components(reranker=reranker)) as engine:
        reranker.script = _raise(TenantIsolationError("leak"))
        with pytest.raises(TenantIsolationError):
            await engine.retrieve("asyncio", scope=ACME)


# NLI ----------------------------------------------------------------------------------------
class _ScriptedNLI(RuleNLI):
    def __init__(self, script: Callable[[list[NLIJudgement]], Sequence[NLIJudgement]]) -> None:
        self.script = script

    async def judge(self, pairs: Sequence[NLIPair]) -> Sequence[NLIJudgement]:
        return self.script(list(await super().judge(pairs)))


async def test_nli_judges_targets_for_query_and_claims() -> None:
    config = RetrievalConfig(nli=NLIConfig(enabled=True, nli_k=1))
    async with ingested(retrieval=config, components=Components(nli=RuleNLI())) as engine:
        result = await engine.retrieve(
            "asyncio",
            scope=ACME,
            claims=["asyncio loops"],
            config=RetrievalExecutionConfig(include_candidates=True),
        )
    assert result.candidates is not None
    [first, *rest] = result.candidates
    assert [j.hypothesis_ref for j in first.nli] == ["query", "claim:0"]
    assert rest
    assert all(c.nli == () for c in rest)


async def test_nli_misalignment_degrades_or_fails() -> None:
    nli = _ScriptedNLI(lambda judgements: judgements[:-1])
    config = RetrievalConfig(nli=NLIConfig(enabled=True))
    async with ingested(retrieval=config, components=Components(nli=nli)) as engine:
        short = await engine.retrieve("asyncio", scope=ACME)
        nli.script = lambda judgements: [
            j.model_copy(update={"hypothesis_ref": "other"}) for j in judgements
        ]
        misaligned = await engine.retrieve("asyncio", scope=ACME)
    for result in (short, misaligned):
        assert _warning(result, WarningCode.STAGE_FAILED_DEGRADED).details == {
            "stage": "nli",
            "error_type": "ComponentContractError",
        }

    strict = RetrievalConfig(nli=NLIConfig(enabled=True, on_error="fail"))
    async with ingested(retrieval=strict, components=Components(nli=nli)) as engine:
        with pytest.raises(ComponentContractError):
            await engine.retrieve("asyncio", scope=ACME)


# consolidation and warnings ----------------------------------------------------------------
async def test_dense_channel_semantic_relations_and_query_vector_cache() -> None:
    config = RetrievalConfig(consolidation=ConsolidationConfig(semantic_relations=True))
    embedding = HashEmbedding()
    async with ingested(retrieval=config, components=Components(embedding=embedding)) as engine:
        first = await engine.retrieve("asyncio", scope=ACME)
        cache = runtime(engine).pipeline._c.embedding_cache
        assert cache is not None
        second = await engine.retrieve("asyncio", scope=ACME)
    assert "index.dense" in first.snapshot.generations
    assert second.evidence == first.evidence


async def test_semantic_relations_without_embedder_and_without_cache() -> None:
    config = RetrievalConfig(
        consolidation=ConsolidationConfig(semantic_relations=True), embedding_cache_size=0
    )
    async with ingested(retrieval=config) as engine:
        assert (await engine.retrieve("asyncio", scope=ACME)).evidence
    async with ingested(retrieval=config, components=Components(embedding=HashEmbedding())) as e:
        assert runtime(e).pipeline._c.embedding_cache is None
        assert (await e.retrieve("asyncio", scope=ACME)).evidence


class _TruncatingConsolidator(HierarchicalConsolidator):
    async def consolidate(self, candidates: Any, context: Any) -> Any:
        evidence, _ = await super().consolidate(candidates, context)
        return evidence, ["ev-omitted"]


async def test_truncated_contradictions_are_reported() -> None:
    config = RetrievalConfig(consolidation=ConsolidationConfig(contradiction_overflow=0))
    components = Components(consolidator=_TruncatingConsolidator())
    async with ingested(retrieval=config, components=components) as engine:
        result = await engine.retrieve("asyncio", scope=ACME)
    warning = _warning(result, WarningCode.EVIDENCE_TRUNCATED_CONTRADICTIONS)
    assert warning.severity == Severity.INFO
    assert warning.details == {"evidence_ids": ["ev-omitted"]}


async def test_stale_counters_become_warnings(monkeypatch: pytest.MonkeyPatch) -> None:
    async with ingested() as engine:
        _patch_candidates(monkeypatch, engine, stale_kept=2, stale_dropped=1)
        result = await engine.retrieve("asyncio", scope=ACME)
    stale = [w for w in result.warnings if w.code == WarningCode.STALE_DOCUMENT_VERSION]
    assert [(w.severity, w.details) for w in stale] == [
        (Severity.WARNING, {"count": 2}),
        (Severity.INFO, {"count": 1}),
    ]
    assert result.status == ResultStatus.PARTIAL


async def test_stale_versions_reach_results_when_allowed() -> None:
    lifecycle_free = IngestionConfig(index_wait=IndexWait.NONE)
    async with ingested(ingestion=lifecycle_free) as engine:
        await engine.run_lifecycle_once(ACME)
        await engine.ingest([text_doc("d1", "Python trio nurseries.")], scope=ACME)
        # the new version is not indexed yet: only the old version's hits exist
        allowed = await engine.retrieve(
            "coroutines", scope=ACME, config=RetrievalExecutionConfig(allow_stale_versions=True)
        )
        denied = await engine.retrieve("coroutines", scope=ACME)
    assert allowed.evidence
    assert WarningCode.STALE_DOCUMENT_VERSION in _codes(allowed)
    assert denied.evidence == ()


async def test_log_content_includes_query() -> None:
    from retrievalloom import EngineConfig, EngineResources, LifecycleConfig, RetrievalEngine
    from tests.support import shared_manager

    engine = RetrievalEngine(
        EngineConfig(lifecycle=LifecycleConfig(background=False), log_content=True),
        resources=EngineResources(resource_manager=shared_manager()),
    )
    async with engine:
        await engine.ingest(DOCS, scope=ACME)
        assert runtime(engine).pipeline._c.log_content
        assert (await engine.retrieve("asyncio", scope=ACME)).evidence


# graph and structure channels ---------------------------------------------------------------
def _rich_doc(doc_id: str, topic: str, **metadata: Any) -> DocumentInput:
    return DocumentInput(
        _id=doc_id,
        metadata=metadata,
        representations=(
            TextRepresentation(text=f"{topic} asyncio event loops schedule coroutines."),
            StructureRepresentation(
                representation_id="s",
                root=StructureNodeInput(
                    title="Guide",
                    children=(StructureNodeInput(title="Loops", text=f"{topic} loops run tasks."),),
                ),
            ),
        ),
    )


CHANNELS = RetrievalConfig(
    channels=ChannelsConfig(
        graph=GraphChannelConfig(enabled=True), structure=StructureChannelConfig(enabled=True)
    )
)


@pytest.mark.graph
@pytest.mark.structure
async def test_graph_and_structure_channels_are_tenant_isolated() -> None:
    components = Components(graph_extractor=CapitalizedExtractor())
    async with opened(retrieval=CHANNELS, components=components) as engine:
        await engine.ingest([_rich_doc("a1", "Python", lang="en")], scope=ACME)
        await engine.ingest([_rich_doc("b1", "Python", lang="en")], scope=GLOBEX)
        parts = runtime(engine).pipeline._c
        with pytest.raises(ConfigurationError, match="graph retriever"):
            RetrievalPipeline(dataclasses.replace(parts, graph_retriever=None))
        results = {}
        for name, execution in {
            "graph": RetrievalExecutionConfig(index=False, structure=False),
            "structure": RetrievalExecutionConfig(index=False, graph=False),
            "all": RetrievalExecutionConfig(),
        }.items():
            results[name] = await engine.retrieve(
                "Python loops",
                scope=ACME,
                config=execution.model_copy(update={"include_candidates": True}),
            )
        filtered = await engine.retrieve(
            "Python loops", scope=ACME, filters=Eq(field="lang", value="en")
        )
    for name, result in results.items():
        assert result.evidence, name
        assert {s.document_id for e in result.evidence for s in e.sources} == {"a1"}, name
        assert all(c.scope == ACME for c in result.candidates or ()), name
    channels = {h.method.channel for c in results["all"].candidates or () for h in c.hits}
    assert channels == {Channel.INDEX, Channel.GRAPH, Channel.STRUCTURE}
    post = _warning(filtered, WarningCode.FILTER_POST_APPLIED)
    assert post.details == {"methods": ["graph.graph", "structure.structure"]}
