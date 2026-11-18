"""Request validation and execution-plan resolution."""

from __future__ import annotations

from typing import Any

import pytest

from retrievalloom import (
    ChannelsConfig,
    ConfigurationError,
    Eq,
    FuzzyConfig,
    GraphChannelConfig,
    IndexChannelConfig,
    MethodConfig,
    MissingIndexPolicy,
    NLIConfig,
    Or,
    RerankingConfig,
    RetrievalConfig,
    RetrievalExecutionConfig,
    Scope,
    Severity,
    StructureChannelConfig,
    TenantPolicy,
    ValidationError,
    WarningCode,
)
from retrievalloom.domain.records import Channel
from retrievalloom.domain.retrieval import RetrievalRequest
from retrievalloom.evidence.fusion import CombSUMFusion, RRFFusion, WeightedScoreFusion
from retrievalloom.index.fuzzy import RapidFuzzIndex
from retrievalloom.index.keyword import AhoCorasickKeywordIndex
from retrievalloom.index.sparse import BM25sIndex
from retrievalloom.lifecycle.units import make_unit
from retrievalloom.retrieval.planning import resolve_plan, validate_request

SCOPE = Scope(tenant_id="acme")


def _units() -> dict[str, Any]:
    units = {}
    for method, backend in (
        ("keyword", AhoCorasickKeywordIndex()),
        ("fuzzy", RapidFuzzIndex()),
        ("sparse", BM25sIndex()),
    ):
        unit = make_unit(Channel.INDEX, method, backend, instance_id="inst")
        units[unit.name] = unit
    return units


def _request(execution: RetrievalExecutionConfig | None = None, **kwargs: Any) -> RetrievalRequest:
    values: dict[str, Any] = {"request_id": "r", "scope": SCOPE, "query": "hello", "top_k": 5}
    values.update(kwargs)
    return RetrievalRequest(execution=execution or RetrievalExecutionConfig(), **values)


def _plan(config: RetrievalConfig, request: RetrievalRequest, **kwargs: Any) -> Any:
    options: dict[str, Any] = {
        "method_fusion": None,
        "channel_fusion": None,
        "has_reranker": False,
        "has_nli": False,
    }
    options.update(kwargs)
    return resolve_plan(request, config, _units(), **options)


def _codes(plan: Any) -> list[tuple[WarningCode, Severity, str | None]]:
    return [(w.code, w.severity, w.method) for w in plan.warnings]


@pytest.mark.parametrize(
    ("config", "request_kwargs", "limit"),
    [
        (RetrievalConfig(max_query_chars=3), {}, 3),
        (
            RetrievalConfig(default_tenant_policy=TenantPolicy(max_top_k=2)),
            {"top_k": 3},
            2,
        ),
        (
            RetrievalConfig(max_filter_nodes=2),
            {"filters": Or(clauses=(Eq(field="a", value=1), Eq(field="b", value=2)))},
            2,
        ),
    ],
)
def test_validate_request_bounds(
    config: RetrievalConfig, request_kwargs: dict[str, Any], limit: int
) -> None:
    with pytest.raises(ValidationError) as caught:
        validate_request(_request(**request_kwargs), config, max_claims=32)
    assert caught.value.details["limit"] == limit


def test_validate_request_claim_limit_and_valid_request() -> None:
    config = RetrievalConfig()
    validate_request(_request(claims=("c",) * 32), config, max_claims=32)
    with pytest.raises(ValidationError) as caught:
        validate_request(_request(claims=("c",) * 33), config, max_claims=32)
    assert caught.value.details["limit"] == 32
    validate_request(_request(claims=("c",) * 33), config, max_claims=64)


def test_tenant_policy_limits_candidate_k_and_defaults() -> None:
    config = RetrievalConfig(
        channels=ChannelsConfig(
            index=IndexChannelConfig(keyword=MethodConfig(candidate_k=900, weight=2.0))
        ),
        tenant_policies={"acme": TenantPolicy(max_candidate_k=7)},
    )
    plan = _plan(config, _request())
    keyword = plan.method("index.keyword")
    assert keyword is not None
    assert keyword.candidate_k == 7
    clamped = next(w for w in plan.warnings if w.code == WarningCode.LIMIT_CLAMPED)
    assert clamped.severity == Severity.INFO
    assert clamped.details == {"setting": "candidate_k", "requested": 900, "applied": 7}
    assert keyword.weight == 2.0
    assert not keyword.fail
    assert plan.method("index.dense") is None
    # dense is enabled by configuration but has no backend: informational only
    assert (WarningCode.METHOD_UNAVAILABLE, Severity.INFO, "dense") in _codes(plan)
    assert plan.timeout == config.request_timeout
    assert plan.missing_index_policy == MissingIndexPolicy.SKIP
    assert isinstance(plan.method_fusion, RRFFusion)
    assert plan.channel_weights == {"index": 1.0, "graph": 1.0, "structure": 1.0}
    assert not plan.allow_stale


def test_method_overrides() -> None:
    config = RetrievalConfig(
        channels=ChannelsConfig(
            index=IndexChannelConfig(fuzzy=FuzzyConfig(enabled=False, on_error="fail"))
        )
    )
    default = _plan(config, _request())
    assert [m.name for m in default.methods] == ["index.keyword", "index.sparse"]

    plan = _plan(
        config,
        _request(RetrievalExecutionConfig(keyword=False, fuzzy=True, dense=True)),
    )
    assert [m.name for m in plan.methods] == ["index.fuzzy", "index.sparse"]
    assert plan.method("index.fuzzy").fail
    # explicitly requested dense without a backend escalates to a warning
    assert (WarningCode.METHOD_UNAVAILABLE, Severity.WARNING, "dense") in _codes(plan)

    off = _plan(config, _request(RetrievalExecutionConfig(index=False)))
    assert off.methods == ()


def test_channel_enablement_and_tenant_policy() -> None:
    config = RetrievalConfig(
        channels=ChannelsConfig(
            graph=GraphChannelConfig(enabled=False), structure=StructureChannelConfig(enabled=True)
        ),
        tenant_policies={"acme": TenantPolicy(allowed_channels=(Channel.INDEX, Channel.GRAPH))},
    )
    plan = _plan(config, _request())
    # structure is configured on but forbidden by the tenant policy: silently off
    assert all(w.channel != Channel.STRUCTURE for w in plan.warnings)

    plan = _plan(config, _request(RetrievalExecutionConfig(graph=True)))
    assert (WarningCode.METHOD_UNAVAILABLE, Severity.WARNING, None) in _codes(plan)

    with pytest.raises(ConfigurationError) as caught:
        _plan(config, _request(RetrievalExecutionConfig(structure=True)))
    assert caught.value.details == {"channel": "structure"}

    other = Scope(tenant_id="other")
    plan = _plan(
        config,
        _request(RetrievalExecutionConfig(structure=False), scope=other),
    )
    assert all(w.channel != Channel.STRUCTURE for w in plan.warnings)
    plan = _plan(config, _request(scope=other))
    assert any(
        w.channel == Channel.STRUCTURE and w.severity == Severity.INFO for w in plan.warnings
    )


def test_reranking_and_nli_resolution() -> None:
    config = RetrievalConfig(
        reranking=RerankingConfig(on_error="fail"), nli=NLIConfig(enabled=True, on_error="fail")
    )
    plan = _plan(config, _request())
    assert not plan.reranking
    assert not plan.nli
    infos = {(w.code, w.severity) for w in plan.warnings}
    assert WarningCode.RERANKER_UNAVAILABLE not in {code for code, _ in infos}
    assert (WarningCode.STAGE_SKIPPED, Severity.INFO) in infos

    plan = _plan(config, _request(RetrievalExecutionConfig(reranking=True, nli=True)))
    severities = {(w.code, w.severity) for w in plan.warnings}
    assert (WarningCode.RERANKER_UNAVAILABLE, Severity.WARNING) in severities
    assert (WarningCode.STAGE_SKIPPED, Severity.WARNING) in severities

    plan = _plan(config, _request(), has_reranker=True, has_nli=True)
    assert plan.reranking
    assert plan.reranking_fail
    assert plan.nli
    assert plan.nli_fail

    plan = _plan(
        config,
        _request(RetrievalExecutionConfig(reranking=False, nli=False)),
        has_reranker=True,
        has_nli=True,
    )
    assert not plan.reranking
    assert not plan.nli


def test_execution_overrides_fusion_timeout_policy_and_staleness() -> None:
    config = RetrievalConfig(allow_stale_versions=True)
    supplied = CombSUMFusion()
    plan = _plan(config, _request(), method_fusion=supplied, channel_fusion=supplied)
    assert plan.method_fusion is supplied
    assert plan.channel_fusion is supplied
    assert plan.allow_stale

    plan = _plan(
        config,
        _request(
            RetrievalExecutionConfig(
                method_fusion="combsum",
                channel_fusion="weighted_score",
                timeout=1.5,
                missing_index_policy="fail",
                allow_stale_versions=False,
            )
        ),
        method_fusion=supplied,
    )
    assert isinstance(plan.method_fusion, CombSUMFusion)
    assert plan.method_fusion is not supplied
    assert isinstance(plan.channel_fusion, WeightedScoreFusion)
    assert plan.timeout == 1.5
    assert plan.missing_index_policy == MissingIndexPolicy.FAIL
    assert not plan.allow_stale
