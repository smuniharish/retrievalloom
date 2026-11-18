"""Retrieval planning: request validation and resolution of the execution plan."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from retrievalloom.domain.filters import measure
from retrievalloom.domain.records import Channel, IndexMethod, MissingIndexPolicy
from retrievalloom.domain.retrieval import RetrievalWarning, Severity, WarningCode
from retrievalloom.errors import ConfigurationError, ValidationError
from retrievalloom.evidence.fusion import make_fusion

if TYPE_CHECKING:
    from collections.abc import Mapping

    from retrievalloom.config import MethodConfig, RetrievalConfig, TenantPolicy
    from retrievalloom.contracts.evidence import FusionABC
    from retrievalloom.domain.retrieval import RetrievalExecutionConfig, RetrievalRequest
    from retrievalloom.lifecycle.units import IndexUnit

GRAPH_UNIT = "graph.graph"
STRUCTURE_UNIT = "structure.structure"
INDEX_METHODS = tuple(method.value for method in IndexMethod)


@dataclass(frozen=True, slots=True)
class PlannedMethod:
    """One method the request executes."""

    unit: IndexUnit
    candidate_k: int
    weight: float
    fail: bool

    @property
    def name(self) -> str:
        """Unit name ``<channel>.<method>``."""
        return self.unit.name


@dataclass(frozen=True, slots=True)
class ExecutionPlan:
    """Resolved execution plan of one request."""

    methods: tuple[PlannedMethod, ...]
    top_k: int
    timeout: float
    missing_index_policy: MissingIndexPolicy
    method_fusion: FusionABC
    channel_fusion: FusionABC
    channel_weights: dict[str, float]
    reranking: bool
    reranking_fail: bool
    nli: bool
    nli_fail: bool
    allow_stale: bool
    warnings: tuple[RetrievalWarning, ...]

    def method(self, name: str) -> PlannedMethod | None:
        """Planned method called ``name``."""
        return next((method for method in self.methods if method.name == name), None)


def validate_request(
    request: RetrievalRequest, config: RetrievalConfig, *, max_claims: int
) -> None:
    """Check request bounds that the model cannot know.

    Raises:
        ValidationError: The query, ``top_k``, the filter or the claims exceed a bound.
    """
    if len(request.query) > config.max_query_chars:
        raise ValidationError(
            "query exceeds max_query_chars", details={"limit": config.max_query_chars}
        )
    policy = config.tenant_policy(request.scope.tenant_id)
    if request.top_k > policy.max_top_k:
        raise ValidationError(
            "top_k exceeds the tenant maximum", details={"limit": policy.max_top_k}
        )
    _, nodes = measure(request.filters)
    if nodes > config.max_filter_nodes:
        raise ValidationError(
            "filter has too many nodes", details={"limit": config.max_filter_nodes}
        )
    if len(request.claims) > max_claims:
        raise ValidationError(
            f"at most {max_claims} claims are allowed per request",
            details={"limit": max_claims, "hint": "raise EngineLimits.max_claims"},
        )


def _info(
    code: WarningCode, message: str, channel: Channel, method: str | None
) -> RetrievalWarning:
    return RetrievalWarning(
        code=code, severity=Severity.INFO, channel=channel, method=method, message=message
    )


def _warn(
    code: WarningCode, message: str, channel: Channel, method: str | None
) -> RetrievalWarning:
    return RetrievalWarning(code=code, channel=channel, method=method, message=message)


class _Planner:
    def __init__(
        self,
        config: RetrievalConfig,
        policy: TenantPolicy,
        execution: RetrievalExecutionConfig,
        units: Mapping[str, IndexUnit],
    ) -> None:
        self.config = config
        self.policy = policy
        self.execution = execution
        self.units = units
        self.methods: list[PlannedMethod] = []
        self.warnings: list[RetrievalWarning] = []

    def channel_enabled(self, channel: Channel, configured: bool) -> bool:
        override = getattr(self.execution, channel.value)
        allowed = channel in self.policy.allowed_channels
        if override is True and not allowed:
            raise ConfigurationError(
                "channel is not allowed for this tenant", details={"channel": channel.value}
            )
        if override is False or not allowed:
            return False
        return override is True or configured

    def add(
        self,
        channel: Channel,
        unit_name: str,
        method: str | None,
        config: MethodConfig,
        *,
        explicit: bool,
    ) -> None:
        unit = self.units.get(unit_name)
        if unit is None:
            message = "method has no configured backend"
            self.warnings.append(
                _warn(WarningCode.METHOD_UNAVAILABLE, message, channel, method)
                if explicit
                else _info(WarningCode.METHOD_UNAVAILABLE, message, channel, method)
            )
            return
        candidate_k = min(config.candidate_k, self.policy.max_candidate_k)
        if candidate_k < config.candidate_k:
            self.warnings.append(
                RetrievalWarning(
                    code=WarningCode.LIMIT_CLAMPED,
                    severity=Severity.INFO,
                    channel=channel,
                    method=method,
                    message="candidate_k reduced to the tenant policy's max_candidate_k",
                    details={
                        "setting": "candidate_k",
                        "requested": config.candidate_k,
                        "applied": candidate_k,
                    },
                )
            )
        self.methods.append(
            PlannedMethod(
                unit=unit,
                candidate_k=candidate_k,
                weight=config.weight,
                fail=config.on_error == "fail",
            )
        )

    def index(self) -> None:
        channel = self.config.channels.index
        if not self.channel_enabled(Channel.INDEX, channel.enabled):
            return
        for name in INDEX_METHODS:
            method_config = channel.method(name)
            override = getattr(self.execution, name)
            if override is False:
                continue
            if override is None and not method_config.enabled:
                continue
            self.add(
                Channel.INDEX,
                f"{Channel.INDEX.value}.{name}",
                name,
                method_config,
                explicit=override is True,
            )

    def single(self, channel: Channel, unit_name: str, config: MethodConfig) -> None:
        if self.channel_enabled(channel, config.enabled):
            self.add(
                channel,
                unit_name,
                None,
                config,
                explicit=getattr(self.execution, channel.value) is True,
            )


def resolve_plan(
    request: RetrievalRequest,
    config: RetrievalConfig,
    units: Mapping[str, IndexUnit],
    *,
    method_fusion: FusionABC | None,
    channel_fusion: FusionABC | None,
    has_reranker: bool,
    has_nli: bool,
) -> ExecutionPlan:
    """Combine configuration, tenant policy and per-request overrides into a plan.

    Raises:
        ConfigurationError: An override enables a channel the tenant policy forbids.
    """
    execution = request.execution
    policy = config.tenant_policy(request.scope.tenant_id)
    planner = _Planner(config, policy, execution, units)
    planner.index()
    planner.single(Channel.GRAPH, GRAPH_UNIT, config.channels.graph)
    planner.single(Channel.STRUCTURE, STRUCTURE_UNIT, config.channels.structure)

    reranking = config.reranking.enabled if execution.reranking is None else execution.reranking
    if reranking and not has_reranker:
        # Configured reranking without a reranker is a silent no-op; only an explicit
        # per-request demand reports the missing component.
        if execution.reranking:
            planner.warnings.append(
                RetrievalWarning(
                    code=WarningCode.RERANKER_UNAVAILABLE,
                    severity=Severity.WARNING,
                    message="no reranker is configured",
                )
            )
        reranking = False
    nli = config.nli.enabled if execution.nli is None else execution.nli
    if nli and not has_nli:
        planner.warnings.append(
            RetrievalWarning(
                code=WarningCode.STAGE_SKIPPED,
                severity=Severity.WARNING if execution.nli else Severity.INFO,
                message="no NLI model is configured",
                details={"stage": "nli"},
            )
        )
        nli = False

    if execution.method_fusion is not None:
        method_fusion = make_fusion(
            config.method_fusion.model_copy(update={"method": execution.method_fusion})
        )
    if execution.channel_fusion is not None:
        channel_fusion = make_fusion(
            config.channel_fusion.model_copy(update={"method": execution.channel_fusion})
        )
    channels = config.channels
    weights = {
        Channel.INDEX.value: channels.index.weight,
        Channel.GRAPH.value: channels.graph.weight,
        Channel.STRUCTURE.value: channels.structure.weight,
    }
    timeout = execution.timeout or config.request_timeout
    return ExecutionPlan(
        methods=tuple(planner.methods),
        top_k=request.top_k,
        timeout=timeout,
        missing_index_policy=MissingIndexPolicy(
            execution.missing_index_policy or config.missing_index_policy
        ),
        method_fusion=method_fusion or make_fusion(config.method_fusion),
        channel_fusion=channel_fusion or make_fusion(config.channel_fusion),
        channel_weights=weights,
        reranking=reranking,
        reranking_fail=config.reranking.on_error == "fail",
        nli=nli,
        nli_fail=config.nli.on_error == "fail",
        allow_stale=(
            config.allow_stale_versions
            if execution.allow_stale_versions is None
            else execution.allow_stale_versions
        ),
        warnings=tuple(planner.warnings),
    )


__all__ = [
    "GRAPH_UNIT",
    "STRUCTURE_UNIT",
    "ExecutionPlan",
    "PlannedMethod",
    "resolve_plan",
    "validate_request",
]
