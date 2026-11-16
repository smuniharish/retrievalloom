"""Retrieval-side model: hits, candidates, evidence, requests and results."""

from __future__ import annotations

from enum import StrEnum
from typing import Literal

from pydantic import Field, model_validator

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
from retrievalloom.domain.filters import FilterExpression
from retrievalloom.domain.inputs import Scope, TextSpan
from retrievalloom.domain.records import Channel, ObjectKind, SourceReference


class MethodRef(FrozenModel):
    """Address of one retrieval method: ``(channel, method)``."""

    channel: Channel
    method: str = Field(min_length=1)

    @property
    def key(self) -> str:
        """``<channel>.<method>``."""
        return f"{self.channel.value}.{self.method}"

    @classmethod
    def parse(cls, key: str) -> MethodRef:
        """Parse ``<channel>.<method>``."""
        channel, _, method = key.partition(".")
        return cls(channel=Channel(channel), method=method)


class ScoreRange(StrEnum):
    """Kind of raw score a method emits."""

    UNBOUNDED = "unbounded"
    UNIT_INTERVAL = "unit_interval"
    DISTANCE = "distance"
    SIMILARITY = "similarity"
    RANK_ONLY = "rank_only"


class ScoreSemantics(FrozenModel):
    """Direction and range of a raw score."""

    higher_is_better: bool = True
    range: ScoreRange = ScoreRange.UNBOUNDED


class MatchDetail(FrozenModel):
    """Why a method matched (term, fuzzy match, graph path, structure path ...)."""

    kind: str
    term: str | None = None
    matched: str | None = None
    query_span: TextSpan | None = None
    score: float | None = None
    path: tuple[str, ...] = ()
    details: Metadata = Field(default_factory=dict)


class ObjectRef(FrozenModel):
    """Pointer to a derived object in one generation."""

    scope_key: str
    object_id: EngineId
    generation_id: str


class RetrievalHit(FrozenModel):
    """One method's raw opinion about one object."""

    object_ref: ObjectRef
    method: MethodRef
    backend_id: str
    raw_score: float | None = None
    score_semantics: ScoreSemantics = Field(default_factory=ScoreSemantics)
    rank: PositiveInt
    matched: tuple[MatchDetail, ...] = ()
    path_refs: tuple[EngineId, ...] = ()
    config_fingerprint: Digest | None = None

    @property
    def generation_id(self) -> str:
        """Generation the hit was read from."""
        return self.object_ref.generation_id


class ScoreCard(FrozenModel):
    """Stage scores; each stage writes its own field and never overwrites another."""

    normalized: dict[str, float] = Field(default_factory=dict)
    method_fusion: float | None = None
    channel_fusion: float | None = None
    rerank: float | None = None
    final: float | None = None


class NLILabel(StrEnum):
    """NLI label."""

    ENTAILMENT = "entailment"
    CONTRADICTION = "contradiction"
    NEUTRAL = "neutral"


class NLIJudgement(FrozenModel):
    """Judgement of one (premise, hypothesis) pair."""

    label: NLILabel
    confidence: float = Field(ge=0.0, le=1.0)
    scores: dict[NLILabel, float] = Field(default_factory=dict)
    model_identity: str
    hypothesis_ref: str


class CandidateFlags(FrozenModel):
    """Candidate flags."""

    stale_version: bool = False
    partial_coverage: bool = False


class Candidate(FrozenModel):
    """Canonical normalised retrieval unit (one per object per request)."""

    candidate_id: EngineId
    scope: Scope
    object_id: EngineId
    kind: ObjectKind
    merged_object_ids: tuple[EngineId, ...] = ()
    text: str | None = None
    content_hash: Digest
    sources: tuple[SourceReference, ...] = Field(min_length=1)
    hits: tuple[RetrievalHit, ...] = Field(min_length=1)
    scores: ScoreCard = Field(default_factory=ScoreCard)
    nli: tuple[NLIJudgement, ...] = ()
    flags: CandidateFlags = Field(default_factory=CandidateFlags)
    payload: Metadata = Field(default_factory=dict)

    @property
    def channels(self) -> frozenset[Channel]:
        """Channels that contributed hits."""
        return frozenset(hit.method.channel for hit in self.hits)


class StageRecord(FrozenModel):
    """One processing stage applied to a result."""

    stage: str
    component_id: str
    component_version: str
    config_fingerprint: Digest | None = None


class Provenance(FrozenModel):
    """How evidence got into the result."""

    sources: tuple[SourceReference, ...] = Field(min_length=1)
    hits: tuple[RetrievalHit, ...] = Field(min_length=1)
    stage_trail: tuple[StageRecord, ...] = ()


class RelationType(StrEnum):
    """Relation between two evidence members."""

    IDENTICAL = "identical"
    SPAN_OVERLAP = "span_overlap"
    SAME_SECTION = "same_section"
    PARENT_CHILD = "parent_child"
    COMPLEMENTARY = "complementary"
    SEMANTICALLY_SIMILAR = "semantically_similar"
    CONTRADICTS = "contradicts"


class EvidenceRelation(FrozenModel):
    """Relation between two candidates (by candidate id)."""

    a: EngineId
    b: EngineId
    type: RelationType


class EvidenceSegment(FrozenModel):
    """One member's text with its own sources (no untraceable merged blobs)."""

    candidate_id: EngineId
    object_id: EngineId
    text: str | None
    sources: tuple[SourceReference, ...] = Field(min_length=1)


class NLISummary(FrozenModel):
    """Counts of member NLI labels."""

    entailment: NonNegativeInt = 0
    contradiction: NonNegativeInt = 0
    neutral: NonNegativeInt = 0
    has_contradiction: bool = False


class CompletenessReport(FrozenModel):
    """What is known to be missing or stale for an evidence item."""

    missing_spans: tuple[EngineId, ...] = ()
    stale_sources: tuple[EngineId, ...] = ()
    unresolved_sources: tuple[EngineId, ...] = ()


class Evidence(VersionedModel):
    """Consolidated output unit."""

    evidence_id: EngineId
    scope: Scope
    segments: tuple[EvidenceSegment, ...] = Field(min_length=1)
    member_candidate_ids: tuple[EngineId, ...] = Field(min_length=1)
    relations: tuple[EvidenceRelation, ...] = ()
    linked_evidence_ids: tuple[EngineId, ...] = ()
    sources: tuple[SourceReference, ...] = Field(min_length=1)
    score: float
    scores: ScoreCard = Field(default_factory=ScoreCard)
    nli_summary: NLISummary = Field(default_factory=NLISummary)
    completeness: CompletenessReport = Field(default_factory=CompletenessReport)
    provenance: Provenance
    display_text: str | None = None


class WarningCode(StrEnum):
    """Stable retrieval and ingestion warning codes."""

    CHANNEL_DISABLED = "channel_disabled"
    METHOD_DISABLED = "method_disabled"
    METHOD_UNAVAILABLE = "method_unavailable"
    INDEX_MISSING = "index_missing"
    PARTIAL_COVERAGE = "partial_coverage"
    STALE_COVERAGE = "stale_coverage"
    BACKFILL_SCHEDULED = "backfill_scheduled"
    STAGE_SKIPPED = "stage_skipped"
    STAGE_FAILED_DEGRADED = "stage_failed_degraded"
    TIMEOUT_PARTIAL = "timeout_partial"
    STALE_DOCUMENT_VERSION = "stale_document_version"
    RERANKER_UNAVAILABLE = "reranker_unavailable"
    EVIDENCE_TRUNCATED_CONTRADICTIONS = "evidence_truncated_contradictions"
    PROVENANCE_RECORDING_FAILED = "provenance_recording_failed"
    FILTER_POST_APPLIED = "filter_post_applied"
    POSSIBLE_REDELIVERY = "possible_redelivery"
    REDELIVERY_ADVISORY_ONLY = "redelivery_advisory_only"
    INDEX_WAIT_TIMEOUT = "index_wait_timeout"


class Severity(StrEnum):
    """Warning severity."""

    INFO = "info"
    WARNING = "warning"
    ERROR = "error"


class RetrievalWarning(FrozenModel):
    """Content-free warning."""

    code: WarningCode
    severity: Severity = Severity.WARNING
    channel: Channel | None = None
    method: str | None = None
    message: str
    details: Metadata = Field(default_factory=dict)


class CoverageSummary(FrozenModel):
    """Coverage counts per method key and the ready fraction."""

    counts: dict[str, dict[str, int]] = Field(default_factory=dict)
    fraction_ready: dict[str, float] = Field(default_factory=dict)


class RetrievalSnapshot(FrozenModel):
    """Consistent view captured before any method executes."""

    snapshot_seq: NonNegativeInt
    generations: dict[str, str] = Field(default_factory=dict)
    fingerprints: dict[str, Digest] = Field(default_factory=dict)


class ResultStatus(StrEnum):
    """Overall retrieval status."""

    COMPLETE = "complete"
    PARTIAL = "partial"
    EMPTY = "empty"


class RetrievalExecutionConfig(FrozenModel):
    """Per-request tri-state overrides; ``None`` inherits the engine configuration."""

    index: bool | None = None
    graph: bool | None = None
    structure: bool | None = None
    keyword: bool | None = None
    fuzzy: bool | None = None
    sparse: bool | None = None
    dense: bool | None = None
    reranking: bool | None = None
    nli: bool | None = None
    top_k: PositiveInt | None = None
    timeout: float | None = Field(default=None, gt=0)
    missing_index_policy: Literal["skip", "fail", "schedule_backfill"] | None = None
    include_candidates: bool = False
    method_fusion: Literal["rrf", "weighted_rrf", "weighted_score", "combsum", "combmnz"] | None = (
        None
    )
    channel_fusion: (
        Literal["rrf", "weighted_rrf", "weighted_score", "combsum", "combmnz"] | None
    ) = None
    allow_stale_versions: bool | None = None


class RetrievalRequest(VersionedModel):
    """Validated retrieval request."""

    request_id: str
    scope: Scope
    query: str = Field(min_length=1)
    filters: FilterExpression | None = None
    top_k: PositiveInt
    claims: tuple[str, ...] = ()
    principal: Identifier | None = None
    execution: RetrievalExecutionConfig = Field(default_factory=RetrievalExecutionConfig)

    @model_validator(mode="after")
    def _check_query(self) -> RetrievalRequest:
        if not self.query.strip():
            raise ValueError("query must not be blank")
        return self


class RetrievalResult(VersionedModel):
    """Result of one retrieval."""

    request_id: str
    scope: Scope
    evidence: tuple[Evidence, ...] = ()
    candidates: tuple[Candidate, ...] | None = None
    warnings: tuple[RetrievalWarning, ...] = ()
    coverage: CoverageSummary = Field(default_factory=CoverageSummary)
    snapshot: RetrievalSnapshot
    timings: dict[str, float] = Field(default_factory=dict)
    status: ResultStatus
