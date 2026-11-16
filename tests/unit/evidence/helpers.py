"""Builders for evidence-stage tests (candidates with full control over provenance)."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from retrievalloom.contracts.evidence import ConsolidationContext
from retrievalloom.domain.hashing import sha256_digest, stable_id
from retrievalloom.domain.inputs import Scope, TextSpan
from retrievalloom.domain.records import Channel, ObjectKind, SourceReference
from retrievalloom.domain.retrieval import (
    Candidate,
    CandidateFlags,
    MethodRef,
    NLIJudgement,
    NLILabel,
    ObjectRef,
    RetrievalHit,
    ScoreCard,
)

if TYPE_CHECKING:
    from collections.abc import Sequence

SCOPE = Scope(tenant_id="acme", namespace="docs", collection="main")
OTHER_SCOPE = Scope(tenant_id="globex", namespace="docs", collection="main")


def object_id(name: str) -> str:
    """Stable object id for ``name``."""
    return stable_id("obj", name)


def source(
    name: str,
    *,
    document: str = "d1",
    version: int = 1,
    span: tuple[int, int] | None = None,
    section: tuple[str, ...] = (),
    scope: Scope = SCOPE,
) -> SourceReference:
    """Source reference of object ``name`` inside ``document``."""
    return SourceReference(
        scope=scope,
        document_id=document,
        document_uid=stable_id("doc", scope.key, document),
        document_version=version,
        representation_id="text",
        object_id=object_id(name),
        span=TextSpan(start=span[0], end=span[1]) if span else None,
        section_path=section,
    )


def hit(name: str, *, method: str = "sparse", scope: Scope = SCOPE, rank: int = 1) -> RetrievalHit:
    """Retrieval hit for object ``name``."""
    return RetrievalHit(
        object_ref=ObjectRef(scope_key=scope.key, object_id=object_id(name), generation_id="g1"),
        method=MethodRef(channel=Channel.INDEX, method=method),
        backend_id="test",
        raw_score=1.0,
        rank=rank,
    )


def judgement(label: NLILabel, hypothesis: str = "h1") -> NLIJudgement:
    """NLI judgement for ``hypothesis``."""
    return NLIJudgement(
        label=label, confidence=0.9, model_identity="test", hypothesis_ref=hypothesis
    )


def candidate(
    name: str,
    *,
    final: float | None = 0.5,
    document: str = "d1",
    version: int = 1,
    span: tuple[int, int] | None = None,
    section: tuple[str, ...] = (),
    sources: Sequence[SourceReference] | None = None,
    hits: Sequence[RetrievalHit] | None = None,
    kind: ObjectKind = ObjectKind.CHUNK,
    text: str | None = "",
    content: str | None = None,
    nli: Sequence[NLIJudgement] = (),
    payload: dict[str, Any] | None = None,
    flags: CandidateFlags | None = None,
    scope: Scope = SCOPE,
    rerank: float | None = None,
) -> Candidate:
    """Candidate named ``name`` with one source/hit unless given explicitly."""
    return Candidate(
        candidate_id=stable_id("cand", name),
        scope=scope,
        object_id=object_id(name),
        kind=kind,
        text=(text or name) if text is not None else None,
        content_hash=sha256_digest(content if content is not None else name),
        sources=tuple(sources)
        if sources is not None
        else (
            source(
                name, document=document, version=version, span=span, section=section, scope=scope
            ),
        ),
        hits=tuple(hits) if hits is not None else (hit(name, scope=scope),),
        scores=ScoreCard(final=final, rerank=rerank),
        nli=tuple(nli),
        payload=payload or {},
        flags=flags or CandidateFlags(),
    )


def context(top_k: int = 10, overflow: int = 0, **values: Any) -> ConsolidationContext:
    """Consolidation context."""
    return ConsolidationContext(top_k=top_k, contradiction_overflow=overflow, **values)
