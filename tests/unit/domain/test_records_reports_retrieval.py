"""Records, reports and retrieval models: behaviour beyond plain field storage."""

from __future__ import annotations

from typing import Any

import pytest
from pydantic import ValidationError as PydanticValidationError

from retrievalloom.domain.base import utc_now
from retrievalloom.domain.filters import Eq
from retrievalloom.domain.hashing import sha256_digest, stable_id
from retrievalloom.domain.inputs import Scope
from retrievalloom.domain.records import (
    Channel,
    DocumentRecord,
    DocumentState,
    ErrorInfo,
    IngestOutcome,
    ObjectKind,
    SourceReference,
)
from retrievalloom.domain.reports import DocumentIngestResult, HealthReport, IngestReport
from retrievalloom.domain.retrieval import (
    Candidate,
    MethodRef,
    NLIJudgement,
    NLILabel,
    ObjectRef,
    RetrievalExecutionConfig,
    RetrievalHit,
    RetrievalRequest,
)
from retrievalloom.errors import BackendTimeoutError, VersionConflictError

SCOPE = Scope(tenant_id="t")
DIGEST = sha256_digest("content")


# --- ErrorInfo -------------------------------------------------------------------------


def test_error_info_from_library_error_keeps_contract_and_sanitises_details() -> None:
    marker = object()
    exc = BackendTimeoutError(
        "backend slow",
        details={
            "none": None,
            "s": "x",
            "i": 1,
            "f": 1.5,
            "b": True,
            "items": (1, "two"),
            "listed": [Scope(tenant_id="a").tenant_id],
            "obj": marker,
        },
    )
    info = ErrorInfo.from_exception(exc)
    assert (info.code, info.category, info.retryable, info.message) == (
        "backend_timeout",
        "backend",
        True,
        "backend slow",
    )
    assert info.details == {
        "none": None,
        "s": "x",
        "i": 1,
        "f": 1.5,
        "b": True,
        "items": ["1", "two"],
        "listed": ["a"],
        "obj": str(marker),
    }
    # the result is JSON-serialisable
    assert ErrorInfo.model_validate_json(info.model_dump_json()) == info


def test_error_info_from_foreign_exception_does_not_leak_arguments() -> None:
    info = ErrorInfo.from_exception(KeyError("secret document text"))
    assert info.code == "unexpected_error"
    assert info.category == "internal"
    assert not info.retryable
    assert info.message == "KeyError"
    assert "secret" not in info.model_dump_json()


def test_error_info_respects_overridden_retryable() -> None:
    info = ErrorInfo.from_exception(VersionConflictError("lost", retryable=False))
    assert info.retryable is False


# --- DocumentRecord -------------------------------------------------------------------


def _record_payload(**overrides: Any) -> dict[str, Any]:
    now = utc_now()
    payload: dict[str, Any] = {
        "_id": "doc",
        "_counter": 1,
        "_version": 1,
        "_content_hash": DIGEST,
        "_created_at": now,
        "_updated_at": now,
        "document_uid": stable_id("doc", "doc"),
        "scope": {"tenant_id": "t"},
        "hash_profile": "rl-hash-v1",
    }
    payload.update(overrides)
    return payload


def test_document_record_aliases_roundtrip() -> None:
    record = DocumentRecord.model_validate(_record_payload())
    assert record.state is DocumentState.ACTIVE
    dumped = record.model_dump(mode="json", by_alias=True)
    assert {"_id", "_counter", "_version", "_content_hash"} <= dumped.keys()
    assert DocumentRecord.model_validate(dumped) == record


@pytest.mark.parametrize(
    "overrides",
    [
        {"_version": 0},
        {"_content_hash": "sha256:short"},
        {"_created_at": "2024-01-01T00:00:00"},
        {"document_uid": "doc_xyz"},
        {"row_revision": -1},
        {"state": "archived"},
    ],
)
def test_document_record_rejections(overrides: dict[str, Any]) -> None:
    with pytest.raises(PydanticValidationError):
        DocumentRecord.model_validate(_record_payload(**overrides))


# --- reports ---------------------------------------------------------------------------


def test_ingest_report_outcome_counts_and_failures() -> None:
    documents = (
        DocumentIngestResult(document_id="a", outcome=IngestOutcome.CREATED),
        DocumentIngestResult(document_id="b", outcome=IngestOutcome.CREATED),
        DocumentIngestResult(document_id="c", outcome=IngestOutcome.REJECTED),
        DocumentIngestResult(document_id="d", outcome=IngestOutcome.FAILED),
        DocumentIngestResult(document_id="e", outcome=IngestOutcome.UNCHANGED),
    )
    report = IngestReport(scope=SCOPE, documents=documents)
    assert report.outcomes == {
        IngestOutcome.CREATED: 2,
        IngestOutcome.REJECTED: 1,
        IngestOutcome.FAILED: 1,
        IngestOutcome.UNCHANGED: 1,
    }
    assert [document.document_id for document in report.failed] == ["c", "d"]
    empty = IngestReport(scope=SCOPE, documents=())
    assert empty.outcomes == {}
    assert empty.failed == ()


def test_health_report_nests() -> None:
    report = HealthReport.model_validate(
        {"component": "engine", "status": "degraded", "components": [{"component": "child"}]}
    )
    assert report.components[0].status == "healthy"


# --- retrieval -------------------------------------------------------------------------


def test_method_ref_key_roundtrip() -> None:
    ref = MethodRef(channel=Channel.INDEX, method="dense")
    assert ref.key == "index.dense"
    assert MethodRef.parse(ref.key) == ref
    assert MethodRef.parse("graph.entity.x") == MethodRef(channel=Channel.GRAPH, method="entity.x")
    with pytest.raises(ValueError, match="'nochannel' is not a valid Channel"):
        MethodRef.parse("nochannel.x")
    with pytest.raises(PydanticValidationError):
        MethodRef.parse("index")


def _hit(channel: Channel, method: str, generation: str = "gen") -> RetrievalHit:
    return RetrievalHit(
        object_ref=ObjectRef(
            scope_key=SCOPE.key, object_id=stable_id("obj", "1"), generation_id=generation
        ),
        method=MethodRef(channel=channel, method=method),
        backend_id="b",
        rank=1,
    )


def test_hit_generation_and_candidate_channels() -> None:
    hits = (
        _hit(Channel.INDEX, "dense", "g1"),
        _hit(Channel.INDEX, "keyword"),
        _hit(Channel.GRAPH, "x"),
    )
    assert hits[0].generation_id == "g1"
    source = SourceReference(
        scope=SCOPE,
        document_id="doc",
        document_uid=stable_id("doc", "doc"),
        document_version=1,
        representation_id="default",
    )
    candidate = Candidate(
        candidate_id=stable_id("cand", "1"),
        scope=SCOPE,
        object_id=stable_id("obj", "1"),
        kind=ObjectKind.CHUNK,
        content_hash=DIGEST,
        sources=(source,),
        hits=hits,
    )
    assert candidate.channels == frozenset({Channel.INDEX, Channel.GRAPH})
    with pytest.raises(PydanticValidationError):
        Candidate.model_validate({**candidate.model_dump(), "hits": []})
    with pytest.raises(PydanticValidationError):
        RetrievalHit.model_validate({**hits[0].model_dump(), "rank": 0})


def test_nli_judgement_confidence_bounds() -> None:
    NLIJudgement(label=NLILabel.NEUTRAL, confidence=1.0, model_identity="m", hypothesis_ref="h")
    with pytest.raises(PydanticValidationError):
        NLIJudgement(label=NLILabel.NEUTRAL, confidence=1.5, model_identity="m", hypothesis_ref="h")


def test_retrieval_request_validation() -> None:
    request = RetrievalRequest.model_validate(
        {
            "request_id": "r",
            "scope": {"tenant_id": "t"},
            "query": "q",
            "top_k": 3,
            "filters": {"op": "eq", "field": "a", "value": 1},
        }
    )
    assert request.filters == Eq(field="a", value=1)
    assert request.execution == RetrievalExecutionConfig()
    for bad in ({"query": "   "}, {"query": ""}, {"top_k": 0}):
        with pytest.raises(PydanticValidationError):
            RetrievalRequest.model_validate(
                {"request_id": "r", "scope": {"tenant_id": "t"}, "query": "q", "top_k": 1} | bad
            )


def test_execution_config_rejects_invalid_overrides() -> None:
    with pytest.raises(PydanticValidationError):
        RetrievalExecutionConfig.model_validate({"timeout": 0})
    with pytest.raises(PydanticValidationError):
        RetrievalExecutionConfig.model_validate({"method_fusion": "max"})
    with pytest.raises(PydanticValidationError):
        RetrievalExecutionConfig.model_validate({"missing_index_policy": "ignore"})
