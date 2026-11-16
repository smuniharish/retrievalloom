"""AgxaiProvenanceRecorder against a real in-memory agxai ``XAIRuntime``."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pytest
from agxai import EvidenceType, ProvenanceStore, XAIRuntime

from retrievalloom.domain.records import IngestOutcome
from retrievalloom.domain.reports import DeleteReport, DocumentIngestResult, IngestReport
from retrievalloom.domain.retrieval import (
    NLILabel,
    ResultStatus,
    RetrievalRequest,
    RetrievalResult,
    RetrievalSnapshot,
    RetrievalWarning,
    WarningCode,
)
from retrievalloom.errors import TenantIsolationError
from retrievalloom.evidence.consolidation import HierarchicalConsolidator
from retrievalloom.integrations.agxai import AgxaiProvenanceRecorder
from tests.unit.evidence.helpers import SCOPE, candidate, context, judgement, source

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from agxai import Run


@pytest.fixture
async def runtime() -> AsyncIterator[XAIRuntime]:
    instance = XAIRuntime(application_id="app", tenant_id=SCOPE.tenant_id)
    try:
        yield instance
    finally:
        await instance.close()


async def retrieval(*, rerank: float | None = None) -> tuple[RetrievalRequest, RetrievalResult]:
    a = candidate(
        "a",
        final=0.9,
        sources=[source("a", span=(0, 10)), source("a", version=2, span=(0, 10))],
        nli=[judgement(NLILabel.ENTAILMENT)],
        rerank=rerank,
    )
    b = candidate("b", final=0.8, span=(0, 10))
    c = candidate("c", final=0.4, document="d2", version=3, nli=[judgement(NLILabel.CONTRADICTION)])
    evidence, _ = await HierarchicalConsolidator().consolidate([a, b, c], context())
    request = RetrievalRequest(request_id="req-1", scope=SCOPE, query="question", top_k=5)
    result = RetrievalResult(
        request_id="req-1",
        scope=SCOPE,
        evidence=tuple(evidence),
        snapshot=RetrievalSnapshot(snapshot_seq=7),
        status=ResultStatus.PARTIAL,
        warnings=(RetrievalWarning(code=WarningCode.TIMEOUT_PARTIAL, message="slow"),),
    )
    return request, result


async def links(runtime: XAIRuntime, run: Run, evidence_id: str) -> list[Any]:
    store = runtime.registry.get(ProvenanceStore)
    assert store is not None
    return list(await store.parents(evidence_id, context=run.execution.context))


async def test_records_retrieval_and_links_in_the_active_run(runtime: XAIRuntime) -> None:
    recorder = AgxaiProvenanceRecorder(runtime, reranker_id="flashrank:tiny")
    request, result = await retrieval()
    run = await runtime.start_run()
    with runtime.activate_run(run):
        await recorder.record_retrieval(request, result)
    (recorded,) = run.execution.retrievals
    assert recorded.retriever_id == "retrievalloom:main"
    assert recorded.query_reference == "req-1"
    assert recorded.reranker is None
    assert recorded.metadata == {
        "request_id": "req-1",
        "scope_key": SCOPE.key,
        "status": "partial",
        "snapshot_seq": 7,
        "warnings": ["timeout_partial"],
    }
    merged, other = result.evidence
    first, second = recorded.documents
    assert (first.rank, second.rank) == (1, 2)
    assert first.document_id == "d1"
    assert first.chunk_id == merged.segments[0].object_id
    assert first.score == merged.score
    assert first.content_reference == merged.evidence_id
    assert first.metadata == {
        "document_ids": ["d1"],
        "versions": [1, 2],
        "objects": [segment.object_id for segment in merged.segments],
        "has_contradiction": True,
    }
    assert second.content_reference == other.evidence_id
    assert second.metadata["versions"] == [3]

    merged_links = await links(runtime, run, merged.evidence_id)
    assert [(link.source_id, link.relation) for link in merged_links] == [
        (merged.sources[0].document_uid, "derived_from")
    ]
    assert merged_links[0].metadata == {"request_id": "req-1"}
    assert len(await links(runtime, run, other.evidence_id)) == 1


async def test_reports_reranker_and_can_skip_links(runtime: XAIRuntime) -> None:
    recorder = AgxaiProvenanceRecorder(runtime, reranker_id="flashrank:tiny", record_links=False)
    request, result = await retrieval(rerank=0.7)
    run = await runtime.start_run()
    with runtime.activate_run(run):
        await recorder.record_retrieval(request, result)
    assert run.execution.retrievals[0].reranker == "flashrank:tiny"
    assert await links(runtime, run, result.evidence[0].evidence_id) == []
    assert recorder.info != AgxaiProvenanceRecorder(runtime).info


async def test_without_a_run_nothing_is_recorded(runtime: XAIRuntime) -> None:
    recorder = AgxaiProvenanceRecorder(runtime, run_provider=lambda: None)
    request, result = await retrieval()
    await recorder.record_retrieval(request, result)
    await recorder.record_ingestion(DeleteReport(scope=SCOPE, deleted=("d1",)))
    assert runtime.errors == ()


async def test_run_provider_supplies_the_run(runtime: XAIRuntime) -> None:
    run = await runtime.start_run()
    recorder = AgxaiProvenanceRecorder(runtime, run_provider=lambda: run)
    request, result = await retrieval()
    await recorder.record_retrieval(request, result)
    assert len(run.execution.retrievals) == 1


async def test_rejects_a_run_of_another_tenant(runtime: XAIRuntime) -> None:
    foreign = await runtime.start_run({"metadata": {"xai_tenant_id": "globex"}})
    recorder = AgxaiProvenanceRecorder(runtime, run_provider=lambda: foreign)
    request, result = await retrieval()
    with pytest.raises(TenantIsolationError):
        await recorder.record_retrieval(request, result)
    with pytest.raises(TenantIsolationError):
        await recorder.record_ingestion(DeleteReport(scope=SCOPE))
    assert foreign.execution.retrievals == []


async def test_records_ingestion_and_deletion_counts(runtime: XAIRuntime) -> None:
    recorder = AgxaiProvenanceRecorder(runtime)
    ingest = IngestReport(
        scope=SCOPE,
        documents=(
            DocumentIngestResult(document_id="d1", outcome=IngestOutcome.CREATED),
            DocumentIngestResult(document_id="d2", outcome=IngestOutcome.CREATED),
            DocumentIngestResult(document_id="d3", outcome=IngestOutcome.FAILED),
        ),
    )
    delete = DeleteReport(scope=SCOPE, deleted=("d1", "d2"), not_found=("zz",))
    run = await runtime.start_run()
    with runtime.activate_run(run):
        await recorder.record_ingestion(ingest)
        await recorder.record_ingestion(delete)
    ingested, deleted = run.evidence
    assert ingested.evidence_type == EvidenceType.CUSTOM
    assert ingested.summary == "retrievalloom ingestion"
    assert ingested.content_reference == SCOPE.key
    assert ingested.metadata == {"scope_key": SCOPE.key, "counts": {"created": 2, "failed": 1}}
    assert deleted.summary == "retrievalloom deletion"
    assert deleted.metadata["counts"] == {"deleted": 2, "already_deleted": 0, "not_found": 1}
