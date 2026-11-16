"""agxai integration: record retrieval provenance into an application-owned ``XAIRuntime``."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from agxai import EvidenceType, RetrievedDocument

from retrievalloom.contracts.component import ComponentInfo, make_info
from retrievalloom.contracts.infra import ProvenanceRecorderABC
from retrievalloom.domain.reports import DeleteReport
from retrievalloom.errors import TenantIsolationError

if TYPE_CHECKING:
    from collections.abc import Callable

    from agxai import Run, XAIRuntime

    from retrievalloom.domain.inputs import Scope
    from retrievalloom.domain.reports import IngestReport
    from retrievalloom.domain.retrieval import Evidence, RetrievalRequest, RetrievalResult

RETRIEVER_PREFIX = "retrievalloom"


class AgxaiProvenanceRecorder(ProvenanceRecorderABC):
    """Maps results one way onto agxai (RetrievalLoom → agxai) inside the active agxai run.

    Each retrieval becomes one ``record_retrieval`` call with one ``RetrievedDocument`` per
    evidence item (``content_reference`` is the evidence id; no text is copied), plus a
    ``derived_from`` provenance link from every source document to its evidence. Ingestion
    and deletion become one ``custom`` evidence record with outcome counts only.

    Recording needs a run: the runtime's current run, or one returned by ``run_provider``.
    Without a run nothing is recorded. The runtime stays application-owned and is never
    closed here.

    Args:
        runtime: The application's ``XAIRuntime``.
        run_provider: Returns the run to record into (defaults to ``runtime.current_run``).
        record_links: Also record a provenance link per evidence source.
        reranker_id: Reranker identity reported when evidence carries rerank scores.

    Raises:
        TenantIsolationError: (when recording) the run's tenant differs from the scope's.
    """

    def __init__(
        self,
        runtime: XAIRuntime,
        *,
        run_provider: Callable[[], Run | None] | None = None,
        record_links: bool = True,
        reranker_id: str | None = None,
    ) -> None:
        self._runtime = runtime
        self._reranker_id = reranker_id
        self._run_provider = run_provider
        self._record_links = record_links
        self._info = make_info(
            component="provenance_recorder",
            implementation="agxai",
            version="1",
            config={"record_links": record_links},
        )

    @property
    def info(self) -> ComponentInfo:
        """Component identity."""
        return self._info

    def _run(self, scope: Scope) -> Run | None:
        run = self._run_provider() if self._run_provider is not None else None
        if run is None:
            run = self._runtime.current_run
        if run is None:
            return None
        tenant = run.execution.context.tenant_id
        if tenant is not None and tenant != scope.tenant_id:
            raise TenantIsolationError(
                "agxai run belongs to another tenant", details={"scope": scope.key}
            )
        return run

    async def record_retrieval(self, request: RetrievalRequest, result: RetrievalResult) -> None:
        """Record ``result`` as one agxai retrieval execution."""
        run = self._run(result.scope)
        if run is None:
            return
        documents = [_document(rank, item) for rank, item in enumerate(result.evidence, start=1)]
        await self._runtime.record_retrieval(
            f"{RETRIEVER_PREFIX}:{result.scope.collection}",
            documents=documents,
            query_reference=request.request_id,
            reranker=self._reranker_id
            if any(item.scores.rerank is not None for item in result.evidence)
            else None,
            metadata={
                "request_id": result.request_id,
                "scope_key": result.scope.key,
                "status": result.status.value,
                "snapshot_seq": result.snapshot.snapshot_seq,
                "warnings": [warning.code.value for warning in result.warnings],
            },
            run=run,
        )
        if not self._record_links:
            return
        for item in result.evidence:
            for document_uid in dict.fromkeys(source.document_uid for source in item.sources):
                await self._runtime.record_provenance(
                    document_uid,
                    item.evidence_id,
                    "derived_from",
                    metadata={"request_id": result.request_id},
                    run=run,
                )

    async def record_ingestion(self, report: IngestReport | DeleteReport) -> None:
        """Record outcome counts of an ingestion or deletion (no content)."""
        run = self._run(report.scope)
        if run is None:
            return
        if isinstance(report, DeleteReport):
            summary = "retrievalloom deletion"
            counts: dict[str, Any] = {
                "deleted": len(report.deleted),
                "already_deleted": len(report.already_deleted),
                "not_found": len(report.not_found),
            }
        else:
            summary = "retrievalloom ingestion"
            counts = {outcome.value: count for outcome, count in report.outcomes.items()}
        await self._runtime.record_evidence(
            EvidenceType.CUSTOM,
            summary=summary,
            content_reference=report.scope.key,
            metadata={"scope_key": report.scope.key, "counts": counts},
            run=run,
        )


def _document(rank: int, item: Evidence) -> RetrievedDocument:
    first = item.sources[0]
    return RetrievedDocument(
        document_id=first.document_id,
        chunk_id=item.segments[0].object_id,
        rank=rank,
        score=item.score,
        content_reference=item.evidence_id,
        metadata={
            "document_ids": list(dict.fromkeys(source.document_id for source in item.sources)),
            "versions": sorted({source.document_version for source in item.sources}),
            "objects": [segment.object_id for segment in item.segments],
            "has_contradiction": item.nli_summary.has_contradiction,
        },
    )


__all__ = ["AgxaiProvenanceRecorder"]
