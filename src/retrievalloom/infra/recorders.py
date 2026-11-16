"""Default provenance recorder."""

from __future__ import annotations

from typing import TYPE_CHECKING

from retrievalloom.contracts.component import ComponentInfo, make_info
from retrievalloom.contracts.infra import ProvenanceRecorderABC

if TYPE_CHECKING:
    from retrievalloom.domain.reports import DeleteReport, IngestReport
    from retrievalloom.domain.retrieval import RetrievalRequest, RetrievalResult


class NoOpProvenanceRecorder(ProvenanceRecorderABC):
    """Records nothing (used when no agxai runtime is configured)."""

    _INFO = make_info(component="provenance_recorder", implementation="noop", version="1")

    @property
    def info(self) -> ComponentInfo:
        """Component identity."""
        return self._INFO

    async def record_retrieval(self, request: RetrievalRequest, result: RetrievalResult) -> None:
        """Do nothing."""
        del request, result

    async def record_ingestion(self, report: IngestReport | DeleteReport) -> None:
        """Do nothing."""
        del report
