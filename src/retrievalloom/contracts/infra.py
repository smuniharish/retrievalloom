"""Infrastructure contracts: cache, access policy and provenance recording."""

from __future__ import annotations

from abc import abstractmethod
from enum import StrEnum
from typing import TYPE_CHECKING, Any

from retrievalloom.contracts.component import ComponentABC
from retrievalloom.domain.base import FrozenModel

if TYPE_CHECKING:
    from retrievalloom.domain.filters import FilterExpression
    from retrievalloom.domain.inputs import Scope
    from retrievalloom.domain.reports import DeleteReport, IngestReport
    from retrievalloom.domain.retrieval import RetrievalRequest, RetrievalResult


class CacheABC(ComponentABC):
    """Async key-value cache. Keys are engine-built and always include the scope key."""

    @abstractmethod
    async def get(self, namespace: str, key: str) -> Any | None:
        """Return the cached value or ``None``."""

    @abstractmethod
    async def set(self, namespace: str, key: str, value: Any) -> None:
        """Store a value."""

    @abstractmethod
    async def invalidate(self, namespace: str, prefix: str | None = None) -> int:
        """Remove entries of ``namespace`` (optionally only keys starting with ``prefix``)."""


class Action(StrEnum):
    """Authorised operations."""

    INGEST = "ingest"
    RETRIEVE = "retrieve"
    DELETE = "delete"
    BACKFILL = "backfill"
    REBUILD = "rebuild"
    STATUS = "status"


class AccessDecision(FrozenModel):
    """Access policy decision."""

    allowed: bool
    reason: str | None = None


class AccessPolicyABC(ComponentABC):
    """Authorises operations and contributes row filters for retrieval."""

    @abstractmethod
    async def authorize(
        self, principal: str | None, scope: Scope, action: Action
    ) -> AccessDecision:
        """Decide whether ``principal`` may perform ``action`` in ``scope``."""

    @abstractmethod
    async def row_filter(self, principal: str | None, scope: Scope) -> FilterExpression | None:
        """Return a filter every visible source document must satisfy, or ``None``."""


class ProvenanceRecorderABC(ComponentABC):
    """Records retrieval and ingestion provenance in an external system."""

    @abstractmethod
    async def record_retrieval(self, request: RetrievalRequest, result: RetrievalResult) -> None:
        """Record one retrieval."""

    @abstractmethod
    async def record_ingestion(self, report: IngestReport | DeleteReport) -> None:
        """Record one ingestion or deletion."""
