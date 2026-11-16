"""Engine builders, documents and small failure-injecting doubles for retrieval tests."""

from __future__ import annotations

import contextlib
from typing import TYPE_CHECKING, Any

from retrievalloom import (
    ChunkInput,
    ChunksRepresentation,
    Components,
    DocumentInput,
    EngineResources,
    LifecycleConfig,
    RetrievalConfig,
    Scope,
    TextRepresentation,
    create_retrieval_engine,
)
from retrievalloom.contracts.component import ComponentInfo, make_info
from retrievalloom.contracts.infra import (
    AccessDecision,
    AccessPolicyABC,
    CacheABC,
    ProvenanceRecorderABC,
)
from tests.support import shared_manager

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from retrievalloom import IngestionConfig, RetrievalEngine
    from retrievalloom.contracts.infra import Action
    from retrievalloom.domain.filters import FilterExpression
    from retrievalloom.domain.reports import DeleteReport, IngestReport
    from retrievalloom.domain.retrieval import RetrievalRequest, RetrievalResult

ACME = Scope(tenant_id="acme")
GLOBEX = Scope(tenant_id="globex")


def build_engine(
    *,
    retrieval: RetrievalConfig | None = None,
    components: Components | None = None,
    lifecycle: LifecycleConfig | None = None,
    ingestion: IngestionConfig | None = None,
    **resources: Any,
) -> RetrievalEngine:
    """Deterministic engine on the shared resource manager."""
    return create_retrieval_engine(
        retrieval=retrieval or RetrievalConfig(),
        components=components,
        lifecycle=lifecycle or LifecycleConfig(background=False),
        ingestion=ingestion,
        resources=EngineResources(resource_manager=shared_manager(), **resources),
    )


@contextlib.asynccontextmanager
async def opened(**kwargs: Any) -> AsyncGenerator[RetrievalEngine]:
    """``build_engine`` inside ``async with``."""
    async with build_engine(**kwargs) as engine:
        yield engine


def text_doc(doc_id: str, text: str, **metadata: Any) -> DocumentInput:
    """Single text representation document."""
    return DocumentInput(
        _id=doc_id, metadata=metadata, representations=(TextRepresentation(text=text),)
    )


def chunks_doc(doc_id: str, *texts: str, **metadata: Any) -> DocumentInput:
    """Document of explicit chunks."""
    return DocumentInput(
        _id=doc_id,
        metadata=metadata,
        representations=(ChunksRepresentation(chunks=tuple(ChunkInput(text=t) for t in texts)),),
    )


def runtime(engine: RetrievalEngine) -> Any:
    """The engine's private runtime (tests reach into wiring for failure injection)."""
    value = engine._runtime
    assert value is not None
    return value


def unit(engine: RetrievalEngine, name: str) -> Any:
    """Index unit ``name`` of an open engine."""
    return runtime(engine).units[name]


class RecordingRecorder(ProvenanceRecorderABC):
    """Records calls; optionally raises."""

    _INFO = make_info(component="provenance_recorder", implementation="recording", version="1")

    def __init__(self, error: BaseException | None = None) -> None:
        self.error = error
        self.retrievals: list[tuple[RetrievalRequest, RetrievalResult]] = []
        self.ingestions: list[IngestReport | DeleteReport] = []

    @property
    def info(self) -> ComponentInfo:
        """Component identity."""
        return self._INFO

    async def record_retrieval(self, request: RetrievalRequest, result: RetrievalResult) -> None:
        """Record or raise."""
        if self.error is not None:
            raise self.error
        self.retrievals.append((request, result))

    async def record_ingestion(self, report: IngestReport | DeleteReport) -> None:
        """Record or raise."""
        if self.error is not None:
            raise self.error
        self.ingestions.append(report)


class TablePolicy(AccessPolicyABC):
    """Denies listed actions and returns a fixed row filter per principal."""

    _INFO = make_info(component="access_policy", implementation="table", version="1")

    def __init__(
        self,
        *,
        denied: set[Action] | None = None,
        filters: dict[str | None, FilterExpression] | None = None,
    ) -> None:
        self.denied = denied or set()
        self.filters = filters or {}
        self.closed = False

    @property
    def info(self) -> ComponentInfo:
        """Component identity."""
        return self._INFO

    async def authorize(
        self, principal: str | None, scope: Scope, action: Action
    ) -> AccessDecision:
        """Deny listed actions."""
        del principal, scope
        if action in self.denied:
            return AccessDecision(allowed=False, reason="listed")
        return AccessDecision(allowed=True)

    async def row_filter(self, principal: str | None, scope: Scope) -> FilterExpression | None:
        """Configured filter of ``principal``."""
        del scope
        return self.filters.get(principal)

    async def close(self) -> None:
        """Remember closing (application-owned components must never be closed)."""
        self.closed = True


class DictCache(CacheABC):
    """Unbounded dict cache counting hits."""

    _INFO = make_info(component="cache", implementation="dict", version="1")

    def __init__(self) -> None:
        self.data: dict[tuple[str, str], Any] = {}
        self.hits = 0

    @property
    def info(self) -> ComponentInfo:
        """Component identity."""
        return self._INFO

    async def get(self, namespace: str, key: str) -> Any | None:
        """Lookup."""
        value = self.data.get((namespace, key))
        if value is not None:
            self.hits += 1
        return value

    async def set(self, namespace: str, key: str, value: Any) -> None:
        """Store."""
        self.data[namespace, key] = value

    async def invalidate(self, namespace: str, prefix: str | None = None) -> int:
        """Drop entries."""
        keys = [k for k in self.data if k[0] == namespace and k[1].startswith(prefix or "")]
        for k in keys:
            del self.data[k]
        return len(keys)
