"""Custom infrastructure: TTL cache, tenant/role access policy, JSONL audit recorder."""

from __future__ import annotations

import json
import threading
import time
from collections import OrderedDict
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

from retrievalloom.contracts.component import ComponentInfo, make_info
from retrievalloom.contracts.infra import (
    AccessDecision,
    AccessPolicyABC,
    Action,
    CacheABC,
    ProvenanceRecorderABC,
)
from retrievalloom.domain.filters import In
from retrievalloom.domain.reports import DeleteReport
from retrievalloom.errors import ConfigurationError, LifecycleError
from retrievalloom.runtime.offloader import WorkloadKind

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

    from retrievalloom.contracts.component import ComponentContext
    from retrievalloom.domain.filters import FilterExpression
    from retrievalloom.domain.inputs import Scope
    from retrievalloom.domain.reports import IngestReport
    from retrievalloom.domain.retrieval import RetrievalRequest, RetrievalResult
    from retrievalloom.runtime.offloader import Offloader


class TTLCache(CacheABC):
    """In-process LRU cache whose entries expire after ``ttl`` seconds.

    Expired entries are dropped lazily on read and when the namespace is full.

    Args:
        ttl: Time to live in seconds.
        max_entries: Entries kept per namespace (least recently used evicted first).
        clock: Monotonic clock (injectable for tests).
    """

    def __init__(
        self,
        *,
        ttl: float = 300.0,
        max_entries: int = 10_000,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if ttl <= 0 or max_entries < 1:
            raise ConfigurationError("ttl must be positive and max_entries at least 1")
        self._ttl = ttl
        self._max = max_entries
        self._clock = clock
        self._data: dict[str, OrderedDict[str, tuple[float, Any]]] = {}
        self.hits = 0
        self.misses = 0
        self._info = make_info(
            component="cache",
            implementation="ttl-lru",
            version="1",
            config={"ttl": ttl, "max_entries": max_entries},
        )

    @property
    def info(self) -> ComponentInfo:
        """Component identity."""
        return self._info

    async def get(self, namespace: str, key: str) -> Any | None:
        """Return a live value (refreshing its LRU position) or ``None``."""
        entries = self._data.get(namespace)
        entry = entries.get(key) if entries is not None else None
        if entry is None or entry[0] <= self._clock():
            if entry is not None and entries is not None:
                del entries[key]
            self.misses += 1
            return None
        assert entries is not None
        entries.move_to_end(key)
        self.hits += 1
        return entry[1]

    async def set(self, namespace: str, key: str, value: Any) -> None:
        """Store ``value`` until ``now + ttl``."""
        entries = self._data.setdefault(namespace, OrderedDict())
        now = self._clock()
        entries[key] = (now + self._ttl, value)
        entries.move_to_end(key)
        if len(entries) > self._max:
            for stale in [k for k, (expires, _) in entries.items() if expires <= now]:
                del entries[stale]
            while len(entries) > self._max:
                entries.popitem(last=False)

    async def invalidate(self, namespace: str, prefix: str | None = None) -> int:
        """Remove a namespace or the keys starting with ``prefix``."""
        entries = self._data.get(namespace)
        if not entries:
            return 0
        if prefix is None:
            removed = len(entries)
            del self._data[namespace]
            return removed
        doomed = [key for key in entries if key.startswith(prefix)]
        for key in doomed:
            del entries[key]
        return len(doomed)

    async def close(self) -> None:
        """Drop every entry."""
        self._data.clear()


ROLE_ACTIONS: dict[str, frozenset[Action]] = {
    "viewer": frozenset({Action.RETRIEVE, Action.STATUS}),
    "editor": frozenset({Action.RETRIEVE, Action.STATUS, Action.INGEST, Action.DELETE}),
    "admin": frozenset(Action),
}
ROLE_VISIBILITY: dict[str, tuple[str, ...] | None] = {
    "viewer": ("public",),
    "editor": ("public", "internal"),
    "admin": None,
}


class TenantRoleAccessPolicy(AccessPolicyABC):
    """Principals look like ``"<tenant>:<role>"`` (optionally ``":<user>"``).

    * A principal may only act in scopes of its own tenant (cross-tenant is denied).
    * The role decides the allowed actions (``ROLE_ACTIONS``).
    * The row filter restricts what a role sees by the documents' ``visibility`` metadata
      (``ROLE_VISIBILITY``); admins see everything. Documents without ``visibility`` are
      therefore visible to admins only - set it at ingestion.
    * ``service_principals`` (e.g. a lifecycle worker) are allowed everything.

    Args:
        service_principals: Principals with unrestricted access.
        allow_anonymous: Treat ``principal=None`` as a viewer of every tenant (default deny).
    """

    def __init__(
        self, *, service_principals: frozenset[str] = frozenset(), allow_anonymous: bool = False
    ) -> None:
        self._service = service_principals
        self._anonymous = allow_anonymous
        self._info = make_info(
            component="access_policy",
            implementation="tenant-role",
            version="1",
            config={"service": sorted(service_principals), "anonymous": allow_anonymous},
        )

    @property
    def info(self) -> ComponentInfo:
        """Component identity."""
        return self._info

    def _parse(self, principal: str | None, scope: Scope) -> tuple[str, str] | AccessDecision:
        if principal is None:
            if self._anonymous:
                return scope.tenant_id, "viewer"
            return AccessDecision(allowed=False, reason="anonymous access is disabled")
        tenant, _, rest = principal.partition(":")
        role = rest.partition(":")[0]
        if role not in ROLE_ACTIONS:
            return AccessDecision(allowed=False, reason=f"unknown role {role!r}")
        if tenant != scope.tenant_id:
            return AccessDecision(
                allowed=False, reason=f"principal of tenant {tenant!r} cannot access this tenant"
            )
        return tenant, role

    async def authorize(
        self, principal: str | None, scope: Scope, action: Action
    ) -> AccessDecision:
        """Deny cross-tenant access and actions outside the role."""
        if principal is not None and principal in self._service:
            return AccessDecision(allowed=True, reason="service principal")
        parsed = self._parse(principal, scope)
        if isinstance(parsed, AccessDecision):
            return parsed
        _, role = parsed
        if action not in ROLE_ACTIONS[role]:
            return AccessDecision(allowed=False, reason=f"role {role!r} may not {action.value}")
        return AccessDecision(allowed=True, reason=f"role {role!r}")

    async def row_filter(self, principal: str | None, scope: Scope) -> FilterExpression | None:
        """Visibility filter of the principal's role (``None`` for admins and services)."""
        if principal is not None and principal in self._service:
            return None
        parsed = self._parse(principal, scope)
        if isinstance(parsed, AccessDecision):
            return In(field="visibility", values=("__denied__",))
        allowed = ROLE_VISIBILITY[parsed[1]]
        return None if allowed is None else In(field="visibility", values=allowed)


class JsonlProvenanceRecorder(ProvenanceRecorderABC):
    """Append-only JSONL audit log of retrievals, ingestions and deletions.

    Each retrieval line names the request, principal, query, the evidence returned and
    the exact document versions it came from, so an answer can be traced later. Writes
    run off the event loop and are serialised with a lock.

    Args:
        path: Log file (parent directories are created).
        include_query: Store the query text (disable for sensitive queries).
    """

    def __init__(self, path: str | Path, *, include_query: bool = True) -> None:
        self._path = Path(path)
        self._include_query = include_query
        self._lock = threading.Lock()
        self._offloader: Offloader | None = None
        self._info = make_info(
            component="provenance_recorder",
            implementation="jsonl",
            version="1",
            config={"include_query": include_query},
        )

    @property
    def info(self) -> ComponentInfo:
        """Component identity."""
        return self._info

    @property
    def path(self) -> Path:
        """The log file."""
        return self._path

    async def initialize(self, context: ComponentContext) -> None:
        """Create the log directory."""
        self._offloader = context.offloader
        await context.offloader.run(
            WorkloadKind.IO_RETRIEVAL, lambda: self._path.parent.mkdir(parents=True, exist_ok=True)
        )

    def _append(self, record: Mapping[str, Any]) -> None:
        line = json.dumps(record, sort_keys=True, default=str) + "\n"
        with self._lock, self._path.open("a", encoding="utf-8") as handle:
            handle.write(line)

    async def _write(self, record: dict[str, Any]) -> None:
        if self._offloader is None:
            raise LifecycleError("provenance recorder is not initialised")
        record["at"] = datetime.now(UTC).isoformat()
        await self._offloader.run(WorkloadKind.IO_RETRIEVAL, self._append, record)

    async def record_retrieval(self, request: RetrievalRequest, result: RetrievalResult) -> None:
        """One line per retrieval with the returned evidence and source versions."""
        await self._write(
            {
                "event": "retrieval",
                "request_id": request.request_id,
                "tenant": request.scope.tenant_id,
                "collection": request.scope.collection,
                "principal": request.principal,
                "query": request.query if self._include_query else None,
                "status": result.status.value,
                "evidence": [
                    {
                        "evidence_id": item.evidence_id,
                        "score": round(item.score, 6),
                        "sources": sorted(
                            {f"{s.document_id}@v{s.document_version}" for s in item.sources}
                        ),
                    }
                    for item in result.evidence
                ],
                "warnings": [warning.code.value for warning in result.warnings],
            }
        )

    async def record_ingestion(self, report: IngestReport | DeleteReport) -> None:
        """One line per ingest or delete call."""
        if isinstance(report, DeleteReport):
            await self._write(
                {
                    "event": "delete",
                    "tenant": report.scope.tenant_id,
                    "collection": report.scope.collection,
                    "deleted": list(report.deleted),
                    "not_found": list(report.not_found),
                }
            )
            return
        await self._write(
            {
                "event": "ingest",
                "tenant": report.scope.tenant_id,
                "collection": report.scope.collection,
                "documents": [
                    {
                        "document_id": document.document_id,
                        "outcome": document.outcome.value,
                        "version": document.version,
                    }
                    for document in report.documents
                ],
            }
        )
