"""Retrieval snapshot: commit sequence, generations, reader registration and coverage policy."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from cachetools import LRUCache

from retrievalloom.domain.records import CoverageState, DocumentState, MissingIndexPolicy
from retrievalloom.domain.retrieval import (
    CoverageSummary,
    RetrievalSnapshot,
    RetrievalWarning,
    Severity,
    WarningCode,
)
from retrievalloom.errors import CoverageError

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Mapping, Sequence

    from retrievalloom.domain.inputs import Scope
    from retrievalloom.domain.records import IndexGeneration
    from retrievalloom.lifecycle.indexer import ReaderTracker
    from retrievalloom.lifecycle.units import IndexUnit
    from retrievalloom.retrieval.planning import ExecutionPlan, PlannedMethod
    from retrievalloom.storage.base import DocumentMetadataStoreABC, UnitKey

    BackfillScheduler = Callable[[Scope, Sequence[IndexUnit]], Awaitable[str | None]]

# Scopes whose last snapshot reads are kept for reuse.
_MEMO_SCOPES = 256
_NOT_READY = (
    CoverageState.PENDING,
    CoverageState.PROCESSING,
    CoverageState.STALE,
    CoverageState.FAILED,
)


@dataclass(frozen=True, slots=True)
class ReadyMethod:
    """A planned method bound to the generation it reads."""

    planned: PlannedMethod
    generation_id: str
    fingerprint: str
    partial: bool


@dataclass(slots=True)
class Snapshot:
    """Consistent view captured before any method executes."""

    seq: int
    active_documents: int
    methods: list[ReadyMethod] = field(default_factory=list)
    coverage: CoverageSummary = field(default_factory=CoverageSummary)
    warnings: list[RetrievalWarning] = field(default_factory=list)
    acquired: list[str] = field(default_factory=list)

    def model(self) -> RetrievalSnapshot:
        """Public form of the snapshot."""
        return RetrievalSnapshot(
            snapshot_seq=self.seq,
            generations={item.planned.name: item.generation_id for item in self.methods},
            fingerprints={item.planned.name: item.fingerprint for item in self.methods},
        )


@dataclass(slots=True)
class _Reads:
    """Snapshot inputs read under one snapshot token (``None``: not reusable)."""

    token: str | None
    documents: dict[str, int]
    counts: dict[str, dict[str, int]]
    generations: dict[UnitKey, IndexGeneration | None] = field(default_factory=dict)

    def copy(self) -> _Reads:
        """A copy whose generation map can be extended independently."""
        return _Reads(self.token, self.documents, self.counts, dict(self.generations))


def _warning(
    code: WarningCode, message: str, method: PlannedMethod, severity: Severity, **details: Any
) -> RetrievalWarning:
    unit = method.unit
    return RetrievalWarning(
        code=code,
        severity=severity,
        channel=unit.channel,
        method=unit.method,
        message=message,
        details=dict(details),
    )


class SnapshotReader:
    """Captures snapshots and registers in-process readers of the generations they use."""

    def __init__(
        self,
        store: DocumentMetadataStoreABC,
        readers: ReaderTracker,
        *,
        instance_id: str,
        schedule_backfill: BackfillScheduler | None = None,
    ) -> None:
        self._store = store
        self._readers = readers
        self._instance_id = instance_id
        self._schedule_backfill = schedule_backfill
        self._memo: LRUCache[str, _Reads] = LRUCache(maxsize=_MEMO_SCOPES)

    async def _read(self, scope: Scope, plan: ExecutionPlan) -> tuple[int, _Reads]:
        """Commit sequence and snapshot inputs, reusing reads of an unchanged scope.

        Reads under one snapshot token see equal document counts, coverage counts,
        pointers and generations (see ``snapshot_marker``), so an entry read under the
        token observed in this transaction is reused and only pointers of units it lacks
        are read.
        """
        async with self._store.transaction(scope, write=False) as transaction:
            seq, token = await transaction.snapshot_marker(scope.key)
            reads = self._memo.get(scope.key) if token is not None else None
            if reads is None or reads.token != token:
                reads = _Reads(
                    token=token,
                    documents=dict(await transaction.count_documents(scope.key)),
                    counts={
                        key: dict(value)
                        for key, value in (
                            await transaction.coverage_counts(scope.key, ["", self._instance_id])
                        ).items()
                    },
                )
            else:
                reads = reads.copy()
            for method in plan.methods:
                key = method.unit.key(scope)
                if key in reads.generations:
                    continue
                pointer = await transaction.get_pointer(key)
                reads.generations[key] = (
                    None
                    if pointer is None
                    else await transaction.get_generation(pointer.generation_id)
                )
        if token is not None:
            self._memo[scope.key] = reads
        return seq, reads

    async def capture(self, scope: Scope, plan: ExecutionPlan) -> Snapshot:
        """Capture the snapshot of ``plan`` and acquire its generations.

        The caller must :meth:`release` the snapshot. Nothing stays acquired when this
        method raises.

        Raises:
            CoverageError: A method has no index and the policy is ``fail``.
        """
        seq, reads = await self._read(scope, plan)
        generations: dict[str, IndexGeneration | None] = {
            method.name: reads.generations[method.unit.key(scope)] for method in plan.methods
        }
        counts = reads.counts
        active = int(reads.documents.get(DocumentState.ACTIVE.value, 0))
        snapshot = Snapshot(
            seq=seq,
            active_documents=active,
            coverage=CoverageSummary(
                counts={key: dict(value) for key, value in counts.items()},
                fraction_ready={key: _fraction(value, active) for key, value in counts.items()},
            ),
        )
        if active == 0:
            return snapshot
        try:
            await self._bind(scope, plan, snapshot, generations)
        except BaseException:
            self.release(snapshot)
            raise
        return snapshot

    async def _bind(
        self,
        scope: Scope,
        plan: ExecutionPlan,
        snapshot: Snapshot,
        generations: dict[str, IndexGeneration | None],
    ) -> None:
        coverage = snapshot.coverage.counts
        backfill: list[IndexUnit] = []
        missing: list[str] = []
        for method in plan.methods:
            unit = method.unit
            generation = generations[method.name]
            if generation is None:
                missing.append(method.name)
                backfill.append(unit)
                snapshot.warnings.append(
                    _warning(
                        WarningCode.INDEX_MISSING,
                        "method has no published index",
                        method,
                        Severity.WARNING,
                    )
                )
                continue
            if generation.dependency_fingerprint != unit.fingerprint:
                snapshot.warnings.append(
                    _warning(
                        WarningCode.STALE_COVERAGE,
                        "index was built with a different configuration and is being rebuilt",
                        method,
                        Severity.WARNING,
                    )
                )
                continue
            generation_id = await self._acquire(scope, method, generation.generation_id)
            if generation_id is None:
                snapshot.warnings.append(
                    _warning(
                        WarningCode.STAGE_SKIPPED,
                        "index generation is being replaced",
                        method,
                        Severity.WARNING,
                    )
                )
                continue
            snapshot.acquired.append(generation_id)
            states = coverage.get(method.name, {})
            not_ready = {state.value: states.get(state.value, 0) for state in _NOT_READY}
            covered = states.get(CoverageState.READY.value, 0) + states.get(
                CoverageState.NOT_APPLICABLE.value, 0
            )
            partial = any(not_ready.values()) or covered < snapshot.active_documents
            if partial:
                snapshot.warnings.append(
                    _warning(
                        WarningCode.PARTIAL_COVERAGE,
                        "index does not cover every active document yet",
                        method,
                        Severity.WARNING,
                        **{k: v for k, v in not_ready.items() if v},
                        covered=covered,
                        active=snapshot.active_documents,
                    )
                )
                if not_ready[CoverageState.FAILED.value]:
                    backfill.append(unit)
            snapshot.methods.append(
                ReadyMethod(
                    planned=method,
                    generation_id=generation_id,
                    fingerprint=generation.dependency_fingerprint,
                    partial=partial,
                )
            )
        if missing and plan.missing_index_policy == MissingIndexPolicy.FAIL:
            raise CoverageError("required indexes are missing", details={"methods": missing})
        if (
            backfill
            and plan.missing_index_policy == MissingIndexPolicy.SCHEDULE_BACKFILL
            and self._schedule_backfill is not None
        ):
            task_id = await self._schedule_backfill(scope, backfill)
            if task_id is not None:
                snapshot.warnings.append(
                    RetrievalWarning(
                        code=WarningCode.BACKFILL_SCHEDULED,
                        severity=Severity.INFO,
                        message="a backfill task was scheduled",
                        details={"task_id": task_id, "units": [u.name for u in backfill]},
                    )
                )

    async def _acquire(self, scope: Scope, method: PlannedMethod, generation_id: str) -> str | None:
        """Register a reader; re-read the pointer once when the generation is retiring."""
        if await self._register(scope, method, generation_id):
            return generation_id
        async with self._store.transaction(scope, write=False) as transaction:
            pointer = await transaction.get_pointer(method.unit.key(scope))
            generation = (
                None if pointer is None else await transaction.get_generation(pointer.generation_id)
            )
        if (
            generation is None
            or generation.generation_id == generation_id
            or generation.dependency_fingerprint != method.unit.fingerprint
            or not await self._register(scope, method, generation.generation_id)
        ):
            return None
        return generation.generation_id

    async def _register(self, scope: Scope, method: PlannedMethod, generation_id: str) -> bool:
        """Count an in-process reader and, for durable backends, hold a durable lease."""
        if not self._readers.try_acquire(generation_id):
            return False
        if method.unit.instance:
            return True
        try:
            leased = await self._readers.lease(
                self._store, scope, generation_id, instance_id=self._instance_id
            )
        except BaseException:
            self._readers.release(generation_id)
            raise
        if not leased:
            self._readers.release(generation_id)
        return leased

    def release(self, snapshot: Snapshot) -> None:
        """Release every generation the snapshot acquired (idempotent)."""
        while snapshot.acquired:
            self._readers.release(snapshot.acquired.pop())


def _fraction(states: Mapping[str, int], active: int) -> float:
    covered = states.get(CoverageState.READY.value, 0) + states.get(
        CoverageState.NOT_APPLICABLE.value, 0
    )
    if active <= 0:
        return 0.0
    return min(1.0, covered / active)


__all__ = ["ReadyMethod", "Snapshot", "SnapshotReader"]
