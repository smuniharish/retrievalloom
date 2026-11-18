"""Lifecycle coordination through refresh-engine: discovery, retries, scheduling, tasks."""

from __future__ import annotations

import asyncio
import contextlib
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

from refresh_engine import (
    AsyncScheduler,
    DiscoveryResult,
    RefreshConfig,
    RefreshEngine,
    RefreshMode,
    RefreshOperation,
    Resource,
    ResourceSnapshot,
    ResourceSource,
    RetryConfig,
    TimeoutConfig,
)
from resourcera import WorkloadProfile, WorkloadRequirements

from retrievalloom.config import EngineLimits
from retrievalloom.domain.hashing import random_id
from retrievalloom.domain.records import (
    CoverageState,
    ErrorInfo,
    LifecycleTask,
    TaskKind,
    TaskState,
)
from retrievalloom.domain.reports import LifecycleTaskRef, LifecycleTaskResult
from retrievalloom.errors import (
    BackendTimeoutError,
    BackendUnavailableError,
    ConfigurationError,
    LifecycleError,
    OperationTimeoutError,
    ResourceAdmissionError,
    VersionConflictError,
)
from retrievalloom.lifecycle.indexer import task_units
from retrievalloom.runtime.concurrency import AdaptiveLimiter
from retrievalloom.storage.refresh_state import MetadataStateStore

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Sequence

    from refresh_engine import PlanAction, RefreshRequest, RefreshResult

    from retrievalloom.config import LifecycleConfig
    from retrievalloom.domain.base import Clock
    from retrievalloom.domain.inputs import Scope
    from retrievalloom.lifecycle.indexer import UnitIndexer
    from retrievalloom.lifecycle.purge import DocumentEraser
    from retrievalloom.lifecycle.units import IndexUnit
    from retrievalloom.storage.base import DocumentMetadataStoreABC

RETRYABLE = (
    BackendUnavailableError,
    BackendTimeoutError,
    VersionConflictError,
    ResourceAdmissionError,
    OperationTimeoutError,
)
_TERMINAL = {TaskState.SUCCEEDED, TaskState.FAILED, TaskState.CANCELLED}
_STATE_NAMESPACE = "lifecycle"
# Automatic lifecycle concurrency: index units mix CPU (tokenising, encoding) and I/O.
LIFECYCLE_FLOOR = 4
LIFECYCLE_REQUIREMENTS = WorkloadRequirements(cpu=0.5, memory="64MB", profile=WorkloadProfile.MIXED)  # type: ignore[arg-type]


@dataclass(frozen=True, slots=True)
class _Target:
    scope: Scope
    unit: IndexUnit


def resource_id(scope: Scope, unit: IndexUnit) -> str:
    """refresh-engine resource id of a unit in a scope."""
    return f"{scope.key}:{unit.name}:{unit.instance or '-'}"


class _UnitSource(ResourceSource):
    """Discovers the (scope, unit) pairs that lag behind the metadata store."""

    def __init__(self, coordinator: LifecycleCoordinator) -> None:
        self._coordinator = coordinator

    async def discover(self) -> DiscoveryResult:
        return DiscoveryResult(self._coordinator.discover_targets(), complete=False)

    async def snapshot(self, resource: Resource, /) -> ResourceSnapshot:
        return ResourceSnapshot(resource.resource_id, metadata=dict(resource.metadata))


class _UnitOperation(RefreshOperation):
    """Runs the indexer for one discovered (scope, unit)."""

    def __init__(self, coordinator: LifecycleCoordinator) -> None:
        self._coordinator = coordinator

    async def __call__(
        self,
        resource: Resource,
        _snapshot: ResourceSnapshot | None,
        _action: PlanAction,
        _request: RefreshRequest,
        /,
    ) -> None:
        await self._coordinator.process(resource.resource_id)


class LifecycleCoordinator:
    """Drives index units to coverage with refresh-engine and manages lifecycle tasks."""

    def __init__(
        self,
        store: DocumentMetadataStoreABC,
        indexer: UnitIndexer,
        units: Sequence[IndexUnit],
        config: LifecycleConfig,
        *,
        instance_id: str,
        engine_id: str,
        clock: Clock,
        logger: Any,
        eraser: DocumentEraser | None = None,
        limiter: AdaptiveLimiter | None = None,
    ) -> None:
        self._store = store
        self._indexer = indexer
        self._units = tuple(units)
        self._config = config
        self._instance_id = instance_id
        self._engine_id = engine_id
        self._clock = clock
        self._logger = logger
        self._eraser = eraser
        self._limiter = limiter or AdaptiveLimiter.from_setting(
            "lifecycle",
            config.max_concurrency,
            floor=LIFECYCLE_FLOOR,
            ceiling=EngineLimits().max_lifecycle_concurrency,
            requirements=LIFECYCLE_REQUIREMENTS,
        )
        self._housekeeping_interval = timedelta(seconds=config.housekeeping_interval)
        self._started_at = clock()
        self._housekept_at = datetime.min.replace(tzinfo=UTC)
        self._retried_at = datetime.min.replace(tzinfo=UTC)
        self._targets: dict[str, _Target] = {}
        self._scheduler: AsyncScheduler | None = None
        self._kicked: asyncio.Task[None] | None = None
        self._refresh = RefreshEngine(
            _UnitSource(self),
            _UnitOperation(self),
            store=MetadataStateStore(store, _STATE_NAMESPACE),
            config=RefreshConfig(
                # refresh-engine schedules up to the ceiling; the limiter admits units.
                max_concurrency=self._limiter.maximum,
                retry=RetryConfig(
                    max_attempts=config.max_attempts,
                    initial_delay=0.05,
                    max_delay=2.0,
                    retryable_exceptions=RETRYABLE,
                ),
                timeout=TimeoutConfig(),
            ),
        )

    @property
    def units(self) -> tuple[IndexUnit, ...]:
        """Configured index units."""
        return self._units

    def unit(self, name: str) -> IndexUnit:
        """Configured unit called ``name`` (``<channel>.<method>``)."""
        for unit in self._units:
            if unit.name == name:
                return unit
        raise ConfigurationError("unknown or disabled index unit", details={"unit": name})

    # refresh-engine callbacks -----------------------------------------------------------
    async def discover_targets(self) -> AsyncIterator[Resource]:
        """Yield every (scope, unit) that needs work."""
        async with self._store.transaction(write=False) as transaction:
            scopes = await transaction.list_scopes()
        for scope in scopes:
            for unit in self._units:
                if await self._indexer.needs_work(scope, unit):
                    rid = resource_id(scope, unit)
                    self._targets[rid] = _Target(scope, unit)
                    yield Resource(rid, metadata={"scope": scope.key, "unit": unit.name})

    async def process(self, rid: str) -> None:
        """Index one discovered target."""
        target = self._targets.get(rid)
        if target is None:
            raise LifecycleError("unknown lifecycle resource", details={"resource_id": rid})
        async with self._limiter.slot():
            try:
                # The deadline starts once a permit is held, so queueing never times out.
                async with asyncio.timeout(self._config.unit_timeout):
                    await self._indexer.run(target.scope, target.unit)
            except TimeoutError as exc:
                self._limiter.signal_overload("indexing timed out")
                raise OperationTimeoutError(
                    "index unit round timed out",
                    details={"unit": target.unit.name, "timeout": self._config.unit_timeout},
                ) from exc
            except (BackendTimeoutError, BackendUnavailableError, OperationTimeoutError) as exc:
                self._limiter.signal_overload(f"indexing raised {type(exc).__name__}")
                raise

    # rounds -----------------------------------------------------------------------------
    async def run_once(self, scope: Scope | None = None) -> RefreshResult:
        """Run one lifecycle round (all scopes, or only ``scope``).

        The round first evicts objects other engines purged from this engine's
        engine-local units; a failed eviction is logged and retried next round.
        """
        await self._evict_purged()
        if scope is None:
            result = await self._refresh.refresh(mode=RefreshMode.FULL)
        else:
            result = await self._refresh.refresh(
                mode=RefreshMode.TARGETED,
                resource_ids=[resource_id(scope, unit) for unit in self._units],
            )
        await self._settle_tasks(scope, result)
        if result.errors:
            self._logger.warning(
                "lifecycle_round_errors",
                status=result.status.value,
                errors=[issue.message for issue in result.errors][:10],
            )
        return result

    async def maintain(self, *, housekeeping: bool = True) -> None:
        """Heartbeat this instance, reconcile generations and (optionally) housekeep."""
        now = self._clock()
        async with self._store.transaction(write=True) as transaction:
            await transaction.heartbeat_instance(
                self._instance_id,
                self._engine_id,
                started_at=self._started_at,
                expires_at=now + timedelta(seconds=self._config.instance_ttl),
            )
            expired = [
                instance
                for instance in await transaction.expired_instances(now=now)
                if instance != self._instance_id
            ]
            await transaction.purge_expired_leases(now=now)
            scopes = await transaction.list_scopes()
        for scope in scopes:
            for unit in self._units:
                await self._indexer.reconcile(scope, unit)
        await self._retry_failed(scopes, now)
        for instance in expired:
            async with self._store.transaction(write=True) as transaction:
                await transaction.purge_instance(instance)
            self._logger.info("engine_instance_purged", instance_id=instance)
        if not housekeeping:
            return
        self._housekept_at = now
        for scope in scopes:
            await self.housekeep(scope)

    async def _retry_failed(self, scopes: Sequence[Scope], now: datetime) -> None:
        """Queue failed coverage again whose retryable error has outlived its backoff.

        Coverage fails after ``max_attempts`` attempts; when the last error is retryable
        (a backend outage, a timeout) the row is set pending again once
        ``retry_failed_after * 2 ** (attempts - max_attempts)`` seconds (at most
        ``retry_failed_max_delay``) have passed since it failed. ``attempts`` is kept,
        so every further failure doubles the delay; a backfill resets it. The sweep
        runs at most every ``retry_failed_after / 2`` seconds and is idempotent, so
        engines sharing the store may run it concurrently.
        """
        config = self._config
        initial = config.retry_failed_after
        if initial is None or now - self._retried_at < timedelta(seconds=initial / 2):
            return
        self._retried_at = now
        for scope in scopes:
            for unit in self._units:
                requeued = await self._requeue_failed(scope, unit, now, initial)
                if requeued:
                    self._logger.info(
                        "failed_coverage_requeued",
                        scope=scope.key,
                        unit=unit.name,
                        documents=requeued,
                    )

    async def _requeue_failed(
        self, scope: Scope, unit: IndexUnit, now: datetime, initial: float
    ) -> int:
        config = self._config
        key = unit.key(scope)
        requeued = 0
        cursor: str | None = None
        while True:
            async with self._store.transaction(scope, write=True) as transaction:
                rows = await transaction.list_coverage(
                    key,
                    (CoverageState.FAILED,),
                    limit=config.batch_size,
                    after_document_uid=cursor,
                )
                due = [
                    row
                    for row in rows
                    if row.last_error is not None
                    and row.last_error.retryable
                    and now - row.updated_at
                    >= timedelta(
                        seconds=min(
                            initial * 2 ** min(32, max(0, row.attempts - config.max_attempts)),
                            config.retry_failed_max_delay,
                        )
                    )
                ]
                await transaction.upsert_coverage(
                    [
                        row.model_copy(update={"state": CoverageState.PENDING, "updated_at": now})
                        for row in due
                    ]
                )
            requeued += len(due)
            if len(rows) < config.batch_size:
                return requeued
            cursor = rows[-1].document_uid

    async def housekeep(self, scope: Scope) -> None:
        """Purge expired tombstones; prune events, version history and purge log of ``scope``.

        Failures are logged and retried on the next maintenance round.
        """
        config = self._config
        now = self._clock()
        try:
            if self._eraser is not None and config.purge_after is not None:
                await self._eraser.purge_expired(
                    scope,
                    deleted_before=now - timedelta(seconds=config.purge_after),
                    limit=config.batch_size,
                )
            versions = 0
            events = 0
            purge_log = 0
            # Events first: a version stays while an event references it.
            while config.event_retention is not None:
                async with self._store.transaction(scope, write=True) as transaction:
                    pruned = await transaction.prune_events(
                        scope.key,
                        before=now - timedelta(seconds=config.event_retention),
                        limit=config.batch_size,
                    )
                events += pruned
                if pruned < config.batch_size:
                    break
            while config.max_versions is not None:
                async with self._store.transaction(scope, write=True) as transaction:
                    pruned = await transaction.prune_versions(
                        scope.key, keep=config.max_versions, limit=config.batch_size
                    )
                versions += pruned
                if pruned < config.batch_size:
                    break
            while True:
                async with self._store.transaction(scope, write=True) as transaction:
                    pruned = await transaction.prune_purge_log(
                        scope.key,
                        before=now - timedelta(seconds=config.purge_log_retention),
                        limit=config.batch_size,
                    )
                purge_log += pruned
                if pruned < config.batch_size:
                    break
        except Exception as exc:
            self._logger.error(
                "lifecycle_housekeeping_failed", scope=scope.key, error_type=type(exc).__name__
            )
            return
        if versions or events or purge_log:
            self._logger.info(
                "history_pruned",
                scope=scope.key,
                versions=versions,
                events=events,
                purge_log=purge_log,
            )

    async def _evict_purged(self) -> None:
        if self._eraser is None:
            return
        try:
            await self._eraser.evict_purged()
        except Exception as exc:
            self._logger.error("purge_eviction_failed", error_type=type(exc).__name__)

    async def tick(self) -> None:
        """One background round: maintenance, then indexing."""
        try:
            due = self._clock() - self._housekept_at >= self._housekeeping_interval
            await self.maintain(housekeeping=due)
            await self.run_once()
        except Exception as exc:
            info = ErrorInfo.from_exception(exc)
            self._logger.error(
                "lifecycle_tick_failed",
                error_type=type(exc).__name__,
                error_code=info.code,
                error=info.message,
                details=info.details,
            )

    async def start(self, *, rebuild: bool = False, rebuild_timeout: float | None = None) -> None:
        """Register the instance and start the background scheduler when enabled.

        With ``rebuild`` one lifecycle round runs before the scheduler starts, so
        ephemeral backends are rebuilt when this returns. ``rebuild_timeout`` bounds
        that round; on timeout the remaining work is left to the background rounds.
        A failed round stops the coordinator and re-raises.
        """
        if self._eraser is not None:
            await self._eraser.follow_purge_log()
        await self.maintain()
        if rebuild:
            try:
                async with asyncio.timeout(rebuild_timeout):
                    await self.run_once()
            except TimeoutError:
                self._logger.warning("startup_rebuild_timeout", timeout=rebuild_timeout)
            except BaseException:
                await self.stop()
                raise
        if self._config.background:
            self._scheduler = AsyncScheduler(
                self.tick, self._config.poll_interval, run_immediately=True
            )
            await self._scheduler.start()

    async def stop(self) -> None:
        """Stop the scheduler and refresh engine, then deregister this instance.

        Deregistering purges the instance's metadata rows (coverage, visibility, pointers
        and generations of its non-durable units), whose data disappears with it.
        """
        if self._scheduler is not None:
            await self._scheduler.stop(cancel=True, wait=True)
            self._scheduler = None
        if self._kicked is not None:
            self._kicked.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._kicked
        await self._refresh.close(cancel=True, wait=True)
        async with self._store.transaction(write=True) as transaction:
            await transaction.purge_instance(self._instance_id)
        self._logger.info("engine_instance_deregistered", instance_id=self._instance_id)

    def kick(self, scope: Scope | None = None) -> None:
        """Start a prompt background round (no-op without background scheduling)."""
        if self._scheduler is None or self._kicked is not None:
            return
        self._kicked = asyncio.create_task(self._kick_round(scope))

    async def _kick_round(self, scope: Scope | None) -> None:
        try:
            await self.run_once(scope)
        except Exception as exc:
            self._logger.error("lifecycle_round_failed", error_type=type(exc).__name__)
        finally:
            self._kicked = None

    @property
    def background(self) -> bool:
        """Whether a background scheduler is running."""
        return self._scheduler is not None

    # tasks ------------------------------------------------------------------------------
    async def request(
        self, scope: Scope, kind: TaskKind, units: Sequence[IndexUnit]
    ) -> LifecycleTaskRef:
        """Queue a BACKFILL or REBUILD task for ``units`` of ``scope``."""
        now = self._clock()
        task = LifecycleTask(
            task_id=random_id("task"),
            scope_key=scope.key,
            kind=kind,
            target={"units": [unit.name for unit in units]},
            state=TaskState.QUEUED,
            created_at=now,
            updated_at=now,
        )
        async with self._store.transaction(scope, write=True) as transaction:
            await transaction.ensure_scope(scope)
            await transaction.put_task(task, expected_revision=None)
            if kind == TaskKind.BACKFILL:
                for unit in units:
                    await self._reset_failed(transaction, scope, unit)
        self._logger.info(
            "lifecycle_task_queued",
            scope=scope.key,
            task_id=task.task_id,
            kind=kind.value,
            units=[unit.name for unit in units],
        )
        self.kick()
        return LifecycleTaskRef(task_id=task.task_id, scope=scope, kind=kind)

    async def _reset_failed(self, transaction: Any, scope: Scope, unit: IndexUnit) -> None:
        key = unit.key(scope)
        while True:
            rows = await transaction.list_coverage(key, (CoverageState.FAILED,), limit=1000)
            if not rows:
                return
            now = self._clock()
            await transaction.upsert_coverage(
                [
                    row.model_copy(
                        update={"state": CoverageState.PENDING, "attempts": 0, "updated_at": now}
                    )
                    for row in rows
                ]
            )

    async def _settle_tasks(self, scope: Scope | None, result: RefreshResult) -> None:
        """Complete caught-up BACKFILL tasks; fail tasks whose units keep failing."""
        failed_ids = {
            execution.resource_id for execution in result.executions if not execution.succeeded
        }
        async with self._store.transaction(scope, write=False) as transaction:
            tasks = await transaction.list_tasks(
                scope_key=None if scope is None else scope.key,
                states=(TaskState.QUEUED, TaskState.RUNNING),
            )
            scopes = {item.key: item for item in await transaction.list_scopes()}
        for task in tasks:
            task_scope = scopes.get(task.scope_key)
            if task_scope is None:
                continue  # pragma: no cover - tasks are created with their scope
            unknown = sorted(set(task_units(task)) - {unit.name for unit in self._units})
            if unknown:
                await self._finish(
                    task_scope,
                    task.task_id,
                    TaskState.FAILED,
                    ErrorInfo.from_exception(
                        LifecycleError(
                            "task targets units that are not configured",
                            details={"units": unknown},
                        )
                    ),
                )
                continue
            units = [unit for unit in self._units if unit.name in task_units(task)]
            if any(resource_id(task_scope, unit) in failed_ids for unit in units):
                await self._bump(task_scope, task.task_id)
            elif task.kind == TaskKind.BACKFILL and not [
                unit for unit in units if await self._indexer.needs_work(task_scope, unit)
            ]:
                await self._finish(task_scope, task.task_id, TaskState.SUCCEEDED)

    async def _finish(
        self, scope: Scope, task_id: str, state: TaskState, error: ErrorInfo | None = None
    ) -> None:
        async with self._store.transaction(scope, write=True) as transaction:
            task = await transaction.get_task(task_id)
            if task is None or task.state in _TERMINAL:
                return
            await transaction.put_task(
                task.model_copy(
                    update={"state": state, "error": error, "updated_at": self._clock()}
                ),
                expected_revision=task.row_revision,
            )

    async def _bump(self, scope: Scope, task_id: str) -> None:
        async with self._store.transaction(scope, write=True) as transaction:
            task = await transaction.get_task(task_id)
            if task is None or task.state in _TERMINAL:
                return
            attempts = task.attempts + 1
            failed = attempts >= self._config.max_attempts
            await transaction.put_task(
                task.model_copy(
                    update={
                        "attempts": attempts,
                        "state": TaskState.FAILED if failed else TaskState.RUNNING,
                        "error": ErrorInfo.from_exception(
                            LifecycleError("units did not reach coverage")
                        )
                        if failed
                        else None,
                        "updated_at": self._clock(),
                    }
                ),
                expected_revision=task.row_revision,
            )

    async def task_result(self, task_id: str, scope: Scope) -> LifecycleTaskResult:
        """Current state of a task."""
        async with self._store.transaction(scope, write=False) as transaction:
            task = await transaction.get_task(task_id)
        if task is None or task.scope_key != scope.key:
            raise LifecycleError("unknown lifecycle task", details={"task_id": task_id})
        return LifecycleTaskResult(
            task_id=task.task_id,
            kind=task.kind,
            state=task.state,
            attempts=task.attempts,
            error=task.error,
            details={"units": list(task_units(task))},
        )

    async def wait_for(
        self,
        ref: LifecycleTaskRef,
        *,
        timeout: float | None,  # noqa: ASYNC109 - public API bound documented in the plan
    ) -> LifecycleTaskResult:
        """Wait until a task is terminal, running rounds itself without a background scheduler.

        Raises:
            OperationTimeoutError: When ``timeout`` elapses first.
        """
        try:
            async with asyncio.timeout(timeout):
                while True:
                    result = await self.task_result(ref.task_id, ref.scope)
                    if result.state in _TERMINAL:
                        return result
                    if self.background:
                        await asyncio.sleep(min(self._config.poll_interval, 0.25))
                    else:
                        await self.run_once(ref.scope)
        except TimeoutError as exc:
            raise OperationTimeoutError(
                "lifecycle task did not finish in time", details={"task_id": ref.task_id}
            ) from exc


__all__ = ["RETRYABLE", "LifecycleCoordinator", "resource_id"]
