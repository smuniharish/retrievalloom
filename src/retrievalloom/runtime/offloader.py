"""Runs blocking work off the event loop with fairness, admission control and timeouts."""

from __future__ import annotations

import asyncio
import functools
from concurrent.futures import Executor, ThreadPoolExecutor
from enum import StrEnum
from typing import TYPE_CHECKING, Any

from pydantic import Field
from resourcera import (
    AdmissionRejectedError,
    ResourceConfig,
    ResourceError,
    ResourceLease,
    ResourceManager,
    WorkloadProfile,
    WorkloadRequirements,
)

from retrievalloom._logging import get_logger
from retrievalloom.domain.base import FrozenModel
from retrievalloom.errors import LifecycleError, OperationTimeoutError, ResourceAdmissionError

if TYPE_CHECKING:
    from collections.abc import Callable

_log = get_logger("runtime.offloader")


class WorkloadKind(StrEnum):
    """Kinds of blocking work, mapped to resource profiles."""

    IO_RETRIEVAL = "io_retrieval"
    FUZZY_MATCH = "fuzzy_match"
    SPARSE_INDEX = "sparse_index"
    SPARSE_QUERY = "sparse_query"
    KEYWORD_BUILD = "keyword_build"
    KEYWORD_QUERY = "keyword_query"
    VECTOR = "vector"
    EMBEDDING = "embedding"
    RERANK = "rerank"
    NLI = "nli"
    GRAPH_QUERY = "graph_query"
    STRUCTURE_INDEX = "structure_index"
    STRUCTURE_QUERY = "structure_query"
    HASHING = "hashing"
    EMBEDDING_VALIDATION = "embedding_validation"


_IO_KINDS = frozenset(
    {WorkloadKind.IO_RETRIEVAL, WorkloadKind.GRAPH_QUERY, WorkloadKind.STRUCTURE_QUERY}
)


class OffloaderConfig(FrozenModel):
    """Offloader settings.

    Attributes:
        max_workers: Threads of the engine-owned executor.
        max_concurrent_jobs_per_tenant: Default per-tenant fair-share limit.
        admission_wait_timeout: Seconds to wait for resourcera admission.
        memory_per_job: Memory requirement declared per job.
        cpu_per_job: Optional CPU requirement (needs warm resourcera samples).
        shutdown_timeout: Seconds ``close`` waits for running threads.
        resource_config: Settings for the engine-owned resourcera manager. The default
            raises resourcera's monitoring CPU budget to 5% so transient sampling
            spikes on busy hosts do not suspend admission.
    """

    max_workers: int = Field(default=8, ge=1, le=512)
    max_concurrent_jobs_per_tenant: int = Field(default=4, ge=1)
    admission_wait_timeout: float = Field(default=5.0, gt=0)
    memory_per_job: str = "16MB"
    cpu_per_job: float | None = Field(default=None, gt=0)
    shutdown_timeout: float = Field(default=30.0, gt=0)
    resource_config: ResourceConfig = Field(
        default_factory=lambda: ResourceConfig(cpu_budget_fraction=0.05)
    )


class Offloader:
    """Executes synchronous callables in an executor without blocking the loop.

    Permits (tenant semaphore and resourcera lease) are released by the executor
    future's completion, not by the awaiting coroutine, so cancelling or timing out a
    caller never over-commits capacity while the thread is still running.

    Args:
        config: Offloader settings.
        executor: Application-owned executor; the engine creates and owns one otherwise.
        resource_manager: Application-owned resourcera manager; engine-owned otherwise.
        tenant_limit: Optional callable returning the per-tenant job limit.
    """

    def __init__(
        self,
        config: OffloaderConfig | None = None,
        *,
        executor: Executor | None = None,
        resource_manager: ResourceManager | None = None,
        tenant_limit: Callable[[str], int] | None = None,
    ) -> None:
        self._config = config or OffloaderConfig()
        self._executor = executor
        self._owns_executor = executor is None
        self._resources = resource_manager
        self._owns_resources = resource_manager is None
        self._tenant_limit = tenant_limit
        self._semaphores: dict[str, asyncio.Semaphore] = {}
        self._outstanding: set[asyncio.Future[Any]] = set()
        self._releases: set[asyncio.Task[None]] = set()
        self._started = False
        self._closed = False

    @property
    def resource_manager(self) -> ResourceManager | None:
        """Resource manager in use (after ``start``)."""
        return self._resources

    @property
    def outstanding(self) -> int:
        """Number of jobs whose threads have not finished."""
        return len(self._outstanding)

    async def start(self) -> None:
        """Create engine-owned executor and resource manager (idempotent)."""
        if self._started:
            return
        if self._executor is None:
            self._executor = ThreadPoolExecutor(
                max_workers=self._config.max_workers, thread_name_prefix="retrievalloom"
            )
        if self._resources is None:
            self._resources = ResourceManager(self._config.resource_config)
        if self._owns_resources:
            await self._resources.start()
        self._started = True

    def _semaphore(self, tenant: str) -> asyncio.Semaphore:
        semaphore = self._semaphores.get(tenant)
        if semaphore is None:
            limit = (
                self._tenant_limit(tenant)
                if self._tenant_limit is not None
                else self._config.max_concurrent_jobs_per_tenant
            )
            semaphore = asyncio.Semaphore(limit)
            self._semaphores[tenant] = semaphore
        return semaphore

    def _requirements(self, kind: WorkloadKind) -> WorkloadRequirements:
        profile = WorkloadProfile.IO_BOUND if kind in _IO_KINDS else WorkloadProfile.CPU_BOUND
        return WorkloadRequirements(
            cpu=self._config.cpu_per_job,
            memory=self._config.memory_per_job,  # type: ignore[arg-type]
            profile=profile,
        )

    async def run[T](
        self,
        kind: WorkloadKind,
        fn: Callable[..., T],
        /,
        *args: Any,
        tenant: str | None = None,
        timeout: float | None = None,  # noqa: ASYNC109 - deadline must not cancel the thread
        executor: Executor | None = None,
    ) -> T:
        """Run ``fn(*args)`` in an executor under fairness, admission and a deadline.

        Args:
            kind: Workload kind (selects the resource profile).
            fn: Synchronous callable.
            *args: Positional arguments for ``fn``.
            tenant: Tenant whose fair-share semaphore applies; ``None`` uses a shared pool.
            timeout: Seconds before ``OperationTimeoutError``; the thread keeps running.
            executor: Dedicated executor (for thread-affine libraries).

        Raises:
            LifecycleError: Before ``start`` or after ``close``.
            ResourceAdmissionError: When resourcera rejects the job.
            OperationTimeoutError: When the deadline passes.
        """
        if not self._started or self._closed:
            raise LifecycleError("offloader is not running")
        manager = self._resources
        assert manager is not None
        semaphore = self._semaphore(tenant or "")
        await semaphore.acquire()
        try:
            lease = await manager.reserve(
                self._requirements(kind),
                wait=True,
                wait_timeout_seconds=self._config.admission_wait_timeout,
            )
        except (AdmissionRejectedError, ResourceError) as exc:
            semaphore.release()
            raise ResourceAdmissionError(
                "resource admission rejected the job", details={"workload": kind.value}
            ) from exc
        except BaseException:
            semaphore.release()
            raise
        loop = asyncio.get_running_loop()
        try:
            future = loop.run_in_executor(executor or self._executor, functools.partial(fn, *args))
        except BaseException:
            semaphore.release()
            self._release_lease(manager, lease)
            raise
        self._outstanding.add(future)
        future.add_done_callback(functools.partial(self._finished, semaphore, manager, lease))
        try:
            async with asyncio.timeout(timeout):
                return await asyncio.shield(future)
        except TimeoutError as exc:
            raise OperationTimeoutError(
                "offloaded job exceeded its deadline", details={"workload": kind.value}
            ) from exc

    def _finished(
        self,
        semaphore: asyncio.Semaphore,
        manager: ResourceManager,
        lease: ResourceLease,
        future: asyncio.Future[Any],
    ) -> None:
        self._outstanding.discard(future)
        semaphore.release()
        if not future.cancelled():
            future.exception()  # mark retrieved; the awaiting caller (if any) re-raises it
        self._release_lease(manager, lease)

    def _release_lease(self, manager: ResourceManager, lease: ResourceLease) -> None:
        task = asyncio.get_running_loop().create_task(self._release(manager, lease))
        self._releases.add(task)
        task.add_done_callback(self._releases.discard)

    @staticmethod
    async def _release(manager: ResourceManager, lease: ResourceLease) -> None:
        try:
            await manager.release(lease)
        except ResourceError:  # pragma: no cover - lease already expired or manager closed
            _log.warning("offloader.lease_release_failed", lease_id=lease.id)

    async def close(self) -> None:
        """Wait for running jobs (bounded) and close engine-owned resources (idempotent)."""
        if self._closed:
            return
        self._closed = True
        if self._outstanding:
            _done, pending = await asyncio.wait(
                set(self._outstanding), timeout=self._config.shutdown_timeout
            )
            if pending:
                _log.warning("offloader.shutdown_timeout", pending=len(pending))
        if self._releases:
            await asyncio.gather(*self._releases, return_exceptions=True)
        if self._owns_executor and isinstance(self._executor, ThreadPoolExecutor):
            self._executor.shutdown(wait=False, cancel_futures=True)
        if self._owns_resources and self._resources is not None and self._started:
            await self._resources.close()
