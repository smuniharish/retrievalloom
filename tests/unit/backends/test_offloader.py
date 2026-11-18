"""Offloader: admission, fairness, deadlines, cancellation, draining and ownership."""

from __future__ import annotations

import asyncio
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, cast

import pytest
from resourcera import ResourceError, ResourceManager, WorkloadProfile

from retrievalloom.errors import LifecycleError, OperationTimeoutError, ResourceAdmissionError
from retrievalloom.runtime.offloader import Offloader, OffloaderConfig, WorkloadKind
from tests.support import shared_manager

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Callable


class FakeManager:
    """Scriptable stand-in for resourcera: grants, rejects or blocks reservations."""

    def __init__(self, *script: str) -> None:
        self.script = list(script)
        self.requirements: list[Any] = []
        self.released: list[Any] = []

    async def reserve(self, requirements: Any, **_: Any) -> Any:
        self.requirements.append(requirements)
        action = self.script.pop(0) if self.script else "grant"
        if action == "reject":
            raise ResourceError("no capacity")
        if action == "block":
            await asyncio.Event().wait()
        return SimpleNamespace(id=f"lease-{len(self.requirements)}")

    config = SimpleNamespace(max_concurrency=64)

    async def release(self, lease: Any) -> None:
        self.released.append(lease.id)

    async def recommend_concurrency(self, *_: Any, **__: Any) -> Any:
        return SimpleNamespace(estimated_limit=None, limiting_resource=None)


def fake(manager: FakeManager) -> ResourceManager:
    return cast("ResourceManager", manager)


async def eventually(predicate: Callable[[], bool], deadline_s: float = 5.0) -> None:
    deadline = time.monotonic() + deadline_s
    while not predicate():
        assert time.monotonic() < deadline, "condition not reached"
        await asyncio.sleep(0.005)


@pytest.fixture
async def executor() -> AsyncIterator[ThreadPoolExecutor]:
    pool = ThreadPoolExecutor(max_workers=4)
    try:
        yield pool
    finally:
        pool.shutdown(wait=True)


async def started(
    manager: ResourceManager | None = None,
    *,
    executor: ThreadPoolExecutor | None = None,
    **config: Any,
) -> Offloader:
    instance = Offloader(
        OffloaderConfig(**config),
        executor=executor,
        resource_manager=manager or shared_manager(),
    )
    await instance.start()
    return instance


async def test_run_returns_result_and_propagates_errors() -> None:
    offloader = await started()
    try:
        assert await offloader.run(WorkloadKind.HASHING, pow, 2, 10) == 1024

        def boom() -> None:
            raise ValueError("bad input")

        with pytest.raises(ValueError, match="bad input"):
            await offloader.run(WorkloadKind.HASHING, boom, tenant="t1")
        await eventually(lambda: offloader.outstanding == 0)
    finally:
        await offloader.close()


async def test_run_requires_a_running_offloader() -> None:
    offloader = Offloader(resource_manager=shared_manager())
    with pytest.raises(LifecycleError):
        await offloader.run(WorkloadKind.HASHING, int)
    await offloader.start()
    await offloader.start()  # idempotent
    await offloader.close()
    await offloader.close()  # idempotent
    with pytest.raises(LifecycleError):
        await offloader.run(WorkloadKind.HASHING, int)


async def test_close_without_start_creates_nothing() -> None:
    offloader = Offloader()
    await offloader.close()
    assert offloader.resource_manager is None


async def test_owned_executor_and_resource_manager_are_created_and_closed() -> None:
    offloader = Offloader(OffloaderConfig(max_workers=2))
    await offloader.start()
    manager = offloader.resource_manager
    assert manager is not None
    assert manager is not shared_manager()
    names = await offloader.run(WorkloadKind.HASHING, lambda: threading.current_thread().name)
    assert names.startswith("retrievalloom")
    await offloader.close()
    with pytest.raises(LifecycleError):
        await offloader.run(WorkloadKind.HASHING, int)


async def test_application_executor_and_manager_stay_open(executor: ThreadPoolExecutor) -> None:
    offloader = await started(executor=executor)
    assert offloader.resource_manager is shared_manager()
    await offloader.run(WorkloadKind.HASHING, int)
    await offloader.close()
    assert executor.submit(int, "7").result() == 7
    # The shared manager still admits work for a fresh offloader.
    again = await started()
    assert await again.run(WorkloadKind.IO_RETRIEVAL, int, "3") == 3
    await again.close()


async def test_dedicated_executor_runs_the_job(executor: ThreadPoolExecutor) -> None:
    dedicated = ThreadPoolExecutor(max_workers=1, thread_name_prefix="dedicated")
    offloader = await started(executor=executor)
    try:
        name = await offloader.run(
            WorkloadKind.VECTOR, lambda: threading.current_thread().name, executor=dedicated
        )
        assert name.startswith("dedicated")
    finally:
        await offloader.close()
        dedicated.shutdown(wait=True)


async def test_workload_kinds_map_to_profiles() -> None:
    manager = FakeManager()
    offloader = await started(fake(manager), memory_per_job="8MB", cpu_per_job=0.5)
    try:
        await offloader.run(WorkloadKind.GRAPH_QUERY, int)
        await offloader.run(WorkloadKind.RERANK, int)
    finally:
        await offloader.close()
    io, cpu = manager.requirements
    assert io.profile == WorkloadProfile.IO_BOUND
    assert cpu.profile == WorkloadProfile.CPU_BOUND
    assert cpu.cpu == 0.5
    assert manager.released == ["lease-1", "lease-2"]


async def test_admission_rejection_is_structured_and_frees_the_tenant_slot() -> None:
    manager = FakeManager("reject")
    offloader = await started(fake(manager), max_concurrent_jobs_per_tenant=1)
    try:
        with pytest.raises(ResourceAdmissionError) as caught:
            await offloader.run(WorkloadKind.NLI, int, tenant="t1")
        assert caught.value.details == {"workload": "nli"}
        # The single tenant slot was returned: the next job is admitted immediately.
        assert await asyncio.wait_for(offloader.run(WorkloadKind.NLI, int, tenant="t1"), 2) == 0
    finally:
        await offloader.close()


async def test_cancel_during_admission_frees_the_tenant_slot() -> None:
    manager = FakeManager("block")
    offloader = await started(fake(manager), max_concurrent_jobs_per_tenant=1)
    try:
        waiting = asyncio.create_task(offloader.run(WorkloadKind.NLI, int, tenant="t1"))
        await eventually(lambda: len(manager.requirements) == 1)
        waiting.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiting
        assert await asyncio.wait_for(offloader.run(WorkloadKind.NLI, int, tenant="t1"), 2) == 0
        await eventually(lambda: manager.released == ["lease-2"])
    finally:
        await offloader.close()


async def test_submit_failure_releases_slot_and_lease() -> None:
    manager = FakeManager()
    offloader = await started(fake(manager), max_concurrent_jobs_per_tenant=1)
    dead = ThreadPoolExecutor(max_workers=1)
    dead.shutdown()
    try:
        with pytest.raises(RuntimeError):
            await offloader.run(WorkloadKind.HASHING, int, tenant="t1", executor=dead)
        await eventually(lambda: manager.released == ["lease-1"])
        assert await asyncio.wait_for(offloader.run(WorkloadKind.HASHING, int, tenant="t1"), 2) == 0
    finally:
        await offloader.close()


async def test_tenant_semaphores_are_independent_and_use_tenant_limit() -> None:
    limits = {"slow": 1, "fast": 2}
    gate = threading.Event()
    offloader = Offloader(resource_manager=shared_manager(), tenant_limit=limits.__getitem__)
    await offloader.start()
    started_jobs: list[str] = []

    def job(name: str) -> str:
        started_jobs.append(name)
        gate.wait(5)
        return name

    try:
        first = asyncio.create_task(
            offloader.run(WorkloadKind.HASHING, job, "slow-1", tenant="slow")
        )
        second = asyncio.create_task(
            offloader.run(WorkloadKind.HASHING, job, "slow-2", tenant="slow")
        )
        fast = [
            asyncio.create_task(
                offloader.run(WorkloadKind.HASHING, job, f"fast-{i}", tenant="fast")
            )
            for i in range(2)
        ]
        await eventually(lambda: len(started_jobs) == 3)
        # "slow" is limited to one concurrent job; "fast" ran both of its jobs meanwhile.
        assert "slow-2" not in started_jobs
        assert {"fast-0", "fast-1"} <= set(started_jobs)
        gate.set()
        assert await asyncio.gather(first, second, *fast) == [
            "slow-1",
            "slow-2",
            "fast-0",
            "fast-1",
        ]
    finally:
        gate.set()
        await offloader.close()


async def test_timeout_raises_but_keeps_thread_and_permit_until_done() -> None:
    gate = threading.Event()
    finished = threading.Event()

    def job() -> str:
        gate.wait(5)
        finished.set()
        return "late"

    offloader = await started(max_concurrent_jobs_per_tenant=1)
    try:
        with pytest.raises(OperationTimeoutError) as caught:
            await offloader.run(WorkloadKind.RERANK, job, tenant="t1", timeout=0.05)
        assert caught.value.details == {"workload": "rerank"}
        assert offloader.outstanding == 1
        assert not finished.is_set()
        # The tenant permit is still held by the running thread.
        follower = asyncio.create_task(offloader.run(WorkloadKind.RERANK, int, tenant="t1"))
        await asyncio.sleep(0.05)
        assert not follower.done()
        gate.set()
        assert await asyncio.wait_for(follower, 2) == 0
        assert finished.is_set()
        await eventually(lambda: offloader.outstanding == 0)
    finally:
        gate.set()
        await offloader.close()


async def test_cancelling_the_caller_does_not_cancel_the_thread() -> None:
    gate = threading.Event()
    finished = threading.Event()

    def job() -> None:
        gate.wait(5)
        finished.set()

    offloader = await started(max_concurrent_jobs_per_tenant=1)
    try:
        caller = asyncio.create_task(offloader.run(WorkloadKind.NLI, job, tenant="t1"))
        await eventually(lambda: offloader.outstanding == 1)
        caller.cancel()
        with pytest.raises(asyncio.CancelledError):
            await caller
        assert offloader.outstanding == 1
        gate.set()
        await eventually(lambda: offloader.outstanding == 0)
        assert finished.is_set()
    finally:
        gate.set()
        await offloader.close()


async def test_executor_cancelled_job_releases_permits() -> None:
    gate = threading.Event()
    pool = ThreadPoolExecutor(max_workers=1)
    manager = FakeManager()
    offloader = await started(fake(manager), executor=pool)
    try:
        blocker = asyncio.create_task(offloader.run(WorkloadKind.HASHING, gate.wait, 5))
        queued = asyncio.create_task(offloader.run(WorkloadKind.HASHING, int))
        await eventually(lambda: offloader.outstanding == 2)
        pool.shutdown(wait=False, cancel_futures=True)
        with pytest.raises(asyncio.CancelledError):
            await queued
        gate.set()
        assert await blocker is True
        await eventually(lambda: offloader.outstanding == 0 and len(manager.released) == 2)
    finally:
        gate.set()
        await offloader.close()
        pool.shutdown(wait=True)


async def test_close_drains_running_jobs_and_pending_releases(
    executor: ThreadPoolExecutor,
) -> None:
    manager = FakeManager()
    offloader = await started(fake(manager), executor=executor)

    def job() -> str:
        time.sleep(0.1)
        return "done"

    running = asyncio.create_task(offloader.run(WorkloadKind.HASHING, job))
    await eventually(lambda: offloader.outstanding == 1)
    await offloader.close()
    assert offloader.outstanding == 0
    assert manager.released == ["lease-1"]
    assert await running == "done"


async def test_close_gives_up_after_shutdown_timeout(executor: ThreadPoolExecutor) -> None:
    gate = threading.Event()
    offloader = await started(executor=executor, shutdown_timeout=0.05)
    caller = asyncio.create_task(offloader.run(WorkloadKind.HASHING, gate.wait, 5))
    await eventually(lambda: offloader.outstanding == 1)
    began = time.monotonic()
    await offloader.close()
    assert time.monotonic() - began < 1.0
    assert offloader.outstanding == 1
    gate.set()
    assert await caller is True
    await eventually(lambda: offloader.outstanding == 0)
    await asyncio.sleep(0.01)  # let the lease release task finish


async def test_event_loop_stays_responsive_during_blocking_job() -> None:
    offloader = await started()
    lags: list[float] = []
    stop = asyncio.Event()

    async def ticker() -> None:
        interval = 0.01
        while not stop.is_set():
            before = time.perf_counter()
            await asyncio.sleep(interval)
            lags.append(time.perf_counter() - before - interval)

    try:
        ticking = asyncio.create_task(ticker())
        await asyncio.sleep(0.02)
        result = await offloader.run(WorkloadKind.RERANK, time.sleep, 0.5)
        stop.set()
        await ticking
    finally:
        await offloader.close()
    assert result is None
    assert len(lags) >= 10
    assert max(lags) < 0.15


async def test_cpu_and_workload_caps_apply_across_tenants() -> None:
    gate = threading.Event()
    running: list[str] = []

    def job(name: str) -> str:
        running.append(name)
        gate.wait(5)
        return name

    offloader = await started(max_cpu_jobs=1, workload_limits={WorkloadKind.EMBEDDING: 1})
    try:
        cpu = asyncio.create_task(offloader.run(WorkloadKind.RERANK, job, "rerank", tenant="a"))
        await eventually(lambda: running == ["rerank"])
        blocked = [
            asyncio.create_task(offloader.run(WorkloadKind.NLI, job, "nli", tenant="b")),
            asyncio.create_task(offloader.run(WorkloadKind.EMBEDDING, job, "embed-1", tenant="c")),
        ]
        # I/O-bound kinds are not limited by the CPU cap.
        io = asyncio.create_task(offloader.run(WorkloadKind.IO_RETRIEVAL, job, "io", tenant="d"))
        await eventually(lambda: "io" in running)
        await asyncio.sleep(0.05)
        assert sorted(running) == ["io", "rerank"]
        gate.set()
        assert await asyncio.gather(cpu, io, *blocked) == ["rerank", "io", "nli", "embed-1"]
    finally:
        gate.set()
        await offloader.close()


async def test_workload_limit_holds_its_slot_until_the_thread_finishes() -> None:
    gate = threading.Event()
    offloader = await started(workload_limits={WorkloadKind.GRAPH_QUERY: 1})
    try:
        with pytest.raises(OperationTimeoutError):
            await offloader.run(WorkloadKind.GRAPH_QUERY, gate.wait, 5, tenant="a", timeout=0.05)
        follower = asyncio.create_task(offloader.run(WorkloadKind.GRAPH_QUERY, int, tenant="b"))
        await asyncio.sleep(0.05)
        assert not follower.done()
        # Other kinds are not limited.
        assert await asyncio.wait_for(offloader.run(WorkloadKind.HASHING, int, tenant="b"), 2) == 0
        gate.set()
        assert await asyncio.wait_for(follower, 2) == 0
    finally:
        gate.set()
        await offloader.close()


async def test_cancel_while_waiting_for_a_cpu_slot_frees_the_tenant_slot() -> None:
    gate = threading.Event()
    offloader = await started(max_cpu_jobs=1, max_concurrent_jobs_per_tenant=1)
    try:
        holder = asyncio.create_task(offloader.run(WorkloadKind.RERANK, gate.wait, 5, tenant="a"))
        await eventually(lambda: offloader.outstanding == 1)
        waiting = asyncio.create_task(offloader.run(WorkloadKind.RERANK, int, tenant="b"))
        await asyncio.sleep(0.02)
        waiting.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiting
        # Tenant "b" got its only slot back: its I/O job (no CPU slot needed) runs now.
        assert (
            await asyncio.wait_for(offloader.run(WorkloadKind.IO_RETRIEVAL, int, tenant="b"), 2)
            == 0
        )
        gate.set()
        assert await holder is True
        assert await asyncio.wait_for(offloader.run(WorkloadKind.RERANK, int, tenant="b"), 2) == 0
    finally:
        gate.set()
        await offloader.close()


def test_workload_limits_must_be_positive() -> None:
    with pytest.raises(ValueError, match="greater than or equal to 1"):
        OffloaderConfig(workload_limits={WorkloadKind.RERANK: 0})
    with pytest.raises(ValueError, match="greater than or equal to 1"):
        OffloaderConfig.model_validate({"max_cpu_jobs": 0})
