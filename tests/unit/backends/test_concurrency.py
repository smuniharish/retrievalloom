"""AdaptiveLimiter: permits, resizing from resourcera recommendations and overload."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import Any

import pytest
from resourcera import ResourceError, WorkloadProfile, WorkloadRequirements

from retrievalloom.runtime.concurrency import AdaptiveLimiter, AutoConcurrency

_NEEDS = WorkloadRequirements(memory="16MB", profile=WorkloadProfile.IO_BOUND)  # type: ignore[arg-type]


class _Manager:
    """Recommends a scripted spare capacity."""

    def __init__(self, spare: int | None) -> None:
        self.spare: int | Exception | None = spare

    async def recommend_concurrency(self, *_: Any, **__: Any) -> Any:
        if isinstance(self.spare, Exception):
            raise self.spare
        return SimpleNamespace(estimated_limit=self.spare, limiting_resource="memory")


async def _waiters(limiter: AdaptiveLimiter, count: int) -> list[asyncio.Task[None]]:
    tasks = [asyncio.create_task(limiter.acquire()) for _ in range(count)]
    await asyncio.sleep(0)
    return tasks


async def test_fixed_limit_queues_and_a_loop_callback_can_release() -> None:
    limiter = AdaptiveLimiter.from_setting("x", 2, floor=1, ceiling=8, requirements=_NEEDS)
    assert not limiter.adaptive
    assert limiter.limit == 2
    await limiter.acquire()
    await limiter.acquire()
    [third] = await _waiters(limiter, 1)
    assert limiter.waiting == 1
    asyncio.get_running_loop().call_soon(limiter.release)
    await asyncio.wait_for(third, 1)
    assert limiter.active == 2
    assert limiter.state()["mode"] == "fixed"


async def test_cancelled_waiter_does_not_leak_a_permit() -> None:
    limiter = AdaptiveLimiter("x", minimum=1, maximum=1)
    await limiter.acquire()
    [waiter] = await _waiters(limiter, 1)
    waiter.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiter
    limiter.release()
    assert limiter.active == 0
    async with limiter.slot():
        assert limiter.active == 1
    assert limiter.active == 0


async def test_cancellation_after_grant_returns_the_permit() -> None:
    limiter = AdaptiveLimiter("x", minimum=1, maximum=1)
    await limiter.acquire()
    [waiter] = await _waiters(limiter, 1)
    limiter.release()  # grants the waiter
    waiter.cancel()  # lands before the waiter resumes
    with pytest.raises(asyncio.CancelledError):
        await waiter
    assert limiter.active == 0


async def test_auto_limit_grows_with_queued_work_by_at_most_a_quarter() -> None:
    setting = AutoConcurrency(minimum=4, maximum=100, interval=60)
    limiter = AdaptiveLimiter.from_setting("x", setting, floor=1, ceiling=8, requirements=_NEEDS)
    manager = _Manager(spare=50)
    await limiter.start(manager)  # type: ignore[arg-type]
    try:
        assert limiter.limit == 4  # no queued work, so no growth
        for _ in range(4):
            await limiter.acquire()
        tasks = await _waiters(limiter, 3)
        await limiter.adjust()
        assert limiter.limit == 5  # 4 + max(1, 4 // 4)
        await asyncio.sleep(0)
        assert sum(task.done() for task in tasks) == 1
        manager.spare = 0  # capacity gone: shrink at once to what is active
        await limiter.adjust()
        assert limiter.limit == 5
        manager.spare = None  # unknown capacity holds the limit
        await limiter.adjust()
        assert limiter.limit == 5
        assert "holding" in limiter.state()["reason"]
        manager.spare = ResourceError("no samples")
        await limiter.adjust()
        assert limiter.limit == 5
        for task in tasks:
            task.cancel()
    finally:
        await limiter.close()


async def test_capacity_drop_shrinks_but_never_below_minimum() -> None:
    setting = AutoConcurrency(minimum=2, maximum=16, interval=60)
    limiter = AdaptiveLimiter.from_setting("x", setting, floor=1, ceiling=8, requirements=_NEEDS)
    manager = _Manager(spare=16)
    await limiter.start(manager)  # type: ignore[arg-type]
    try:
        for _ in range(2):
            await limiter.acquire()
        tasks = await _waiters(limiter, 20)
        for _ in range(10):
            await limiter.adjust()
            await asyncio.sleep(0)
        assert limiter.limit == 16
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        for _ in range(limiter.active):
            limiter.release()
        manager.spare = 0
        await limiter.adjust()
        assert limiter.limit == 2
        assert limiter.state()["limiting_resource"] == "memory"
    finally:
        await limiter.close()


async def test_overload_halves_and_blocks_growth_for_an_interval() -> None:
    setting = AutoConcurrency(minimum=1, maximum=64, interval=60)
    limiter = AdaptiveLimiter.from_setting("x", setting, floor=8, ceiling=64, requirements=_NEEDS)
    await limiter.start(_Manager(spare=64))  # type: ignore[arg-type]
    try:
        assert limiter.limit == 1  # explicit minimum wins over the floor
        limiter._resize(40, "test")
        limiter.signal_overload("backend timed out")
        assert limiter.limit == 20
        assert limiter.state()["reason"].startswith("overload")
        tasks = await _waiters(limiter, 30)
        await limiter.adjust()
        assert limiter.limit == 20  # recent overload: no growth
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
    finally:
        await limiter.close()
    fixed = AdaptiveLimiter("y", minimum=3, maximum=3)
    fixed.signal_overload("ignored")
    assert fixed.limit == 3


def test_auto_bounds_and_validation() -> None:
    assert AutoConcurrency().bounds(8, 256) == (8, 256)
    assert AutoConcurrency(maximum=4).bounds(8, 256) == (4, 4)
    assert AutoConcurrency(minimum=2, maximum=9).bounds(8, 256) == (2, 9)
    with pytest.raises(ValueError, match="minimum must not exceed maximum"):
        AutoConcurrency(minimum=5, maximum=4)
    with pytest.raises(ValueError, match="1 <= minimum <= maximum"):
        AdaptiveLimiter("x", minimum=0, maximum=4)
