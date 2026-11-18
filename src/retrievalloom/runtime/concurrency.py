"""Concurrency limits sized from measured host capacity through resourcera."""

from __future__ import annotations

import asyncio
import contextlib
import time
from collections import deque
from typing import TYPE_CHECKING, Annotated, Any, Literal

from pydantic import BeforeValidator, Field, model_validator
from resourcera import ResourceError

from retrievalloom._logging import get_logger
from retrievalloom.domain.base import FrozenModel

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from resourcera import ResourceManager, WorkloadRequirements

_log = get_logger("runtime.concurrency")


class AutoConcurrency(FrozenModel):
    """Concurrency that follows measured host capacity.

    The limit starts at ``minimum``, grows while work is queued and resourcera reports
    spare capacity, shrinks as soon as capacity drops, and halves on overload signals
    such as backend timeouts. It never leaves ``[minimum, maximum]``.

    Attributes:
        minimum: Lowest limit; ``None`` selects the setting's documented floor.
        maximum: Highest limit; ``None`` selects the matching ``EngineLimits`` value.
        interval: Seconds between capacity re-evaluations.
    """

    mode: Literal["auto"] = "auto"
    minimum: int | None = Field(default=None, ge=1)
    maximum: int | None = Field(default=None, ge=1)
    interval: float = Field(default=2.0, gt=0)

    @model_validator(mode="after")
    def _check(self) -> AutoConcurrency:
        if self.minimum is not None and self.maximum is not None and self.minimum > self.maximum:
            raise ValueError("minimum must not exceed maximum")
        return self

    def bounds(self, floor: int, ceiling: int) -> tuple[int, int]:
        """Resolved ``(minimum, maximum)`` given the default floor and ceiling."""
        maximum = self.maximum if self.maximum is not None else ceiling
        minimum = self.minimum if self.minimum is not None else floor
        return min(minimum, maximum), maximum


def _coerce(value: Any) -> Any:
    return AutoConcurrency() if value == "auto" else value


Concurrency = Annotated[Annotated[int, Field(ge=1)] | AutoConcurrency, BeforeValidator(_coerce)]
"""A fixed limit, or ``AutoConcurrency`` (also accepted as the string ``"auto"``)."""


def concurrency_maximum(value: int | AutoConcurrency, ceiling: int) -> int:
    """Highest limit ``value`` may reach."""
    if isinstance(value, int):
        return value
    return value.maximum if value.maximum is not None else ceiling


class AdaptiveLimiter:
    """Resizable permit pool; fixed, or adjusted from resourcera recommendations.

    Permits are plain counters, so one task may acquire and another (or a future
    callback) release. Lowering the limit never revokes granted permits; new work
    waits until the active count falls below the new limit.

    Args:
        name: Name used in logs and health details.
        minimum: Lowest limit (the starting limit in adaptive mode).
        maximum: Highest limit.
        requirements: Per-operation demand; ``None`` keeps the limit fixed at ``maximum``.
        interval: Seconds between adjustments.
    """

    def __init__(
        self,
        name: str,
        *,
        minimum: int,
        maximum: int,
        requirements: WorkloadRequirements | None = None,
        interval: float = 2.0,
    ) -> None:
        if not 1 <= minimum <= maximum:
            raise ValueError("concurrency bounds require 1 <= minimum <= maximum")
        self.name = name
        self._minimum = minimum
        self._maximum = maximum
        self._requirements = requirements
        self._interval = interval
        self._limit = maximum if requirements is None else minimum
        self._active = 0
        self._waiters: deque[asyncio.Future[None]] = deque()
        self._task: asyncio.Task[None] | None = None
        self._manager: ResourceManager | None = None
        self._overloaded_at = float("-inf")
        self._limiting: str | None = None
        self._reason = "fixed" if requirements is None else "waiting for measurements"

    @classmethod
    def from_setting(
        cls,
        name: str,
        setting: int | AutoConcurrency,
        *,
        floor: int,
        ceiling: int,
        requirements: WorkloadRequirements,
    ) -> AdaptiveLimiter:
        """Limiter for a configured ``Concurrency`` value."""
        if isinstance(setting, int):
            return cls(name, minimum=setting, maximum=setting)
        minimum, maximum = setting.bounds(floor, ceiling)
        return cls(
            name,
            minimum=minimum,
            maximum=maximum,
            requirements=requirements,
            interval=setting.interval,
        )

    @property
    def adaptive(self) -> bool:
        """Whether the limit follows measured capacity."""
        return self._requirements is not None

    @property
    def limit(self) -> int:
        """Current number of permits."""
        return self._limit

    @property
    def maximum(self) -> int:
        """Highest limit."""
        return self._maximum

    @property
    def active(self) -> int:
        """Granted permits."""
        return self._active

    @property
    def waiting(self) -> int:
        """Tasks waiting for a permit."""
        return sum(1 for waiter in self._waiters if not waiter.done())

    def state(self) -> dict[str, Any]:
        """Content-free state for health reports."""
        return {
            "mode": "auto" if self.adaptive else "fixed",
            "limit": self._limit,
            "minimum": self._minimum,
            "maximum": self._maximum,
            "active": self._active,
            "waiting": self.waiting,
            "limiting_resource": self._limiting,
            "reason": self._reason,
        }

    # permits ----------------------------------------------------------------------------
    async def acquire(self) -> None:
        """Wait for a permit."""
        if self._active < self._limit and not self.waiting:
            self._active += 1
            return
        waiter = asyncio.get_running_loop().create_future()
        self._waiters.append(waiter)
        try:
            await waiter
        except BaseException:
            if waiter.done() and not waiter.cancelled():
                self.release()  # granted just before the cancellation landed
            else:
                waiter.cancel()
            raise

    def release(self) -> None:
        """Return a permit (callable from any callback on the loop)."""
        self._active -= 1
        self._wake()

    @contextlib.asynccontextmanager
    async def slot(self) -> AsyncIterator[None]:
        """Hold one permit for the block."""
        await self.acquire()
        try:
            yield
        finally:
            self.release()

    def _wake(self) -> None:
        while self._waiters and self._active < self._limit:
            waiter = self._waiters.popleft()
            if waiter.done():
                continue
            self._active += 1
            waiter.set_result(None)

    def _resize(self, limit: int, reason: str) -> None:
        limit = max(self._minimum, min(self._maximum, limit))
        self._reason = reason
        if limit == self._limit:
            return
        previous, self._limit = self._limit, limit
        _log.info(
            "concurrency_adjusted",
            limiter=self.name,
            previous=previous,
            limit=limit,
            limiting_resource=self._limiting,
            reason=reason,
        )
        self._wake()

    def signal_overload(self, reason: str) -> None:
        """Halve an adaptive limit after timeouts or unavailability of a dependency."""
        if not self.adaptive:
            return
        self._overloaded_at = time.monotonic()
        self._resize(self._limit // 2, f"overload: {reason}")

    # adjustment -------------------------------------------------------------------------
    async def start(self, manager: ResourceManager) -> None:
        """Start periodic adjustment (no-op for fixed limits; idempotent)."""
        if not self.adaptive or self._task is not None:
            return
        self._manager = manager
        await self.adjust()
        self._task = asyncio.create_task(self._run(), name=f"retrievalloom-{self.name}")

    async def close(self) -> None:
        """Stop adjusting; granted permits stay valid."""
        task, self._task = self._task, None
        if task is not None:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

    async def _run(self) -> None:
        while True:
            await asyncio.sleep(self._interval)
            try:
                await self.adjust()
            except Exception as exc:  # the limit is kept; the next round retries
                _log.warning(
                    "concurrency_adjustment_failed",
                    limiter=self.name,
                    error_type=type(exc).__name__,
                )

    async def adjust(self) -> None:
        """Re-evaluate capacity once.

        Decreases apply immediately. Increases need queued work, no overload signal
        within the last interval, and grow the limit by at most a quarter per round.
        """
        manager, requirements = self._manager, self._requirements
        if manager is None or requirements is None:
            return
        try:
            recommendation = await manager.recommend_concurrency(
                requirements, min_concurrency=1, max_concurrency=self._maximum
            )
        except ResourceError as exc:
            self._reason = f"holding: resource data unavailable ({type(exc).__name__})"
            return
        self._limiting = recommendation.limiting_resource
        spare = recommendation.estimated_limit
        if spare is None:
            self._reason = "holding: capacity is not measurable yet"
            return
        target = self._active + spare
        if target < self._limit:
            self._resize(target, f"capacity decreased ({self._limiting})")
            return
        recent = time.monotonic() - self._overloaded_at < self._interval
        if target == self._limit or recent or not self.waiting:
            self._reason = "holding"
            return
        step = max(1, self._limit // 4)
        self._resize(min(target, self._limit + step), f"capacity allows {target}")


__all__ = ["AdaptiveLimiter", "AutoConcurrency", "Concurrency", "concurrency_maximum"]
