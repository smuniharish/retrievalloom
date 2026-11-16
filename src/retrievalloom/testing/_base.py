"""Shared base of every contract kit: component lifecycle fixtures and generic checks."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

import pytest

from retrievalloom._logging import get_logger
from retrievalloom.contracts.component import ComponentABC, ComponentContext, ComponentInfo
from retrievalloom.domain.base import utc_now
from retrievalloom.domain.hashing import random_id
from retrievalloom.domain.reports import HealthReport
from retrievalloom.infra.metrics import NullMetrics
from retrievalloom.runtime.offloader import Offloader, OffloaderConfig

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from pathlib import Path

    from resourcera import ResourceManager


class ComponentContract(ABC):
    """Base class of the contract kits.

    Subclass a kit in a ``Test*`` class and implement ``make_component``; pytest then runs
    every inherited check against the component. The kit initialises the component with a
    real ``ComponentContext`` (an ``Offloader`` with its own resourcera manager unless
    ``resource_manager`` returns a shared, already started one) and closes it afterwards.
    """

    @abstractmethod
    async def make_component(self, tmp_path: Path) -> Any:
        """Create a fresh, uninitialised component under test."""

    def resource_manager(self) -> ResourceManager | None:
        """Started resourcera manager to share between tests (``None`` creates one per test)."""
        return None

    async def prepare(self, component: Any) -> None:
        """Bring an initialised component into a usable state (no-op by default)."""
        del component

    @pytest.fixture
    async def component_context(self, tmp_path: Path) -> AsyncIterator[ComponentContext]:
        """Context handed to ``initialize``."""
        offloader = Offloader(
            OffloaderConfig(max_workers=4), resource_manager=self.resource_manager()
        )
        await offloader.start()
        try:
            yield ComponentContext(
                engine_id=random_id("engine"),
                offloader=offloader,
                clock=utc_now,
                logger=get_logger("testing"),
                metrics=NullMetrics(),
                data_dir=tmp_path,
            )
        finally:
            await offloader.close()

    @pytest.fixture
    async def component(
        self, component_context: ComponentContext, tmp_path: Path
    ) -> AsyncIterator[Any]:
        """Initialised component; closed after the test."""
        component = await self.make_component(tmp_path)
        assert isinstance(component, ComponentABC)
        await component.initialize(component_context)
        try:
            await self.prepare(component)
            yield component
        finally:
            await component.close()

    async def test_info_is_stable(self, component: Any) -> None:
        """``info`` is a stable ``ComponentInfo`` with a digestable fingerprint."""
        info = component.info
        assert isinstance(info, ComponentInfo)
        assert info == component.info
        assert info.fingerprint.digest == component.info.fingerprint.digest

    async def test_health_reports_component(self, component: Any) -> None:
        """``health`` returns a report naming the component."""
        report = await component.health()
        assert isinstance(report, HealthReport)
        assert report.component == component.info.name

    async def test_initialize_and_close_are_idempotent(
        self, component: Any, component_context: ComponentContext
    ) -> None:
        """A second ``initialize`` and repeated ``close`` calls are harmless."""
        await component.initialize(component_context)
        await component.close()
        await component.close()
