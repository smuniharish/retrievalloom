"""Fixtures of the backend unit tests: a started offloader and a component context."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from retrievalloom._logging import get_logger
from retrievalloom.contracts.component import ComponentContext
from retrievalloom.domain.base import utc_now
from retrievalloom.domain.hashing import random_id
from retrievalloom.infra.metrics import NullMetrics
from retrievalloom.runtime.offloader import Offloader, OffloaderConfig
from tests.support import shared_manager

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from pathlib import Path


@pytest.fixture
async def offloader() -> AsyncIterator[Offloader]:
    """Started offloader on the session resource manager."""
    instance = Offloader(OffloaderConfig(max_workers=4), resource_manager=shared_manager())
    await instance.start()
    try:
        yield instance
    finally:
        await instance.close()


@pytest.fixture
def context(offloader: Offloader, tmp_path: Path) -> ComponentContext:
    """Component context bound to ``offloader``."""
    return ComponentContext(
        engine_id=random_id("engine"),
        offloader=offloader,
        clock=utc_now,
        logger=get_logger("tests.backends"),
        metrics=NullMetrics(),
        data_dir=tmp_path,
    )
