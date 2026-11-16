"""Shared fixtures: one started resourcera manager for the whole (session-loop) test run."""

from __future__ import annotations

import contextlib
import sqlite3
from typing import TYPE_CHECKING

import pytest
from qdrant_client.local.persistence import CollectionPersistence
from resourcera import ResourceConfig, ResourceManager

from tests.support import SHARED

if TYPE_CHECKING:
    from collections.abc import AsyncIterator


def _seed_qdrant_thread_probe() -> None:
    """Pre-answer qdrant-client's one-time SQLite threadsafety probe.

    Upstream probes with ``with sqlite3.connect(":memory:")``, which never closes the
    connection; under ``filterwarnings=error`` the leaked handle fails a random later test.
    """
    with contextlib.closing(sqlite3.connect(":memory:")) as connection:
        row = connection.execute(
            "select * from pragma_compile_options where compile_options like 'THREADSAFE=%'"
        ).fetchone()
    CollectionPersistence.CHECK_SAME_THREAD = row[0] != "THREADSAFE=1"


_seed_qdrant_thread_probe()


@pytest.fixture(scope="session", autouse=True)
async def shared_resource_manager() -> AsyncIterator[ResourceManager]:
    """Start one manager per session; starting one costs about a second of sampling."""
    manager = ResourceManager(ResourceConfig(cpu_budget_fraction=0.05))
    await manager.start()
    SHARED.append(manager)
    try:
        yield manager
    finally:
        SHARED.clear()
        await manager.close()
