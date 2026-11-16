"""Fixtures of the lifecycle tests."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from tests.unit.lifecycle.helpers import open_store

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from retrievalloom.storage.sqlite import SQLiteMetadataStore


@pytest.fixture
async def store() -> AsyncIterator[SQLiteMetadataStore]:
    """Migrated in-memory SQLite metadata store."""
    opened = await open_store()
    try:
        yield opened
    finally:
        await opened.close()
