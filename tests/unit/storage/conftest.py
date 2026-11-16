"""Fixtures of the storage unit tests."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from retrievalloom.storage.sqlite import SQLiteMetadataStore
from tests.unit.storage.factories import OTHER, SCOPE

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from pathlib import Path


@pytest.fixture
async def store() -> AsyncIterator[SQLiteMetadataStore]:
    """Migrated in-memory store with both test scopes registered."""
    component = SQLiteMetadataStore()
    await component.initialize()
    await component.migrate()
    async with component.transaction(write=True) as tx:
        await tx.ensure_scope(SCOPE)
        await tx.ensure_scope(OTHER)
    try:
        yield component
    finally:
        await component.close()


@pytest.fixture
async def file_store(tmp_path: Path) -> AsyncIterator[SQLiteMetadataStore]:
    """Migrated file-backed store."""
    component = SQLiteMetadataStore(tmp_path / "nested" / "meta.db")
    await component.initialize()
    await component.migrate()
    try:
        yield component
    finally:
        await component.close()
