"""Live-suite gating and shared builders.

Every test here is marked ``live``; ``qdrant``/``models`` tests are skipped unless
``RL_TEST_QDRANT_URL``/``RL_TEST_MODELS_DIR`` are set, so the default offline run
never touches external services.
"""

from __future__ import annotations

import dataclasses
import os
from contextlib import asynccontextmanager
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest

from retrievalloom import EngineResources, LifecycleConfig, create_retrieval_engine
from retrievalloom.domain.hashing import random_id
from tests.support import shared_manager

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from retrievalloom import Components, RetrievalEngine

LIVE_DIR = Path(__file__).parent
QDRANT_URL = os.environ.get("RL_TEST_QDRANT_URL")
QDRANT_GRPC_PORT = int(os.environ.get("RL_TEST_QDRANT_GRPC_PORT", "6334"))
MODELS_DIR = os.environ.get("RL_TEST_MODELS_DIR")


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    """Mark every live test and skip the ones whose service is not configured."""
    del config
    skip_qdrant = pytest.mark.skip(reason="set RL_TEST_QDRANT_URL to run Qdrant server tests")
    skip_models = pytest.mark.skip(reason="set RL_TEST_MODELS_DIR to run real-model tests")
    for item in items:
        if LIVE_DIR not in Path(str(item.path)).parents:
            continue
        item.add_marker(pytest.mark.live)
        if item.get_closest_marker("qdrant") and not QDRANT_URL:
            item.add_marker(skip_qdrant)
        if item.get_closest_marker("models") and not MODELS_DIR:
            item.add_marker(skip_models)


@pytest.fixture
def qdrant_url() -> str:
    """Configured Qdrant server URL."""
    assert QDRANT_URL is not None
    return QDRANT_URL


@pytest.fixture
def models_dir() -> Path:
    """Directory holding the reference models."""
    assert MODELS_DIR is not None
    path = Path(MODELS_DIR).resolve()
    if not path.is_dir():
        pytest.fail(f"RL_TEST_MODELS_DIR does not exist: {path}")
    return path


@pytest.fixture
def prefix() -> str:
    """Unique Qdrant collection prefix, so tests never share server collections."""
    return random_id("rl").replace("-", "_")[:40]


async def drop_collections(url: str, prefix: str) -> None:
    """Delete every server collection created under ``prefix``."""
    from qdrant_client import AsyncQdrantClient

    client = AsyncQdrantClient(url=url)
    try:
        response = await client.get_collections()
        for collection in response.collections:
            if collection.name.startswith(prefix):
                await client.delete_collection(collection.name)
    finally:
        await client.close()


def make_engine(data_dir: Path | None = None, **layers: Any) -> RetrievalEngine:
    """Engine with a foreground lifecycle and the shared session resource manager."""
    layers.setdefault("lifecycle", LifecycleConfig(background=False))
    return create_retrieval_engine(
        resources=EngineResources(resource_manager=shared_manager(), data_dir=data_dir),
        **layers,
    )


@asynccontextmanager
async def running_engine(
    components: Components, data_dir: Path | None = None, **layers: Any
) -> AsyncIterator[RetrievalEngine]:
    """Initialised engine; the supplied (application-owned) components are closed after it."""
    try:
        async with make_engine(data_dir, components=components, **layers) as engine:
            yield engine
    finally:
        for field in dataclasses.fields(components):
            component = getattr(components, field.name)
            if component is not None:
                await component.close()
