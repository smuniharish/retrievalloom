"""LadybugDB single-owner semantics across processes (spawned owner engine)."""

from __future__ import annotations

import asyncio
import multiprocessing
import time
from typing import TYPE_CHECKING, Any

import pytest

from retrievalloom import (
    ChannelsConfig,
    DocumentInput,
    EngineResources,
    GraphChannelConfig,
    GraphEntityInput,
    GraphRelationshipInput,
    GraphRepresentation,
    LifecycleConfig,
    RetrievalConfig,
    RetrievalEngine,
    RetrievalExecutionConfig,
    Scope,
    TextRepresentation,
    create_retrieval_engine,
)
from retrievalloom.errors import BackendUnavailableError

if TYPE_CHECKING:
    from pathlib import Path

pytestmark = pytest.mark.graph

SCOPE = Scope(tenant_id="acme")
DOCUMENT = DocumentInput(
    _id="notes",
    representations=(
        TextRepresentation(text="Ada Lovelace wrote notes on the Analytical Engine."),
        GraphRepresentation(
            representation_id="graph",
            entities=(
                GraphEntityInput(key="ada", name="Ada Lovelace"),
                GraphEntityInput(key="engine", name="Analytical Engine"),
            ),
            relationships=(
                GraphRelationshipInput(
                    source_key="ada", target_key="engine", relation_type="wrote_about"
                ),
            ),
        ),
    ),
)


def engine(data_dir: Path) -> RetrievalEngine:
    return create_retrieval_engine(
        retrieval=RetrievalConfig(channels=ChannelsConfig(graph=GraphChannelConfig(enabled=True))),
        lifecycle=LifecycleConfig(background=False),
        resources=EngineResources(data_dir=data_dir),
    )


def own_the_graph(data_dir: Path, ready: Any, release: Any) -> None:
    async def main() -> None:
        async with engine(data_dir) as owner:
            await owner.ingest([DOCUMENT], scope=SCOPE)
            ready.set()
            await asyncio.to_thread(release.wait, 60)

    asyncio.run(main())


async def test_second_process_fails_fast_until_the_owner_closes(tmp_path: Path) -> None:
    context = multiprocessing.get_context("spawn")
    ready, release = context.Event(), context.Event()
    owner = context.Process(target=own_the_graph, args=(tmp_path, ready, release))
    owner.start()
    try:
        assert await asyncio.to_thread(ready.wait, 120), "owner process did not start"
        began = time.monotonic()
        with pytest.raises(BackendUnavailableError, match="locked by another process") as caught:
            async with engine(tmp_path):
                pass
        assert time.monotonic() - began < 10
        assert caught.value.details["path"] == str(tmp_path / "graph" / "graph.lbug")
        assert caught.value.retryable
    finally:
        release.set()
        await asyncio.to_thread(owner.join, 60)
    assert owner.exitcode == 0
    async with engine(tmp_path) as successor:
        result = await successor.retrieve(
            "Ada Lovelace", scope=SCOPE, config=RetrievalExecutionConfig(index=False)
        )
    assert result.evidence
