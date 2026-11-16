"""Durable embedded backends (Qdrant path, Ladybug file, TreeSearch directory) survive restarts."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from retrievalloom import (
    Components,
    DocumentInput,
    GraphEntityInput,
    GraphRelationshipInput,
    GraphRepresentation,
    RetrievalConfig,
    RetrievalExecutionConfig,
    Scope,
    StructureNodeInput,
    StructureRepresentation,
    TextRepresentation,
)
from retrievalloom.config import ChannelsConfig, GraphChannelConfig, StructureChannelConfig
from retrievalloom.domain.records import GenerationState
from retrievalloom.domain.retrieval import Channel, ResultStatus
from tests.fakes import HashEmbedding
from tests.live.conftest import running_engine

if TYPE_CHECKING:
    from pathlib import Path

    from retrievalloom import RetrievalEngine, RetrievalResult


def _entry_names(directory: Path) -> set[str]:
    return {path.name for path in directory.iterdir()}


pytestmark = [pytest.mark.graph, pytest.mark.structure]

ALL_CHANNELS = RetrievalConfig(
    channels=ChannelsConfig(
        graph=GraphChannelConfig(enabled=True), structure=StructureChannelConfig(enabled=True)
    )
)
SCOPE = Scope(tenant_id="acme")
DURABLE = {"index.dense", "graph.graph", "structure.structure"}
DOCUMENT = DocumentInput(
    _id="guide",
    representations=(
        TextRepresentation(text="Python asyncio event loops schedule coroutines."),
        GraphRepresentation(
            representation_id="g",
            entities=(
                GraphEntityInput(key="py", name="Python", entity_type="language"),
                GraphEntityInput(key="aio", name="asyncio", entity_type="library"),
            ),
            relationships=(
                GraphRelationshipInput(source_key="aio", target_key="py", relation_type="part_of"),
            ),
        ),
        StructureRepresentation(
            representation_id="s",
            root=StructureNodeInput(
                title="Guide",
                children=(
                    StructureNodeInput(title="Loops", text="Event loops run asyncio tasks."),
                ),
            ),
        ),
    ),
)


def methods(result: RetrievalResult) -> set[tuple[Channel, str]]:
    """Every (channel, method) that contributed a hit."""
    return {
        (hit.method.channel, hit.method.method)
        for item in result.evidence
        for hit in item.provenance.hits
    }


async def durable_generations(engine: RetrievalEngine) -> dict[str, str]:
    """Active generation per durable method."""
    status = await engine.status(scope=SCOPE)
    return {
        item.method: item.generation_id
        for item in status.generations
        if item.state is GenerationState.ACTIVE and item.method in DURABLE
    }


async def test_durable_backends_survive_restart(tmp_path: Path) -> None:
    """A restarted engine serves every channel from its files without re-indexing them."""
    async with running_engine(
        Components(embedding=HashEmbedding()), tmp_path, retrieval=ALL_CHANNELS
    ) as engine:
        report = await engine.ingest([DOCUMENT], scope=SCOPE)
        assert not report.warnings
        before = await durable_generations(engine)
        assert set(before) == DURABLE
        first = await engine.retrieve("asyncio event loops", scope=SCOPE)
    assert {"graph", "structure", "metadata.db"} <= _entry_names(tmp_path)

    embedding = HashEmbedding()
    async with running_engine(
        Components(embedding=embedding), tmp_path, retrieval=ALL_CHANNELS
    ) as restarted:
        assert await durable_generations(restarted) == before
        assert embedding.calls == 0  # nothing durable was re-embedded on start
        again = await restarted.retrieve("asyncio event loops", scope=SCOPE)
        assert again.status is ResultStatus.COMPLETE
        assert methods(again) == methods(first)
        assert {Channel.INDEX, Channel.GRAPH, Channel.STRUCTURE} <= {c for c, _ in methods(again)}
        assert [e.sources[0].document_id for e in again.evidence] == [
            e.sources[0].document_id for e in first.evidence
        ]
        graph_only = await restarted.retrieve(
            "Python", scope=SCOPE, config=RetrievalExecutionConfig(index=False, structure=False)
        )
        assert graph_only.evidence
        assert {channel for channel, _ in methods(graph_only)} == {Channel.GRAPH}
