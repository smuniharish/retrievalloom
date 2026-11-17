"""Channel on/off combinations and restarts with other channels on one durable store."""

from __future__ import annotations

import contextlib
from typing import TYPE_CHECKING

import pytest
from pydantic import ValidationError

from retrievalloom import (
    ChannelsConfig,
    Components,
    ConfigurationError,
    GraphChannelConfig,
    IndexChannelConfig,
    LifecycleConfig,
    MethodConfig,
    RerankingConfig,
    RetrievalConfig,
    StructureChannelConfig,
)
from retrievalloom.config import FuzzyConfig
from tests.fakes import CapitalizedExtractor, HashEmbedding
from tests.unit.retrieval import channel_scenarios as scenarios
from tests.unit.retrieval.helpers import ACME, build_engine, opened, runtime

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from pathlib import Path

    from retrievalloom import RetrievalEngine

pytestmark = [pytest.mark.graph, pytest.mark.structure]

METHODS = ("keyword", "fuzzy", "sparse", "dense")


def _components() -> Components:
    return Components(embedding=HashEmbedding(), graph_extractor=CapitalizedExtractor())


def _opener(data_dir: Path) -> scenarios.Opener:
    @contextlib.asynccontextmanager
    async def open_engine(
        channels: frozenset[str], lifecycle: LifecycleConfig
    ) -> AsyncIterator[RetrievalEngine]:
        async with opened(
            retrieval=scenarios.retrieval_config(channels),
            components=_components(),
            lifecycle=lifecycle,
            data_dir=data_dir,
        ) as engine:
            yield engine

    return open_engine


@pytest.mark.parametrize("channels", scenarios.SUBSETS, ids=scenarios.subset_id)
async def test_each_channel_combination(channels: frozenset[str], tmp_path: Path) -> None:
    await scenarios.one_combination(_opener(tmp_path), ACME, channels)


async def test_every_combination_in_turn_on_one_store(tmp_path: Path) -> None:
    await scenarios.every_channel_combination(_opener(tmp_path), ACME)


async def test_enabling_channels_later_covers_existing_documents(tmp_path: Path) -> None:
    await scenarios.enable_later(_opener(tmp_path), ACME)


async def test_catch_up_window_is_partial_until_built(tmp_path: Path) -> None:
    await scenarios.catch_up_window(_opener(tmp_path), ACME)


async def test_changes_while_disabled_are_honoured_on_reenable(tmp_path: Path) -> None:
    await scenarios.disable_change_and_reenable(_opener(tmp_path), ACME)


async def test_swapping_channels(tmp_path: Path) -> None:
    await scenarios.swap_channels(_opener(tmp_path), ACME)


def _index_only(enabled: set[str]) -> RetrievalConfig:
    index = IndexChannelConfig(
        keyword=MethodConfig(enabled="keyword" in enabled),
        fuzzy=FuzzyConfig(enabled="fuzzy" in enabled),
        sparse=MethodConfig(enabled="sparse" in enabled),
        dense=MethodConfig(enabled="dense" in enabled),
    )
    return RetrievalConfig(
        reranking=RerankingConfig(enabled=False), channels=ChannelsConfig(index=index)
    )


@pytest.mark.parametrize(
    "enabled",
    [{"keyword"}, {"fuzzy"}, {"sparse"}, {"dense"}, {"keyword", "dense"}, set(METHODS)],
    ids=lambda enabled: "+".join(sorted(enabled)),
)
async def test_index_method_subsets(enabled: set[str], tmp_path: Path) -> None:
    async with opened(
        retrieval=_index_only(enabled), components=_components(), data_dir=tmp_path
    ) as engine:
        assert set(runtime(engine).units) == {f"index.{m}" for m in enabled}
        await engine.ingest(scenarios.CORPUS, scope=ACME)
        result = await scenarios.complete(engine, "Alpha Gamma", ACME)
        methods = {hit.method.method for item in result.evidence for hit in item.provenance.hits}
        assert methods <= enabled
        status = await engine.status(scope=ACME)
    assert set(status.coverage) == {f"index.{m}" for m in enabled}


@pytest.mark.parametrize(
    "index",
    [
        IndexChannelConfig(enabled=False),
        IndexChannelConfig(
            keyword=MethodConfig(enabled=False),
            fuzzy=FuzzyConfig(enabled=False),
            sparse=MethodConfig(enabled=False),
            dense=MethodConfig(enabled=False),
        ),
    ],
    ids=["channels-off", "methods-off"],
)
def test_nothing_enabled_is_rejected(index: IndexChannelConfig) -> None:
    with pytest.raises(ValidationError, match="at least one channel"):
        ChannelsConfig(index=index)


def test_graph_alone_is_accepted() -> None:
    config = ChannelsConfig(
        index=IndexChannelConfig(enabled=False), graph=GraphChannelConfig(enabled=True)
    )
    assert config.graph.enabled
    assert ChannelsConfig(
        index=IndexChannelConfig(enabled=False), structure=StructureChannelConfig(enabled=True)
    ).structure.enabled


async def test_no_usable_method_is_a_configuration_error() -> None:
    # Dense only, but no embedding: nothing would be indexed or queried.
    engine = build_engine(retrieval=_index_only({"dense"}), components=Components())
    with pytest.raises(ConfigurationError, match="no retrieval method"):
        await engine.initialize()
