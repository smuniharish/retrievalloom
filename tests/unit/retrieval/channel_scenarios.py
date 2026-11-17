"""Channel on/off scenarios on one durable store, shared by the offline and live suites.

Every scenario restarts the engine with different channel configurations through an
:data:`Opener`, which opens an engine for a set of enabled channels on the same durable
location (SQLite file + local Qdrant + Ladybug + TreeSearch offline; Postgres and a
Qdrant server in the live suite).
"""

from __future__ import annotations

import itertools
from typing import TYPE_CHECKING

from retrievalloom import (
    ChannelsConfig,
    DocumentInput,
    GraphChannelConfig,
    IndexChannelConfig,
    LifecycleConfig,
    RerankingConfig,
    RetrievalConfig,
    StructureChannelConfig,
    StructureNodeInput,
    StructureRepresentation,
    TextRepresentation,
)
from retrievalloom.domain.retrieval import ResultStatus, WarningCode

if TYPE_CHECKING:
    from collections.abc import Callable
    from contextlib import AbstractAsyncContextManager

    from retrievalloom import RetrievalEngine, Scope
    from retrievalloom.domain.retrieval import RetrievalResult

    type Opener = Callable[
        [frozenset[str], LifecycleConfig], AbstractAsyncContextManager[RetrievalEngine]
    ]

CHANNELS = ("index", "graph", "structure")
SUBSETS = [
    frozenset(subset)
    for size in range(1, len(CHANNELS) + 1)
    for subset in itertools.combinations(CHANNELS, size)
]
FOREGROUND = LifecycleConfig(background=False)
UNIT = {"index": "index.", "graph": "graph.graph", "structure": "structure.structure"}


def subset_id(channels: frozenset[str]) -> str:
    return "+".join(channel for channel in CHANNELS if channel in channels)


def retrieval_config(channels: frozenset[str]) -> RetrievalConfig:
    return RetrievalConfig(
        reranking=RerankingConfig(enabled=False),
        channels=ChannelsConfig(
            index=IndexChannelConfig(enabled="index" in channels),
            graph=GraphChannelConfig(enabled="graph" in channels),
            structure=StructureChannelConfig(enabled="structure" in channels),
        ),
    )


def doc(document_id: str, text: str, *, tree: bool = True) -> DocumentInput:
    """Text document; with ``tree`` also a one-node structure representation."""
    representations: list[TextRepresentation | StructureRepresentation] = [
        TextRepresentation(text=text)
    ]
    if tree:
        representations.append(
            StructureRepresentation(
                representation_id="tree",
                root=StructureNodeInput(title=document_id, text=text),
            )
        )
    return DocumentInput(_id=document_id, representations=tuple(representations))


CORPUS = [
    doc("a", "Alpha Gamma report"),
    doc("b", "Beta Delta notes"),
    doc("c", "Charlie Echo memo", tree=False),
]


def channels_for(result: RetrievalResult, document_id: str) -> set[str]:
    """Channels that produced hits for ``document_id``."""
    return {
        hit.method.channel.value
        for item in result.evidence
        if document_id in {source.document_id for source in item.sources}
        for hit in item.provenance.hits
    }


def channels_of(result: RetrievalResult) -> set[str]:
    return {hit.method.channel.value for item in result.evidence for hit in item.provenance.hits}


async def complete(engine: RetrievalEngine, query: str, scope: Scope) -> RetrievalResult:
    result = await engine.retrieve(query, scope=scope, top_k=20)
    # Nothing degraded: complete, or empty because nothing matches.
    expected = ResultStatus.COMPLETE if result.evidence else ResultStatus.EMPTY
    assert result.status is expected, (query, result.warnings)
    assert result.warnings == (), (query, result.warnings)
    return result


async def coverage(engine: RetrievalEngine, scope: Scope, channels: frozenset[str]) -> dict:
    """Coverage counts of the units of ``channels`` (status also lists disabled units)."""
    status = await engine.status(scope=scope)
    return {
        unit: counts
        for unit, counts in status.coverage.items()
        if any(unit.startswith(UNIT[channel]) for channel in channels)
    }


# scenarios ----------------------------------------------------------------------------------
async def one_combination(open_engine: Opener, scope: Scope, channels: frozenset[str]) -> None:
    """``channels`` serve exactly their channels, completely and without warnings."""
    async with open_engine(channels, FOREGROUND) as engine:
        await engine.ingest(CORPUS, scope=scope)
        result = await complete(engine, "Alpha", scope)
        assert channels_for(result, "a") == set(channels), subset_id(channels)
        assert channels_of(result) <= channels
        covered = await coverage(engine, scope, channels)
    if "graph" in channels:
        assert covered["graph.graph"] == {"ready": 3}
    if "structure" in channels:
        # ``c`` has no structure representation: nothing to index, not an error.
        assert covered["structure.structure"] == {"ready": 2, "not_applicable": 1}


async def every_channel_combination(open_engine: Opener, scope: Scope) -> None:
    """Every subset in turn on one store (each restart switches the configuration)."""
    for channels in SUBSETS:
        await one_combination(open_engine, scope, channels)


async def enable_later(open_engine: Opener, scope: Scope) -> None:
    """Channels enabled on a populated store cover the documents ingested before."""
    async with open_engine(frozenset({"index"}), FOREGROUND) as engine:
        await engine.ingest(CORPUS, scope=scope)
        assert channels_for(await complete(engine, "Alpha", scope), "a") == {"index"}
    everything = frozenset(CHANNELS)
    async with open_engine(everything, FOREGROUND) as engine:
        covered = await coverage(engine, scope, everything)
        assert covered["graph.graph"] == {"ready": 3}
        assert covered["structure.structure"] == {"ready": 2, "not_applicable": 1}
        assert channels_for(await complete(engine, "Alpha", scope), "a") == set(CHANNELS)
        # The graph of ``c`` was extracted on enable although it predates the graph.
        assert "graph" in channels_for(await complete(engine, "Charlie Echo", scope), "c")


async def catch_up_window(open_engine: Opener, scope: Scope) -> None:
    """Until the new units are built, results are partial and say why."""
    async with open_engine(frozenset({"index"}), FOREGROUND) as engine:
        await engine.ingest(CORPUS, scope=scope)
    everything = frozenset(CHANNELS)
    lazy = LifecycleConfig(background=False, auto_rebuild_ephemeral=False)
    async with open_engine(everything, lazy) as engine:
        result = await engine.retrieve("Alpha", scope=scope)
        assert result.status is ResultStatus.PARTIAL
        missing = {w.method for w in result.warnings if w.code is WarningCode.INDEX_MISSING}
        assert {"graph", "structure"} <= missing, result.warnings
        task = await engine.backfill(scope=scope)
        await engine.wait_for(task, timeout=120)
        assert channels_for(await complete(engine, "Alpha", scope), "a") == set(CHANNELS)
    waiting = LifecycleConfig(background=True, wait_for_rebuild=True, poll_interval=0.2)
    async with open_engine(everything, waiting) as engine:
        assert channels_for(await complete(engine, "Alpha", scope), "a") == set(CHANNELS)


async def disable_change_and_reenable(open_engine: Opener, scope: Scope) -> None:
    """Changes made while channels were off are honoured when they are on again."""
    everything = frozenset(CHANNELS)
    async with open_engine(everything, FOREGROUND) as engine:
        await engine.ingest(CORPUS, scope=scope)
        assert channels_for(await complete(engine, "Beta", scope), "b") == set(CHANNELS)
    async with open_engine(frozenset({"index"}), FOREGROUND) as engine:
        assert channels_of(await complete(engine, "Alpha", scope)) == {"index"}
        await engine.status(scope=scope)  # stale rows of disabled units do not break it
        await engine.delete(["a", "b"], scope=scope)
        assert (await engine.purge(["b"], scope=scope)).purged == ("b",)
        await engine.ingest(
            [doc("c", "Charlie Foxtrot memo"), doc("d", "Hotel India log")], scope=scope
        )
    async with open_engine(everything, FOREGROUND) as engine:
        covered = await coverage(engine, scope, everything)
        assert covered["graph.graph"] == {"ready": 2, "deleted": 1}
        assert covered["structure.structure"] == {"ready": 2, "deleted": 1}
        for query, gone in (("Alpha Gamma", "a"), ("Beta Delta", "b")):
            result = await complete(engine, query, scope)
            assert channels_for(result, gone) == set(), (query, gone)
        assert channels_for(await complete(engine, "Echo", scope), "c") <= {"index"}
        assert channels_for(await complete(engine, "Foxtrot", scope), "c") == set(CHANNELS)
        assert channels_for(await complete(engine, "Hotel", scope), "d") == set(CHANNELS)


async def swap_channels(open_engine: Opener, scope: Scope) -> None:
    """Graph-only, then structure-only, then graph-only again on one store."""
    graph, structure = frozenset({"graph"}), frozenset({"structure"})
    async with open_engine(graph, FOREGROUND) as engine:
        await engine.ingest(CORPUS, scope=scope)
        assert channels_of(await complete(engine, "Alpha", scope)) == {"graph"}
    async with open_engine(structure, FOREGROUND) as engine:
        await engine.ingest([doc("b", "Beta Kilo notes")], scope=scope)
        assert channels_of(await complete(engine, "Alpha", scope)) == {"structure"}
    async with open_engine(graph, FOREGROUND) as engine:
        assert channels_for(await complete(engine, "Kilo", scope), "b") == {"graph"}
        assert channels_for(await complete(engine, "Delta", scope), "b") == set()
