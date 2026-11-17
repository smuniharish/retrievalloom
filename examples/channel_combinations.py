"""Channel combinations: choose channels, and turn them on or off as your needs change.

RetrievalLoom searches through three channels. The **index** channel matches words,
misspellings and meaning; the **structure** channel searches a document's section outline;
the **graph** channel follows entities and their relationships. You pick channels with
:class:`~retrievalloom.ChannelsConfig` when the engine opens. To change them on an existing
store, close the engine and open it again with the new configuration on the same
``data_dir`` (or the same metadata store and backends). Documents you already ingested are
caught up automatically; you never re-ingest.

The walkthrough, on a small set of operations runbooks:

1. **Start with the index channel only** and ingest the runbooks.
2. **Turn on the structure channel later.** Reopen with index + structure; the engine
   indexes the existing runbooks for the new channel while it opens.
3. **Turn on the graph channel too, and catch up explicitly.** With the startup rebuild
   off, results are ``empty`` or ``partial`` and name the missing methods until
   ``backfill`` + ``wait_for`` has built them. The graph extractor runs on the runbooks
   ingested while the graph was off. ``LifecycleConfig(wait_for_rebuild=True)`` instead
   opens the engine only once everything is built.
4. **Every combination.** Reopen the same store with each of the seven channel
   combinations, from index only to graph + structure without the index, and see which
   channels answer. A configuration with nothing enabled is rejected.
5. **Per request.** ``RetrievalExecutionConfig`` narrows one request to some channels
   without reopening the engine.
6. **Turn a channel off and on again.** A runbook deleted while the graph was off is gone
   from the graph once it is back on.

Runs offline with the ``graph`` and ``structure`` extras::

    python examples/channel_combinations.py
"""

import asyncio
import itertools
import tempfile
from contextlib import AbstractAsyncContextManager
from pathlib import Path

import pydantic
from _shared import quiet_logs
from adapters import CapitalizedEntityExtractor

from retrievalloom import (
    ChannelsConfig,
    Components,
    DocumentInput,
    EngineResources,
    GraphChannelConfig,
    IndexChannelConfig,
    LifecycleConfig,
    MethodConfig,
    RerankingConfig,
    RetrievalConfig,
    RetrievalEngine,
    RetrievalExecutionConfig,
    Scope,
    StructureChannelConfig,
    StructureNodeInput,
    StructureRepresentation,
    TextRepresentation,
    create_retrieval_engine,
)

SCOPE = Scope(tenant_id="northwind", collection="runbooks")
CHANNELS = ("index", "structure", "graph")
RUNBOOKS = {
    "db-failover": (
        "Database failover",
        [
            ("Detect", "Team Harbor watches Postgres replication lag."),
            ("Promote", "Team Harbor promotes the Postgres replica in Frankfurt Region."),
        ],
    ),
    "cache-flush": (
        "Cache flush",
        [("Flush", "Team Falcon flushes Redis before a Checkout release.")],
    ),
    "vpn-access": ("VPN access", [("Request", "Team Falcon grants VPN access to contractors.")]),
}


def runbook(document_id: str) -> DocumentInput:
    """Plain text for the index (and graph) channels, an outline for the structure channel."""
    title, sections = RUNBOOKS[document_id]
    text = " ".join(body for _, body in sections)
    outline = StructureNodeInput(
        title=title,
        children=tuple(StructureNodeInput(title=name, text=body) for name, body in sections),
    )
    return DocumentInput(
        _id=document_id,
        representations=(
            TextRepresentation(text=text),
            StructureRepresentation(representation_id="outline", root=outline),
        ),
    )


def channels_config(*channels: str) -> ChannelsConfig:
    """A ``ChannelsConfig`` with exactly ``channels`` enabled."""
    return ChannelsConfig(
        # No embedding model in this offline example, so the dense method stays off.
        index=IndexChannelConfig(enabled="index" in channels, dense=MethodConfig(enabled=False)),
        structure=StructureChannelConfig(enabled="structure" in channels),
        graph=GraphChannelConfig(enabled="graph" in channels),
    )


def engine_for(
    data_dir: Path, *channels: str, lifecycle: LifecycleConfig | None = None
) -> AbstractAsyncContextManager[RetrievalEngine]:
    """An engine on ``data_dir`` with exactly ``channels`` enabled."""
    return create_retrieval_engine(
        retrieval=RetrievalConfig(
            channels=channels_config(*channels),
            reranking=RerankingConfig(enabled=False),
        ),
        components=Components(graph_extractor=CapitalizedEntityExtractor()),
        lifecycle=lifecycle or LifecycleConfig(background=False),
        resources=EngineResources(data_dir=data_dir),
    )


async def ask(
    engine: RetrievalEngine,
    query: str,
    config: RetrievalExecutionConfig | None = None,
    *,
    text: bool = False,
) -> None:
    result = await engine.retrieve(query, scope=SCOPE, top_k=4, config=config)
    found = []
    for item in result.evidence:
        channels = "+".join(sorted({hit.method.channel.value for hit in item.provenance.hits}))
        documents = ",".join(sorted({source.document_id for source in item.sources}))
        label = f" {(item.display_text or '')[:28]!r}" if text else ""
        found.append(f"{documents}[{channels}]{label}")
    warnings = sorted({f"{w.code.value}:{w.method}" for w in result.warnings})
    print(f"   {result.status.value} {found}" + (f" warnings={warnings}" if warnings else ""))


async def answering_channels(engine: RetrievalEngine, query: str) -> list[str]:
    result = await engine.retrieve(query, scope=SCOPE, top_k=6)
    return sorted(
        {hit.method.channel.value for item in result.evidence for hit in item.provenance.hits}
    )


async def coverage(engine: RetrievalEngine, *units: str) -> None:
    status = await engine.status(scope=SCOPE)
    for unit in units:
        print(f"   {unit}: {status.coverage.get(unit, {})}")


async def main() -> None:
    quiet_logs()
    with tempfile.TemporaryDirectory() as tmp:
        data_dir = Path(tmp)
        # Without the startup rebuild, the catch-up window is visible (step 3).
        lazy = LifecycleConfig(background=False, auto_rebuild_ephemeral=False)

        print("1. index only")
        async with engine_for(data_dir, "index") as engine:
            await engine.ingest([runbook(name) for name in RUNBOOKS], scope=SCOPE)
            await ask(engine, "promote the Postgres replica")

        print("2. structure turned on later (caught up while the engine opens)")
        async with engine_for(data_dir, "index", "structure") as engine:
            await coverage(engine, "index.keyword", "structure.structure")
            await ask(engine, "promote the Postgres replica")

        print("3. graph turned on as well, caught up by an explicit backfill")
        async with engine_for(data_dir, *CHANNELS, lifecycle=lazy) as engine:
            print("   before backfill:")
            await ask(engine, "promote the Postgres replica")
            await engine.wait_for(await engine.backfill(scope=SCOPE), timeout=120)
            print("   after backfill:")
            await coverage(engine, "graph.graph")
            await ask(engine, "promote the Postgres replica")

        print("4. every combination on the same store")
        for size in (1, 2, 3):
            for combo in itertools.combinations(CHANNELS, size):
                async with engine_for(data_dir, *combo) as engine:
                    found = await answering_channels(engine, "Team Harbor Postgres replica")
                    print(f"   {' + '.join(combo):<26} answered by {found}")
        try:
            channels_config()
        except pydantic.ValidationError:
            print("   nothing enabled              rejected")

        print("5. one channel per request: 'Who works on Postgres?'")
        async with engine_for(data_dir, *CHANNELS) as engine:
            for channel in CHANNELS:
                only = RetrievalExecutionConfig(
                    index=channel == "index",
                    structure=channel == "structure",
                    graph=channel == "graph",
                )
                print(f"   {channel}:")
                await ask(engine, "Who works on Postgres?", only, text=True)

        print("6. graph off; vpn-access deleted meanwhile")
        async with engine_for(data_dir, "index", "structure") as engine:
            await engine.delete(["vpn-access"], scope=SCOPE)
            await ask(engine, "Team Falcon contractors")

        print("   graph on again (the startup round catches up)")
        async with engine_for(data_dir, *CHANNELS) as engine:
            await coverage(engine, "graph.graph")
            await ask(engine, "Team Falcon contractors", RetrievalExecutionConfig(index=False))


if __name__ == "__main__":
    asyncio.run(main())
