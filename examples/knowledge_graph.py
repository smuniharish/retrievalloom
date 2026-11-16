"""Knowledge-graph QA: extracted entities on Neo4j, fused with the index channel.

Platform notes (architecture pages, team directory, postmortems) are plain text. At ingest
the application's ``CapitalizedEntityExtractor`` (``GraphExtractorABC``) turns sentences
such as "Team Falcon owns Payments Gateway" into entities and typed relationships, and
``AliasGraphNormalizer`` (``GraphNormalizerABC``) merges spellings ("PG", "Postgres") into
one canonical entity. The graph is stored in Neo4j by ``Neo4jGraphStore``
(``GraphStoreABC``); the engine's default neighbourhood retriever expands the entities
named in a question up to two hops. Graph evidence and index evidence (BM25, keyword,
fuzzy) are fused with RRF, so an answer cites both the facts and the source passages.

Uses Neo4j when ``RL_EXAMPLES_NEO4J_URL`` is set (``examples/compose.yaml``), otherwise
the embedded LadybugDB graph store (``graph`` extra)::

    $env:RL_EXAMPLES_NEO4J_URL="bolt://localhost:7687"   # optional
    python examples/knowledge_graph.py
"""

import asyncio
import os

from _shared import env, load, quiet_logs, run_prefix
from adapters import AliasGraphNormalizer, CapitalizedEntityExtractor, Neo4jGraphStore

from retrievalloom import (
    ChannelsConfig,
    Components,
    DocumentInput,
    GraphChannelConfig,
    LifecycleConfig,
    RerankingConfig,
    RetrievalConfig,
    RetrievalExecutionConfig,
    Scope,
    SourceDescriptor,
    TextRepresentation,
    create_retrieval_engine,
)

SCOPE = Scope(tenant_id="northwind", collection="platform")
ALIASES = {"PG": "Postgres", "Postgres Cluster": "Postgres", "Checkout": "Checkout Service"}
TYPES = {
    "Maria Okafor": "person",
    "Jonas Berg": "person",
    "Team Falcon": "team",
    "Team Harbor": "team",
    "Frankfurt Region": "region",
    "Postgres": "database",
    "Orders DB": "database",
}


def notes() -> list[DocumentInput]:
    return [
        DocumentInput(
            _id=note["id"],
            source=SourceDescriptor(uri=f"wiki://platform/{note['id']}", title=note["title"]),
            representations=(TextRepresentation(text=note["text"]),),
        )
        for note in load("platform_notes.json")
    ]


async def main() -> None:
    quiet_logs()
    url = env("NEO4J_URL")
    store = None
    if url:
        store = Neo4jGraphStore(
            url,
            user=env("NEO4J_USER") or "neo4j",
            password=env("NEO4J_PASSWORD") or os.environ.get("NEO4J_PASSWORD", "retrievalloom"),
            label=run_prefix("kg").upper().replace("_", ""),
        )
    print(f"graph store: {'neo4j' if store else 'ladybug (embedded)'}")
    engine = create_retrieval_engine(
        retrieval=RetrievalConfig(
            channels=ChannelsConfig(graph=GraphChannelConfig(enabled=True, depth=2)),
            reranking=RerankingConfig(enabled=False),
        ),
        components=Components(
            graph_extractor=CapitalizedEntityExtractor(entity_types=TYPES),
            graph_normalizer=AliasGraphNormalizer(ALIASES, entity_types=TYPES),
            graph_store=store,
        ),
        lifecycle=LifecycleConfig(background=False),
    )
    try:
        async with engine:
            await engine.ingest(notes(), scope=SCOPE)
            status = await engine.status(scope=SCOPE)
            print(f"graph coverage: {status.coverage.get('graph.graph', {})}")

            question = "Who operates Postgres and where is it hosted?"
            graph_only = RetrievalExecutionConfig(index=False)
            result = await engine.retrieve(question, scope=SCOPE, top_k=8, config=graph_only)
            print(f"\nQ: {question} (graph channel only)")
            names = {s.object_id: s.text for item in result.evidence for s in item.segments}
            for item in result.evidence:
                hit = item.provenance.hits[0]
                hops = next(
                    (d.details["distance"] for d in hit.matched if d.kind == "graph_path"), 0
                )
                cited = sorted({source.document_id for source in item.sources})
                print(f"  hops={hops} {item.display_text!r} <- {cited}")
                if hops:
                    path = " -> ".join(str(names.get(ref, ref)) for ref in hit.path_refs)
                    print(f"     path: {path}")

            question = (
                "Which team leads the database behind Checkout and what happened in the incident?"
            )
            result = await engine.retrieve(question, scope=SCOPE, top_k=6)
            print(f"\nQ: {question} (graph + index fused)")
            for rank, item in enumerate(result.evidence, start=1):
                channels = sorted({hit.method.channel.value for hit in item.provenance.hits})
                print(f"  #{rank} {channels} {(item.display_text or '')[:90]!r}")
    finally:
        if store is not None:
            await store.drop_label()
            await store.close()


if __name__ == "__main__":
    asyncio.run(main())
