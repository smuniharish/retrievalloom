"""Graph channel on LadybugDB: entity matching plus multi-hop neighbourhood evidence.

The application supplies entities and relationships per document (a ``graph``
representation). The engine merges them into per-scope graph aggregates, stores them in
an embedded LadybugDB database, and the default retriever finds entities named in the
query and expands their neighbourhood up to ``depth`` hops. Every graph evidence item
lists the documents that asserted it, the hop distance and the traversal path.

Requires the ``graph`` extra (``pip install "retrievalloom[graph]"``).

Run from the repository root::

    python examples/graph_channel.py
"""

import asyncio

from _shared import quiet_logs

from retrievalloom import (
    ChannelsConfig,
    DocumentInput,
    GraphChannelConfig,
    GraphEntityInput,
    GraphRelationshipInput,
    GraphRepresentation,
    LifecycleConfig,
    RetrievalConfig,
    RetrievalExecutionConfig,
    Scope,
    TextRepresentation,
    create_retrieval_engine,
)

PEOPLE = {
    "ada": GraphEntityInput(key="ada", name="Ada Lovelace", entity_type="person"),
    "babbage": GraphEntityInput(key="babbage", name="Charles Babbage", entity_type="person"),
    "engine": GraphEntityInput(
        key="engine", name="Analytical Engine", entity_type="machine", aliases=("AE",)
    ),
    "menabrea": GraphEntityInput(key="menabrea", name="Luigi Menabrea", entity_type="person"),
}


def document(doc_id: str, text: str, *relations: tuple[str, str, str]) -> DocumentInput:
    keys = sorted({key for source, _, target in relations for key in (source, target)})
    graph = GraphRepresentation(
        representation_id="graph",
        entities=tuple(PEOPLE[key] for key in keys),
        relationships=tuple(
            GraphRelationshipInput(source_key=source, target_key=target, relation_type=kind)
            for source, kind, target in relations
        ),
    )
    return DocumentInput(_id=doc_id, representations=(TextRepresentation(text=text), graph))


DOCUMENTS = [
    document(
        "notes",
        "Ada Lovelace wrote notes on the Analytical Engine.",
        ("ada", "wrote_about", "engine"),
    ),
    document(
        "design",
        "Charles Babbage designed the Analytical Engine.",
        ("babbage", "designed", "engine"),
    ),
    document(
        "lecture",
        "Luigi Menabrea described the engine after a lecture by Babbage.",
        ("menabrea", "described", "engine"),
        ("babbage", "lectured_to", "menabrea"),
    ),
]


async def main() -> None:
    quiet_logs()
    scope = Scope(tenant_id="acme")
    # depth=2: two entity hops from every seed entity named in the query.
    retrieval = RetrievalConfig(
        channels=ChannelsConfig(graph=GraphChannelConfig(enabled=True, depth=2))
    )
    engine = create_retrieval_engine(
        retrieval=retrieval, lifecycle=LifecycleConfig(background=False)
    )
    async with engine:
        await engine.ingest(DOCUMENTS, scope=scope)
        # Graph channel only, so every evidence item below comes from the graph.
        graph_only = RetrievalExecutionConfig(index=False)
        result = await engine.retrieve(
            "Who worked with Ada Lovelace?", scope=scope, top_k=10, config=graph_only
        )
        print(f"status={result.status.value} evidence={len(result.evidence)}")
        # Traversal paths hold object ids; name them with the texts of the returned objects.
        names = {
            segment.object_id: segment.text for item in result.evidence for segment in item.segments
        }
        for rank, item in enumerate(result.evidence, start=1):
            hit = item.provenance.hits[0]
            hops = next((d.details["distance"] for d in hit.matched if d.kind == "graph_path"), 0)
            documents = sorted({source.document_id for source in item.sources})
            print(
                f"#{rank} score={item.score:.4f} hops={hops} {item.display_text!r} from {documents}"
            )
            if hops:
                print(
                    "     path: " + " -> ".join(str(names.get(ref, ref)) for ref in hit.path_refs)
                )


if __name__ == "__main__":
    asyncio.run(main())
