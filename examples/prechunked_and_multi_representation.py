"""Pre-chunked input and a document with several representations.

The application owns parsing and chunking. ``ChunksRepresentation`` passes chunks with
their character spans, pages and section paths; the engine indexes them as given and
reports them in every evidence source. Adjacent matching chunks of one representation
are merged into one evidence item with one segment per chunk.

One document can carry several representations, each feeding one channel: the index
channel reads the first ``chunks`` representation (or, without one, the first ``text``
representation), and the structure channel reads the first ``structure`` representation.
Every evidence source names the representation it came from.

Requires the ``structure`` extra (``pip install "retrievalloom[structure]"``).

Run from the repository root::

    python examples/prechunked_and_multi_representation.py
"""

import asyncio

from _shared import quiet_logs

from retrievalloom import (
    ChannelsConfig,
    ChunkInput,
    ChunksRepresentation,
    DocumentInput,
    LifecycleConfig,
    RetrievalConfig,
    Scope,
    StructureChannelConfig,
    StructureNodeInput,
    StructureRepresentation,
    TextSpan,
    create_retrieval_engine,
)
from retrievalloom.domain.inputs import PageRange

# (local id, section path, page, text) of the application's chunks.
BODY = [
    ("intro", ("Manual", "Introduction"), 1, "The pump moves coolant through the engine."),
    ("filter", ("Manual", "Maintenance"), 2, "Replace the coolant filter every 500 hours."),
    ("drain", ("Manual", "Maintenance"), 2, "Drain old coolant before fitting a new filter."),
    ("warranty", ("Manual", "Warranty"), 3, "The warranty covers the pump housing for two years."),
]


def body() -> ChunksRepresentation:
    """Chunks with character spans into the original manual text."""
    chunks, offset = [], 0
    for local_id, section, page, text in BODY:
        chunks.append(
            ChunkInput(
                local_id=local_id,
                text=text,
                span=TextSpan(start=offset, end=offset + len(text), basis="original"),
                pages=PageRange(start=page, end=page),
                section_path=section,
            )
        )
        offset += len(text) + 1
    return ChunksRepresentation(representation_id="body", chunks=tuple(chunks))


def outline() -> StructureRepresentation:
    """The manual's table of contents as a tree of sections with short abstracts."""
    sections = (
        StructureNodeInput(title="Introduction", text="What the coolant pump does."),
        StructureNodeInput(title="Maintenance", text="Service intervals for the filter."),
        StructureNodeInput(title="Warranty", text="Coverage period and exclusions."),
    )
    return StructureRepresentation(
        representation_id="outline",
        root=StructureNodeInput(title="Pump manual", children=sections),
    )


async def main() -> None:
    quiet_logs()
    scope = Scope(tenant_id="acme")
    manual = DocumentInput(_id="pump-manual", representations=(body(), outline()))
    retrieval = RetrievalConfig(
        channels=ChannelsConfig(structure=StructureChannelConfig(enabled=True))
    )
    engine = create_retrieval_engine(
        retrieval=retrieval, lifecycle=LifecycleConfig(background=False)
    )
    async with engine:
        await engine.ingest([manual], scope=scope)
        for query in ("replace coolant filter", "warranty"):
            result = await engine.retrieve(query, scope=scope, top_k=3)
            print(f"query: {query!r}")
            for rank, item in enumerate(result.evidence, start=1):
                relations = sorted({relation.type.value for relation in item.relations})
                channels = sorted({hit.method.channel.value for hit in item.provenance.hits})
                print(f"  #{rank} score={item.score:.4f} channels={channels} relations={relations}")
                for segment in item.segments:
                    for source in segment.sources:
                        where = " > ".join(source.section_path)
                        if source.span is not None and source.pages is not None:
                            where += f" (chars {source.span.start}-{source.span.end}"
                            where += f", p. {source.pages.start})"
                        print(f"     [{source.representation_id}] {where}: {segment.text}")


if __name__ == "__main__":
    asyncio.run(main())
