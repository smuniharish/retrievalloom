"""Structure channel: TreeSearch over an application-provided document tree.

The application passes each document's outline as a ``structure`` representation (nested
``StructureNodeInput`` nodes). The structure channel ranks nodes by title and text with
TreeSearch's FTS5 index. Each evidence source carries the node's section path; with
``include_candidates=True`` each candidate's payload carries the node's level, parent and
children; and the structure engine answers navigation questions (parent, children,
siblings, path to the root) about any returned node.

Requires the ``structure`` extra (``pip install "retrievalloom[structure]"``).

Run from the repository root::

    python examples/structure_channel.py
"""

import asyncio

from _shared import quiet_logs

from retrievalloom import (
    ChannelsConfig,
    Components,
    DocumentInput,
    LifecycleConfig,
    RetrievalConfig,
    RetrievalExecutionConfig,
    Scope,
    StructureChannelConfig,
    StructureNodeInput,
    StructureRepresentation,
    create_retrieval_engine,
)
from retrievalloom.contracts.indexes import NavigationDirection
from retrievalloom.domain.retrieval import RetrievalHit
from retrievalloom.structure.treesearch_engine import TreeSearchStructureEngine


def node(title: str, text: str = "", *children: StructureNodeInput) -> StructureNodeInput:
    return StructureNodeInput(title=title, text=text, children=children)


GUIDE = node(
    "Admin guide",
    "",
    node(
        "Installation",
        "Install the server package on Linux or Windows.",
        node("Linux", "Use the apt repository and enable the systemd unit."),
        node("Windows", "Run the MSI installer and start the service."),
    ),
    node(
        "Backups",
        "Back up the data directory nightly.",
        node("Restore", "Stop the service, then copy the backup into the data directory."),
    ),
)


async def navigate(
    structure: TreeSearchStructureEngine,
    scope: Scope,
    hit: RetrievalHit,
    direction: NavigationDirection,
) -> list[str]:
    ref = hit.object_ref
    return list(await structure.navigate(scope, hit.generation_id, ref.object_id, direction))


async def main() -> None:
    quiet_logs()
    scope = Scope(tenant_id="acme")
    # Application-owned structure engine (in memory), kept to ask navigation questions.
    structure = TreeSearchStructureEngine()
    engine = create_retrieval_engine(
        retrieval=RetrievalConfig(
            channels=ChannelsConfig(structure=StructureChannelConfig(enabled=True))
        ),
        components=Components(structure_engine=structure),
        lifecycle=LifecycleConfig(background=False),
    )
    async with engine:
        try:
            guide = DocumentInput(
                _id="admin-guide", representations=(StructureRepresentation(root=GUIDE),)
            )
            await engine.ingest([guide], scope=scope)
            # Structure channel only; candidates carry the node payloads.
            config = RetrievalExecutionConfig(index=False, include_candidates=True)
            for query in ("restore the backup", "installation on Linux or Windows"):
                result = await engine.retrieve(query, scope=scope, top_k=2, config=config)
                payloads = {c.object_id: c.payload for c in result.candidates or ()}
                print(f"query: {query!r}")
                for rank, item in enumerate(result.evidence, start=1):
                    # Related nodes (parent/child, same section) are merged into one item.
                    relations = sorted({r.type.value for r in item.relations})
                    print(f"  #{rank} score={item.score:.4f} relations={relations}")
                    for segment in item.segments:
                        path = " > ".join(segment.sources[0].section_path)
                        print(f"     {path}: {segment.text}")
                    # Navigate from the best-scoring node of this evidence item. PATH lists
                    # node ids root first, aligned with the node's section path.
                    hit = item.provenance.hits[0]
                    oid = hit.object_ref.object_id
                    section_path = next(
                        s.sources[0].section_path for s in item.segments if s.object_id == oid
                    )
                    path_ids = await navigate(structure, scope, hit, NavigationDirection.PATH)
                    title = dict(zip(path_ids, section_path, strict=True))
                    parent = await navigate(structure, scope, hit, NavigationDirection.PARENT)
                    children = await navigate(structure, scope, hit, NavigationDirection.CHILDREN)
                    siblings = await navigate(structure, scope, hit, NavigationDirection.SIBLINGS)
                    print(
                        f"     top node {section_path[-1]!r}: level={payloads[oid]['level']} "
                        f"parent={[title[p] for p in parent]} children={len(children)} "
                        f"siblings={len(siblings)}"
                    )
        finally:
            # Close the application-owned engine while the RetrievalEngine (and the
            # offloader it runs on) is still open.
            await structure.close()


if __name__ == "__main__":
    asyncio.run(main())
