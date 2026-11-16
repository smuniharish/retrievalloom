"""Graph normaliser, neighbourhood retriever and the LadybugDB store."""

from __future__ import annotations

import asyncio
import threading
from typing import TYPE_CHECKING, Any

import pytest

from retrievalloom.contracts.component import make_info
from retrievalloom.contracts.indexes import (
    GenerationSpec,
    GraphNode,
    GraphStoreABC,
    GraphSubgraph,
    IndexItem,
)
from retrievalloom.contracts.models import GraphExtraction
from retrievalloom.domain.hashing import sha256_digest, stable_id
from retrievalloom.domain.inputs import GraphEntityInput, GraphRelationshipInput, Scope
from retrievalloom.domain.records import Channel, ObjectKind
from retrievalloom.domain.reports import HealthStatus
from retrievalloom.domain.retrieval import (
    MatchDetail,
    MethodRef,
    ObjectRef,
    RetrievalHit,
    ScoreRange,
    ScoreSemantics,
)
from retrievalloom.errors import (
    BackendError,
    ComponentContractError,
    ConfigurationError,
    LifecycleError,
)
from retrievalloom.graph.normalizer import IdentityGraphNormalizer, canonical_key
from retrievalloom.graph.retriever import NeighborhoodGraphRetriever

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Sequence
    from pathlib import Path

    from retrievalloom.contracts.component import ComponentContext
    from retrievalloom.graph.ladybug_store import LadybugGraphStore

SCOPE = Scope(tenant_id="tenant-a")
OTHER = Scope(tenant_id="tenant-b")
NAMES: dict[str, str] = {}
ENTITY_METHOD = MethodRef(channel=Channel.GRAPH, method="graph.entity")


def oid(name: str) -> str:
    identifier = stable_id("obj", name)
    NAMES[identifier] = name
    return identifier


def label(object_id: str) -> str:
    return NAMES[object_id]


def entity(name: str, *aliases: str, display: str | None = None) -> IndexItem:
    return IndexItem(
        object_id=oid(name),
        kind=ObjectKind.GRAPH_ENTITY,
        text=display or name,
        payload={"name": display, "aliases": list(aliases)} if display else {},
    )


def relation(name: str, source: str, target: str) -> IndexItem:
    return IndexItem(
        object_id=oid(name),
        kind=ObjectKind.GRAPH_RELATIONSHIP,
        text=name,
        payload={"source_entity_id": oid(source), "target_entity_id": oid(target)},
    )


# normaliser --------------------------------------------------------------------------------
def test_canonical_key_folds_and_collapses_whitespace() -> None:
    assert canonical_key("  New\t\nYORK  City ") == "new york city"
    assert canonical_key("Stra\u00dfe") == "strasse"


async def test_entities_are_keyed_and_duplicates_merged() -> None:
    extraction = GraphExtraction(
        entities=(
            GraphEntityInput(name="Python", aliases=("CPython", "python ", "Py")),
            GraphEntityInput(
                key=" PYTHON", name="Python language", aliases=("py", "PyLang"), description="x"
            ),
            GraphEntityInput(name="Python", entity_type="snake"),
            GraphEntityInput(name="   "),
        )
    )
    graph = await IdentityGraphNormalizer().normalize(extraction, SCOPE)
    assert [(item.entity_type, item.canonical_key) for item in graph.entities] == [
        ("entity", "python"),
        ("snake", "python"),
    ]
    language = graph.entities[0]
    # The first occurrence's name/description win; aliases are unioned by canonical form.
    assert language.name == "Python"
    assert language.description is None
    assert language.aliases == ("CPython", "Py", "Python language", "PyLang")
    assert language.source_keys == ("Python", " PYTHON")


async def test_relationships_resolve_endpoints_and_merge_duplicates() -> None:
    extraction = GraphExtraction(
        entities=(
            GraphEntityInput(key="py", name="Python", entity_type="language"),
            GraphEntityInput(name="Asyncio", entity_type="library"),
        ),
        relationships=(
            GraphRelationshipInput(source_key="Asyncio", target_key="py", relation_type="part_of"),
            # Same relationship through canonical forms: merged, first description wins.
            GraphRelationshipInput(
                source_key=" ASYNCIO ", target_key="PY", relation_type="part_of", description="dup"
            ),
            # Unknown endpoints become default entities, reused by later relationships.
            GraphRelationshipInput(source_key="Guido", target_key="py", relation_type="created"),
            GraphRelationshipInput(source_key="Guido", target_key="Asyncio", relation_type="liked"),
            # Blank unknown endpoints drop the relationship.
            GraphRelationshipInput(source_key="py", target_key="  ", relation_type="broken"),
        ),
    )
    graph = await IdentityGraphNormalizer().normalize(extraction, SCOPE)
    assert [(e.entity_type, e.canonical_key, e.name) for e in graph.entities] == [
        ("entity", "guido", "Guido"),
        ("language", "py", "Python"),
        ("library", "asyncio", "Asyncio"),
    ]
    summary = [
        (r.source_key, r.relation_type, r.target_key, r.description) for r in graph.relationships
    ]
    assert summary == [
        ("guido", "created", "py", None),
        ("guido", "liked", "asyncio", None),
        ("asyncio", "part_of", "py", None),
    ]


async def test_undirected_relationships_are_ordered_and_deduplicated() -> None:
    extraction = GraphExtraction(
        relationships=(
            GraphRelationshipInput(
                source_key="Zed", target_key="Amy", relation_type="knows", directed=False
            ),
            GraphRelationshipInput(
                source_key="Amy", target_key="Zed", relation_type="knows", directed=False
            ),
            GraphRelationshipInput(
                source_key="Amy", target_key="Zed", relation_type="knows", qualifier="since 2020"
            ),
            GraphRelationshipInput(source_key="Zed", target_key="Amy", relation_type="knows"),
        )
    )
    graph = await IdentityGraphNormalizer().normalize(extraction, SCOPE)
    assert [(r.source_key, r.target_key, r.directed, r.qualifier) for r in graph.relationships] == [
        ("amy", "zed", False, None),
        ("amy", "zed", True, "since 2020"),
        ("zed", "amy", True, None),
    ]
    assert IdentityGraphNormalizer().info == IdentityGraphNormalizer().info


# retriever with a scripted store -----------------------------------------------------------
class ScriptedGraphStore(GraphStoreABC):
    """Returns fixed seeds and a fixed neighbourhood; records calls."""

    def __init__(self, seeds: Sequence[str], subgraph: GraphSubgraph) -> None:
        self.seeds = list(seeds)
        self.subgraph = subgraph
        self.calls: list[tuple[str, Any]] = []
        self._info = make_info(component="graph_store", implementation="scripted", version="1")

    @property
    def info(self) -> Any:
        return self._info

    @property
    def capabilities(self) -> Any:
        raise NotImplementedError

    async def find_entities(
        self, scope: Scope, generation_id: str, terms: Sequence[str], limit: int
    ) -> Sequence[RetrievalHit]:
        self.calls.append(("find", (list(terms), limit)))
        return [
            RetrievalHit(
                object_ref=ObjectRef(
                    scope_key=scope.key, object_id=object_id, generation_id=generation_id
                ),
                method=ENTITY_METHOD,
                backend_id="scripted",
                raw_score=1.0,
                score_semantics=ScoreSemantics(higher_is_better=True, range=ScoreRange.UNBOUNDED),
                rank=rank,
                matched=(MatchDetail(kind="entity_name", term="seed"),),
            )
            for rank, object_id in enumerate(self.seeds[:limit], start=1)
        ]

    async def neighborhood(
        self, scope: Scope, generation_id: str, seeds: Sequence[str], depth: int, limit: int
    ) -> GraphSubgraph:
        self.calls.append(("hood", (list(seeds), depth, limit)))
        return self.subgraph

    async def create_generation(self, scope: Scope, spec: GenerationSpec) -> None:
        raise NotImplementedError

    async def write(self, scope: Scope, generation_id: str, items: Sequence[IndexItem]) -> Any:
        raise NotImplementedError

    async def remove(self, scope: Scope, generation_id: str, object_ids: Sequence[str]) -> int:
        raise NotImplementedError

    async def update_metadata(self, scope: Scope, generation_id: str, updates: Any) -> int:
        raise NotImplementedError

    async def seal(self, scope: Scope, generation_id: str) -> Any:
        raise NotImplementedError

    async def drop_generation(self, scope: Scope, generation_id: str) -> None:
        raise NotImplementedError

    async def list_generations(self, scope: Scope) -> Sequence[str]:
        raise NotImplementedError


@pytest.mark.parametrize("kwargs", [{"depth": 0}, {"max_ngram": 0}, {"seed_limit": 0}])
def test_retriever_rejects_invalid_configuration(kwargs: dict[str, int]) -> None:
    with pytest.raises(ConfigurationError):
        NeighborhoodGraphRetriever(**kwargs)


def test_retriever_terms_are_ngrams_longest_first() -> None:
    retriever = NeighborhoodGraphRetriever(max_ngram=2)
    assert retriever.info.implementation == "neighborhood"
    assert retriever.info != NeighborhoodGraphRetriever(max_ngram=3).info
    assert retriever.terms("New York new") == ["new york", "york new", "new", "york"]
    assert NeighborhoodGraphRetriever(max_ngram=5).terms("a b") == ["a b", "a", "b"]
    assert retriever.terms("  ?! ") == []


async def test_retriever_short_circuits() -> None:
    store = ScriptedGraphStore([], GraphSubgraph())
    retriever = NeighborhoodGraphRetriever()
    assert await retriever.retrieve(SCOPE, "gen", "!!", store, 10) == []
    assert await retriever.retrieve(SCOPE, "gen", "python", store, 0) == []
    assert store.calls == []
    assert await retriever.retrieve(SCOPE, "gen", "python", store, 10) == []
    assert [call[0] for call in store.calls] == ["find"]


async def test_retriever_ranks_seeds_then_neighbours_and_skips_duplicates() -> None:
    seed, near, rel, far = oid("seed"), oid("near"), oid("rel"), oid("far")
    subgraph = GraphSubgraph(
        nodes=(
            GraphNode(object_id=far, distance=2, path=(seed, rel, near, rel, far)),
            GraphNode(object_id=near, distance=1, path=(seed, rel, near)),
            GraphNode(object_id=seed, distance=1, path=(seed,)),
        ),
        relationships=(GraphNode(object_id=rel, distance=1, path=(seed, rel)),),
    )
    store = ScriptedGraphStore([seed], subgraph)
    retriever = NeighborhoodGraphRetriever(depth=2, seed_limit=3)
    hits = await retriever.retrieve(SCOPE, "gen", "Seed near", store, 3)
    assert store.calls == [
        ("find", (["seed near", "seed", "near"], 3)),
        ("hood", ([seed], 2, 3)),
    ]
    assert label(hits[0].object_ref.object_id) == "seed"
    assert {label(hit.object_ref.object_id) for hit in hits[1:]} == {"near", "rel"}
    assert [hit.raw_score for hit in hits] == [1.0, 0.5, 0.5]
    assert hits[0].matched[0].kind == "entity_name"
    near_hit = next(hit for hit in hits if hit.object_ref.object_id == near)
    assert near_hit.path_refs == (seed, rel, near)
    assert near_hit.matched[0].details == {"distance": 1}
    assert {hit.backend_id for hit in hits} == {"scripted"}


# LadybugDB store ---------------------------------------------------------------------------
def spec(generation_id: str = "gen") -> GenerationSpec:
    return GenerationSpec(
        generation_id=generation_id, method="graph", dependency_fingerprint=sha256_digest("g")
    )


@pytest.fixture
async def graph(context: ComponentContext) -> AsyncIterator[LadybugGraphStore]:
    from retrievalloom.graph.ladybug_store import LadybugGraphStore

    store = LadybugGraphStore()
    await store.initialize(context)
    try:
        yield store
    finally:
        await store.close()


@pytest.mark.graph
def test_ladybug_configuration() -> None:
    from retrievalloom.graph.ladybug_store import LadybugGraphStore

    with pytest.raises(ConfigurationError):
        LadybugGraphStore(buffer_pool_size=-1)
    assert not LadybugGraphStore().capabilities.durable
    assert LadybugGraphStore("graph.lbug").capabilities.durable


@pytest.mark.graph
async def test_ladybug_lifecycle_errors(context: ComponentContext, tmp_path: Path) -> None:
    from retrievalloom.graph.ladybug_store import LadybugGraphStore

    store = LadybugGraphStore()
    with pytest.raises(LifecycleError, match="not initialised"):
        await store.seal(SCOPE, "gen")
    assert (await store.health()).status is HealthStatus.UNHEALTHY
    await store.close()

    broken = LadybugGraphStore(tmp_path)  # a directory is not a database file
    with pytest.raises(BackendError, match="ladybug query failed"):
        await broken.initialize(context)
    report = await broken.health()
    assert report.status is HealthStatus.UNHEALTHY
    assert "not initialised" in (report.message or "")  # fully released
    await broken.close()
    await broken.close()


@pytest.mark.graph
async def test_ladybug_failed_open_releases_the_database(
    context: ComponentContext, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from retrievalloom.graph import ladybug_store

    path = tmp_path / "db.lbug"
    store = ladybug_store.LadybugGraphStore(path)
    with monkeypatch.context() as patch:
        patch.setattr(ladybug_store, "_SCHEMA", ("NOT A STATEMENT",))
        with pytest.raises(BackendError):
            await store.initialize(context)
    assert store._database is None
    original = ladybug_store.ladybug.Connection

    def refused(_database: Any) -> Any:
        raise RuntimeError("no connection")

    with monkeypatch.context() as patch:
        patch.setattr(ladybug_store.ladybug, "Connection", refused)
        with pytest.raises(BackendError, match="ladybug query failed"):
            await store.initialize(context)
    assert store._database is None
    assert ladybug_store.ladybug.Connection is original
    await store.initialize(context)  # the file is not left locked
    assert (await store.health()).status is HealthStatus.HEALTHY
    await store.close()


@pytest.mark.graph
async def test_ladybug_cancelled_initialize_closes_the_late_open(
    context: ComponentContext, monkeypatch: pytest.MonkeyPatch
) -> None:
    from retrievalloom.graph import ladybug_store

    started, gate = threading.Event(), threading.Event()
    original = ladybug_store.ladybug.Database

    def slow(*args: Any, **kwargs: Any) -> Any:
        started.set()
        gate.wait(5)
        return original(*args, **kwargs)

    monkeypatch.setattr(ladybug_store.ladybug, "Database", slow)
    store = ladybug_store.LadybugGraphStore()
    task = asyncio.create_task(store.initialize(context))
    assert await asyncio.to_thread(started.wait, 5)
    task.cancel()
    await asyncio.sleep(0.05)
    gate.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert store._database is None
    assert store._connection is None
    assert (await store.health()).status is HealthStatus.UNHEALTHY


@pytest.mark.graph
async def test_ladybug_backend_failures_are_wrapped(
    graph: LadybugGraphStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    from retrievalloom.graph import ladybug_store

    def failing(*_: Any) -> list[list[Any]]:
        raise RuntimeError("disk on fire")

    monkeypatch.setattr(ladybug_store, "_rows", failing)
    report = await graph.health()
    assert report.status is HealthStatus.UNHEALTHY
    assert "ladybug query failed" in (report.message or "")
    with pytest.raises(BackendError) as caught:
        await graph.list_generations(SCOPE)
    assert caught.value.details == {"error": "disk on fire"}


@pytest.mark.graph
async def test_ladybug_write_is_atomic_and_validated(graph: LadybugGraphStore) -> None:
    await graph.create_generation(SCOPE, spec())
    assert (await graph.write(SCOPE, "gen", [])).written == 0
    with pytest.raises(ComponentContractError, match="endpoints") as caught:
        await graph.write(SCOPE, "gen", [entity("a"), relation("r", "a", "missing")])
    assert caught.value.details == {"object_id": oid("r")}
    # The whole batch was rolled back, including the valid entity.
    assert (await graph.seal(SCOPE, "gen")).object_count == 0
    chunk = IndexItem(object_id=oid("chunk"), kind=ObjectKind.CHUNK, text="text")
    with pytest.raises(ComponentContractError, match="graph entities or relationships"):
        await graph.write(SCOPE, "gen", [chunk])
    assert await graph.remove(SCOPE, "gen", []) == 0


@pytest.mark.graph
async def test_ladybug_entities_use_text_when_name_missing(graph: LadybugGraphStore) -> None:
    await graph.write(
        SCOPE, "gen", [entity("py", "CPython", display="Python"), entity("Rust Lang")]
    )
    hits = await graph.find_entities(SCOPE, "gen", ["  ", "rust   LANG", "cpython", "python"], 5)
    assert [label(hit.object_ref.object_id) for hit in hits] == ["py", "Rust Lang"]
    assert [hit.raw_score for hit in hits] == [2.0, 1.0]
    assert [detail.term for detail in hits[0].matched] == ["cpython", "python"]
    assert await graph.find_entities(SCOPE, "gen", ["  "], 5) == []
    assert await graph.find_entities(SCOPE, "gen", ["python"], 0) == []
    stats = await graph.seal(SCOPE, "gen")
    assert stats.object_count == 2
    assert stats.details == {"entity": 2}


@pytest.mark.graph
async def test_ladybug_update_metadata(graph: LadybugGraphStore) -> None:
    await graph.write(SCOPE, "gen", [entity("a"), entity("b")])
    updated = await graph.update_metadata(
        SCOPE, "gen", {oid("a"): {"v": 1}, oid("missing"): {"v": 1}}
    )
    assert updated == 1
    assert await graph.update_metadata(OTHER, "gen", {oid("a"): {"v": 2}}) == 0


@pytest.mark.graph
async def test_ladybug_neighborhood_paths_and_limits(graph: LadybugGraphStore) -> None:
    # a -r1-> c <-r2- b ; c -r3-> d
    await graph.write(
        SCOPE,
        "gen",
        [
            relation("r1", "a", "c"),
            relation("r2", "b", "c"),
            relation("r3", "c", "d"),
            entity("a"),
            entity("b"),
            entity("c"),
            entity("d"),
        ],
    )
    with pytest.raises(ConfigurationError):
        await graph.neighborhood(SCOPE, "gen", [oid("a")], 0, 5)
    with pytest.raises(ConfigurationError):
        await graph.neighborhood(SCOPE, "gen", [oid("a")], 6, 5)
    assert await graph.neighborhood(SCOPE, "gen", [], 1, 5) == GraphSubgraph()
    assert await graph.neighborhood(SCOPE, "gen", [oid("a")], 1, 0) == GraphSubgraph()

    both = await graph.neighborhood(SCOPE, "gen", [oid("a"), oid("b")], 2, 10)
    distances = {label(node.object_id): node.distance for node in both.nodes}
    assert distances == {"c": 1, "d": 2}
    # c is reached from both seeds at the same length: the smallest path wins.
    c_node = next(node for node in both.nodes if label(node.object_id) == "c")
    assert c_node.path == min((oid("a"), oid("r1"), oid("c")), (oid("b"), oid("r2"), oid("c")))
    assert {label(node.object_id) for node in both.relationships} == {"r1", "r2", "r3"}

    limited = await graph.neighborhood(SCOPE, "gen", [oid("a")], 2, 1)
    assert [label(node.object_id) for node in limited.nodes] == ["c"]
    assert len(limited.relationships) == 1


@pytest.mark.graph
async def test_ladybug_relationship_rewrite_moves_links(graph: LadybugGraphStore) -> None:
    await graph.write(
        SCOPE, "gen", [entity("a"), entity("b"), entity("c"), relation("r", "a", "b")]
    )
    await graph.write(SCOPE, "gen", [relation("r", "a", "c")])
    hood = await graph.neighborhood(SCOPE, "gen", [oid("a")], 1, 10)
    assert [label(node.object_id) for node in hood.nodes] == ["c"]
    stats = await graph.seal(SCOPE, "gen")
    assert stats.details == {"entity": 3, "relationship": 1}


@pytest.mark.graph
async def test_ladybug_file_store_survives_reopen(
    context: ComponentContext, tmp_path: Path
) -> None:
    from retrievalloom.graph.ladybug_store import LadybugGraphStore

    path = tmp_path / "nested" / "graph.lbug"
    first = LadybugGraphStore(path)
    await first.initialize(context)
    await first.write(SCOPE, "gen", [entity("py", display="Python")])
    await first.close()
    second = LadybugGraphStore(path)
    await second.initialize(context)
    try:
        hits = await second.find_entities(SCOPE, "gen", ["python"], 5)
        assert [label(hit.object_ref.object_id) for hit in hits] == ["py"]
        assert await second.list_generations(SCOPE) == ["gen"]
    finally:
        await second.close()


@pytest.mark.graph
async def test_retriever_over_ladybug(graph: LadybugGraphStore) -> None:
    await graph.write(
        SCOPE,
        "gen",
        [
            entity("py", display="Python"),
            entity("aio", display="asyncio"),
            entity("ev", display="Event Loop"),
            relation("part", "aio", "py"),
            relation("has", "aio", "ev"),
        ],
    )
    retriever = NeighborhoodGraphRetriever(depth=2)
    hits = await retriever.retrieve(SCOPE, "gen", "the event loop in python", graph, 10)
    labels = [label(hit.object_ref.object_id) for hit in hits]
    # Seeds (named in the query) come first, then their neighbourhood.
    assert set(labels[:2]) == {"ev", "py"}
    assert set(labels) == {"py", "ev", "aio", "part", "has"}
    assert all(hit.backend_id == "ladybug" for hit in hits)
    assert await retriever.retrieve(OTHER, "gen", "python", graph, 10) == []
