"""TreeSearch structure engine paths the contract kit does not reach."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pytest

from retrievalloom.contracts.indexes import GenerationSpec, IndexItem, NavigationDirection
from retrievalloom.domain.filters import Eq
from retrievalloom.domain.hashing import sha256_digest, stable_id
from retrievalloom.domain.inputs import Scope
from retrievalloom.domain.records import ObjectKind
from retrievalloom.domain.reports import HealthStatus
from retrievalloom.errors import (
    BackendError,
    ComponentContractError,
    GenerationError,
    LifecycleError,
)

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Sequence
    from pathlib import Path

    from retrievalloom.contracts.component import ComponentContext
    from retrievalloom.domain.retrieval import RetrievalHit
    from retrievalloom.runtime.offloader import Offloader
    from retrievalloom.structure.treesearch_engine import TreeSearchStructureEngine

pytestmark = pytest.mark.structure

SCOPE = Scope(tenant_id="tenant-a")
NAMES: dict[str, str] = {}


def oid(name: str) -> str:
    identifier = stable_id("obj", name)
    NAMES[identifier] = name
    return identifier


def names(ids: Sequence[str]) -> list[str]:
    return [NAMES[identifier] for identifier in ids]


def hit_names(hits: Sequence[RetrievalHit]) -> list[str]:
    return names([hit.object_ref.object_id for hit in hits])


def spec(generation_id: str = "gen") -> GenerationSpec:
    return GenerationSpec(
        generation_id=generation_id, method="test", dependency_fingerprint=sha256_digest("test")
    )


def node(
    name: str,
    text: str,
    *,
    parent: str | None = None,
    ordinal: int = 0,
    document: str | None = "doc",
    metadata: dict[str, Any] | None = None,
    kind: str = ObjectKind.STRUCTURE_NODE,
) -> IndexItem:
    payload: dict[str, Any] = {"title": name, "ordinal": ordinal}
    if parent is not None:
        payload["parent_object_id"] = oid(parent)
    return IndexItem(
        object_id=oid(name),
        document_uid=document,
        version=1 if document is not None else None,
        kind=kind,
        text=text,
        metadata=metadata or {},
        payload=payload,
    )


BOOK = (
    node("root", "guide to python programming", metadata={"lang": "en"}),
    node("ch1", "asyncio event loops", parent="root", ordinal=1, metadata={"lang": "en"}),
    node("ch2", "packaging wheels", parent="root", ordinal=2, metadata={"lang": "de"}),
    node("sec", "asyncio tasks and futures", parent="ch1", ordinal=1, metadata={"lang": "de"}),
)


def make_engine(directory: Path | None = None) -> TreeSearchStructureEngine:
    from retrievalloom.structure.treesearch_engine import TreeSearchStructureEngine

    return TreeSearchStructureEngine(directory)


@pytest.fixture
async def engine(context: ComponentContext) -> AsyncIterator[TreeSearchStructureEngine]:
    store = make_engine()
    await store.initialize(context)
    await store.create_generation(SCOPE, spec())
    await store.write(SCOPE, "gen", BOOK)
    yield store
    await store.close()


def test_tree_rejects_parent_cycles() -> None:
    from retrievalloom.structure.treesearch_engine import _tree

    items = [
        node("top", "top"),
        node("a", "a", parent="b"),
        node("b", "b", parent="a"),
    ]
    with pytest.raises(ComponentContractError, match="cycle") as caught:
        _tree(items)
    assert sorted(caught.value.details["nodes"]) == sorted([oid("a"), oid("b")])


def test_tree_treats_self_parent_and_unknown_parent_as_roots() -> None:
    from retrievalloom.structure.treesearch_engine import _tree

    tree = _tree([node("solo", "x", parent="solo", ordinal=2), node("orphan", "y", parent="gone")])
    assert names([entry["node_id"] for entry in tree]) == ["orphan", "solo"]


async def test_lifecycle_before_initialize(context: ComponentContext) -> None:
    store = make_engine()
    report = await store.health()
    assert report.message == "not initialised"
    with pytest.raises(LifecycleError):
        await store.create_generation(SCOPE, spec())
    await store.close()  # closing an unopened engine is a no-op
    await store.initialize(context)
    await store.initialize(context)  # idempotent
    assert (await store.health()).status is HealthStatus.HEALTHY
    await store.close()
    await store.close()


async def test_close_after_the_offloader_stopped(
    context: ComponentContext, offloader: Offloader
) -> None:
    store = make_engine()
    await store.initialize(context)
    await store.create_generation(SCOPE, spec())
    await store.write(SCOPE, "gen", BOOK)
    await offloader.close()
    # App-owned components may be closed after the engine (and its offloader) closed.
    await store.close()
    assert (await store.health()).message == "not initialised"


async def test_write_validation(engine: TreeSearchStructureEngine) -> None:
    with pytest.raises(ComponentContractError, match="structure nodes"):
        await engine.write(SCOPE, "gen", [node("chunk", "x", kind=ObjectKind.CHUNK)])
    with pytest.raises(GenerationError):
        await engine.write(SCOPE, "missing", [node("x", "x")])
    with pytest.raises(ComponentContractError, match="cycle"):
        await engine.write(
            SCOPE,
            "gen",
            [node("p", "p", parent="q", document="d"), node("q", "q", parent="p", document="d")],
        )


async def test_missing_generation_behaviour(engine: TreeSearchStructureEngine) -> None:
    assert await engine.remove(SCOPE, "missing", [oid("root")]) == 0
    for call in (
        engine.update_metadata(SCOPE, "missing", {}),
        engine.seal(SCOPE, "missing"),
        engine.search(SCOPE, "missing", "asyncio", 5, None),
        engine.navigate(SCOPE, "missing", oid("root"), NavigationDirection.PARENT),
    ):
        with pytest.raises(GenerationError):
            await call


async def test_remove_lifts_children_and_deletes_empty_documents(
    engine: TreeSearchStructureEngine,
) -> None:
    assert await engine.remove(SCOPE, "gen", [oid("ch1"), oid("unknown")]) == 1
    children = await engine.navigate(SCOPE, "gen", oid("root"), NavigationDirection.CHILDREN)
    assert names(children) == ["sec", "ch2"]
    assert (await engine.seal(SCOPE, "gen")).object_count == 3

    removed = await engine.remove(SCOPE, "gen", [oid("root"), oid("sec"), oid("ch2")])
    assert removed == 3
    stats = await engine.seal(SCOPE, "gen")
    assert (stats.object_count, stats.details) == (0, {"documents": 0})
    assert await engine.search(SCOPE, "gen", "asyncio", 5, None) == []


async def test_update_metadata_changes_filter_results(engine: TreeSearchStructureEngine) -> None:
    german = Eq(field="lang", value="de")
    assert hit_names(await engine.search(SCOPE, "gen", "asyncio", 5, german)) == ["sec"]
    updated = await engine.update_metadata(
        SCOPE, "gen", {oid("ch1"): {"lang": "de"}, oid("unknown"): {"lang": "de"}}
    )
    assert updated == 1
    assert set(hit_names(await engine.search(SCOPE, "gen", "asyncio", 5, german))) == {
        "ch1",
        "sec",
    }


async def test_search_limits_and_blank_queries(engine: TreeSearchStructureEngine) -> None:
    assert await engine.search(SCOPE, "gen", "asyncio", 0, None) == []
    assert await engine.search(SCOPE, "gen", "   ", 5, None) == []
    hits = await engine.search(SCOPE, "gen", "asyncio", 1, None)
    assert len(hits) == 1
    assert hits[0].rank == 1
    assert hits[0].matched[0].kind == "structure_node"


async def test_search_skips_stale_index_rows(
    engine: TreeSearchStructureEngine, monkeypatch: pytest.MonkeyPatch
) -> None:
    generation = engine._generations[(SCOPE.key, "gen")]
    original = generation.index.search

    def with_stale(query: str, top_k: int) -> list[dict[str, Any]]:
        return [{"node_id": oid("ghost")}, *original(query, top_k=top_k)]

    monkeypatch.setattr(generation.index, "search", with_stale)
    hits = await engine.search(SCOPE, "gen", "packaging", 5, None)
    assert hit_names(hits) == ["ch2"]


async def test_navigation(engine: TreeSearchStructureEngine) -> None:
    parent = NavigationDirection.PARENT
    assert await engine.navigate(SCOPE, "gen", oid("root"), parent) == []
    assert names(await engine.navigate(SCOPE, "gen", oid("sec"), parent)) == ["ch1"]
    assert await engine.navigate(SCOPE, "gen", oid("ghost"), parent) == []
    siblings = await engine.navigate(SCOPE, "gen", oid("ch1"), NavigationDirection.SIBLINGS)
    assert names(siblings) == ["ch2"]


async def test_items_without_document_are_their_own_document(
    engine: TreeSearchStructureEngine,
) -> None:
    await engine.write(SCOPE, "gen", [node("loose", "standalone note", document=None)])
    stats = await engine.seal(SCOPE, "gen")
    assert stats.details == {"documents": 2}
    assert hit_names(await engine.search(SCOPE, "gen", "standalone", 5, None)) == ["loose"]


async def test_backend_errors_are_wrapped(
    engine: TreeSearchStructureEngine, monkeypatch: pytest.MonkeyPatch
) -> None:
    generation = engine._generations[(SCOPE.key, "gen")]

    def broken(query: str, top_k: int) -> list[dict[str, Any]]:
        raise RuntimeError("disk on fire")

    monkeypatch.setattr(generation.index, "search", broken)
    with pytest.raises(BackendError) as caught:
        await engine.search(SCOPE, "gen", "asyncio", 5, None)
    assert caught.value.details == {"error": "disk on fire"}


async def test_on_disk_generations_survive_reopen_and_drop(
    context: ComponentContext, tmp_path: Path
) -> None:
    directory = tmp_path / "structure"
    first = make_engine(directory)
    assert first.capabilities.durable
    await first.initialize(context)
    assert await first.list_generations(SCOPE) == []
    await first.create_generation(SCOPE, spec())
    await first.write(SCOPE, "gen", BOOK)
    await first.close()

    second = make_engine(directory)
    await second.initialize(context)
    try:
        assert await second.list_generations(SCOPE) == ["gen"]
        with pytest.raises(GenerationError):
            await second.seal(SCOPE, "never")
        stats = await second.seal(SCOPE, "gen")
        assert (stats.object_count, stats.details) == (4, {"documents": 1})
        assert "ch1" in hit_names(await second.search(SCOPE, "gen", "asyncio", 5, None))

        await second.drop_generation(SCOPE, "gen")
        await second.drop_generation(SCOPE, "gen")  # idempotent
        assert await second.list_generations(SCOPE) == []
        assert not list((directory / SCOPE.key).glob("gen.db*"))
    finally:
        await second.close()


async def test_in_memory_drop_forgets_generation(engine: TreeSearchStructureEngine) -> None:
    assert not engine.capabilities.durable
    assert await engine.list_generations(SCOPE) == ["gen"]
    await engine.drop_generation(SCOPE, "gen")
    assert await engine.list_generations(SCOPE) == []
    with pytest.raises(GenerationError):
        await engine.seal(SCOPE, "gen")
