"""QdrantVectorStore: modes, filter translation, error wrapping and generation edges."""

from __future__ import annotations

import math
from typing import TYPE_CHECKING, Any

import httpx
import pytest
from qdrant_client import AsyncQdrantClient, models
from qdrant_client.http.exceptions import ResponseHandlingException, UnexpectedResponse

from retrievalloom.contracts.indexes import GenerationSpec, IndexItem
from retrievalloom.domain.filters import And, Eq, Exists, In, Not, Or, Range
from retrievalloom.domain.hashing import sha256_digest, stable_id
from retrievalloom.domain.inputs import Scope
from retrievalloom.domain.records import ObjectKind
from retrievalloom.domain.reports import HealthStatus
from retrievalloom.domain.retrieval import ScoreRange
from retrievalloom.errors import (
    BackendError,
    BackendTimeoutError,
    BackendUnavailableError,
    ComponentContractError,
    ConfigurationError,
    GenerationError,
    LifecycleError,
)
from retrievalloom.index.vector import QdrantVectorStore, translate_filter

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Sequence
    from pathlib import Path

    from retrievalloom.contracts.component import ComponentContext
    from retrievalloom.domain.filters import FilterExpression
    from retrievalloom.domain.retrieval import RetrievalHit
    from retrievalloom.runtime.offloader import Offloader

SCOPE = Scope(tenant_id="tenant-a")
OTHER = Scope(tenant_id="tenant-b")
NAMES: dict[str, str] = {}
LOCAL_PAYLOAD_INDEX = pytest.mark.filterwarnings(
    "ignore:Payload indexes have no effect in the local Qdrant:UserWarning"
)


def oid(name: str) -> str:
    identifier = stable_id("obj", name)
    NAMES[identifier] = name
    return identifier


def names(hits: Sequence[RetrievalHit]) -> list[str]:
    return [NAMES[hit.object_ref.object_id] for hit in hits]


def spec(generation_id: str = "gen", dimensions: int | None = 2) -> GenerationSpec:
    return GenerationSpec(
        generation_id=generation_id,
        method="dense",
        dependency_fingerprint=sha256_digest("test"),
        dimensions=dimensions,
    )


def item(
    name: str, vector: tuple[float, ...] | None, metadata: dict[str, Any] | None = None
) -> IndexItem:
    return IndexItem(
        object_id=oid(name),
        kind=ObjectKind.CHUNK,
        text=name,
        metadata=metadata or {},
        vector=vector,
    )


@pytest.fixture
async def store(context: ComponentContext) -> AsyncIterator[QdrantVectorStore]:
    instance = QdrantVectorStore()
    await instance.initialize(context)
    try:
        yield instance
    finally:
        await instance.close()


@pytest.fixture
async def client() -> AsyncIterator[AsyncQdrantClient]:
    instance = AsyncQdrantClient(location=":memory:")
    try:
        yield instance
    finally:
        await instance.close()


def unexpected(status: int) -> UnexpectedResponse:
    return UnexpectedResponse(status, "error", b"{}", httpx.Headers())


# filter translation ------------------------------------------------------------------------
def test_translate_filter_exact_predicates() -> None:
    assert translate_filter(None) == (None, True)
    condition, exact = translate_filter(Eq(field="lang", value="en"))
    assert exact
    assert condition == models.FieldCondition(key="meta.lang", match=models.MatchValue(value="en"))
    condition, exact = translate_filter(In(field="n", values=(1, 2)))
    assert exact
    assert condition == models.FieldCondition(key="meta.n", match=models.MatchAny(any=[1, 2]))
    condition, exact = translate_filter(In(field="s", values=("a", "b")))
    assert exact
    assert condition == models.FieldCondition(key="meta.s", match=models.MatchAny(any=["a", "b"]))


@pytest.mark.parametrize(
    "expression",
    [
        Eq(field="x", value=1.5),
        Eq(field="x", value=True),
        In(field="x", values=("a", 1)),
        In(field="x", values=(True,)),
        Range(field="x", gte="2020-01-01"),
        Exists(field="x"),
    ],
)
def test_translate_filter_untranslatable_predicates_are_unconstrained(
    expression: FilterExpression,
) -> None:
    assert translate_filter(expression) == (None, False)


def test_translate_filter_range_is_pushed_down_but_inexact() -> None:
    condition, exact = translate_filter(Range(field="n", gt=1, lte=5.5))
    assert not exact
    assert condition == models.FieldCondition(key="meta.n", range=models.Range(gt=1.0, lte=5.5))


def test_translate_filter_boolean_combinations() -> None:
    eq = Eq(field="a", value="x")
    loose = Exists(field="b")
    condition, exact = translate_filter(And(clauses=(eq, loose)))
    assert not exact
    assert isinstance(condition, models.Filter)
    assert isinstance(condition.must, list)
    assert len(condition.must) == 1
    assert translate_filter(And(clauses=(loose,))) == (None, False)
    assert translate_filter(Or(clauses=(eq, loose))) == (None, False)
    condition, exact = translate_filter(Or(clauses=(eq, Eq(field="a", value="y"))))
    assert exact
    assert isinstance(condition, models.Filter)
    assert isinstance(condition.should, list)
    assert len(condition.should) == 2
    condition, exact = translate_filter(Not(clause=eq))
    assert exact
    assert isinstance(condition, models.Filter)
    assert condition.must_not is not None
    assert translate_filter(Not(clause=loose)) == (None, False)
    # Negating a broader condition would be narrower than the expression: not pushed down.
    assert translate_filter(Not(clause=Range(field="n", gt=1))) == (None, False)


# configuration -----------------------------------------------------------------------------
@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"location": ":memory:", "path": "x"}, "at most one"),
        ({"url": "http://qdrant:6333", "path": "x"}, "at most one"),
        ({"distance": "manhattan"}, "distance"),
        ({"overfetch_factor": 0}, "overfetch"),
        ({"collection_prefix": "bad-prefix"}, "collection_prefix"),
    ],
)
def test_invalid_configuration(kwargs: dict[str, Any], message: str) -> None:
    with pytest.raises(ConfigurationError, match=message):
        QdrantVectorStore(**kwargs)


def test_modes_and_capabilities(tmp_path: Path) -> None:
    memory = QdrantVectorStore()
    assert memory.info.details == {} or memory.info.fingerprint is not None
    assert not memory.capabilities.durable
    assert QdrantVectorStore(path=str(tmp_path)).capabilities.durable
    assert QdrantVectorStore(url="http://qdrant:6333").capabilities.durable
    infos = {
        QdrantVectorStore().info.fingerprint.digest,
        QdrantVectorStore(path=str(tmp_path)).info.fingerprint.digest,
        QdrantVectorStore(url="http://qdrant:6333").info.fingerprint.digest,
        QdrantVectorStore(distance="dot").info.fingerprint.digest,
    }
    assert len(infos) == 4


async def test_uninitialised_store_raises_lifecycle_error() -> None:
    store = QdrantVectorStore()
    with pytest.raises(LifecycleError):
        await store.seal(SCOPE, "gen")
    with pytest.raises(LifecycleError):
        await store.health()
    await store.close()  # closing an unopened store is harmless


# local modes -------------------------------------------------------------------------------
async def test_item_validation(store: QdrantVectorStore) -> None:
    with pytest.raises(ComponentContractError, match="dimensions"):
        await store.create_generation(SCOPE, spec(dimensions=None))
    await store.create_generation(SCOPE, spec())
    bad_items = [
        (item("a", None), "need a vector"),
        (item("a", ()), "need a vector"),
        (item("a", (1.0, 0.0, 0.0)), "dimensions differ"),
        # Pydantic rejects NaN on validated items; the store re-checks unvalidated ones.
        (item("a", (1.0, 1.0)).model_copy(update={"vector": (math.nan, 1.0)}), "non-finite"),
    ]
    for bad, message in bad_items:
        with pytest.raises(ComponentContractError, match=message):
            await store.write(SCOPE, "gen", [bad])
    assert (await store.write(SCOPE, "gen", [])).written == 0


async def test_write_without_create_and_unknown_generations(store: QdrantVectorStore) -> None:
    report = await store.write(SCOPE, "implicit", [item("a", (1.0, 0.0)), item("b", (0.0, 1.0))])
    assert report.written == 2
    assert (await store.seal(SCOPE, "implicit")).object_count == 2
    with pytest.raises(ComponentContractError, match="dimensions differ"):
        await store.write(SCOPE, "implicit", [item("c", (1.0, 0.0, 0.0))])
    with pytest.raises(GenerationError):
        await store.seal(SCOPE, "unknown")
    with pytest.raises(GenerationError):
        await store.search(SCOPE, "unknown", (1.0, 0.0), 5, None)
    assert await store.remove(SCOPE, "unknown", [oid("a")]) == 0
    assert await store.update_metadata(SCOPE, "unknown", {oid("a"): {}}) == 0
    assert await store.remove(SCOPE, "implicit", []) == 0
    assert await store.remove(SCOPE, "implicit", [oid("missing")]) == 0
    assert await store.update_metadata(SCOPE, "implicit", {oid("missing"): {"x": 1}}) == 0
    with pytest.raises(ComponentContractError, match="query vector"):
        await store.search(SCOPE, "implicit", (1.0, 0.0, 0.0), 5, None)


async def test_inexact_filters_overfetch_and_post_filter(context: ComponentContext) -> None:
    store = QdrantVectorStore(overfetch_factor=3)
    await store.initialize(context)
    try:
        await store.create_generation(SCOPE, spec())
        items = [
            item(f"p{index}", (1.0, index / 10), {"tags": [index, 100]}) for index in range(1, 7)
        ]
        await store.write(SCOPE, "gen", items)
        # Qdrant matches arrays by any element (100 >= 50); the evaluator compares the list
        # itself, so every candidate is post-filtered away.
        assert await store.search(SCOPE, "gen", (1.0, 0.0), 2, Range(field="tags", gte=50)) == []
        # Exists is not pushed down: candidates are over-fetched and post-filtered.
        hits = await store.search(SCOPE, "gen", (1.0, 0.0), 2, Exists(field="tags"))
        assert names(hits) == ["p1", "p2"]
        assert [hit.rank for hit in hits] == [1, 2]
    finally:
        await store.close()


async def test_euclid_distance_reports_distance_semantics(context: ComponentContext) -> None:
    store = QdrantVectorStore(distance="euclid", collection_prefix="euclid_store")
    await store.initialize(context)
    try:
        await store.write(SCOPE, "gen", [item("near", (1.0, 0.0)), item("far", (9.0, 9.0))])
        hits = await store.search(SCOPE, "gen", (1.0, 0.1), 2, None)
        assert names(hits) == ["near", "far"]
        near, far = hits[0].raw_score, hits[1].raw_score
        assert near is not None
        assert far is not None
        assert near < far
        assert hits[0].score_semantics.range is ScoreRange.DISTANCE
        assert not hits[0].score_semantics.higher_is_better
    finally:
        await store.close()


async def test_list_generations_pages_through_points(store: QdrantVectorStore) -> None:
    await store.write(SCOPE, "big", [item(f"x{index}", (1.0, index + 1.0)) for index in range(300)])
    await store.write(SCOPE, "small", [item("y", (1.0, 0.0))])
    await store.write(OTHER, "foreign", [item("z", (1.0, 0.0))])
    assert await store.list_generations(SCOPE) == ["big", "small"]
    assert await store.list_generations(OTHER) == ["foreign"]
    await store.drop_generation(SCOPE, "big")
    assert await store.list_generations(SCOPE) == ["small"]


async def test_persistent_path_resolves_generations_after_restart(
    context: ComponentContext, tmp_path: Path
) -> None:
    path = str(tmp_path / "qdrant")
    first = QdrantVectorStore(path=path)
    await first.initialize(context)
    await first.create_generation(SCOPE, spec("g2"))
    await first.write(SCOPE, "g2", [item("a", (1.0, 0.0), {"v": 1})])
    await first.write(SCOPE, "g3", [item("b", (1.0, 0.0, 0.0))])
    await first.close()

    second = QdrantVectorStore(path=path)
    await second.initialize(context)
    try:
        # Mappings are rebuilt by scanning collections (2- and 3-dimensional ones).
        assert (await second.seal(SCOPE, "g3")).object_count == 1
        assert await second.update_metadata(SCOPE, "g2", {oid("a"): {"v": 2}}) == 1
        hits = await second.search(SCOPE, "g2", (1.0, 0.0), 5, Eq(field="v", value=2))
        assert names(hits) == ["a"]
        assert await second.remove(SCOPE, "g3", [oid("b")]) == 1
        # Emptied generations are still listed until dropped.
        assert await second.list_generations(SCOPE) == ["g2", "g3"]
    finally:
        await second.close()


async def test_close_after_the_offloader_stopped(
    context: ComponentContext, offloader: Offloader
) -> None:
    store = QdrantVectorStore()
    await store.initialize(context)
    await store.create_generation(SCOPE, spec("g2"))
    await offloader.close()
    # App-owned components may be closed after the engine (and its offloader) closed.
    await store.close()
    assert store._client is None


# application-owned client (server code path) -----------------------------------------------
@LOCAL_PAYLOAD_INDEX
async def test_application_client_is_used_and_never_closed(
    context: ComponentContext, client: AsyncQdrantClient
) -> None:
    await client.create_collection(
        "retrievalloom_abc", vectors_config=models.VectorParams(size=2, distance="Cosine")
    )
    await client.create_collection(
        "other_2", vectors_config=models.VectorParams(size=2, distance="Cosine")
    )
    # An existing collection (created by another engine) is reused as is.
    await client.create_collection(
        "retrievalloom_2", vectors_config=models.VectorParams(size=2, distance="Cosine")
    )
    store = QdrantVectorStore(client=client)
    await store.initialize(context)
    await store.initialize(context)
    assert store.capabilities.durable
    await store.write(SCOPE, "gen", [item("a", (1.0, 0.0))])
    assert names(await store.search(SCOPE, "gen", (1.0, 0.0), 5, None)) == ["a"]
    # Unrelated collections (foreign prefix, non-numeric suffix) are ignored.
    assert await store.list_generations(SCOPE) == ["gen"]
    assert (await store.health()).status is HealthStatus.HEALTHY
    await store.close()
    assert await client.collection_exists("retrievalloom_2")


@LOCAL_PAYLOAD_INDEX
async def test_collection_creation_race_is_tolerated(
    context: ComponentContext, client: AsyncQdrantClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    create = client.create_collection

    async def concurrent_creator(**kwargs: Any) -> bool:
        await create(**kwargs)
        raise unexpected(409)

    monkeypatch.setattr(client, "create_collection", concurrent_creator)
    store = QdrantVectorStore(client=client)
    await store.initialize(context)
    await store.create_generation(SCOPE, spec())
    assert await client.collection_exists("retrievalloom_2")


async def test_collection_creation_failure_is_raised(
    context: ComponentContext, client: AsyncQdrantClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def failing(**_: Any) -> bool:
        raise unexpected(400)

    monkeypatch.setattr(client, "create_collection", failing)
    store = QdrantVectorStore(client=client)
    await store.initialize(context)
    with pytest.raises(BackendError) as caught:
        await store.create_generation(SCOPE, spec())
    assert not isinstance(caught.value, BackendUnavailableError)
    assert caught.value.details == {"op": "create_collection", "status": 400}


@pytest.mark.parametrize(
    ("error", "expected"),
    [
        (httpx.ReadTimeout("slow"), BackendTimeoutError),
        (httpx.ConnectError("refused"), BackendUnavailableError),
        (ResponseHandlingException(ValueError("garbled")), BackendUnavailableError),
        (unexpected(503), BackendUnavailableError),
        (unexpected(500), BackendError),
    ],
)
async def test_backend_errors_are_wrapped_and_health_is_unhealthy(
    context: ComponentContext,
    client: AsyncQdrantClient,
    monkeypatch: pytest.MonkeyPatch,
    error: Exception,
    expected: type[BackendError],
) -> None:
    async def broken(**_: Any) -> Any:
        raise error

    monkeypatch.setattr(client, "get_collections", broken)
    store = QdrantVectorStore(client=client)
    await store.initialize(context)
    report = await store.health()
    assert report.status is HealthStatus.UNHEALTHY
    assert report.component == store.info.name
    with pytest.raises(expected) as caught:
        await store.list_generations(SCOPE)
    assert type(caught.value) is expected
    assert caught.value.__cause__ is error


@LOCAL_PAYLOAD_INDEX
async def test_foreign_points_returned_by_the_backend_are_dropped(
    context: ComponentContext, client: AsyncQdrantClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = QdrantVectorStore(client=client)
    await store.initialize(context)
    await store.write(SCOPE, "gen", [item("mine", (1.0, 0.0))])
    query = client.query_points

    async def leaky(**kwargs: Any) -> Any:
        response = await query(**kwargs)
        foreign = models.ScoredPoint(
            id=987654321,
            version=0,
            score=2.0,
            payload={"scope_key": OTHER.key, "object_id": oid("theirs"), "meta": {}},
        )
        response.points.insert(0, foreign)
        return response

    monkeypatch.setattr(client, "query_points", leaky)
    assert names(await store.search(SCOPE, "gen", (1.0, 0.0), 5, None)) == ["mine"]


async def test_url_mode_creates_and_closes_its_own_async_client(
    context: ComponentContext, monkeypatch: pytest.MonkeyPatch
) -> None:
    from retrievalloom.index import vector

    made: list[dict[str, Any]] = []
    closed: list[bool] = []

    class OfflineClient(AsyncQdrantClient):
        def __init__(self, **kwargs: Any) -> None:
            made.append(kwargs)
            # No server here: skip qdrant's background version-check thread.
            super().__init__(**kwargs, check_compatibility=False)

        async def close(self, grpc_grace: float | None = None, **kwargs: Any) -> None:
            closed.append(True)
            await super().close(grpc_grace, **kwargs)

    monkeypatch.setattr(vector, "AsyncQdrantClient", OfflineClient)
    store = QdrantVectorStore(url="https://127.0.0.1:9", api_key="secret", timeout=1)
    await store.initialize(context)
    assert made == [{"url": "https://127.0.0.1:9", "api_key": "secret", "timeout": 1}]
    await store.close()
    assert closed == [True]
    with pytest.raises(LifecycleError):
        await store.list_generations(SCOPE)
