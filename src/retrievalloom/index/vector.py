"""Dense vector store on Qdrant (local in-memory/on-disk mode or a Qdrant server)."""

from __future__ import annotations

import functools
import math
import uuid
from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING, Any

import httpx
from qdrant_client import AsyncQdrantClient, QdrantClient, models
from qdrant_client.http.exceptions import ResponseHandlingException, UnexpectedResponse

from retrievalloom.contracts.component import ComponentInfo, make_info
from retrievalloom.contracts.indexes import (
    GenerationStats,
    IndexCapabilities,
    VectorStoreABC,
    WriteReport,
)
from retrievalloom.domain.filters import And, Eq, Exists, In, Not, Or, Range, evaluate
from retrievalloom.domain.records import Channel
from retrievalloom.domain.reports import HealthReport, HealthStatus
from retrievalloom.domain.retrieval import (
    MethodRef,
    ObjectRef,
    RetrievalHit,
    ScoreRange,
    ScoreSemantics,
)
from retrievalloom.errors import (
    BackendError,
    BackendTimeoutError,
    BackendUnavailableError,
    ComponentContractError,
    ConfigurationError,
    GenerationError,
    LifecycleError,
)
from retrievalloom.runtime.offloader import WorkloadKind

if TYPE_CHECKING:
    from collections.abc import Iterator, Mapping, Sequence

    from retrievalloom.contracts.component import ComponentContext
    from retrievalloom.contracts.indexes import GenerationSpec, IndexItem
    from retrievalloom.domain.base import Metadata
    from retrievalloom.domain.filters import FilterExpression
    from retrievalloom.domain.inputs import Scope
    from retrievalloom.runtime.offloader import Offloader

_METHOD = MethodRef(channel=Channel.INDEX, method="dense")
_SEMANTICS = ScoreSemantics(higher_is_better=True, range=ScoreRange.SIMILARITY)
_POINT_NAMESPACE = uuid.UUID("8a4d0d5e-7f0c-5b8e-9a51-52c1a1f0c0de")
_META = "meta"
_SCROLL_PAGE = 256


def point_id(scope_key: str, generation_id: str, object_id: str) -> str:
    """Deterministic Qdrant point id of an object in a generation."""
    return str(uuid.uuid5(_POINT_NAMESPACE, f"{scope_key}\x1f{generation_id}\x1f{object_id}"))


def translate_filter(
    expression: FilterExpression | None,
) -> tuple[models.Filter | models.FieldCondition | None, bool]:
    """Translate a filter AST into a Qdrant condition over ``meta.*``.

    Returns ``(condition, exact)``. ``condition`` is never narrower than the expression
    (``None`` means "no constraint"); ``exact`` is false when the result is broader, in
    which case callers over-fetch and post-filter with ``evaluate``.

    Limitation: Qdrant does not match an integer ``Eq``/``In`` value against a float
    payload value (``1`` vs ``1.0``); store numeric metadata with consistent types.
    """
    if expression is None:
        return None, True
    if isinstance(expression, And | Or):
        parts = [translate_filter(clause) for clause in expression.clauses]
        exact = all(part_exact for _, part_exact in parts)
        conditions = [condition for condition, _ in parts if condition is not None]
        if isinstance(expression, And):
            return (models.Filter(must=conditions) if conditions else None), exact
        if len(conditions) < len(parts):
            return None, False
        return models.Filter(should=conditions), exact
    if isinstance(expression, Not):
        inner, inner_exact = translate_filter(expression.clause)
        if inner is None or not inner_exact:
            return None, False
        return models.Filter(must_not=[inner]), True
    key = f"{_META}.{expression.field}"
    if isinstance(expression, Eq):
        value = expression.value
        if isinstance(value, str) or (isinstance(value, int) and not isinstance(value, bool)):
            return models.FieldCondition(key=key, match=models.MatchValue(value=value)), True
        return None, False
    if isinstance(expression, In):
        strings = [value for value in expression.values if isinstance(value, str)]
        integers = [
            value
            for value in expression.values
            if isinstance(value, int) and not isinstance(value, bool)
        ]
        if len(strings) == len(expression.values):
            return models.FieldCondition(key=key, match=models.MatchAny(any=strings)), True
        if len(integers) == len(expression.values):
            return models.FieldCondition(key=key, match=models.MatchAny(any=integers)), True
        return None, False
    if isinstance(expression, Range):
        bounds = {
            "gt": expression.gt,
            "gte": expression.gte,
            "lt": expression.lt,
            "lte": expression.lte,
        }
        numeric = {
            name: float(bound) for name, bound in bounds.items() if isinstance(bound, int | float)
        }
        if len(numeric) == sum(bound is not None for bound in bounds.values()):
            # Qdrant matches arrays by any element; the evaluator does not, so it is broader.
            return models.FieldCondition(key=key, range=models.Range(**numeric)), False
        return None, False
    assert isinstance(expression, Exists)
    # Qdrant "is empty" also treats [] as empty while the AST treats it as present.
    return None, False


class QdrantVectorStore(VectorStoreABC):
    """Generation-aware dense vector store on Qdrant.

    Each dimensionality uses one collection ``{collection_prefix}_{dims}``; a point holds
    ``scope_key``, ``generation_id``, ``object_id`` and the object's filter metadata under
    ``meta``. Every operation is constrained by scope and generation, so tenants never see
    each other's vectors. Local modes run the synchronous client on a dedicated
    single-thread executor via the engine offloader; server mode uses ``AsyncQdrantClient``.

    Args:
        location: ``":memory:"`` for an ephemeral local store (the default).
        path: Directory of a persistent local store.
        url: Qdrant server URL.
        api_key: Server API key.
        client: Application-owned ``AsyncQdrantClient`` (never closed by the engine).
        collection_prefix: Collection name prefix.
        distance: ``cosine``, ``dot`` or ``euclid``.
        timeout: Server request timeout in seconds.
        overfetch_factor: Candidate multiplier when a filter is only partly pushed down.
    """

    def __init__(
        self,
        *,
        location: str | None = None,
        path: str | None = None,
        url: str | None = None,
        api_key: str | None = None,
        client: AsyncQdrantClient | None = None,
        collection_prefix: str = "retrievalloom",
        distance: str = "cosine",
        timeout: int | None = None,
        overfetch_factor: int = 4,
    ) -> None:
        if sum(value is not None for value in (location, path, url, client)) > 1:
            raise ConfigurationError("pass at most one of location, path, url or client")
        distances = {
            "cosine": models.Distance.COSINE,
            "dot": models.Distance.DOT,
            "euclid": models.Distance.EUCLID,
        }
        if distance not in distances:
            raise ConfigurationError("distance must be cosine, dot or euclid")
        if overfetch_factor < 1:
            raise ConfigurationError("overfetch_factor must be at least 1")
        if not collection_prefix.replace("_", "").isalnum():
            raise ConfigurationError("collection_prefix must be alphanumeric or '_'")
        self._location = location if any((location, path, url, client)) else ":memory:"
        self._path = path
        self._url = url
        self._api_key = api_key
        self._timeout = timeout
        self._prefix = collection_prefix
        self._distance = distances[distance]
        self._overfetch = overfetch_factor
        self._client: AsyncQdrantClient | QdrantClient | None = client
        self._owns_client = client is None
        self._server = client is not None or url is not None
        self._executor: ThreadPoolExecutor | None = None
        self._offloader: Offloader | None = None
        self._collections: dict[tuple[str, str], str] = {}
        self._known_collections: set[str] = set()
        self._semantics = (
            ScoreSemantics(higher_is_better=False, range=ScoreRange.DISTANCE)
            if self._distance is models.Distance.EUCLID
            else _SEMANTICS
        )
        mode = "server" if self._server else ("path" if path else "memory")
        self._info = make_info(
            component="vector_store",
            implementation="qdrant",
            version="1",
            config={"mode": mode, "prefix": collection_prefix, "distance": distance},
        )

    @property
    def info(self) -> ComponentInfo:
        """Component identity."""
        return self._info

    @property
    def capabilities(self) -> IndexCapabilities:
        """Mutable in place; durable unless in-memory."""
        return IndexCapabilities(
            mutable_in_place=True, durable=self._location is None, filter_pushdown=True
        )

    async def initialize(self, context: ComponentContext) -> None:
        """Connect (idempotent)."""
        self._offloader = context.offloader
        if self._client is not None:
            return
        if self._url is not None:
            self._client = AsyncQdrantClient(
                url=self._url, api_key=self._api_key, timeout=self._timeout
            )
            return
        self._executor = ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="retrievalloom-qdrant"
        )
        factory = functools.partial(QdrantClient, location=self._location, path=self._path)
        self._client = await context.offloader.run(
            WorkloadKind.VECTOR, factory, executor=self._executor
        )

    async def close(self) -> None:
        """Close an engine-owned client and executor (idempotent)."""
        client, self._client = self._client, None
        if client is not None and self._owns_client:
            if isinstance(client, AsyncQdrantClient):
                await client.close()
            # An owned sync client only exists once initialize set both; the check narrows types.
            elif self._offloader is not None and self._executor is not None:  # pragma: no branch
                await self._offloader.run(
                    WorkloadKind.VECTOR, client.close, executor=self._executor
                )
        if self._executor is not None:
            self._executor.shutdown(wait=True)
            self._executor = None
        self._collections.clear()
        self._known_collections.clear()

    async def health(self) -> HealthReport:
        """Healthy when the backend answers a collection listing."""
        try:
            await self._call(None, "get_collections")
        except BackendError as exc:
            return HealthReport(
                component=self.info.name, status=HealthStatus.UNHEALTHY, message=str(exc)
            )
        return HealthReport(component=self.info.name)

    async def _call(self, tenant: str | None, name: str, **kwargs: Any) -> Any:
        client = self._client
        if client is None or self._offloader is None:
            raise LifecycleError("qdrant vector store is not initialised")
        try:
            if isinstance(client, AsyncQdrantClient):
                return await getattr(client, name)(**kwargs)
            call = functools.partial(getattr(client, name), **kwargs)
            return await self._offloader.run(
                WorkloadKind.VECTOR, call, tenant=tenant, executor=self._executor
            )
        except httpx.TimeoutException as exc:
            raise BackendTimeoutError("qdrant request timed out", details={"op": name}) from exc
        except (httpx.TransportError, ResponseHandlingException) as exc:
            raise BackendUnavailableError("qdrant is unreachable", details={"op": name}) from exc
        except UnexpectedResponse as exc:
            error = BackendUnavailableError if exc.status_code in {502, 503, 504} else BackendError
            raise error(
                "qdrant request failed", details={"op": name, "status": exc.status_code}
            ) from exc

    def _name(self, dimensions: int) -> str:
        return f"{self._prefix}_{dimensions}"

    @staticmethod
    def _scope_filter(scope_key: str, generation_id: str | None = None) -> list[Any]:
        conditions: list[Any] = [
            models.FieldCondition(key="scope_key", match=models.MatchValue(value=scope_key))
        ]
        if generation_id is not None:
            conditions.append(
                models.FieldCondition(
                    key="generation_id", match=models.MatchValue(value=generation_id)
                )
            )
        return conditions

    async def _ensure_collection(self, tenant: str, dimensions: int) -> str:
        name = self._name(dimensions)
        if name in self._known_collections:
            return name
        if not await self._call(tenant, "collection_exists", collection_name=name):
            try:
                await self._call(
                    tenant,
                    "create_collection",
                    collection_name=name,
                    vectors_config=models.VectorParams(size=dimensions, distance=self._distance),
                )
            except BackendError:
                if not await self._call(tenant, "collection_exists", collection_name=name):
                    raise
            if self._server:
                for field in ("scope_key", "generation_id"):
                    await self._call(
                        tenant,
                        "create_payload_index",
                        collection_name=name,
                        field_name=field,
                        field_schema=models.PayloadSchemaType.KEYWORD,
                    )
        self._known_collections.add(name)
        return name

    async def _all_collections(self, tenant: str | None) -> list[str]:
        response = await self._call(tenant, "get_collections")
        prefix = f"{self._prefix}_"
        names = sorted(
            collection.name
            for collection in response.collections
            if collection.name.startswith(prefix) and collection.name[len(prefix) :].isdigit()
        )
        self._known_collections.update(names)
        return names

    async def _resolve(self, scope: Scope, generation_id: str) -> str | None:
        """Collection holding a generation (map first, then a scan after restarts)."""
        found = self._collections.get((scope.key, generation_id))
        if found is not None:
            return found
        selector = models.Filter(must=self._scope_filter(scope.key, generation_id))
        for name in await self._all_collections(scope.tenant_id):
            result = await self._call(
                scope.tenant_id, "count", collection_name=name, count_filter=selector, exact=False
            )
            if result.count > 0:
                self._collections[(scope.key, generation_id)] = name
                return name
        return None

    async def create_generation(self, scope: Scope, spec: GenerationSpec) -> None:
        """Ensure the collection of ``spec.dimensions`` exists and record the mapping."""
        if spec.dimensions is None:
            raise ComponentContractError(
                "vector generations need dimensions", details={"generation_id": spec.generation_id}
            )
        name = await self._ensure_collection(scope.tenant_id, spec.dimensions)
        self._collections[(scope.key, spec.generation_id)] = name

    @staticmethod
    def _vector(item: IndexItem, dimensions: int | None) -> list[float]:
        vector = item.vector
        if vector is None or not vector:
            raise ComponentContractError(
                "dense items need a vector", details={"object_id": item.object_id}
            )
        if dimensions is not None and len(vector) != dimensions:
            raise ComponentContractError(
                "vector dimensions differ from the generation",
                details={"object_id": item.object_id, "expected": dimensions, "got": len(vector)},
            )
        if not all(math.isfinite(value) for value in vector):
            raise ComponentContractError(
                "vector has non-finite values", details={"object_id": item.object_id}
            )
        return list(vector)

    async def write(
        self, scope: Scope, generation_id: str, items: Sequence[IndexItem]
    ) -> WriteReport:
        """Upsert items (idempotent by object id)."""
        if not items:
            return WriteReport(written=0)
        name = self._collections.get((scope.key, generation_id))
        dimensions = int(name.rsplit("_", 1)[1]) if name is not None else None
        points = []
        for item in items:
            vector = self._vector(item, dimensions)
            dimensions = len(vector)
            points.append(
                models.PointStruct(
                    id=point_id(scope.key, generation_id, item.object_id),
                    vector=vector,
                    payload={
                        "scope_key": scope.key,
                        "generation_id": generation_id,
                        "object_id": item.object_id,
                        _META: dict(item.metadata),
                    },
                )
            )
        assert dimensions is not None
        if name is None:
            name = await self._ensure_collection(scope.tenant_id, dimensions)
            self._collections[(scope.key, generation_id)] = name
        for batch in _batches(points, self.capabilities.max_batch):
            await self._call(
                scope.tenant_id, "upsert", collection_name=name, points=batch, wait=True
            )
        return WriteReport(written=len(points))

    async def _present(self, scope: Scope, name: str, ids: list[str]) -> list[str]:
        records = await self._call(
            scope.tenant_id, "retrieve", collection_name=name, ids=ids, with_payload=False
        )
        return [str(record.id) for record in records]

    async def remove(self, scope: Scope, generation_id: str, object_ids: Sequence[str]) -> int:
        """Delete points; returns how many existed."""
        name = await self._resolve(scope, generation_id)
        if name is None or not object_ids:
            return 0
        removed = 0
        ids = [point_id(scope.key, generation_id, object_id) for object_id in object_ids]
        for batch in _batches(ids, self.capabilities.max_batch):
            present = await self._present(scope, name, batch)
            if present:
                await self._call(
                    scope.tenant_id,
                    "delete",
                    collection_name=name,
                    points_selector=models.PointIdsList(points=present),  # type: ignore[arg-type]
                    wait=True,
                )
            removed += len(present)
        return removed

    async def update_metadata(
        self, scope: Scope, generation_id: str, updates: Mapping[str, Metadata]
    ) -> int:
        """Replace the ``meta`` payload of present points."""
        name = await self._resolve(scope, generation_id)
        if name is None:
            return 0
        updated = 0
        for object_id, metadata in updates.items():
            identifier = point_id(scope.key, generation_id, object_id)
            if not await self._present(scope, name, [identifier]):
                continue
            await self._call(
                scope.tenant_id,
                "set_payload",
                collection_name=name,
                payload={_META: dict(metadata)},
                points=[identifier],
                wait=True,
            )
            updated += 1
        return updated

    async def seal(self, scope: Scope, generation_id: str) -> GenerationStats:
        """Count the generation's points (Qdrant needs no build step)."""
        name = await self._resolve(scope, generation_id)
        if name is None:
            raise GenerationError("generation not found", details={"generation_id": generation_id})
        result = await self._call(
            scope.tenant_id,
            "count",
            collection_name=name,
            count_filter=models.Filter(must=self._scope_filter(scope.key, generation_id)),
            exact=True,
        )
        return GenerationStats(object_count=result.count)

    async def drop_generation(self, scope: Scope, generation_id: str) -> None:
        """Delete every point of a generation in every collection (idempotent)."""
        selector = models.FilterSelector(
            filter=models.Filter(must=self._scope_filter(scope.key, generation_id))
        )
        for name in await self._all_collections(scope.tenant_id):
            await self._call(
                scope.tenant_id, "delete", collection_name=name, points_selector=selector, wait=True
            )
        self._collections.pop((scope.key, generation_id), None)

    async def list_generations(self, scope: Scope) -> Sequence[str]:
        """Distinct generation ids of ``scope`` (scrolls the scope's points)."""
        found: set[str] = {gid for key, gid in self._collections if key == scope.key}
        selector = models.Filter(must=self._scope_filter(scope.key))
        for name in await self._all_collections(scope.tenant_id):
            offset = None
            while True:
                records, offset = await self._call(
                    scope.tenant_id,
                    "scroll",
                    collection_name=name,
                    scroll_filter=selector,
                    limit=_SCROLL_PAGE,
                    offset=offset,
                    with_payload=["generation_id"],
                )
                found.update(str(record.payload["generation_id"]) for record in records)
                if offset is None:
                    break
        return sorted(found)

    async def search(
        self,
        scope: Scope,
        generation_id: str,
        vector: Sequence[float],
        limit: int,
        filters: FilterExpression | None,
    ) -> Sequence[RetrievalHit]:
        """Nearest neighbours within the scope and generation."""
        name = await self._resolve(scope, generation_id)
        if name is None:
            raise GenerationError("generation not found", details={"generation_id": generation_id})
        dimensions = int(name.rsplit("_", 1)[1])
        if len(vector) != dimensions:
            raise ComponentContractError(
                "query vector dimensions differ from the generation",
                details={"expected": dimensions, "got": len(vector)},
            )
        translated, exact = translate_filter(filters)
        must: list[Any] = self._scope_filter(scope.key, generation_id)
        if translated is not None:
            must.append(translated)
        fetch = limit if exact else limit * self._overfetch
        response = await self._call(
            scope.tenant_id,
            "query_points",
            collection_name=name,
            query=[float(value) for value in vector],
            query_filter=models.Filter(must=must),
            limit=fetch,
            with_payload=True,
        )
        hits: list[RetrievalHit] = []
        for point in response.points:
            payload = point.payload or {}
            if payload.get("scope_key") != scope.key:
                continue
            if not evaluate(filters, payload.get(_META, {})):
                continue
            hits.append(
                RetrievalHit(
                    object_ref=ObjectRef(
                        scope_key=scope.key,
                        object_id=payload["object_id"],
                        generation_id=generation_id,
                    ),
                    method=_METHOD,
                    backend_id=self._info.implementation,
                    raw_score=float(point.score),
                    score_semantics=self._semantics,
                    rank=len(hits) + 1,
                )
            )
            if len(hits) == limit:
                break
        return hits


def _batches[T](values: list[T], size: int) -> Iterator[list[T]]:
    for start in range(0, len(values), size):
        yield values[start : start + size]
