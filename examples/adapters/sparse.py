"""Learned-sparse index: bge-m3 lexical weights stored as Qdrant sparse vectors.

``SparseIndexABC`` is the engine's lexical slot (``index.sparse``), so a learned-sparse
model replaces BM25 without engine changes: ``Components(sparse_index=BgeM3SparseIndex(...))``.

Contract fit: ``SparseQuery.text`` carries the raw query, so the index encodes it with
bge-m3's own tokenizer and ignores the engine tokenizer's ``tokens``. Documents
(``IndexItem.text``) and queries are casefolded the same way before encoding, so this
index works with any engine tokenizer, including stemming or stop-word ones.
"""

from __future__ import annotations

import uuid
from pathlib import Path
from typing import TYPE_CHECKING, Any

import httpx
from qdrant_client import AsyncQdrantClient, models
from qdrant_client.http.exceptions import ResponseHandlingException, UnexpectedResponse

from retrievalloom.contracts.component import ComponentInfo, make_info
from retrievalloom.contracts.indexes import (
    GenerationStats,
    IndexCapabilities,
    SparseIndexABC,
    WriteReport,
)
from retrievalloom.domain.filters import evaluate
from retrievalloom.domain.records import Channel
from retrievalloom.domain.reports import HealthReport, HealthStatus
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
    BackendTimeoutError,
    BackendUnavailableError,
    ConfigurationError,
    LifecycleError,
)
from retrievalloom.index.text import fold
from retrievalloom.index.vector import translate_filter
from retrievalloom.runtime.offloader import WorkloadKind

from ._models import LoadedModel, bge_m3

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from retrievalloom.contracts.component import ComponentContext
    from retrievalloom.contracts.indexes import GenerationSpec, IndexItem, SparseQuery
    from retrievalloom.domain.base import Metadata
    from retrievalloom.domain.filters import FilterExpression
    from retrievalloom.domain.inputs import Scope
    from retrievalloom.runtime.offloader import Offloader

_METHOD = MethodRef(channel=Channel.INDEX, method="sparse")
_SEMANTICS = ScoreSemantics(higher_is_better=True, range=ScoreRange.UNBOUNDED)
_VECTOR = "lexical"
_NAMESPACE = uuid.UUID("0f6e1f0e-3b55-4c1e-9a5e-6c6a3c1b2d41")
_PAGE = 256
_EXPLAIN_TERMS = 3

SparseWeights = dict[int, float]


def _point_id(scope_key: str, generation_id: str, object_id: str) -> str:
    return str(uuid.uuid5(_NAMESPACE, f"{scope_key}\x1f{generation_id}\x1f{object_id}"))


class BgeM3SparseIndex(SparseIndexABC):
    """Generation-aware learned-sparse index on a Qdrant server.

    One collection (``{collection_prefix}_bge_m3_lexical``) holds every scope; each point
    carries ``scope_key``, ``generation_id``, ``object_id`` and filter metadata under
    ``meta``, and every request filters on scope and generation (keyword payload indexes).

    Args:
        model_dir: Local ``bge-m3`` directory (shared with ``BgeM3Embedding``).
        url: Qdrant server URL.
        api_key: Optional server API key.
        collection_prefix: Collection name prefix (alphanumeric or ``_``).
        max_length: Token limit per text (fingerprinted).
        batch_size: Encoding batch size.
        overfetch_factor: Candidate multiplier when a filter is only partly pushed down.
    """

    def __init__(
        self,
        model_dir: str | Path,
        *,
        url: str,
        api_key: str | None = None,
        collection_prefix: str = "retrievalloom",
        max_length: int = 512,
        batch_size: int = 16,
        overfetch_factor: int = 4,
    ) -> None:
        if not collection_prefix.replace("_", "").isalnum():
            raise ConfigurationError("collection_prefix must be alphanumeric or '_'")
        if overfetch_factor < 1:
            raise ConfigurationError("overfetch_factor must be at least 1")
        self._dir = Path(model_dir)
        self._url = url
        self._api_key = api_key
        self._collection = f"{collection_prefix}_bge_m3_lexical"
        self._max_length = max_length
        self._batch_size = batch_size
        self._overfetch = overfetch_factor
        self._client: AsyncQdrantClient | None = None
        self._offloader: Offloader | None = None
        self._loaded: LoadedModel | None = None
        self._ready = False
        self._created: set[tuple[str, str]] = set()
        self._info = make_info(
            component="sparse_index",
            implementation="bge-m3-lexical-qdrant",
            version="1",
            config={"collection": self._collection, "max_length": max_length},
            model_identity="BAAI/bge-m3#lexical",
        )

    @property
    def info(self) -> ComponentInfo:
        """Component identity (the model identity is part of the fingerprint)."""
        return self._info

    @property
    def capabilities(self) -> IndexCapabilities:
        """Mutable in place, durable (server-side) and with filter pushdown."""
        return IndexCapabilities(mutable_in_place=True, durable=True, filter_pushdown=True)

    async def initialize(self, context: ComponentContext) -> None:
        """Load the shared bge-m3 model and connect to Qdrant (idempotent)."""
        self._offloader = context.offloader
        if self._loaded is None:
            self._loaded = await context.offloader.run(WorkloadKind.SPARSE_INDEX, bge_m3, self._dir)
        if self._client is None:
            self._client = AsyncQdrantClient(url=self._url, api_key=self._api_key)

    async def close(self) -> None:
        """Close the Qdrant client (idempotent; the model stays cached)."""
        client, self._client = self._client, None
        if client is not None:
            await client.close()
        self._loaded = None
        self._ready = False

    async def health(self) -> HealthReport:
        """Healthy when Qdrant answers."""
        try:
            await self._call("get_collections")
        except (BackendError, LifecycleError) as exc:
            return HealthReport(
                component=self.info.name, status=HealthStatus.UNHEALTHY, message=str(exc)
            )
        return HealthReport(component=self.info.name)

    async def _call(self, name: str, **kwargs: Any) -> Any:
        if self._client is None:
            raise LifecycleError("sparse index is not initialised")
        try:
            return await getattr(self._client, name)(**kwargs)
        except httpx.TimeoutException as exc:
            raise BackendTimeoutError("qdrant request timed out", details={"op": name}) from exc
        except (httpx.TransportError, ResponseHandlingException) as exc:
            raise BackendUnavailableError("qdrant is unreachable", details={"op": name}) from exc
        except UnexpectedResponse as exc:
            raise BackendError(
                "qdrant request failed", details={"op": name, "status": exc.status_code}
            ) from exc

    async def _ensure_collection(self) -> None:
        if self._ready:
            return
        if not await self._call("collection_exists", collection_name=self._collection):
            try:
                await self._call(
                    "create_collection",
                    collection_name=self._collection,
                    vectors_config={},
                    sparse_vectors_config={_VECTOR: models.SparseVectorParams()},
                )
            except BackendError:
                if not await self._call("collection_exists", collection_name=self._collection):
                    raise
            for field in ("scope_key", "generation_id"):
                await self._call(
                    "create_payload_index",
                    collection_name=self._collection,
                    field_name=field,
                    field_schema=models.PayloadSchemaType.KEYWORD,
                )
        self._ready = True

    def _encode(self, texts: list[str]) -> list[SparseWeights]:
        if self._loaded is None:
            raise LifecycleError("sparse index is not initialised")
        with self._loaded.lock:
            output = self._loaded.model.encode(
                [fold(text) for text in texts],
                batch_size=self._batch_size,
                max_length=self._max_length,
                return_dense=False,
                return_sparse=True,
                return_colbert_vecs=False,
            )
        return [
            {int(token): float(weight) for token, weight in weights.items() if weight > 0}
            for weights in output["lexical_weights"]
        ]

    def _tokens(self, ids: list[int]) -> list[str]:
        assert self._loaded is not None
        with self._loaded.lock:
            tokens = self._loaded.model.tokenizer.convert_ids_to_tokens(ids)
        return [str(token).lstrip("\u2581") for token in tokens]

    async def _run(self, kind: WorkloadKind, tenant: str, fn: Any, *args: Any) -> Any:
        if self._offloader is None:
            raise LifecycleError("sparse index is not initialised")
        return await self._offloader.run(kind, fn, *args, tenant=tenant)

    @staticmethod
    def _scope(scope_key: str, generation_id: str | None = None) -> list[Any]:
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

    async def create_generation(self, scope: Scope, spec: GenerationSpec) -> None:
        """Ensure the collection exists; generations are a payload field."""
        await self._ensure_collection()
        self._created.add((scope.key, spec.generation_id))

    async def write(
        self, scope: Scope, generation_id: str, items: Sequence[IndexItem]
    ) -> WriteReport:
        """Encode texts off the loop and upsert one sparse point per object."""
        if not items:
            return WriteReport(written=0)
        await self._ensure_collection()
        weights = await self._run(
            WorkloadKind.SPARSE_INDEX, scope.tenant_id, self._encode, [i.text for i in items]
        )
        points = [
            models.PointStruct(
                id=_point_id(scope.key, generation_id, item.object_id),
                vector={
                    _VECTOR: models.SparseVector(indices=list(vector), values=list(vector.values()))
                },
                payload={
                    "scope_key": scope.key,
                    "generation_id": generation_id,
                    "object_id": item.object_id,
                    "meta": dict(item.metadata),
                },
            )
            for item, vector in zip(items, weights, strict=True)
        ]
        for start in range(0, len(points), self.capabilities.max_batch):
            await self._call(
                "upsert",
                collection_name=self._collection,
                points=points[start : start + self.capabilities.max_batch],
                wait=True,
            )
        self._created.add((scope.key, generation_id))
        return WriteReport(written=len(points))

    async def _present(self, ids: list[str]) -> list[str]:
        records = await self._call(
            "retrieve", collection_name=self._collection, ids=ids, with_payload=False
        )
        return [str(record.id) for record in records]

    async def remove(self, scope: Scope, generation_id: str, object_ids: Sequence[str]) -> int:
        """Delete points; returns how many existed."""
        if not object_ids:
            return 0
        await self._ensure_collection()
        ids = [_point_id(scope.key, generation_id, object_id) for object_id in object_ids]
        present = await self._present(ids)
        if present:
            await self._call(
                "delete",
                collection_name=self._collection,
                points_selector=models.PointIdsList(points=present),  # type: ignore[arg-type]
                wait=True,
            )
        return len(present)

    async def update_metadata(
        self, scope: Scope, generation_id: str, updates: Mapping[str, Metadata]
    ) -> int:
        """Replace the ``meta`` payload of present points."""
        await self._ensure_collection()
        updated = 0
        for object_id, metadata in updates.items():
            identifier = _point_id(scope.key, generation_id, object_id)
            if not await self._present([identifier]):
                continue
            await self._call(
                "set_payload",
                collection_name=self._collection,
                payload={"meta": dict(metadata)},
                points=[identifier],
                wait=True,
            )
            updated += 1
        return updated

    async def seal(self, scope: Scope, generation_id: str) -> GenerationStats:
        """Count the generation's points (no build step)."""
        await self._ensure_collection()
        result = await self._call(
            "count",
            collection_name=self._collection,
            count_filter=models.Filter(must=self._scope(scope.key, generation_id)),
            exact=True,
        )
        return GenerationStats(object_count=result.count)

    async def drop_generation(self, scope: Scope, generation_id: str) -> None:
        """Delete every point of a generation (idempotent)."""
        await self._ensure_collection()
        await self._call(
            "delete",
            collection_name=self._collection,
            points_selector=models.FilterSelector(
                filter=models.Filter(must=self._scope(scope.key, generation_id))
            ),
            wait=True,
        )
        self._created.discard((scope.key, generation_id))

    async def list_generations(self, scope: Scope) -> Sequence[str]:
        """Generations created here or found on the server for ``scope``."""
        await self._ensure_collection()
        found = {generation for key, generation in self._created if key == scope.key}
        offset = None
        while True:
            records, offset = await self._call(
                "scroll",
                collection_name=self._collection,
                scroll_filter=models.Filter(must=self._scope(scope.key)),
                limit=_PAGE,
                offset=offset,
                with_payload=["generation_id"],
            )
            found.update(str((record.payload or {})["generation_id"]) for record in records)
            if offset is None:
                break
        return sorted(found)

    async def search(
        self,
        scope: Scope,
        generation_id: str,
        query: SparseQuery,
        limit: int,
        filters: FilterExpression | None,
    ) -> Sequence[RetrievalHit]:
        """Dot product of learned term weights; ``matched`` lists the top shared terms."""
        text = (query.text or " ".join(query.tokens)).strip()
        if not text or limit < 1:
            return []
        await self._ensure_collection()
        (weights,) = await self._run(
            WorkloadKind.SPARSE_QUERY, scope.tenant_id, self._encode, [text]
        )
        if not weights:
            return []
        translated, exact = translate_filter(filters)
        must = self._scope(scope.key, generation_id)
        if translated is not None:
            must.append(translated)
        response = await self._call(
            "query_points",
            collection_name=self._collection,
            query=models.SparseVector(indices=list(weights), values=list(weights.values())),
            using=_VECTOR,
            query_filter=models.Filter(must=must),
            limit=limit if exact else limit * self._overfetch,
            with_payload=True,
            with_vectors=[_VECTOR],
        )
        kept: list[tuple[Any, list[tuple[int, float]]]] = []
        for point in response.points:
            payload = point.payload or {}
            if payload.get("scope_key") != scope.key or point.score <= 0:
                continue
            if not evaluate(filters, payload.get("meta", {})):
                continue
            stored = (point.vector or {}).get(_VECTOR)
            shared: list[tuple[int, float]] = []
            if isinstance(stored, models.SparseVector):
                shared = sorted(
                    (
                        (index, value * weights[index])
                        for index, value in zip(stored.indices, stored.values, strict=True)
                        if index in weights
                    ),
                    key=lambda pair: -pair[1],
                )[:_EXPLAIN_TERMS]
            kept.append((point, shared))
            if len(kept) == limit:
                break
        terms = await self._run(
            WorkloadKind.SPARSE_QUERY,
            scope.tenant_id,
            self._tokens,
            [index for _, shared in kept for index, _ in shared],
        )
        names = iter(terms)
        return [
            RetrievalHit(
                object_ref=ObjectRef(
                    scope_key=scope.key,
                    object_id=str((point.payload or {})["object_id"]),
                    generation_id=generation_id,
                ),
                method=_METHOD,
                backend_id=self._info.implementation,
                raw_score=float(point.score),
                score_semantics=_SEMANTICS,
                rank=rank,
                matched=tuple(
                    MatchDetail(kind="learned_term", term=next(names), score=contribution)
                    for _, contribution in shared
                ),
            )
            for rank, (point, shared) in enumerate(kept, start=1)
        ]
