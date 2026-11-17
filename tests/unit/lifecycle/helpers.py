"""Lifecycle test harness: in-memory store, a dict-backed index with failure injection."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

from retrievalloom.config import IngestionConfig, LifecycleConfig
from retrievalloom.contracts.component import ComponentABC, ComponentInfo, make_info
from retrievalloom.contracts.indexes import (
    GenerationalIndexABC,
    GenerationSpec,
    GenerationStats,
    IndexCapabilities,
    IndexItem,
    WriteReport,
)
from retrievalloom.domain.hashing import random_id
from retrievalloom.domain.inputs import (
    ChunkInput,
    ChunksRepresentation,
    DocumentInput,
    Scope,
    TextRepresentation,
)
from retrievalloom.domain.records import Channel
from retrievalloom.errors import GenerationError
from retrievalloom.lifecycle.coordinator import LifecycleCoordinator
from retrievalloom.lifecycle.indexer import ReaderTracker, UnitIndexer
from retrievalloom.lifecycle.ingestion import IngestionService
from retrievalloom.lifecycle.purge import DocumentEraser
from retrievalloom.lifecycle.units import ItemPreparer, make_unit
from retrievalloom.storage.sqlite import SQLiteMetadataStore, SQLiteTransaction

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Mapping, Sequence

    import pytest

    from retrievalloom.domain.base import Metadata
    from retrievalloom.lifecycle.ingestion import GraphDerivation
    from retrievalloom.lifecycle.units import IndexUnit

SCOPE = Scope(tenant_id="acme")
OTHER = Scope(tenant_id="other")


class FakeClock:
    """Manually advanced UTC clock."""

    def __init__(self) -> None:
        self.now = datetime(2026, 1, 1, tzinfo=UTC)

    def __call__(self) -> datetime:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += timedelta(seconds=seconds)


class RecordingLogger:
    """structlog-like logger recording ``(level, event, fields)``."""

    def __init__(self) -> None:
        self.records: list[tuple[str, str, dict[str, Any]]] = []

    def _log(self, level: str, event: str, **fields: Any) -> None:
        self.records.append((level, event, fields))

    def debug(self, event: str, **fields: Any) -> None:
        self._log("debug", event, **fields)

    def info(self, event: str, **fields: Any) -> None:
        self._log("info", event, **fields)

    def warning(self, event: str, **fields: Any) -> None:
        self._log("warning", event, **fields)

    def error(self, event: str, **fields: Any) -> None:
        self._log("error", event, **fields)

    def events(self, level: str | None = None) -> list[str]:
        return [event for lvl, event, _ in self.records if level is None or lvl == level]


@dataclass
class _Generation:
    spec: GenerationSpec
    items: dict[str, IndexItem] = field(default_factory=dict)
    sealed: bool = False


class DictIndex(GenerationalIndexABC):
    """Dict-backed generational index with call recording and failure injection.

    ``fail[method] = exc`` makes every call of ``method`` raise ``exc`` until removed;
    ``hooks[method]`` is awaited once (then removed) when ``method`` is next called.
    """

    def __init__(
        self,
        *,
        name: str = "dict",
        mutable: bool = True,
        durable: bool = False,
        max_batch: int = 256,
        config: Any = None,
    ) -> None:
        self._info = make_info(component="index", implementation=name, version="1", config=config)
        self._capabilities = IndexCapabilities(
            mutable_in_place=mutable, durable=durable, max_batch=max_batch
        )
        self.generations: dict[tuple[str, str], _Generation] = {}
        self.fail: dict[str, BaseException] = {}
        self.hooks: dict[str, Callable[[], Awaitable[None]]] = {}
        self.calls: list[tuple[str, str]] = []

    @property
    def info(self) -> ComponentInfo:
        return self._info

    @property
    def capabilities(self) -> IndexCapabilities:
        return self._capabilities

    async def _enter(self, method: str, generation_id: str) -> None:
        self.calls.append((method, generation_id))
        hook = self.hooks.pop(method, None)
        if hook is not None:
            await hook()
        exc = self.fail.get(method)
        if exc is not None:
            raise exc

    def live(self, scope: Scope, generation_id: str) -> dict[str, IndexItem]:
        return self.generations[(scope.key, generation_id)].items

    async def create_generation(self, scope: Scope, spec: GenerationSpec) -> None:
        await self._enter("create_generation", spec.generation_id)
        self.generations.setdefault((scope.key, spec.generation_id), _Generation(spec))

    async def write(
        self, scope: Scope, generation_id: str, items: Sequence[IndexItem]
    ) -> WriteReport:
        await self._enter("write", generation_id)
        generation = self.generations.get((scope.key, generation_id))
        if generation is None:
            raise GenerationError("generation not found")
        if generation.sealed and not self._capabilities.mutable_in_place:
            raise GenerationError("generation is sealed")
        for item in items:
            generation.items[item.object_id] = item
        return WriteReport(written=len(items))

    async def remove(self, scope: Scope, generation_id: str, object_ids: Sequence[str]) -> int:
        await self._enter("remove", generation_id)
        generation = self.generations.get((scope.key, generation_id))
        if generation is None:
            return 0
        return sum(generation.items.pop(oid, None) is not None for oid in object_ids)

    async def update_metadata(
        self, scope: Scope, generation_id: str, updates: Mapping[str, Metadata]
    ) -> int:
        return 0

    async def seal(self, scope: Scope, generation_id: str) -> GenerationStats:
        await self._enter("seal", generation_id)
        generation = self.generations[(scope.key, generation_id)]
        generation.sealed = True
        return GenerationStats(object_count=len(generation.items))

    async def drop_generation(self, scope: Scope, generation_id: str) -> None:
        await self._enter("drop_generation", generation_id)
        self.generations.pop((scope.key, generation_id), None)

    async def list_generations(self, scope: Scope) -> Sequence[str]:
        return sorted(gid for key, gid in self.generations if key == scope.key)


class Dependency(ComponentABC):
    """Component whose fingerprint (``config``) defines a unit's dependency fingerprint."""

    def __init__(self, config: Any = 1) -> None:
        self._info = make_info(component="dep", implementation="dep", version="1", config=config)

    @property
    def info(self) -> ComponentInfo:
        return self._info


async def open_store() -> SQLiteMetadataStore:
    store = SQLiteMetadataStore()
    await store.initialize()
    await store.migrate()
    return store


def fail_transaction(
    monkeypatch: pytest.MonkeyPatch,
    method: str,
    exc: Callable[[], BaseException],
    times: int = 1_000_000,
) -> list[int]:
    """Make ``SQLiteTransaction.<method>`` raise ``exc()`` for its first ``times`` calls."""
    original = getattr(SQLiteTransaction, method)
    calls = [0]

    async def wrapper(self: SQLiteTransaction, *args: Any, **kwargs: Any) -> Any:
        calls[0] += 1
        if calls[0] <= times:
            raise exc()
        return await original(self, *args, **kwargs)

    monkeypatch.setattr(SQLiteTransaction, method, wrapper)
    return calls


@dataclass
class Harness:
    """Lifecycle services wired around one store."""

    store: SQLiteMetadataStore
    clock: FakeClock
    logger: RecordingLogger
    ingestion: IngestionService
    indexer: UnitIndexer
    coordinator: LifecycleCoordinator
    eraser: DocumentEraser
    units: tuple[IndexUnit, ...]
    readers: ReaderTracker
    instance_id: str

    @property
    def unit(self) -> IndexUnit:
        return self.units[0]

    async def close(self) -> None:
        await self.coordinator.stop()


def build(
    store: SQLiteMetadataStore,
    units: Sequence[IndexUnit],
    *,
    lifecycle: LifecycleConfig | None = None,
    ingestion: IngestionConfig | None = None,
    graph: GraphDerivation | None = None,
    preparer: ItemPreparer | None = None,
    clock: FakeClock | None = None,
    instance_id: str | None = None,
) -> Harness:
    clock = clock or FakeClock()
    logger = RecordingLogger()
    lifecycle = lifecycle or LifecycleConfig(background=False)
    instance = instance_id or random_id("inst")
    readers = ReaderTracker(lease_ttl=lifecycle.lease_ttl, clock=clock)
    service = IngestionService(
        store, ingestion or IngestionConfig(), clock=clock, logger=logger, graph=graph
    )
    indexer = UnitIndexer(
        store,
        preparer or ItemPreparer(tokenizer=None, embedder=None, dictionary=None),
        lifecycle,
        instance_id=instance,
        clock=clock,
        logger=logger,
        readers=readers,
        derive_graph=None if graph is None else service.derive_graph,
    )
    eraser = DocumentEraser(store, service, indexer, units, clock=clock, logger=logger)
    coordinator = LifecycleCoordinator(
        store,
        indexer,
        units,
        lifecycle,
        instance_id=instance,
        engine_id="engine-1",
        clock=clock,
        logger=logger,
        eraser=eraser,
    )
    return Harness(
        store=store,
        clock=clock,
        logger=logger,
        ingestion=service,
        indexer=indexer,
        coordinator=coordinator,
        eraser=eraser,
        units=tuple(units),
        readers=readers,
        instance_id=instance,
    )


def index_unit(
    backend: GenerationalIndexABC,
    *,
    method: str = "sparse",
    channel: Channel = Channel.INDEX,
    instance_id: str = "inst_local",
    dependencies: Sequence[ComponentABC] = (),
) -> IndexUnit:
    return make_unit(channel, method, backend, instance_id=instance_id, dependencies=dependencies)


def text_doc(document_id: str, text: str, **extra: Any) -> DocumentInput:
    return DocumentInput.model_validate(
        {"_id": document_id, "representations": (TextRepresentation(text=text),), **extra}
    )


def chunk_doc(document_id: str, *texts: str, **extra: Any) -> DocumentInput:
    return DocumentInput.model_validate(
        {
            "_id": document_id,
            "representations": (
                ChunksRepresentation(chunks=tuple(ChunkInput(text=text) for text in texts)),
            ),
            **extra,
        }
    )
