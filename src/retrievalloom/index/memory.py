"""Shared machinery of in-memory generational indexes (ephemeral, engine-local)."""

from __future__ import annotations

import asyncio
from abc import abstractmethod
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, ClassVar

from retrievalloom.contracts.indexes import (
    GenerationalIndexABC,
    GenerationSpec,
    GenerationStats,
    IndexCapabilities,
    IndexItem,
    WriteReport,
)
from retrievalloom.domain.filters import evaluate
from retrievalloom.errors import GenerationError, LifecycleError

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping, Sequence

    from retrievalloom.contracts.component import ComponentContext
    from retrievalloom.domain.base import Metadata
    from retrievalloom.domain.filters import FilterExpression
    from retrievalloom.domain.inputs import Scope
    from retrievalloom.runtime.offloader import Offloader, WorkloadKind


@dataclass(slots=True)
class MemoryGeneration:
    """One in-memory generation: staged items before sealing, a mutable structure after."""

    spec: GenerationSpec
    staged: dict[str, IndexItem] = field(default_factory=dict)
    metadata: dict[str, Metadata] = field(default_factory=dict)
    sealed: bool = False
    built: Any = None
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    def admits(self, object_id: str, filters: FilterExpression | None) -> bool:
        """Whether ``object_id`` is present and its metadata matches ``filters``."""
        metadata = self.metadata.get(object_id)
        return metadata is not None and (filters is None or evaluate(filters, metadata))

    def matching(self, object_ids: Iterable[str], filters: FilterExpression | None) -> list[str]:
        """The ids of ``object_ids`` that :meth:`admits`, in order."""
        metadata = self.metadata
        if filters is None:
            return [object_id for object_id in object_ids if object_id in metadata]
        return [
            object_id
            for object_id in object_ids
            if (found := metadata.get(object_id)) is not None and evaluate(filters, found)
        ]


class InMemoryGenerationalIndex(GenerationalIndexABC):
    """In-memory index base whose sealed generations are updated in place.

    Writes before ``seal`` are staged and built in one offloaded pass. After ``seal`` a
    write is analysed in the offloader (:meth:`_analyze`, which may only read the parts
    of the structure that never change) and applied on the event loop (:meth:`_delete`
    of the objects it replaces, then :meth:`_insert`); ``remove`` deletes objects from
    the structure. Both cost time proportional to the changed objects, not to the
    generation, and leave the structure equal to a fresh build of the live objects.
    Changes of one generation are serialised by its lock; :meth:`_maintain` may then
    swap in a reorganised structure (e.g. compacted). Generations are physically
    separate per ``(scope_key, generation_id)``.
    """

    workload: ClassVar[WorkloadKind]

    def __init__(self) -> None:
        self._generations: dict[tuple[str, str], MemoryGeneration] = {}
        self._offloader: Offloader | None = None

    @property
    def capabilities(self) -> IndexCapabilities:
        """Mutable in place, ephemeral, filters evaluated in the backend."""
        return IndexCapabilities(mutable_in_place=True, durable=False, filter_pushdown=True)

    async def initialize(self, context: ComponentContext) -> None:
        """Bind the engine offloader."""
        self._offloader = context.offloader

    async def close(self) -> None:
        """Drop every generation."""
        self._generations.clear()

    @property
    def offloader(self) -> Offloader:
        """Engine offloader (after ``initialize``)."""
        if self._offloader is None:
            raise LifecycleError(f"{self.info.name} is not initialised")
        return self._offloader

    def generation(self, scope: Scope, generation_id: str) -> MemoryGeneration:
        """Return a generation or raise ``GenerationError``."""
        found = self._generations.get((scope.key, generation_id))
        if found is None:
            raise GenerationError("generation not found", details={"generation_id": generation_id})
        return found

    async def create_generation(self, scope: Scope, spec: GenerationSpec) -> None:
        """Create an empty generation (idempotent)."""
        self._generations.setdefault((scope.key, spec.generation_id), MemoryGeneration(spec))

    async def write(
        self, scope: Scope, generation_id: str, items: Sequence[IndexItem]
    ) -> WriteReport:
        """Stage items before ``seal``; insert or replace them in place after it."""
        generation = self.generation(scope, generation_id)
        batch = {item.object_id: item for item in items}
        async with generation.lock:
            if generation.sealed:
                analysis = await self.offloader.run(
                    self.workload,
                    self._analyze,
                    generation.built,
                    list(batch.values()),
                    tenant=scope.tenant_id,
                )
                # Applied without awaiting, so searches never see a half-applied write.
                replaced = [oid for oid in batch if oid in generation.metadata]
                self._delete(generation.built, replaced)
                self._insert(generation.built, analysis)
            else:
                generation.staged.update(batch)
            for object_id, item in batch.items():
                generation.metadata[object_id] = dict(item.metadata)
            if generation.sealed:
                generation.built = await self._maintain(scope, generation.built)
        return WriteReport(written=len(items))

    async def remove(self, scope: Scope, generation_id: str, object_ids: Sequence[str]) -> int:
        """Remove present objects; returns how many were present."""
        generation = self._generations.get((scope.key, generation_id))
        if generation is None:
            return 0
        async with generation.lock:
            present = [oid for oid in dict.fromkeys(object_ids) if oid in generation.metadata]
            for object_id in present:
                del generation.metadata[object_id]
                generation.staged.pop(object_id, None)
            if generation.sealed:
                self._delete(generation.built, present)
                generation.built = await self._maintain(scope, generation.built)
        return len(present)

    async def update_metadata(
        self, scope: Scope, generation_id: str, updates: Mapping[str, Metadata]
    ) -> int:
        """Replace filter metadata of present items."""
        generation = self.generation(scope, generation_id)
        updated = 0
        for object_id, metadata in updates.items():
            if object_id in generation.metadata:
                generation.metadata[object_id] = dict(metadata)
                updated += 1
        return updated

    async def seal(self, scope: Scope, generation_id: str) -> GenerationStats:
        """Build the search structure from the staged items (idempotent)."""
        generation = self.generation(scope, generation_id)
        async with generation.lock:
            if not generation.sealed:
                items = list(generation.staged.values())
                generation.built = await self.offloader.run(
                    self.workload, self._build, generation.spec, items, tenant=scope.tenant_id
                )
                generation.staged = {}
                generation.sealed = True
        return GenerationStats(object_count=len(generation.metadata))

    async def drop_generation(self, scope: Scope, generation_id: str) -> None:
        """Delete a generation (idempotent)."""
        self._generations.pop((scope.key, generation_id), None)

    async def list_generations(self, scope: Scope) -> Sequence[str]:
        """Generations held for ``scope``."""
        return sorted(gid for key, gid in self._generations if key == scope.key)

    def sealed_generation(self, scope: Scope, generation_id: str) -> MemoryGeneration:
        """Return a sealed generation for searching."""
        generation = self.generation(scope, generation_id)
        if not generation.sealed:
            raise GenerationError(
                "generation is not sealed", details={"generation_id": generation_id}
            )
        return generation

    def _build(self, spec: GenerationSpec, items: list[IndexItem]) -> Any:
        """Build a structure holding ``items`` (runs in a worker thread)."""
        structure = self._empty(spec)
        self._insert(structure, self._analyze(structure, items))
        return structure

    @abstractmethod
    def _empty(self, spec: GenerationSpec) -> Any:
        """Return an empty search structure for a generation built with ``spec``."""

    @abstractmethod
    def _analyze(self, structure: Any, items: list[IndexItem]) -> Any:
        """Derive what ``_insert`` needs from ``items`` (worker thread; must not mutate)."""

    @abstractmethod
    def _insert(self, structure: Any, analysis: Any) -> None:
        """Add analysed objects that are not in ``structure``."""

    @abstractmethod
    def _delete(self, structure: Any, object_ids: list[str]) -> None:
        """Remove objects that are in ``structure``."""

    async def _maintain(self, scope: Scope, structure: Any) -> Any:
        """Return the structure to use after a change (e.g. a compacted copy).

        Runs while the generation's lock is held, so no other change interleaves;
        searches may keep reading the previous structure.
        """
        del scope
        return structure
