"""Shared machinery of in-memory generational indexes (ephemeral, engine-local)."""

from __future__ import annotations

from abc import abstractmethod
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from retrievalloom.contracts.indexes import (
    GenerationalIndexABC,
    GenerationSpec,
    GenerationStats,
    IndexCapabilities,
    IndexItem,
    WriteReport,
)
from retrievalloom.errors import GenerationError, LifecycleError

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from retrievalloom.contracts.component import ComponentContext
    from retrievalloom.domain.base import Metadata
    from retrievalloom.domain.inputs import Scope
    from retrievalloom.runtime.offloader import Offloader


@dataclass(slots=True)
class MemoryGeneration:
    """One in-memory generation: staged items before sealing, built structure after."""

    spec: GenerationSpec
    items: dict[str, IndexItem] = field(default_factory=dict)
    metadata: dict[str, Metadata] = field(default_factory=dict)
    tombstones: set[str] = field(default_factory=set)
    sealed: bool = False
    built: Any = None


class InMemoryGenerationalIndex(GenerationalIndexABC):
    """Immutable-after-seal in-memory index base.

    Writes are staged until ``seal`` builds the search structure in the offloader.
    ``remove`` after sealing records a tombstone that masks the object at query time.
    Generations are physically separate per ``(scope_key, generation_id)``.
    """

    def __init__(self) -> None:
        self._generations: dict[tuple[str, str], MemoryGeneration] = {}
        self._offloader: Offloader | None = None

    @property
    def capabilities(self) -> IndexCapabilities:
        """Immutable, ephemeral, filters evaluated in the backend."""
        return IndexCapabilities(mutable_in_place=False, durable=False, filter_pushdown=True)

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
        """Stage items; sealed generations reject writes."""
        generation = self.generation(scope, generation_id)
        if generation.sealed:
            raise GenerationError("generation is sealed", details={"generation_id": generation_id})
        for item in items:
            generation.items[item.object_id] = item
            generation.metadata[item.object_id] = dict(item.metadata)
            generation.tombstones.discard(item.object_id)
        return WriteReport(written=len(items))

    async def remove(self, scope: Scope, generation_id: str, object_ids: Sequence[str]) -> int:
        """Remove staged items or tombstone built ones."""
        generation = self._generations.get((scope.key, generation_id))
        if generation is None:
            return 0
        removed = 0
        for object_id in object_ids:
            if generation.sealed:
                if object_id in generation.metadata and object_id not in generation.tombstones:
                    generation.tombstones.add(object_id)
                    removed += 1
            elif generation.items.pop(object_id, None) is not None:
                generation.metadata.pop(object_id, None)
                removed += 1
        return removed

    async def update_metadata(
        self, scope: Scope, generation_id: str, updates: Mapping[str, Metadata]
    ) -> int:
        """Replace filter metadata of present items."""
        generation = self.generation(scope, generation_id)
        updated = 0
        for object_id, metadata in updates.items():
            if object_id in generation.metadata and object_id not in generation.tombstones:
                generation.metadata[object_id] = dict(metadata)
                updated += 1
        return updated

    async def seal(self, scope: Scope, generation_id: str) -> GenerationStats:
        """Build the search structure (idempotent)."""
        generation = self.generation(scope, generation_id)
        if not generation.sealed:
            items = list(generation.items.values())
            generation.built = await self._build(scope, generation.spec, items)
            generation.items = {}
            generation.sealed = True
        live = len(generation.metadata) - len(generation.tombstones)
        return GenerationStats(object_count=live)

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

    @abstractmethod
    async def _build(self, scope: Scope, spec: GenerationSpec, items: list[IndexItem]) -> Any:
        """Build the search structure of a generation."""
