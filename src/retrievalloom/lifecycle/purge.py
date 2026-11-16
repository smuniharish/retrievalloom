"""Physical erasure of documents (right to erasure) after tombstoning."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from retrievalloom.domain.hashing import random_id
from retrievalloom.domain.records import (
    CoverageState,
    DocumentState,
    GenerationState,
    LifecycleTask,
    LineageRelation,
    ObjectState,
    TaskKind,
    TaskState,
)
from retrievalloom.domain.reports import PurgeReport
from retrievalloom.errors import LifecycleError, VersionConflictError

if TYPE_CHECKING:
    from collections.abc import Sequence
    from datetime import datetime

    from retrievalloom.domain.base import Clock
    from retrievalloom.domain.inputs import Scope
    from retrievalloom.domain.records import DerivedObject, DocumentRecord
    from retrievalloom.lifecycle.indexer import UnitIndexer
    from retrievalloom.lifecycle.ingestion import IngestionService
    from retrievalloom.lifecycle.units import IndexUnit
    from retrievalloom.storage.base import DocumentMetadataStoreABC, MetadataTransactionABC

_CONFLICT_RETRIES = 3
_LIVE_GENERATIONS = (
    GenerationState.BUILDING,
    GenerationState.VALIDATING,
    GenerationState.ACTIVE,
    GenerationState.RETIRING,
)


@dataclass(frozen=True, slots=True)
class _Plan:
    """What erasing one tombstoned document touches."""

    document: DocumentRecord
    objects: tuple[DerivedObject, ...]
    aggregate_ids: tuple[str, ...]
    rebuild: tuple[IndexUnit, ...]


class DocumentEraser:
    """Hard-erases tombstoned documents from every index unit and the metadata store.

    Erasure removes the document's objects (and graph aggregates left without support)
    from every generation of this engine's units that may still serve them, queues a
    rebuild of whole-scope units (graph) that still include the document, then deletes
    every metadata row of the document in one transaction. A backend failure aborts
    before any metadata row is deleted, so a document is never reported as purged while
    a backend may still hold it.
    """

    def __init__(
        self,
        store: DocumentMetadataStoreABC,
        ingestion: IngestionService,
        indexer: UnitIndexer,
        units: Sequence[IndexUnit],
        *,
        clock: Clock,
        logger: Any,
    ) -> None:
        self._store = store
        self._ingestion = ingestion
        self._indexer = indexer
        self._units = tuple(units)
        self._clock = clock
        self._logger = logger

    async def purge(self, document_ids: Sequence[str], scope: Scope) -> PurgeReport:
        """Tombstone (when needed) and physically erase documents of ``scope``."""
        deleted = await self._ingestion.delete(document_ids, scope)
        purged: list[str] = []
        task_ids: list[str] = []
        for document_id in (*deleted.deleted, *deleted.already_deleted):
            await self._erase(scope, document_id, task_ids)
            purged.append(document_id)
        self._logger.info(
            "documents_purged",
            scope=scope.key,
            purged=len(purged),
            not_found=len(deleted.not_found),
        )
        return PurgeReport(
            scope=scope,
            purged=tuple(purged),
            not_found=deleted.not_found,
            task_ids=tuple(task_ids),
        )

    async def purge_expired(self, scope: Scope, *, deleted_before: datetime, limit: int) -> int:
        """Erase documents tombstoned before ``deleted_before`` whose coverage is deleted."""
        purged = 0
        task_ids: list[str] = []
        while True:
            async with self._store.transaction(scope, write=False) as transaction:
                page = await transaction.list_purgeable_documents(
                    scope.key, deleted_before=deleted_before, limit=limit
                )
            for document in page:
                await self._erase(scope, document.document_id, task_ids)
            purged += len(page)
            if len(page) < limit:
                break
        if purged:
            self._logger.info("tombstones_purged", scope=scope.key, purged=purged)
        return purged

    async def _erase(self, scope: Scope, document_id: str, task_ids: list[str]) -> None:
        attempt = 0
        while True:
            plan = await self._plan(scope, document_id)
            if plan is None:
                return
            await self._remove_from_backends(scope, plan)
            if plan.rebuild and not task_ids:
                task_ids.append(await self._queue_rebuild(scope, plan.rebuild))
            try:
                await self._delete_rows(scope, plan)
            except VersionConflictError:
                attempt += 1
                if attempt >= _CONFLICT_RETRIES:
                    raise
                continue
            return

    async def _plan(self, scope: Scope, document_id: str) -> _Plan | None:
        async with self._store.transaction(scope, write=False) as transaction:
            document = await transaction.get_document(scope, document_id)
            if document is None:
                return None
            if document.state != DocumentState.DELETED:
                raise LifecycleError(
                    "document was re-ingested during purge",
                    details={"document_id": document_id},
                )
            uid = document.document_uid
            objects = await transaction.list_document_objects(scope.key, uid)
            aggregate_ids = await self._unsupported_aggregates(transaction, scope, uid)
            aggregates = await transaction.get_objects(scope.key, aggregate_ids)
            rebuild: list[IndexUnit] = []
            for unit in self._units:
                if unit.per_document:
                    continue
                row = (await transaction.get_coverage(unit.key(scope), [uid])).get(uid)
                if row is not None and row.state != CoverageState.DELETED:
                    rebuild.append(unit)
        return _Plan(
            document=document,
            objects=(*objects, *aggregates.values()),
            aggregate_ids=tuple(aggregate_ids),
            rebuild=tuple(rebuild),
        )

    @staticmethod
    async def _unsupported_aggregates(
        transaction: MetadataTransactionABC, scope: Scope, uid: str
    ) -> list[str]:
        """Aggregates the document once supported that no document supports any more."""
        candidates = sorted(
            {
                edge.from_id
                for edge in await transaction.get_lineage(scope.key, uid)
                if edge.relation == LineageRelation.SUPPORTED_BY and edge.to_id == uid
            }
        )
        if not candidates:
            return []
        objects = await transaction.get_objects(scope.key, candidates)
        supported = {
            contribution.object_id
            for contribution in await transaction.get_contributions(scope.key, candidates)
            if contribution.document_uid != uid
        }
        return [
            object_id
            for object_id, obj in objects.items()
            if obj.state != ObjectState.LIVE and object_id not in supported
        ]

    async def _remove_from_backends(self, scope: Scope, plan: _Plan) -> None:
        for unit in self._units:
            object_ids = sorted({obj.object_id for obj in plan.objects if obj.kind in unit.kinds})
            if not object_ids:
                continue
            async with self._indexer.lock(scope, unit):
                async with self._store.transaction(scope, write=False) as transaction:
                    generations = [
                        generation.generation_id
                        for generation in await transaction.list_generations(
                            scope_key=scope.key,
                            states=_LIVE_GENERATIONS,
                            instances=[unit.instance or None],
                        )
                        if generation.channel == unit.channel and generation.method == unit.method
                    ]
                for generation_id in generations:
                    await unit.backend.remove(scope, generation_id, object_ids)

    async def _queue_rebuild(self, scope: Scope, units: Sequence[IndexUnit]) -> str:
        now = self._clock()
        task = LifecycleTask(
            task_id=random_id("task"),
            scope_key=scope.key,
            kind=TaskKind.REBUILD,
            target={"units": [unit.name for unit in units]},
            state=TaskState.QUEUED,
            created_at=now,
            updated_at=now,
        )
        async with self._store.transaction(scope, write=True) as transaction:
            await transaction.put_task(task, expected_revision=None)
        self._logger.info(
            "lifecycle_task_queued",
            scope=scope.key,
            task_id=task.task_id,
            kind=TaskKind.REBUILD.value,
            units=[unit.name for unit in units],
        )
        return task.task_id

    async def _delete_rows(self, scope: Scope, plan: _Plan) -> None:
        document = plan.document
        async with self._store.transaction(scope, write=True) as transaction:
            current = (
                await transaction.get_documents_by_uid(scope.key, [document.document_uid])
            ).get(document.document_uid)
            if current is None:
                return
            if current.row_revision != document.row_revision:
                raise VersionConflictError(
                    "document changed during purge",
                    details={"document_id": document.document_id},
                )
            await transaction.purge_document(
                scope.key, document.document_uid, object_ids=plan.aggregate_ids
            )


__all__ = ["DocumentEraser"]
