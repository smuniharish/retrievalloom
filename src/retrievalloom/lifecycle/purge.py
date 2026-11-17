"""Physical erasure of documents (right to erasure) after tombstoning."""

from __future__ import annotations

import asyncio
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
    from collections.abc import Collection, Sequence
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

    Documents are erased in batches of ``batch_size``: one read transaction plans the
    batch, each live generation of each unit gets one ``remove`` call for the batch's
    objects (and graph aggregates left without support), a rebuild of whole-scope units
    (graph) that still include a document is queued, and one write transaction deletes
    every metadata row of the batch. A backend failure aborts before any metadata row is
    deleted, so a document is never reported as purged while a backend may still hold
    it. Documents changed concurrently are re-planned and retried on their own.

    The write transaction also appends one purge-log entry per document. Other engines
    sharing the metadata store follow the log (:meth:`evict_purged`) and remove the
    purged objects from their engine-local (in-memory) units.
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
        batch_size: int = 500,
    ) -> None:
        if batch_size < 1:
            raise ValueError("batch_size must be at least 1")
        self._store = store
        self._ingestion = ingestion
        self._indexer = indexer
        self._units = tuple(units)
        self._clock = clock
        self._logger = logger
        self._batch_size = batch_size
        self._watermarks: dict[str, int] | None = None
        self._following = asyncio.Lock()

    async def purge(self, document_ids: Sequence[str], scope: Scope) -> PurgeReport:
        """Tombstone (when needed) and physically erase documents of ``scope``."""
        deleted = await self._ingestion.delete(document_ids, scope)
        purged = [*deleted.deleted, *deleted.already_deleted]
        task_ids: list[str] = []
        for start in range(0, len(purged), self._batch_size):
            await self._erase(scope, purged[start : start + self._batch_size], task_ids)
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
        """Erase documents tombstoned before ``deleted_before`` whose coverage is deleted.

        Only coverage of the configured units counts: a unit that is not configured is
        never processed, so its rows would block erasure forever. Durable units that are
        configured again later catch up with the purge log (``UnitIndexer``).
        """
        purged = 0
        task_ids: list[str] = []
        methods = sorted({(unit.channel.value, unit.method) for unit in self._units})
        while True:
            async with self._store.transaction(scope, write=False) as transaction:
                page = await transaction.list_purgeable_documents(
                    scope.key, deleted_before=deleted_before, limit=limit, methods=methods
                )
            await self._erase(scope, [document.document_id for document in page], task_ids)
            purged += len(page)
            if len(page) < limit:
                break
        if purged:
            self._logger.info("tombstones_purged", scope=scope.key, purged=purged)
        return purged

    # erasure ----------------------------------------------------------------------------
    async def _erase(self, scope: Scope, document_ids: Sequence[str], task_ids: list[str]) -> None:
        pending = list(document_ids)
        for _ in range(_CONFLICT_RETRIES):
            plans = await self._plan(scope, pending)
            if not plans:
                return
            await self._remove_from_backends(scope, plans)
            rebuild = {unit.name: unit for plan in plans for unit in plan.rebuild}
            if rebuild and not task_ids:
                task_ids.append(await self._queue_rebuild(scope, list(rebuild.values())))
            try:
                pending = await self._delete_rows(scope, plans)
            except VersionConflictError:
                pending = [plan.document.document_id for plan in plans]
            if not pending:
                return
        raise VersionConflictError(
            "documents kept changing during purge", details={"document_ids": pending}
        )

    async def _plan(self, scope: Scope, document_ids: Sequence[str]) -> list[_Plan]:
        async with self._store.transaction(scope, write=False) as transaction:
            found = await transaction.get_documents(scope, document_ids)
            documents: list[DocumentRecord] = []
            for document_id in dict.fromkeys(document_ids):
                document = found.get(document_id)
                if document is None:
                    continue  # purged meanwhile
                if document.state != DocumentState.DELETED:
                    raise LifecycleError(
                        "document was re-ingested during purge",
                        details={"document_id": document_id},
                    )
                documents.append(document)
            if not documents:
                return []
            uids = [document.document_uid for document in documents]
            owned = await transaction.list_objects_for_documents(
                scope.key, [(uid, None) for uid in uids]
            )
            batch = set(uids)
            aggregate_ids = await self._unsupported_aggregates(transaction, scope, uids, batch)
            aggregates = await transaction.get_objects(
                scope.key, sorted({oid for ids in aggregate_ids.values() for oid in ids})
            )
            rebuild: dict[str, list[IndexUnit]] = {uid: [] for uid in uids}
            for unit in self._units:
                if unit.per_document:
                    continue
                for uid, row in (await transaction.get_coverage(unit.key(scope), uids)).items():
                    if row.state != CoverageState.DELETED:
                        rebuild[uid].append(unit)
        return [
            _Plan(
                document=document,
                objects=(
                    *owned.get(document.document_uid, ()),
                    *(aggregates[oid] for oid in aggregate_ids[document.document_uid]),
                ),
                aggregate_ids=tuple(aggregate_ids[document.document_uid]),
                rebuild=tuple(rebuild[document.document_uid]),
            )
            for document in documents
        ]

    @staticmethod
    async def _unsupported_aggregates(
        transaction: MetadataTransactionABC,
        scope: Scope,
        uids: Sequence[str],
        batch: Collection[str],
    ) -> dict[str, list[str]]:
        """Per document: aggregates it supported that no document outside ``batch`` supports."""
        candidates: dict[str, set[str]] = {uid: set() for uid in uids}
        for edge in await transaction.get_lineage_many(scope.key, uids):
            if edge.relation == LineageRelation.SUPPORTED_BY and edge.to_id in candidates:
                candidates[edge.to_id].add(edge.from_id)
        every = sorted({oid for ids in candidates.values() for oid in ids})
        if not every:
            return {uid: [] for uid in uids}
        objects = await transaction.get_objects(scope.key, every)
        supported = {
            contribution.object_id
            for contribution in await transaction.get_contributions(scope.key, every)
            if contribution.document_uid not in batch
        }
        unsupported = {
            object_id
            for object_id, obj in objects.items()
            if obj.state != ObjectState.LIVE and object_id not in supported
        }
        return {uid: sorted(candidates[uid] & unsupported) for uid in uids}

    async def _remove_from_backends(self, scope: Scope, plans: Sequence[_Plan]) -> None:
        for unit in self._units:
            object_ids = sorted(
                {obj.object_id for plan in plans for obj in plan.objects if obj.kind in unit.kinds}
            )
            if object_ids:
                await self._remove(scope, unit, object_ids)

    async def _remove(self, scope: Scope, unit: IndexUnit, object_ids: Sequence[str]) -> None:
        """Remove objects from every generation of ``unit`` that may still serve them."""
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

    async def _delete_rows(self, scope: Scope, plans: Sequence[_Plan]) -> list[str]:
        """Delete the rows of every unchanged planned document; return the changed ids."""
        async with self._store.transaction(scope, write=True) as transaction:
            current = await transaction.get_documents_by_uid(
                scope.key, [plan.document.document_uid for plan in plans]
            )
            ready: list[_Plan] = []
            changed: list[_Plan] = []
            for plan in plans:
                row = current.get(plan.document.document_uid)
                if row is None:
                    continue  # purged meanwhile
                if row.row_revision == plan.document.row_revision:
                    ready.append(plan)
                else:
                    changed.append(plan)
            # An aggregate shared with a changed document is left to that document's retry.
            contested = {oid for plan in changed for oid in plan.aggregate_ids}
            if ready:
                await transaction.purge_documents(
                    scope.key,
                    {
                        plan.document.document_uid: [
                            oid for oid in plan.aggregate_ids if oid not in contested
                        ]
                        for plan in ready
                    },
                    purged_at=self._clock(),
                )
        return [plan.document.document_id for plan in changed]

    # eviction on other engines ----------------------------------------------------------
    async def follow_purge_log(self) -> None:
        """Start following the purge log from its current end.

        Engine-local units of a starting engine are empty and built from the metadata
        store afterwards, so erasures logged before this call need no eviction.
        """
        async with self._following, self._store.transaction(write=False) as transaction:
            self._watermarks = dict(await transaction.purge_log_heads())

    async def evict_purged(self) -> int:
        """Remove objects purged since the last call from this engine's engine-local units.

        Durable units are shared and were cleaned by the purging engine. When entries
        this engine has not applied were pruned from the log, its engine-local units of
        that scope are rebuilt from the metadata store instead. Starts following the log
        when :meth:`follow_purge_log` was not called. Returns the entries applied.
        """
        units = [unit for unit in self._units if unit.instance]
        if not units:
            return 0
        async with self._following:
            watermarks = self._watermarks
            async with self._store.transaction(write=False) as transaction:
                heads = await transaction.purge_log_heads()
                if watermarks is None:
                    self._watermarks = dict(heads)
                    return 0
                behind = sorted(key for key, head in heads.items() if head > watermarks.get(key, 0))
                if not behind:
                    return 0
                scopes = {scope.key: scope for scope in await transaction.list_scopes()}
            applied = 0
            for scope_key in behind:
                applied += await self._follow(scopes[scope_key], units, watermarks)
            return applied

    async def _follow(
        self, scope: Scope, units: Sequence[IndexUnit], watermarks: dict[str, int]
    ) -> int:
        applied = 0
        while True:
            mark = watermarks.get(scope.key, 0)
            async with self._store.transaction(scope, write=False) as transaction:
                entries = await transaction.list_purge_log(
                    scope.key, after_seq=mark, limit=self._batch_size
                )
            if not entries:
                return applied
            if entries[0].seq != mark + 1:
                self._logger.warning(
                    "purge_log_entries_missed",
                    scope=scope.key,
                    after_seq=mark,
                    next_seq=entries[0].seq,
                )
                for unit in units:
                    await self._indexer.rebuild(scope, unit)
            object_ids = sorted({oid for entry in entries for oid in entry.object_ids})
            for unit in units:
                await self._remove(scope, unit, object_ids)
            watermarks[scope.key] = entries[-1].seq
            applied += len(entries)
            self._logger.info(
                "purged_objects_evicted",
                scope=scope.key,
                documents=len(entries),
                objects=len(object_ids),
            )


__all__ = ["DocumentEraser"]
