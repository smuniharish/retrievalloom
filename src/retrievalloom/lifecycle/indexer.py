"""Unit indexer: builds, publishes, updates and retires backend generations."""

from __future__ import annotations

import asyncio
from collections import defaultdict
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any

from retrievalloom.domain.base import utc_now
from retrievalloom.domain.hashing import random_id
from retrievalloom.domain.records import (
    CoverageRecord,
    CoverageState,
    DocumentState,
    ErrorInfo,
    GenerationState,
    IndexGeneration,
    ObjectKind,
    ObjectState,
    TaskKind,
    TaskState,
)
from retrievalloom.lifecycle.units import DICTIONARY_DIGEST, dictionary_digest

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Sequence

    from retrievalloom.config import LifecycleConfig
    from retrievalloom.domain.base import Clock
    from retrievalloom.domain.inputs import Scope
    from retrievalloom.domain.records import DerivedObject, DocumentRecord, LifecycleTask
    from retrievalloom.lifecycle.units import IndexUnit, ItemPreparer
    from retrievalloom.storage.base import (
        DocumentMetadataStoreABC,
        MetadataTransactionABC,
        PointerRow,
    )

_WORK_STATES = (CoverageState.PENDING, CoverageState.STALE)
_READABLE = frozenset({GenerationState.ACTIVE, GenerationState.RETIRING})


class ReaderTracker:
    """Tracks readers of generations so retirement never drops a generation in use.

    In-process readers are counted. Generations of durable backends, which other engines
    sharing the metadata store may read too, are also protected by a durable reader lease
    per engine instance: :meth:`lease` refreshes it when less than half of ``lease_ttl``
    remains, so a reader holding a lease can rely on it for at least ``lease_ttl / 2``
    seconds. Retirement refuses while any other instance holds an unexpired lease.
    """

    def __init__(self, *, lease_ttl: float = 120.0, clock: Clock = utc_now) -> None:
        self._readers: dict[str, int] = defaultdict(int)
        self._retiring: set[str] = set()
        self._idle: dict[str, asyncio.Event] = {}
        self._lease_ttl = timedelta(seconds=lease_ttl)
        self._clock = clock
        self._leased_until: dict[str, datetime] = {}

    def try_acquire(self, generation_id: str) -> bool:
        """Register a reader; ``False`` when the generation is being retired."""
        if generation_id in self._retiring:
            return False
        self._readers[generation_id] += 1
        self._idle.setdefault(generation_id, asyncio.Event()).clear()
        return True

    def release(self, generation_id: str) -> None:
        """Release a reader registered with :meth:`try_acquire`."""
        self._readers[generation_id] -= 1
        if self._readers[generation_id] <= 0:
            del self._readers[generation_id]
            self._idle.pop(generation_id, asyncio.Event()).set()

    @asynccontextmanager
    async def reading(self, generation_id: str) -> AsyncIterator[bool]:
        """Context form of acquire/release; yields whether the generation was acquired."""
        acquired = self.try_acquire(generation_id)
        try:
            yield acquired
        finally:
            if acquired:
                self.release(generation_id)

    def mark_retiring(self, generation_id: str) -> None:
        """Refuse new readers of ``generation_id``."""
        self._retiring.add(generation_id)

    def forget(self, generation_id: str) -> None:
        """Drop bookkeeping of a dropped generation."""
        self._retiring.discard(generation_id)
        self._leased_until.pop(generation_id, None)

    def readers(self, generation_id: str) -> int:
        """Current reader count."""
        return self._readers.get(generation_id, 0)

    async def lease(
        self,
        store: DocumentMetadataStoreABC,
        scope: Scope,
        generation_id: str,
        *,
        instance_id: str,
    ) -> bool:
        """Hold a durable reader lease on a durable generation; ``False`` once retired."""
        now = self._clock()
        until = self._leased_until.get(generation_id)
        if until is not None and until - now > self._lease_ttl / 2:
            return True
        expires_at = now + self._lease_ttl
        async with store.transaction(scope, write=True) as transaction:
            generation = await transaction.get_generation(generation_id)
            if generation is None or generation.state not in _READABLE:
                return False
            await transaction.put_reader_lease(
                generation_id, instance_id, expires_at=expires_at, now=now
            )
        self._leased_until[generation_id] = expires_at
        return True


class UnitIndexer:
    """Brings one scope's index units up to date with the metadata store.

    Per-document units with in-place mutable backends are updated incrementally; every
    other change (immutable backends, the graph, dependency or keyword dictionary
    changes, rebuild requests) produces a new generation built from the metadata store,
    published by pointer CAS.
    """

    def __init__(
        self,
        store: DocumentMetadataStoreABC,
        preparer: ItemPreparer,
        config: LifecycleConfig,
        *,
        instance_id: str,
        clock: Clock,
        logger: Any,
        readers: ReaderTracker,
    ) -> None:
        self._store = store
        self._preparer = preparer
        self._config = config
        self._instance_id = instance_id
        self._clock = clock
        self._logger = logger
        self._readers = readers
        self._locks: dict[tuple[str, str], asyncio.Lock] = defaultdict(asyncio.Lock)

    # inspection -----------------------------------------------------------------------
    async def needs_work(self, scope: Scope, unit: IndexUnit) -> bool:
        """Whether ``unit`` lags behind the metadata store in ``scope``."""
        key = unit.key(scope)
        async with self._store.transaction(scope, write=False) as transaction:
            pointer = await transaction.get_pointer(key)
            if pointer is None:
                return True
            generation = await transaction.get_generation(pointer.generation_id)
            if generation is None or generation.dependency_fingerprint != unit.fingerprint:
                return True
            if await transaction.list_coverage(key, _WORK_STATES, limit=1):
                return True
            if await transaction.documents_missing_coverage(key, limit=1):
                return True
            return bool(await self._rebuild_tasks(transaction, scope, unit))

    @staticmethod
    async def _rebuild_tasks(
        transaction: MetadataTransactionABC, scope: Scope, unit: IndexUnit
    ) -> list[str]:
        tasks = await transaction.list_tasks(
            scope_key=scope.key, states=(TaskState.QUEUED, TaskState.RUNNING)
        )
        return [
            task.task_id
            for task in tasks
            if task.kind == TaskKind.REBUILD
            and unit.name in task_units(task)
            and unit.name not in rebuilt_units(task)
        ]

    def lock(self, scope: Scope, unit: IndexUnit) -> asyncio.Lock:
        """The in-process lock serialising work on ``unit`` in ``scope``."""
        return self._locks[(scope.key, f"{unit.name}:{unit.instance}")]

    # run ------------------------------------------------------------------------------
    async def run(self, scope: Scope, unit: IndexUnit) -> None:
        """Process all outstanding work of ``unit`` in ``scope``."""
        async with self.lock(scope, unit):
            key = unit.key(scope)
            await self._create_missing_coverage(scope, unit)
            async with self._store.transaction(scope, write=False) as transaction:
                pointer = await transaction.get_pointer(key)
                generation = (
                    None
                    if pointer is None
                    else await transaction.get_generation(pointer.generation_id)
                )
                rebuild = bool(await self._rebuild_tasks(transaction, scope, unit))
            if pointer is not None and unit.per_document:
                await self._remove_deleted(scope, unit, pointer.generation_id)
            full = (
                rebuild
                or pointer is None
                or generation is None
                or generation.dependency_fingerprint != unit.fingerprint
                or (not unit.incremental and await self._has_pending(scope, unit))
                or not await self._same_dictionary(scope, unit, generation)
            )
            if full:
                await self._full_build(scope, unit, pointer)
            else:
                assert pointer is not None
                await self._incremental(scope, unit, pointer.generation_id)

    async def _has_pending(self, scope: Scope, unit: IndexUnit) -> bool:
        async with self._store.transaction(scope, write=False) as transaction:
            return bool(await transaction.list_coverage(unit.key(scope), _WORK_STATES, limit=1))

    async def _same_dictionary(
        self, scope: Scope, unit: IndexUnit, generation: IndexGeneration
    ) -> bool:
        """Whether the keyword dictionary still equals the one ``generation`` was built with.

        A dictionary change (e.g. new keywords in document metadata) affects objects that
        did not change, so it needs a full build rather than an in-place update.
        """
        spec = await self._preparer.spec(unit, scope, generation.generation_id)
        return generation.stats.get(DICTIONARY_DIGEST) == dictionary_digest(spec)

    async def _create_missing_coverage(self, scope: Scope, unit: IndexUnit) -> None:
        key = unit.key(scope)
        while True:
            async with self._store.transaction(scope, write=True) as transaction:
                missing = await transaction.documents_missing_coverage(
                    key, limit=self._config.batch_size
                )
                if not missing:
                    return
                now = self._clock()
                await transaction.upsert_coverage(
                    [
                        CoverageRecord(
                            scope_key=scope.key,
                            document_uid=document.document_uid,
                            channel=unit.channel,
                            method=unit.method,
                            backend_instance_id=unit.instance,
                            target_version=document.version,
                            state=CoverageState.PENDING,
                            updated_at=now,
                        )
                        for document in missing
                    ]
                )
            if len(missing) < self._config.batch_size:
                return

    async def _remove_deleted(self, scope: Scope, unit: IndexUnit, generation_id: str) -> None:
        """Remove every object of deleted documents from the active generation."""
        key = unit.key(scope)
        while True:
            async with self._store.transaction(scope, write=False) as transaction:
                rows = await transaction.list_coverage(key, _WORK_STATES, limit=10_000)
                documents = await transaction.get_documents_by_uid(
                    scope.key, [row.document_uid for row in rows]
                )
                deleted = [
                    row
                    for row in rows
                    if (document := documents.get(row.document_uid)) is None
                    or document.state == DocumentState.DELETED
                ][: self._config.batch_size]
                listed = await transaction.list_objects_for_documents(
                    scope.key, [(row.document_uid, None) for row in deleted], unit.kinds
                )
                object_ids = [obj.object_id for row in deleted for obj in listed[row.document_uid]]
            if not deleted:
                return
            if object_ids:
                await unit.backend.remove(scope, generation_id, object_ids)
            async with self._store.transaction(scope, write=True) as transaction:
                now = self._clock()
                current = await transaction.get_documents_by_uid(
                    scope.key, [row.document_uid for row in deleted]
                )
                await transaction.upsert_coverage(
                    [
                        row.model_copy(
                            update={
                                "state": CoverageState.DELETED,
                                "generation_id": generation_id,
                                "updated_at": now,
                            }
                        )
                        for row in deleted
                        # A document revived meanwhile stays pending; one purged meanwhile
                        # has no rows left, and writing one back would undo the purge.
                        if (document := current.get(row.document_uid)) is not None
                        and document.state == DocumentState.DELETED
                    ]
                )

    # full build -----------------------------------------------------------------------
    async def _full_build(self, scope: Scope, unit: IndexUnit, pointer: PointerRow | None) -> None:
        now = self._clock()
        generation_id = random_id("gen")
        async with self._store.transaction(scope, write=True) as transaction:
            watermark = await transaction.current_commit_seq()
            generation = IndexGeneration(
                generation_id=generation_id,
                scope_key=scope.key,
                channel=unit.channel,
                method=unit.method,
                backend_id=unit.backend.info.name,
                backend_instance_id=unit.instance or None,
                dependency_fingerprint=unit.fingerprint,
                build_watermark_seq=watermark,
                state=GenerationState.BUILDING,
                owner_id=self._instance_id,
                created_at=now,
                heartbeat_at=now,
                predecessor_id=None if pointer is None else pointer.generation_id,
            )
            await transaction.put_generation(generation, expected_revision=None)
            rebuild_tasks = await self._rebuild_tasks(transaction, scope, unit)
        self._logger.info(
            "generation_build_started",
            scope=scope.key,
            unit=unit.name,
            generation_id=generation_id,
            watermark=watermark,
        )
        try:
            spec = await self._preparer.spec(unit, scope, generation_id)
            await unit.backend.create_generation(scope, spec)
            included = await self._write_all(scope, unit, generation_id)
            stats = await unit.backend.seal(scope, generation_id)
            details = stats.model_dump(mode="json")
            if (digest := dictionary_digest(spec)) is not None:
                details[DICTIONARY_DIGEST] = digest
            retired = await self._publish(
                scope,
                unit,
                generation.model_copy(update={"row_revision": 1}),
                pointer,
                watermark,
                included,
                details,
                rebuild_tasks,
            )
        except BaseException as exc:
            await self._fail_generation(scope, unit, generation_id, exc)
            raise
        self._logger.info(
            "generation_published",
            scope=scope.key,
            unit=unit.name,
            generation_id=generation_id,
            objects=stats.object_count,
        )
        if retired is not None:
            await self.retire(scope, unit, retired)

    async def _write_all(
        self, scope: Scope, unit: IndexUnit, generation_id: str
    ) -> dict[str, set[int]]:
        """Write every current object of ``unit``'s kinds; return included versions per uid."""
        included: dict[str, set[int]] = defaultdict(set)
        batch = min(self._config.batch_size, unit.backend.capabilities.max_batch)
        for kind in unit.kinds:
            after: str | None = None
            while True:
                async with self._store.transaction(scope, write=False) as transaction:
                    objects = await transaction.list_current_objects(
                        scope.key, (kind,), after_object_id=after, limit=batch
                    )
                if not objects:
                    break
                await self._write(scope, unit, generation_id, objects)
                for obj in objects:
                    for uid, version in _supporting_versions(obj):
                        included[uid].add(version)
                after = objects[-1].object_id
        return included

    async def _write(
        self, scope: Scope, unit: IndexUnit, generation_id: str, objects: Sequence[DerivedObject]
    ) -> None:
        items = await self._preparer.items(unit, objects, tenant=scope.tenant_id)
        await unit.backend.write(scope, generation_id, items)

    async def _publish(
        self,
        scope: Scope,
        unit: IndexUnit,
        generation: IndexGeneration,
        pointer: PointerRow | None,
        watermark: int,
        included: dict[str, set[int]],
        stats: dict[str, Any],
        rebuild_tasks: list[str],
    ) -> str | None:
        """Swap the pointer and update visibility and coverage in one transaction."""
        key = unit.key(scope)
        now = self._clock()
        async with self._store.transaction(scope, write=True) as transaction:
            await transaction.set_pointer(
                key,
                generation.generation_id,
                expected_revision=None if pointer is None else pointer.row_revision,
            )
            await transaction.put_generation(
                generation.model_copy(
                    update={"state": GenerationState.ACTIVE, "published_at": now, "stats": stats}
                ),
                expected_revision=generation.row_revision,
            )
            retired: str | None = None
            if pointer is not None:
                old = await transaction.get_generation(pointer.generation_id)
                if old is not None and old.state == GenerationState.ACTIVE:
                    await transaction.put_generation(
                        old.model_copy(
                            update={"state": GenerationState.RETIRING, "retired_at": now}
                        ),
                        expected_revision=old.row_revision,
                    )
                    retired = old.generation_id
            seq = await transaction.next_commit_seq()
            after: str | None = None
            while True:
                documents = await transaction.list_documents(
                    scope.key, after_id=after, limit=self._config.batch_size
                )
                if not documents:
                    break
                after = documents[-1].document_id
                coverage = await transaction.get_coverage(
                    key, [document.document_uid for document in documents]
                )
                updates: list[CoverageRecord] = []
                ready: list[tuple[str, int]] = []
                for document in documents:
                    row = coverage.get(document.document_uid)
                    state = self._published_state(document, included, watermark)
                    if state is None:
                        continue
                    if state == CoverageState.READY:
                        ready.append((document.document_uid, document.version))
                    updates.append(
                        CoverageRecord(
                            scope_key=scope.key,
                            document_uid=document.document_uid,
                            channel=unit.channel,
                            method=unit.method,
                            backend_instance_id=unit.instance,
                            generation_id=generation.generation_id,
                            target_version=document.version,
                            indexed_version=document.version
                            if state == CoverageState.READY
                            else None,
                            indexed_metadata_revision=document.metadata_revision
                            if state == CoverageState.READY
                            else None,
                            dependency_fingerprint=unit.fingerprint,
                            state=state,
                            attempts=0,
                            updated_at=now,
                            row_revision=0 if row is None else row.row_revision,
                        )
                    )
                await transaction.open_visibility_many(key, ready, seq)
                await transaction.upsert_coverage(updates)
            for task_id in rebuild_tasks:
                task = await transaction.get_task(task_id)
                if task is not None and task.state in {TaskState.QUEUED, TaskState.RUNNING}:
                    rebuilt = sorted({*rebuilt_units(task), unit.name})
                    done = set(task_units(task)) <= set(rebuilt)
                    await transaction.put_task(
                        task.model_copy(
                            update={
                                "state": TaskState.SUCCEEDED if done else TaskState.RUNNING,
                                "cursor": {**task.cursor, "rebuilt": rebuilt},
                                "updated_at": now,
                            }
                        ),
                        expected_revision=task.row_revision,
                    )
        return retired

    @staticmethod
    def _published_state(
        document: DocumentRecord, included: dict[str, set[int]], watermark: int
    ) -> CoverageState | None:
        """Coverage state after publishing; ``None`` leaves the row untouched."""
        current = document.change_seq <= watermark
        if document.state == DocumentState.DELETED:
            return CoverageState.DELETED if current else None
        if document.version in included.get(document.document_uid, ()):
            # Objects of the current version are in the generation; a change after the
            # watermark may still be missing from them, so the row stays pending.
            return CoverageState.READY if current else CoverageState.PENDING
        return CoverageState.NOT_APPLICABLE if current else None

    async def _fail_generation(
        self, scope: Scope, unit: IndexUnit, generation_id: str, exc: BaseException
    ) -> None:
        self._logger.warning(
            "generation_build_failed",
            scope=scope.key,
            unit=unit.name,
            generation_id=generation_id,
            error_type=type(exc).__name__,
        )
        try:
            async with self._store.transaction(scope, write=True) as transaction:
                generation = await transaction.get_generation(generation_id)
                if generation is not None and generation.state == GenerationState.BUILDING:
                    await transaction.put_generation(
                        generation.model_copy(
                            update={
                                "state": GenerationState.FAILED,
                                "stats": {"error": _error(exc).model_dump(mode="json")},
                            }
                        ),
                        expected_revision=generation.row_revision,
                    )
            await unit.backend.drop_generation(scope, generation_id)
        except Exception as cleanup:
            self._logger.error(
                "generation_cleanup_failed",
                scope=scope.key,
                unit=unit.name,
                generation_id=generation_id,
                error_type=type(cleanup).__name__,
            )

    # incremental ----------------------------------------------------------------------
    async def _incremental(self, scope: Scope, unit: IndexUnit, generation_id: str) -> None:
        key = unit.key(scope)
        while True:
            async with self._store.transaction(scope, write=False) as transaction:
                rows = await transaction.list_coverage(
                    key, _WORK_STATES, limit=self._config.batch_size
                )
                documents = await transaction.get_documents_by_uid(
                    scope.key, [row.document_uid for row in rows]
                )
                active = [
                    (row, document)
                    for row in rows
                    if (document := documents.get(row.document_uid)) is not None
                    # Documents deleted meanwhile are handled by the next round.
                    and document.state == DocumentState.ACTIVE
                ]
                found = await transaction.list_objects_for_documents(
                    scope.key,
                    [(document.document_uid, document.version) for _, document in active],
                    unit.kinds,
                )
                work: list[tuple[CoverageRecord, DocumentRecord, list[DerivedObject]]] = [
                    (
                        row,
                        document,
                        [
                            obj
                            for obj in found[document.document_uid]
                            if obj.state == ObjectState.LIVE
                        ],
                    )
                    for row, document in active
                ]
            if not work:
                return
            try:
                await self._write(
                    scope, unit, generation_id, [obj for _, _, objects in work for obj in objects]
                )
            except BaseException as exc:
                await self._record_failure(scope, [row for row, _, _ in work], exc)
                raise
            stale = await self._commit_incremental(scope, unit, generation_id, work)
            if stale:
                await unit.backend.remove(scope, generation_id, stale)

    async def _commit_incremental(
        self,
        scope: Scope,
        unit: IndexUnit,
        generation_id: str,
        work: list[tuple[CoverageRecord, DocumentRecord, list[DerivedObject]]],
    ) -> list[str]:
        """Publish written documents; return object ids of their other versions."""
        key = unit.key(scope)
        stale: list[str] = []
        now = self._clock()
        async with self._store.transaction(scope, write=True) as transaction:
            current = await transaction.get_documents_by_uid(
                scope.key, [document.document_uid for _, document, _ in work]
            )
            seq = await transaction.next_commit_seq()
            updates: list[CoverageRecord] = []
            published: list[DocumentRecord] = []
            for row, document, objects in work:
                latest = current.get(document.document_uid)
                if latest is None or latest.change_seq != document.change_seq:
                    continue  # changed while writing; stays pending for the next pass
                published.append(document)
                state = CoverageState.READY if objects else CoverageState.NOT_APPLICABLE
                updates.append(
                    row.model_copy(
                        update={
                            "generation_id": generation_id,
                            "target_version": document.version,
                            "indexed_version": document.version,
                            "indexed_metadata_revision": document.metadata_revision,
                            "dependency_fingerprint": unit.fingerprint,
                            "state": state,
                            "attempts": 0,
                            "last_error": None,
                            "updated_at": now,
                        }
                    )
                )
            await transaction.open_visibility_many(
                key, [(document.document_uid, document.version) for document in published], seq
            )
            versions = await transaction.list_objects_for_documents(
                scope.key, [(document.document_uid, None) for document in published], unit.kinds
            )
            for document in published:
                stale.extend(
                    obj.object_id
                    for obj in versions[document.document_uid]
                    if obj.version != document.version
                )
            await transaction.upsert_coverage(updates)
        return stale

    async def _record_failure(
        self, scope: Scope, rows: Sequence[CoverageRecord], exc: BaseException
    ) -> None:
        if not isinstance(exc, Exception):
            return  # cancellation: leave rows pending
        now = self._clock()
        async with self._store.transaction(scope, write=True) as transaction:
            await transaction.upsert_coverage(
                [
                    row.model_copy(
                        update={
                            "attempts": row.attempts + 1,
                            "last_error": _error(exc),
                            "state": CoverageState.FAILED
                            if row.attempts + 1 >= self._config.max_attempts
                            else row.state,
                            "updated_at": now,
                        }
                    )
                    for row in rows
                ]
            )

    # retirement -----------------------------------------------------------------------
    async def retire(self, scope: Scope, unit: IndexUnit, generation_id: str) -> bool:
        """Drop a retiring generation once allowed; ``True`` when it was dropped.

        The generation is fenced (marked ``retired``) before its data is dropped, in the
        same transaction that verifies no other engine holds a reader lease on it, so a
        reader can never lease a generation that is being dropped. A failed drop leaves an
        orphan that :meth:`reconcile` removes.
        """
        async with self._store.transaction(scope, write=False) as transaction:
            generation = await transaction.get_generation(generation_id)
        if generation is None or generation.state != GenerationState.RETIRING:
            return False
        self._readers.mark_retiring(generation_id)
        retention = timedelta(seconds=self._config.generation_retention)
        retired_at = generation.retired_at or generation.created_at
        if self._readers.readers(generation_id) or self._clock() < retired_at + retention:
            return False
        async with self._store.transaction(scope, write=True) as transaction:
            current = await transaction.get_generation(generation_id)
            if current is None or current.state != GenerationState.RETIRING:
                return False
            if not unit.instance:
                await transaction.delete_reader_lease(generation_id, self._instance_id)
                if await transaction.count_reader_leases(generation_id, now=self._clock()):
                    return False
            await transaction.put_generation(
                current.model_copy(update={"state": GenerationState.RETIRED}),
                expected_revision=current.row_revision,
            )
        self._readers.forget(generation_id)
        await unit.backend.drop_generation(scope, generation_id)
        self._logger.info(
            "generation_retired", scope=scope.key, unit=unit.name, generation_id=generation_id
        )
        return True

    async def reconcile(self, scope: Scope, unit: IndexUnit) -> None:
        """Drop retired, abandoned and orphaned generations of ``unit``."""
        async with self.lock(scope, unit):
            await self._reconcile(scope, unit)

    async def _reconcile(self, scope: Scope, unit: IndexUnit) -> None:
        instances = [unit.instance or None]
        async with self._store.transaction(scope, write=False) as transaction:
            generations = [
                generation
                for generation in await transaction.list_generations(
                    scope_key=scope.key, instances=instances
                )
                if generation.channel == unit.channel and generation.method == unit.method
            ]
            expired = set(await transaction.expired_instances(now=self._clock()))
        for generation in generations:
            if generation.state == GenerationState.RETIRING:
                await self.retire(scope, unit, generation.generation_id)
            elif generation.state == GenerationState.BUILDING and generation.owner_id in expired:
                await self._abandon(scope, unit, generation)
        known = {generation.generation_id: generation.state for generation in generations}
        live = {GenerationState.BUILDING, GenerationState.ACTIVE, GenerationState.RETIRING}
        for generation_id in await unit.backend.list_generations(scope):
            state = known.get(generation_id)
            if state is None:
                # Another engine may have registered it after the listing above; its
                # metadata row is always written before the backend generation.
                async with self._store.transaction(scope, write=False) as transaction:
                    fresh = await transaction.get_generation(generation_id)
                state = None if fresh is None else fresh.state
            if state is None or state not in live:
                await unit.backend.drop_generation(scope, generation_id)
                self._logger.info(
                    "orphan_generation_dropped",
                    scope=scope.key,
                    unit=unit.name,
                    generation_id=generation_id,
                )

    async def _abandon(self, scope: Scope, unit: IndexUnit, generation: IndexGeneration) -> None:
        async with self._store.transaction(scope, write=True) as transaction:
            await transaction.put_generation(
                generation.model_copy(update={"state": GenerationState.ABANDONED}),
                expected_revision=generation.row_revision,
            )
        await unit.backend.drop_generation(scope, generation.generation_id)
        self._logger.info(
            "generation_abandoned",
            scope=scope.key,
            unit=unit.name,
            generation_id=generation.generation_id,
        )


def task_units(task: LifecycleTask) -> tuple[str, ...]:
    """Unit names targeted by a lifecycle task."""
    units = task.target.get("units")
    return tuple(str(name) for name in units) if isinstance(units, list) else ()


def rebuilt_units(task: LifecycleTask) -> tuple[str, ...]:
    """Unit names a REBUILD task has already published a new generation for."""
    units = task.cursor.get("rebuilt")
    return tuple(str(name) for name in units) if isinstance(units, list) else ()


def _supporting_versions(obj: DerivedObject) -> list[tuple[str, int]]:
    """``(document_uid, version)`` pairs whose content an object carries."""
    if obj.kind in {ObjectKind.GRAPH_ENTITY, ObjectKind.GRAPH_RELATIONSHIP}:
        return [(source.document_uid, source.document_version) for source in obj.sources]
    if obj.document_uid is None or obj.version is None:
        return []
    return [(obj.document_uid, obj.version)]


def _error(exc: BaseException) -> ErrorInfo:
    return ErrorInfo.from_exception(exc)


__all__ = ["ReaderTracker", "UnitIndexer"]
