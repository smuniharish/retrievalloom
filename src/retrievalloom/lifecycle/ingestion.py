"""Ingestion and deletion: identity, counters, versions, derived objects and coverage."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import timedelta
from typing import TYPE_CHECKING, Any

from retrievalloom.config import FailurePolicy
from retrievalloom.contracts.models import ExtractionSource, GraphExtraction
from retrievalloom.domain.records import (
    CoverageState,
    DocumentRecord,
    DocumentState,
    DocumentVersion,
    ErrorInfo,
    EventStatus,
    IngestionEvent,
    IngestOutcome,
    LineageEdge,
    LineageRelation,
    ObjectState,
    RepresentationSnapshot,
)
from retrievalloom.domain.reports import DeleteReport, DocumentIngestResult, IngestReport
from retrievalloom.domain.retrieval import RetrievalWarning, Severity, WarningCode
from retrievalloom.errors import RetrievalLoomError, ValidationError, VersionConflictError
from retrievalloom.lifecycle.canonical import (
    CanonicalDocument,
    canonicalize,
    document_uid,
    graph_representation,
    index_representation,
    structure_representation,
)
from retrievalloom.lifecycle.derivation import (
    derive_chunks,
    derive_structure,
    graph_contributions,
    lineage_for,
    recompute_aggregates,
)

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence
    from typing import Literal

    from retrievalloom.config import IngestionConfig
    from retrievalloom.contracts.models import (
        GraphExtractorABC,
        GraphNormalizerABC,
        NormalizedGraph,
    )
    from retrievalloom.domain.base import Clock
    from retrievalloom.domain.inputs import DocumentInput, Scope
    from retrievalloom.domain.records import DerivedObject
    from retrievalloom.storage.base import DocumentMetadataStoreABC, MetadataTransactionABC

_CONFLICT_RETRIES = 5
# Documents per write transaction: large enough to amortise round trips, small enough
# not to hold the write lock (one writer on SQLite) for long.
_WRITE_CHUNK = 200


@dataclass(slots=True)
class _Outcome:
    result: DocumentIngestResult
    warnings: list[RetrievalWarning] = field(default_factory=list)
    changed: bool = False


@dataclass(slots=True)
class _Written:
    """A document written by a chunk transaction."""

    index: int
    canonical: CanonicalDocument
    graph: NormalizedGraph | None
    previous: DocumentRecord | None
    record: DocumentRecord
    outcome: IngestOutcome
    content_changed: bool
    changed: bool
    possible: str | None = None
    warnings: list[RetrievalWarning] = field(default_factory=list)


@dataclass(frozen=True, slots=True)
class GraphDerivation:
    """Graph inputs of the ingestion service (``None`` disables graph derivation).

    Attributes:
        normalizer: Assigns canonical keys.
        extractor: Optional extractor used when a document has no ``graph`` representation.
    """

    normalizer: GraphNormalizerABC
    extractor: GraphExtractorABC | None = None


class IngestionService:
    """Allocates counters and versions and derives objects in chunked write transactions."""

    def __init__(
        self,
        store: DocumentMetadataStoreABC,
        config: IngestionConfig,
        *,
        clock: Clock,
        logger: Any,
        graph: GraphDerivation | None = None,
    ) -> None:
        self._store = store
        self._config = config
        self._clock = clock
        self._logger = logger
        self._graph = graph

    # ingest ---------------------------------------------------------------------------
    async def ingest(self, documents: Sequence[DocumentInput], scope: Scope) -> IngestReport:
        """Ingest ``documents`` into ``scope``.

        Documents sharing an id (or an event id) are applied in submission order: the
        batch is split into rounds in which each id appears at most once. Within a round,
        graph extraction runs concurrently (bounded by ``IngestionConfig.concurrency``)
        and documents are written in chunks of up to ``_WRITE_CHUNK``, one write
        transaction per chunk. When a chunk fails, its documents are retried one
        transaction each, so a failing document never fails the others.

        Raises:
            ValidationError: When the batch is too large, or when ``failure_policy`` is
                ``fail_batch`` and any document is invalid.
        """
        limit = self._config.limits.max_documents_per_call
        if len(documents) > limit:
            raise ValidationError(
                "too many documents in one ingest call",
                details={"count": len(documents), "max": limit},
            )
        canonical: list[CanonicalDocument | RetrievalLoomError] = []
        for document in documents:
            try:
                canonical.append(canonicalize(document, scope, self._config))
            except RetrievalLoomError as exc:
                if self._config.failure_policy == FailurePolicy.FAIL_BATCH:
                    raise
                canonical.append(exc)
        outcomes: list[_Outcome | None] = [None] * len(documents)
        rounds: list[list[tuple[int, CanonicalDocument]]] = []
        last_round: dict[str, int] = {}
        for position, item in enumerate(canonical):
            if isinstance(item, RetrievalLoomError):
                outcomes[position] = _Outcome(
                    DocumentIngestResult(
                        document_id=documents[position].id,
                        outcome=IngestOutcome.REJECTED,
                        error=ErrorInfo.from_exception(item),
                    )
                )
                continue
            keys = (f"id:{item.document.id}", f"event:{item.event_id}")
            index = 1 + max(last_round.get(key, -1) for key in keys)
            last_round.update(dict.fromkeys(keys, index))
            if index == len(rounds):
                rounds.append([])
            rounds[index].append((position, item))
        for items in rounds:
            await self._ingest_round(items, outcomes)
        results = [outcome for outcome in outcomes if outcome is not None]
        warnings = [warning for outcome in results for warning in outcome.warnings]
        return IngestReport(
            scope=scope,
            documents=tuple(outcome.result for outcome in results),
            warnings=tuple(warnings),
        )

    async def _ingest_round(
        self, items: list[tuple[int, CanonicalDocument]], outcomes: list[_Outcome | None]
    ) -> None:
        """Ingest documents with distinct ids, filling ``outcomes`` by position."""
        graphs: dict[int, NormalizedGraph | None] = {}
        derivation = self._graph
        if derivation is None:
            graphs = dict.fromkeys((position for position, _ in items), None)
        else:
            semaphore = asyncio.Semaphore(self._config.concurrency)

            async def prepare(position: int, item: CanonicalDocument) -> None:
                async with semaphore:
                    try:
                        graphs[position] = await self._normalized_graph(item, derivation)
                    except RetrievalLoomError as exc:
                        outcomes[position] = self._failed(item, exc)

            async with asyncio.TaskGroup() as group:
                for position, item in items:
                    group.create_task(prepare(position, item))
        ready = [
            (position, item, graphs[position]) for position, item in items if position in graphs
        ]
        try:
            for start in range(0, len(ready), _WRITE_CHUNK):
                chunk = ready[start : start + _WRITE_CHUNK]
                written = await self._write([(item, graph) for _, item, graph in chunk])
                for (position, _, _), outcome in zip(chunk, written, strict=True):
                    outcomes[position] = outcome
        except Exception as exc:
            # Unexpected errors surface as a group, like failures of concurrent tasks.
            raise ExceptionGroup("ingest failed", [exc]) from None

    def _failed(self, canonical: CanonicalDocument, exc: RetrievalLoomError) -> _Outcome:
        document = canonical.document
        self._logger.warning(
            "ingest_failed",
            document_id=document.id,
            error_code=exc.code,
            scope=canonical.scope.key,
        )
        return _Outcome(
            DocumentIngestResult(
                document_id=document.id,
                event_id=canonical.event_id,
                outcome=IngestOutcome.FAILED,
                error=ErrorInfo.from_exception(exc),
            )
        )

    async def _normalized_graph(
        self, canonical: CanonicalDocument, derivation: GraphDerivation
    ) -> NormalizedGraph | None:
        """Normalised graph of the document, or ``None`` when it has no graph input."""
        document = canonical.document
        provided = graph_representation(document)
        if provided is not None:
            extraction = GraphExtraction(
                entities=provided.entities, relationships=provided.relationships
            )
        elif derivation.extractor is not None:
            bound = index_representation(document)
            if bound is None:
                return None
            async with self._store.transaction(canonical.scope, write=False) as transaction:
                current = await transaction.get_document(canonical.scope, document.id)
            if (
                current is not None
                and current.state == DocumentState.ACTIVE
                and current.content_hash == canonical.content_hash
            ):
                return None
            if current is None:
                version = 1
            elif current.content_hash == canonical.content_hash:
                version = current.version
            else:
                version = current.version + 1
            texts = (
                (bound.text,)
                if bound.kind == "text"
                else tuple(chunk.text for chunk in bound.chunks)
            )
            extraction = await derivation.extractor.extract(
                ExtractionSource(
                    scope=canonical.scope,
                    document_id=document.id,
                    document_uid=canonical.document_uid,
                    version=version,
                    representation_id=bound.representation_id,
                    texts=texts,
                    metadata=dict(document.metadata),
                )
            )
        else:
            return None
        return await derivation.normalizer.normalize(extraction, canonical.scope)

    def _graph_representation_id(self, document: DocumentInput) -> str:
        provided = graph_representation(document)
        if provided is not None:
            return provided.representation_id
        bound = index_representation(document)
        return bound.representation_id if bound is not None else "default"

    async def _write(
        self, entries: Sequence[tuple[CanonicalDocument, NormalizedGraph | None]]
    ) -> list[_Outcome]:
        """Write documents with distinct ids in one transaction, isolating failures."""
        if len(entries) > 1:
            try:
                return await self._write_chunk(entries)
            except RetrievalLoomError as exc:
                self._logger.debug(
                    "ingest_chunk_split",
                    scope=entries[0][0].scope.key,
                    documents=len(entries),
                    error_code=exc.code,
                )
        return [await self._write_one(canonical, graph) for canonical, graph in entries]

    async def _write_one(
        self, canonical: CanonicalDocument, graph: NormalizedGraph | None
    ) -> _Outcome:
        try:
            for attempt in range(_CONFLICT_RETRIES):
                try:
                    [outcome] = await self._write_chunk([(canonical, graph)])
                    return outcome
                except VersionConflictError:
                    if attempt == _CONFLICT_RETRIES - 1:
                        raise
                    await asyncio.sleep(0.01 * (attempt + 1))
        except RetrievalLoomError as exc:
            return self._failed(canonical, exc)
        raise AssertionError("unreachable")  # pragma: no cover

    async def _write_chunk(
        self, entries: Sequence[tuple[CanonicalDocument, NormalizedGraph | None]]
    ) -> list[_Outcome]:
        """Allocate counters and versions and derive objects of ``entries`` atomically."""
        scope = entries[0][0].scope
        outcomes: list[_Outcome | None] = [None] * len(entries)
        written: list[_Written] = []
        async with self._store.transaction(scope, write=True) as transaction:
            await transaction.ensure_scope(scope)
            events = await transaction.get_events(
                scope.key, [canonical.event_id for canonical, _ in entries]
            )
            fresh: list[int] = []
            for index, (canonical, _) in enumerate(entries):
                event = events.get(canonical.event_id)
                if event is not None and event.status == EventStatus.COMMITTED:
                    outcomes[index] = _Outcome(
                        DocumentIngestResult(
                            document_id=canonical.document.id,
                            event_id=event.event_id,
                            outcome=IngestOutcome.REPLAYED,
                            counter=event.counter_after,
                            version=event.version_after,
                            possible_redelivery_of=event.possible_redelivery_of,
                        )
                    )
                else:
                    fresh.append(index)
            if fresh:
                written = await self._allocate(
                    transaction, [(index, *entries[index]) for index in fresh]
                )
        for item in written:
            self._logger.debug(
                "document_ingested",
                scope=scope.key,
                document_uid=item.record.document_uid,
                outcome=item.outcome.value,
                version=item.record.version,
            )
            outcomes[item.index] = _Outcome(
                DocumentIngestResult(
                    document_id=item.record.document_id,
                    event_id=item.canonical.event_id,
                    outcome=item.outcome,
                    counter=item.record.counter,
                    version=item.record.version,
                    possible_redelivery_of=item.possible,
                    coverage={},
                ),
                item.warnings,
                item.changed,
            )
        return [outcome for outcome in outcomes if outcome is not None]

    async def _allocate(
        self,
        transaction: MetadataTransactionABC,
        entries: Sequence[tuple[int, CanonicalDocument, NormalizedGraph | None]],
    ) -> list[_Written]:
        """Write the next records of documents that are not replays."""
        scope = entries[0][1].scope
        now = self._clock()
        priors: Mapping[str, IngestionEvent] = {}
        probes = {
            canonical.appearance_fingerprint: canonical.event_id
            for _, canonical, _ in entries
            if canonical.event_tier == 3
        }
        if probes and self._config.redelivery_window_seconds > 0:
            priors = await transaction.find_events_by_fingerprints(
                scope.key,
                probes,
                since=now - timedelta(seconds=self._config.redelivery_window_seconds),
            )
        current = await transaction.get_documents(
            scope, [canonical.document.id for _, canonical, _ in entries]
        )
        seq = await transaction.next_commit_seq()
        written: list[_Written] = []
        for index, canonical, graph in entries:
            previous = current.get(canonical.document.id)
            record, outcome, content_changed, changed = self._next_record(canonical, previous, now)
            if changed:
                record = record.model_copy(update={"change_seq": seq})
            item = _Written(
                index, canonical, graph, previous, record, outcome, content_changed, changed
            )
            prior = priors.get(canonical.appearance_fingerprint)
            if prior is not None:
                item.possible = prior.event_id
                item.warnings.append(
                    RetrievalWarning(
                        code=WarningCode.POSSIBLE_REDELIVERY,
                        severity=Severity.INFO,
                        message="appearance matches a recent appearance; counted",
                        details={"document_id": canonical.document.id, "event_id": prior.event_id},
                    )
                )
            written.append(item)
        await transaction.put_documents(
            [
                (item.record, None if item.previous is None else item.previous.row_revision)
                for item in written
            ]
        )
        new_versions = [item for item in written if item.content_changed]
        await transaction.put_versions(
            scope.key,
            [
                DocumentVersion(
                    document_uid=item.record.document_uid,
                    version=item.record.version,
                    content_hash=item.record.content_hash,
                    hash_profile=item.record.hash_profile,
                    representation_digests=item.record.representation_digests,
                    metadata=item.record.metadata,
                    dataset=item.record.dataset,
                    source=item.record.source,
                    created_at=now,
                    event_id=item.canonical.event_id,
                )
                for item in new_versions
            ],
        )
        await transaction.put_representations(
            scope.key,
            [
                RepresentationSnapshot(
                    document_uid=item.record.document_uid,
                    version=item.record.version,
                    representation_id=representation.representation_id,
                    kind=representation.kind,
                    payload=item.canonical.payloads[representation.representation_id],
                    transformation=representation.transformation,
                    hash_profile=item.record.hash_profile,
                )
                for item in new_versions
                for representation in item.canonical.document.representations
            ],
        )
        changed = [item for item in written if item.changed]
        await self._derive(transaction, scope, changed)
        await transaction.set_documents_coverage_state(
            scope.key,
            {item.record.document_uid: item.record.version for item in changed},
            CoverageState.PENDING,
        )
        await transaction.put_events(
            [
                IngestionEvent(
                    event_id=item.canonical.event_id,
                    scope=scope,
                    document_id=item.record.document_id,
                    document_uid=item.record.document_uid,
                    received_at=now,
                    status=EventStatus.COMMITTED,
                    outcome=item.outcome,
                    counter_after=item.record.counter,
                    version_after=item.record.version,
                    content_hash=item.canonical.content_hash,
                    delivery=item.canonical.document.delivery,
                    appearance_fingerprint=item.canonical.appearance_fingerprint,
                    possible_redelivery_of=item.possible,
                )
                for item in written
            ]
        )
        return written

    def _next_record(
        self, canonical: CanonicalDocument, current: DocumentRecord | None, now: Any
    ) -> tuple[DocumentRecord, IngestOutcome, bool, bool]:
        """Return the next record, its outcome, whether content changed, whether anything did."""
        document = canonical.document
        fields: dict[str, Any] = {
            "metadata": dict(document.metadata),
            "dataset": document.dataset,
            "source": document.source,
            "representation_digests": canonical.representation_digests,
            "representation_kinds": canonical.representation_kinds,
            "content_hash": canonical.content_hash,
            "hash_profile": self._config.hash_profile,
            "updated_at": now,
        }
        if current is None:
            record = DocumentRecord.model_validate(
                {
                    "_id": document.id,
                    "_counter": 1,
                    "_version": 1,
                    "_content_hash": canonical.content_hash,
                    "_created_at": now,
                    "_updated_at": now,
                    "document_uid": canonical.document_uid,
                    "scope": canonical.scope,
                    "metadata": fields["metadata"],
                    "dataset": document.dataset,
                    "source": document.source,
                    "representation_digests": canonical.representation_digests,
                    "representation_kinds": canonical.representation_kinds,
                    "hash_profile": self._config.hash_profile,
                }
            )
            return record, IngestOutcome.CREATED, True, True
        content_changed = current.content_hash != canonical.content_hash
        update = {**fields, "counter": current.counter + 1}
        if content_changed:
            update["version"] = current.version + 1
        if current.state == DocumentState.DELETED:
            update.update(
                state=DocumentState.ACTIVE,
                deleted_at=None,
                lifecycle_revision=current.lifecycle_revision + 1,
            )
            return current.model_copy(update=update), IngestOutcome.REVIVED, content_changed, True
        if content_changed:
            return current.model_copy(update=update), IngestOutcome.UPDATED, True, True
        projection_changed = (
            current.metadata != document.metadata or current.source != document.source
        )
        if projection_changed:
            update["metadata_revision"] = current.metadata_revision + 1
        return (
            current.model_copy(update=update),
            IngestOutcome.UNCHANGED,
            False,
            projection_changed,
        )

    async def _derive(
        self, transaction: MetadataTransactionABC, scope: Scope, changed: Sequence[_Written]
    ) -> None:
        objects: list[DerivedObject] = []
        for item in changed:
            document = item.canonical.document
            bound = index_representation(document)
            if bound is not None:
                objects.extend(derive_chunks(item.record, bound))
            tree = structure_representation(document)
            if tree is not None:
                objects.extend(derive_structure(item.record, tree))
        if objects:
            await transaction.put_objects(objects)
            await transaction.put_lineage(lineage_for(objects))
        for item in changed:
            if item.previous is not None and item.previous.version != item.record.version:
                await transaction.set_document_objects_state(
                    scope.key,
                    item.record.document_uid,
                    ObjectState.SUPERSEDED,
                    exclude_version=item.record.version,
                )
        if self._graph is None:
            return
        affected: set[str] = set()
        for item in changed:
            previous = item.previous
            if (
                item.graph is None
                and previous is not None
                and previous.content_hash == item.record.content_hash
                and previous.state == DocumentState.ACTIVE
            ):
                continue  # unchanged content: contributions are already current
            contributions = (
                graph_contributions(
                    item.record,
                    self._graph_representation_id(item.canonical.document),
                    item.graph,
                )
                if item.graph is not None
                else []
            )
            affected.update(
                await transaction.replace_contributions(
                    scope.key, item.record.document_uid, contributions
                )
            )
        # Aggregates are recomputed from all contributions, so refreshing the union once
        # after every replacement equals refreshing after each document.
        await self.refresh_aggregates(transaction, scope, sorted(affected))

    @staticmethod
    async def refresh_aggregates(
        transaction: MetadataTransactionABC, scope: Scope, object_ids: Sequence[str]
    ) -> None:
        """Recompute graph aggregates from their contributions."""
        if not object_ids:
            return
        contributions = await transaction.get_contributions(scope.key, object_ids)
        documents = await transaction.get_documents_by_uid(
            scope.key, sorted({contribution.document_uid for contribution in contributions})
        )
        existing = await transaction.get_objects(scope.key, object_ids)
        aggregates = recompute_aggregates(scope, object_ids, contributions, documents, existing)
        if not aggregates:
            return
        await transaction.put_objects(aggregates)
        await transaction.put_lineage(
            [
                LineageEdge(
                    scope_key=scope.key,
                    from_id=aggregate.object_id,
                    to_id=source.document_uid,
                    relation=LineageRelation.SUPPORTED_BY,
                )
                for aggregate in aggregates
                if aggregate.state == ObjectState.LIVE
                for source in aggregate.sources
            ]
        )

    # delete ---------------------------------------------------------------------------
    async def delete(self, document_ids: Sequence[str], scope: Scope) -> DeleteReport:
        """Tombstone documents: close visibility, retract derived objects and contributions."""
        unique = list(dict.fromkeys(document_ids))
        attempt = 0
        while True:
            try:
                deleted, already, missing = await self._delete_batch(unique, scope)
                break
            except VersionConflictError:
                attempt += 1
                if attempt >= _CONFLICT_RETRIES:
                    raise
                await asyncio.sleep(0.01 * attempt)
        self._logger.info(
            "documents_deleted", scope=scope.key, deleted=len(deleted), not_found=len(missing)
        )
        return DeleteReport(
            scope=scope,
            deleted=tuple(deleted),
            already_deleted=tuple(already),
            not_found=tuple(missing),
        )

    async def _delete_batch(
        self, document_ids: list[str], scope: Scope
    ) -> tuple[list[str], list[str], list[str]]:
        outcomes: dict[str, list[str]] = {"deleted": [], "already": [], "missing": []}
        async with self._store.transaction(scope, write=True) as transaction:
            await transaction.ensure_scope(scope)
            for document_id in document_ids:
                state = await self._delete_one(transaction, scope, document_id)
                outcomes[state].append(document_id)
        return outcomes["deleted"], outcomes["already"], outcomes["missing"]

    async def _delete_one(
        self, transaction: MetadataTransactionABC, scope: Scope, document_id: str
    ) -> Literal["deleted", "already", "missing"]:
        current = await transaction.get_document(scope, document_id)
        if current is None:
            return "missing"
        if current.state == DocumentState.DELETED:
            return "already"
        now = self._clock()
        seq = await transaction.next_commit_seq()
        record = current.model_copy(
            update={
                "state": DocumentState.DELETED,
                "deleted_at": now,
                "lifecycle_revision": current.lifecycle_revision + 1,
                "change_seq": seq,
            }
        )
        await transaction.put_document(record, expected_revision=current.row_revision)
        await transaction.close_visibility(scope.key, current.document_uid, seq)
        await transaction.set_document_objects_state(
            scope.key, current.document_uid, ObjectState.DELETED
        )
        if self._graph is not None:
            affected = await transaction.replace_contributions(scope.key, current.document_uid, [])
            await self.refresh_aggregates(transaction, scope, affected)
        await transaction.set_document_coverage_state(
            scope.key, current.document_uid, CoverageState.PENDING, target_version=current.version
        )
        return "deleted"


__all__ = ["GraphDerivation", "IngestionService", "document_uid"]
