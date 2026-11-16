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
    from collections.abc import Sequence
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


@dataclass(slots=True)
class _Outcome:
    result: DocumentIngestResult
    warnings: list[RetrievalWarning] = field(default_factory=list)
    changed: bool = False


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
    """Allocates counters and versions and derives objects in one transaction per document."""

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

        Documents sharing an id are applied in submission order; others run concurrently
        (bounded by ``IngestionConfig.concurrency``).

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
        groups: dict[str, list[int]] = {}
        for position, document in enumerate(documents):
            groups.setdefault(document.id, []).append(position)
        outcomes: list[_Outcome | None] = [None] * len(documents)
        semaphore = asyncio.Semaphore(self._config.concurrency)

        async def run_group(positions: list[int]) -> None:
            for position in positions:
                item = canonical[position]
                if isinstance(item, RetrievalLoomError):
                    outcomes[position] = _Outcome(
                        DocumentIngestResult(
                            document_id=documents[position].id,
                            outcome=IngestOutcome.REJECTED,
                            error=ErrorInfo.from_exception(item),
                        )
                    )
                    continue
                async with semaphore:
                    outcomes[position] = await self._ingest_one(item)

        async with asyncio.TaskGroup() as group:
            for positions in groups.values():
                group.create_task(run_group(positions))
        results = [outcome for outcome in outcomes if outcome is not None]
        warnings = [warning for outcome in results for warning in outcome.warnings]
        return IngestReport(
            scope=scope,
            documents=tuple(outcome.result for outcome in results),
            warnings=tuple(warnings),
        )

    async def _ingest_one(self, canonical: CanonicalDocument) -> _Outcome:
        document = canonical.document
        try:
            graph = await self._normalized_graph(canonical)
            for attempt in range(_CONFLICT_RETRIES):
                try:
                    return await self._allocate(canonical, graph)
                except VersionConflictError:
                    if attempt == _CONFLICT_RETRIES - 1:
                        raise
                    await asyncio.sleep(0.01 * (attempt + 1))
        except RetrievalLoomError as exc:
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
        raise AssertionError("unreachable")  # pragma: no cover

    async def _normalized_graph(self, canonical: CanonicalDocument) -> NormalizedGraph | None:
        """Normalised graph of the document, or ``None`` when graph derivation is off."""
        if self._graph is None:
            return None
        document = canonical.document
        provided = graph_representation(document)
        if provided is not None:
            extraction = GraphExtraction(
                entities=provided.entities, relationships=provided.relationships
            )
        elif self._graph.extractor is not None:
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
            extraction = await self._graph.extractor.extract(
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
        return await self._graph.normalizer.normalize(extraction, canonical.scope)

    def _graph_representation_id(self, document: DocumentInput) -> str:
        provided = graph_representation(document)
        if provided is not None:
            return provided.representation_id
        bound = index_representation(document)
        return bound.representation_id if bound is not None else "default"

    async def _allocate(
        self, canonical: CanonicalDocument, graph: NormalizedGraph | None
    ) -> _Outcome:
        document = canonical.document
        scope = canonical.scope
        warnings: list[RetrievalWarning] = []
        async with self._store.transaction(scope, write=True) as transaction:
            await transaction.ensure_scope(scope)
            event = await transaction.get_event(scope.key, canonical.event_id)
            if event is not None and event.status == EventStatus.COMMITTED:
                return _Outcome(
                    DocumentIngestResult(
                        document_id=document.id,
                        event_id=event.event_id,
                        outcome=IngestOutcome.REPLAYED,
                        counter=event.counter_after,
                        version=event.version_after,
                        possible_redelivery_of=event.possible_redelivery_of,
                    )
                )
            now = self._clock()
            possible: str | None = None
            if canonical.event_tier == 3 and self._config.redelivery_window_seconds > 0:
                prior = await transaction.find_event_by_fingerprint(
                    scope.key,
                    canonical.appearance_fingerprint,
                    since=now - timedelta(seconds=self._config.redelivery_window_seconds),
                    exclude_event_id=canonical.event_id,
                )
                if prior is not None:
                    possible = prior.event_id
                    warnings.append(
                        RetrievalWarning(
                            code=WarningCode.POSSIBLE_REDELIVERY,
                            severity=Severity.INFO,
                            message="appearance matches a recent appearance; counted",
                            details={"document_id": document.id, "event_id": prior.event_id},
                        )
                    )
            current = await transaction.get_document(scope, document.id)
            record, outcome, content_changed, changed = self._next_record(canonical, current, now)
            seq = await transaction.next_commit_seq()
            if changed:
                record = record.model_copy(update={"change_seq": seq})
            await transaction.put_document(
                record, expected_revision=None if current is None else current.row_revision
            )
            if content_changed:
                await transaction.put_version(
                    scope.key,
                    DocumentVersion(
                        document_uid=record.document_uid,
                        version=record.version,
                        content_hash=record.content_hash,
                        hash_profile=record.hash_profile,
                        representation_digests=record.representation_digests,
                        metadata=record.metadata,
                        dataset=record.dataset,
                        source=record.source,
                        created_at=now,
                        event_id=canonical.event_id,
                    ),
                )
                await transaction.put_representations(
                    scope.key,
                    [
                        RepresentationSnapshot(
                            document_uid=record.document_uid,
                            version=record.version,
                            representation_id=representation.representation_id,
                            kind=representation.kind,
                            payload=canonical.payloads[representation.representation_id],
                            transformation=representation.transformation,
                            hash_profile=record.hash_profile,
                        )
                        for representation in document.representations
                    ],
                )
            if changed:
                await self._derive(transaction, record, document, current, graph)
                await transaction.set_document_coverage_state(
                    scope.key,
                    record.document_uid,
                    CoverageState.PENDING,
                    target_version=record.version,
                )
            await transaction.put_event(
                IngestionEvent(
                    event_id=canonical.event_id,
                    scope=scope,
                    document_id=document.id,
                    document_uid=record.document_uid,
                    received_at=now,
                    status=EventStatus.COMMITTED,
                    outcome=outcome,
                    counter_after=record.counter,
                    version_after=record.version,
                    content_hash=canonical.content_hash,
                    delivery=document.delivery,
                    appearance_fingerprint=canonical.appearance_fingerprint,
                    possible_redelivery_of=possible,
                )
            )
        self._logger.debug(
            "document_ingested",
            scope=scope.key,
            document_uid=record.document_uid,
            outcome=outcome.value,
            version=record.version,
        )
        return _Outcome(
            DocumentIngestResult(
                document_id=document.id,
                event_id=canonical.event_id,
                outcome=outcome,
                counter=record.counter,
                version=record.version,
                possible_redelivery_of=possible,
                coverage={},
            ),
            warnings,
            changed,
        )

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
        self,
        transaction: MetadataTransactionABC,
        record: DocumentRecord,
        document: DocumentInput,
        previous: DocumentRecord | None,
        graph: NormalizedGraph | None,
    ) -> None:
        scope = record.scope
        objects: list[DerivedObject] = []
        bound = index_representation(document)
        if bound is not None:
            objects.extend(derive_chunks(record, bound))
        tree = structure_representation(document)
        if tree is not None:
            objects.extend(derive_structure(record, tree))
        if objects:
            await transaction.put_objects(objects)
            await transaction.put_lineage(lineage_for(objects))
        if previous is not None and previous.version != record.version:
            await transaction.set_document_objects_state(
                scope.key,
                record.document_uid,
                ObjectState.SUPERSEDED,
                exclude_version=record.version,
            )
        if self._graph is None:
            return
        if (
            graph is None
            and previous is not None
            and previous.content_hash == record.content_hash
            and previous.state == DocumentState.ACTIVE
        ):
            return  # unchanged content: contributions are already current
        contributions = (
            graph_contributions(record, self._graph_representation_id(document), graph)
            if graph is not None
            else []
        )
        affected = await transaction.replace_contributions(
            scope.key, record.document_uid, contributions
        )
        await self.refresh_aggregates(transaction, scope, affected)

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
