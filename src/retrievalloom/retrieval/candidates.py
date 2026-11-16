"""Candidate normalisation with snapshot visibility, filter and source-level authorisation."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from retrievalloom.domain.filters import evaluate
from retrievalloom.domain.hashing import stable_id
from retrievalloom.domain.records import DocumentState, ObjectState
from retrievalloom.domain.retrieval import Candidate, CandidateFlags, ScoreCard
from retrievalloom.errors import TenantIsolationError
from retrievalloom.lifecycle.derivation import document_filter_record, recompute_aggregates

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from retrievalloom.domain.filters import FilterExpression
    from retrievalloom.domain.inputs import Scope
    from retrievalloom.domain.records import DerivedObject, DocumentRecord, SourceReference
    from retrievalloom.domain.retrieval import RetrievalHit
    from retrievalloom.lifecycle.units import IndexUnit
    from retrievalloom.storage.base import DocumentMetadataStoreABC


@dataclass(slots=True)
class CandidateBatch:
    """Visible candidates in first-hit order, plus counts of what was left out."""

    candidates: list[Candidate] = field(default_factory=list)
    hidden_not_visible: int = 0
    filtered_deleted: int = 0
    filtered_out: int = 0
    stale_dropped: int = 0
    stale_kept: int = 0


@dataclass(slots=True)
class _Pending:
    obj: DerivedObject
    hits: list[tuple[str, RetrievalHit]]
    stale: bool
    permitted: list[SourceReference] | None = None


def candidate_id(scope: Scope, object_id: str) -> str:
    """Deterministic candidate id of an object in a scope."""
    return stable_id("cand", scope.key, object_id)


class CandidateBuilder:
    """Groups hits per object and keeps only objects visible at the request snapshot.

    A per-document object (chunk, structure node) is kept when its document is active, its
    version is visible to the unit that produced the hit at ``snapshot_seq``, it is the
    current version (unless stale versions are allowed) and the document passes the request
    filter. A graph aggregate is kept with the subset of its supporting sources that pass
    the same checks; when that subset is smaller than its support, the aggregate is
    recomputed from the permitted contributions only, so no field of a forbidden or
    invisible document leaks.
    """

    def __init__(self, store: DocumentMetadataStoreABC) -> None:
        self._store = store

    async def build(
        self,
        scope: Scope,
        hits_by_unit: Mapping[str, Sequence[RetrievalHit]],
        units: Mapping[str, IndexUnit],
        *,
        snapshot_seq: int,
        filters: FilterExpression | None,
        allow_stale: bool,
        normalized: Mapping[str, Mapping[str, float]],
    ) -> CandidateBatch:
        """Build candidates from ``hits_by_unit`` (unit name to hits, best first).

        Raises:
            TenantIsolationError: A hit or object belongs to another scope.
        """
        order: list[str] = []
        hits: dict[str, list[tuple[str, RetrievalHit]]] = {}
        for unit_name, unit_hits in hits_by_unit.items():
            for hit in unit_hits:
                if hit.object_ref.scope_key != scope.key:
                    raise TenantIsolationError(
                        "backend returned a hit of another scope", details={"unit": unit_name}
                    )
                object_id = hit.object_ref.object_id
                if object_id not in hits:
                    order.append(object_id)
                    hits[object_id] = []
                hits[object_id].append((unit_name, hit))
        batch = CandidateBatch()
        if not order:
            return batch

        async with self._store.transaction(scope, write=False) as transaction:
            objects = await transaction.get_objects(scope.key, order)
            uids = sorted(
                {
                    source.document_uid
                    for obj in objects.values()
                    for source in obj.sources
                    if obj.scope.key == scope.key
                }
            )
            documents = await transaction.get_documents_by_uid(scope.key, uids)
            visible: dict[str, Mapping[str, frozenset[int]]] = {}
            for unit_name in hits_by_unit:
                visible[unit_name] = await transaction.visible_versions(
                    units[unit_name].key(scope), uids, snapshot_seq
                )

        pending: list[_Pending] = []
        for object_id in order:
            obj = objects.get(object_id)
            if obj is None:
                batch.hidden_not_visible += 1
                continue
            if obj.scope.key != scope.key:
                raise TenantIsolationError(
                    "derived object belongs to another scope", details={"object_id": object_id}
                )
            if obj.document_uid is not None and obj.version is not None:
                item = self._document_object(
                    batch, obj, hits[object_id], documents, visible, filters, allow_stale
                )
            else:
                item = self._graph_object(
                    batch, obj, hits[object_id], documents, visible, filters, allow_stale
                )
            if item is not None:
                pending.append(item)

        recompute = [item for item in pending if item.permitted is not None]
        if recompute:
            await self._recompute(scope, recompute, documents)
        for item in pending:
            # A superseded object of a still-visible older version is exactly what
            # ``allow_stale`` admits; any other non-live state is dropped.
            superseded_ok = item.stale and item.obj.state == ObjectState.SUPERSEDED
            if item.obj.state != ObjectState.LIVE and not superseded_ok:
                batch.filtered_out += 1
                continue
            if item.stale:
                batch.stale_kept += 1
            batch.candidates.append(self._candidate(scope, item, normalized))
        return batch

    @staticmethod
    def _document_object(
        batch: CandidateBatch,
        obj: DerivedObject,
        object_hits: Sequence[tuple[str, RetrievalHit]],
        documents: Mapping[str, DocumentRecord],
        visible: Mapping[str, Mapping[str, frozenset[int]]],
        filters: FilterExpression | None,
        allow_stale: bool,
    ) -> _Pending | None:
        uid, version = obj.document_uid, obj.version
        assert uid is not None
        assert version is not None
        document = documents.get(uid)
        if document is None or document.state != DocumentState.ACTIVE:
            batch.filtered_deleted += 1
            return None
        kept = [(unit, hit) for unit, hit in object_hits if version in visible[unit].get(uid, ())]
        if not kept:
            batch.hidden_not_visible += 1
            return None
        stale = version != document.version
        if stale and not allow_stale:
            batch.stale_dropped += 1
            return None
        if filters is not None and not evaluate(
            filters, {**obj.metadata, **document_filter_record(document)}
        ):
            batch.filtered_out += 1
            return None
        return _Pending(obj=obj, hits=kept, stale=stale)

    @staticmethod
    def _graph_object(
        batch: CandidateBatch,
        obj: DerivedObject,
        object_hits: Sequence[tuple[str, RetrievalHit]],
        documents: Mapping[str, DocumentRecord],
        visible: Mapping[str, Mapping[str, frozenset[int]]],
        filters: FilterExpression | None,
        allow_stale: bool,
    ) -> _Pending | None:
        if obj.state != ObjectState.LIVE:
            batch.filtered_deleted += 1
            return None
        units = {unit for unit, _ in object_hits}
        permitted: list[SourceReference] = []
        stale = False
        for source in obj.sources:
            document = documents.get(source.document_uid)
            if document is None or document.state != DocumentState.ACTIVE:
                continue
            if not any(
                source.document_version in visible[unit].get(source.document_uid, ())
                for unit in units
            ):
                continue
            if source.document_version != document.version:
                if not allow_stale:
                    continue
                stale = True
            if filters is not None and not evaluate(filters, document_filter_record(document)):
                continue
            permitted.append(source)
        if not permitted:
            batch.hidden_not_visible += 1
            return None
        return _Pending(
            obj=obj,
            hits=list(object_hits),
            stale=stale,
            permitted=None if len(permitted) == len(obj.sources) else permitted,
        )

    async def _recompute(
        self,
        scope: Scope,
        items: Sequence[_Pending],
        documents: Mapping[str, DocumentRecord],
    ) -> None:
        """Rebuild partially permitted aggregates from their permitted, current sources."""
        async with self._store.transaction(scope, write=False) as transaction:
            contributions = await transaction.get_contributions(
                scope.key, [item.obj.object_id for item in items]
            )
        for item in items:
            assert item.permitted is not None
            allowed = {
                source.document_uid: documents[source.document_uid]
                for source in item.permitted
                if source.document_version == documents[source.document_uid].version
            }
            rebuilt = recompute_aggregates(
                scope,
                [item.obj.object_id],
                [c for c in contributions if c.object_id == item.obj.object_id],
                allowed,
                {item.obj.object_id: item.obj},
            )
            item.obj = (
                rebuilt[0]
                if rebuilt
                else item.obj.model_copy(update={"state": ObjectState.ORPHANED})
            )

    @staticmethod
    def _candidate(
        scope: Scope, item: _Pending, normalized: Mapping[str, Mapping[str, float]]
    ) -> Candidate:
        obj = item.obj
        scores = {
            unit: normalized[unit][obj.object_id]
            for unit, _ in item.hits
            if obj.object_id in normalized.get(unit, {})
        }
        return Candidate(
            candidate_id=candidate_id(scope, obj.object_id),
            scope=scope,
            object_id=obj.object_id,
            kind=obj.kind,
            text=obj.text,
            content_hash=obj.content_hash,
            sources=obj.sources,
            hits=tuple(hit for _, hit in item.hits),
            scores=ScoreCard(normalized=scores),
            flags=CandidateFlags(stale_version=item.stale),
            payload=dict(obj.payload),
        )


__all__ = ["CandidateBatch", "CandidateBuilder", "candidate_id"]
