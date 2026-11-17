"""Ingestion and deletion: identity, counters, versions, events, derived objects, graph."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from retrievalloom.config import FailurePolicy, IngestionConfig, IngestionLimits
from retrievalloom.contracts.component import ComponentInfo, make_info
from retrievalloom.contracts.models import (
    ExtractionSource,
    GraphExtraction,
    GraphExtractorABC,
    GraphNormalizerABC,
    NormalizedGraph,
)
from retrievalloom.domain.inputs import (
    DeliveryProvenance,
    DocumentInput,
    GraphEntityInput,
    GraphRelationshipInput,
    GraphRepresentation,
    Scope,
    StructureNodeInput,
    StructureRepresentation,
    TextRepresentation,
)
from retrievalloom.domain.records import (
    CoverageRecord,
    CoverageState,
    DocumentState,
    EventStatus,
    IngestionEvent,
    IngestOutcome,
    LineageRelation,
    ObjectKind,
    ObjectState,
)
from retrievalloom.domain.retrieval import WarningCode
from retrievalloom.errors import BackendUnavailableError, ValidationError, VersionConflictError
from retrievalloom.graph.normalizer import IdentityGraphNormalizer
from retrievalloom.lifecycle import ingestion
from retrievalloom.lifecycle.canonical import canonicalize, document_uid
from retrievalloom.lifecycle.derivation import is_graph_marker
from retrievalloom.lifecycle.ingestion import GraphDerivation, IngestionService
from retrievalloom.storage.base import UnitKey
from retrievalloom.storage.sqlite import SQLiteTransaction
from tests.fakes import CapitalizedExtractor
from tests.unit.lifecycle.helpers import (
    OTHER,
    SCOPE,
    FakeClock,
    RecordingLogger,
    chunk_doc,
    fail_transaction,
    text_doc,
)

if TYPE_CHECKING:
    from collections.abc import Sequence

    from retrievalloom.domain.records import DerivedObject
    from retrievalloom.storage.sqlite import SQLiteMetadataStore


def service(
    store: SQLiteMetadataStore,
    config: IngestionConfig | None = None,
    graph: GraphDerivation | None = None,
) -> IngestionService:
    return IngestionService(
        store, config or IngestionConfig(), clock=FakeClock(), logger=RecordingLogger(), graph=graph
    )


async def objects_of(store: SQLiteMetadataStore, document_id: str, scope: Scope = SCOPE) -> list:
    async with store.transaction(scope, write=False) as transaction:
        return list(
            await transaction.list_document_objects(scope.key, document_uid(scope, document_id))
        )


async def document(store: SQLiteMetadataStore, document_id: str, scope: Scope = SCOPE):
    async with store.transaction(scope, write=False) as transaction:
        return await transaction.get_document(scope, document_id)


# ingest ---------------------------------------------------------------------------------
async def test_lifecycle_of_one_document(store: SQLiteMetadataStore) -> None:
    ingest = service(store)
    first = (await ingest.ingest([chunk_doc("d1", "alpha", "beta")], SCOPE)).documents[0]
    assert (first.outcome, first.counter, first.version) == (IngestOutcome.CREATED, 1, 1)
    v1 = await objects_of(store, "d1")
    assert sorted(obj.text for obj in v1) == ["alpha", "beta"]

    same = (await ingest.ingest([chunk_doc("d1", "alpha", "beta")], SCOPE)).documents[0]
    assert (same.outcome, same.counter, same.version) == (IngestOutcome.UNCHANGED, 2, 1)

    updated = (await ingest.ingest([chunk_doc("d1", "gamma")], SCOPE)).documents[0]
    assert (updated.outcome, updated.counter, updated.version) == (IngestOutcome.UPDATED, 3, 2)
    states = {(obj.version, obj.text): obj.state for obj in await objects_of(store, "d1")}
    assert states == {
        (1, "alpha"): ObjectState.SUPERSEDED,
        (1, "beta"): ObjectState.SUPERSEDED,
        (2, "gamma"): ObjectState.LIVE,
    }
    [live] = [obj for obj in await objects_of(store, "d1") if obj.version == 2]
    async with store.transaction(SCOPE, write=False) as transaction:
        uid = document_uid(SCOPE, "d1")
        versions = await transaction.get_versions(uid)
        representations = await transaction.get_representations(uid, 2)
        lineage = await transaction.get_lineage(SCOPE.key, live.object_id)
        event = await transaction.get_event(SCOPE.key, updated.event_id or "")
    assert [version.version for version in versions] == [1, 2]
    assert [snapshot.kind for snapshot in representations] == ["chunks"]
    assert {(edge.to_id, edge.relation) for edge in lineage} == {
        (uid, LineageRelation.DERIVED_FROM)
    }
    assert event is not None
    assert event.status == EventStatus.COMMITTED
    assert (event.outcome, event.counter_after, event.version_after) == (
        IngestOutcome.UPDATED,
        3,
        2,
    )


async def test_metadata_projection_and_transient_keys(store: SQLiteMetadataStore) -> None:
    ingest = service(store, IngestionConfig(transient_metadata_keys=("seen",)))
    await ingest.ingest([text_doc("d1", "x", metadata={"seen": 1})], SCOPE)
    before = await document(store, "d1")
    result = (await ingest.ingest([text_doc("d1", "x", metadata={"seen": 2})], SCOPE)).documents[0]
    after = await document(store, "d1")
    assert before is not None
    assert after is not None
    assert result.outcome == IngestOutcome.UNCHANGED
    assert after.version == 1
    assert after.metadata == {"seen": 2}
    assert after.metadata_revision == before.metadata_revision + 1
    assert after.change_seq > before.change_seq
    # Re-sending identical content changes nothing but the counter.
    again = (await ingest.ingest([text_doc("d1", "x", metadata={"seen": 2})], SCOPE)).documents[0]
    latest = await document(store, "d1")
    assert latest is not None
    assert (again.outcome, latest.counter, latest.metadata_revision) == (
        IngestOutcome.UNCHANGED,
        3,
        after.metadata_revision,
    )
    assert latest.change_seq == after.change_seq


async def test_new_document_without_index_representation(store: SQLiteMetadataStore) -> None:
    tree = DocumentInput.model_validate(
        {
            "_id": "t",
            "representations": (
                StructureRepresentation(
                    root=StructureNodeInput(title="Root", children=(StructureNodeInput(text="a"),))
                ),
            ),
        }
    )
    [result] = (await service(store).ingest([tree], SCOPE)).documents
    assert result.outcome == IngestOutcome.CREATED
    kinds = [obj.kind for obj in await objects_of(store, "t")]
    assert kinds == [ObjectKind.STRUCTURE_NODE, ObjectKind.STRUCTURE_NODE]


async def test_documents_with_same_id_apply_in_submission_order(
    store: SQLiteMetadataStore,
) -> None:
    ingest = service(store, IngestionConfig(concurrency=1))
    report = await ingest.ingest(
        [
            text_doc("d1", "one"),
            text_doc("d2", "two"),
            text_doc("d1", "uno"),
            text_doc("d1", "one"),
        ],
        SCOPE,
    )
    assert [(r.document_id, r.outcome, r.version) for r in report.documents] == [
        ("d1", IngestOutcome.CREATED, 1),
        ("d2", IngestOutcome.CREATED, 1),
        ("d1", IngestOutcome.UPDATED, 2),
        ("d1", IngestOutcome.UPDATED, 3),
    ]


async def test_batch_size_limit(store: SQLiteMetadataStore) -> None:
    ingest = service(store, IngestionConfig(limits=IngestionLimits(max_documents_per_call=1)))
    with pytest.raises(ValidationError) as caught:
        await ingest.ingest([text_doc("a", "x"), text_doc("b", "y")], SCOPE)
    assert caught.value.details == {"count": 2, "max": 1}
    assert await document(store, "a") is None


async def test_invalid_document_is_rejected_per_document(store: SQLiteMetadataStore) -> None:
    config = IngestionConfig(limits=IngestionLimits(max_text_chars=3))
    report = await service(store, config).ingest(
        [text_doc("ok", "abc"), text_doc("big", "abcd"), text_doc("foreign", "a", scope=OTHER)],
        SCOPE,
    )
    outcomes = {r.document_id: r for r in report.documents}
    assert outcomes["ok"].outcome == IngestOutcome.CREATED
    assert outcomes["big"].outcome == IngestOutcome.REJECTED
    assert outcomes["big"].error is not None
    assert outcomes["big"].error.code == "validation_error"
    assert outcomes["big"].version is None
    assert outcomes["foreign"].outcome == IngestOutcome.REJECTED
    assert await document(store, "big") is None


async def test_fail_batch_policy_rejects_whole_batch(store: SQLiteMetadataStore) -> None:
    config = IngestionConfig(
        failure_policy=FailurePolicy.FAIL_BATCH, limits=IngestionLimits(max_text_chars=3)
    )
    with pytest.raises(ValidationError):
        await service(store, config).ingest([text_doc("ok", "abc"), text_doc("big", "abcd")], SCOPE)
    assert await document(store, "ok") is None


async def test_replayed_appearance_returns_stored_outcome(store: SQLiteMetadataStore) -> None:
    ingest = service(store)
    first = (await ingest.ingest([text_doc("d1", "x", appearance_id="a1")], SCOPE)).documents[0]
    await ingest.ingest([text_doc("d1", "y")], SCOPE)
    replay = (await ingest.ingest([text_doc("d1", "x", appearance_id="a1")], SCOPE)).documents[0]
    assert replay.outcome == IngestOutcome.REPLAYED
    assert (replay.event_id, replay.counter, replay.version) == (first.event_id, 1, 1)
    current = await document(store, "d1")
    assert current is not None
    assert (current.counter, current.version) == (2, 2)  # replay changed nothing


async def test_pending_event_left_by_a_crash_is_reprocessed(store: SQLiteMetadataStore) -> None:
    ingest = service(store)
    pending_input = text_doc(
        "d1", "x", delivery=DeliveryProvenance(source_system="s", delivery_id="1")
    )
    canonical = canonicalize(pending_input, SCOPE, IngestionConfig())
    async with store.transaction(SCOPE, write=True) as transaction:
        await transaction.ensure_scope(SCOPE)
        await transaction.put_event(
            IngestionEvent(
                event_id=canonical.event_id,
                scope=SCOPE,
                document_id="d1",
                document_uid=canonical.document_uid,
                received_at=FakeClock()(),
                status=EventStatus.PENDING,
                content_hash=canonical.content_hash,
                appearance_fingerprint=canonical.appearance_fingerprint,
            )
        )
    [result] = (await ingest.ingest([pending_input], SCOPE)).documents
    assert result.outcome == IngestOutcome.CREATED
    assert result.event_id == canonical.event_id
    async with store.transaction(SCOPE, write=False) as transaction:
        event = await transaction.get_event(SCOPE.key, canonical.event_id)
    assert event is not None
    assert event.status == EventStatus.COMMITTED
    replay = (await ingest.ingest([pending_input], SCOPE)).documents[0]
    assert replay.outcome == IngestOutcome.REPLAYED


async def test_possible_redelivery_is_flagged_but_counted(store: SQLiteMetadataStore) -> None:
    clock = FakeClock()
    ingest = IngestionService(
        store,
        IngestionConfig(redelivery_window_seconds=60),
        clock=clock,
        logger=RecordingLogger(),
    )
    first = (await ingest.ingest([text_doc("d1", "x")], SCOPE)).documents[0]
    report = await ingest.ingest([text_doc("d1", "x")], SCOPE)
    second = report.documents[0]
    assert second.possible_redelivery_of == first.event_id
    assert second.counter == 2
    assert [w.code for w in report.warnings] == [WarningCode.POSSIBLE_REDELIVERY]
    clock.advance(120)
    late = await ingest.ingest([text_doc("d1", "x")], SCOPE)
    assert late.documents[0].possible_redelivery_of is None
    assert late.warnings == ()
    tier1 = await ingest.ingest([text_doc("d1", "x", appearance_id="z")], SCOPE)
    assert tier1.documents[0].possible_redelivery_of is None


async def test_redelivery_check_can_be_disabled(store: SQLiteMetadataStore) -> None:
    ingest = service(store, IngestionConfig(redelivery_window_seconds=0))
    await ingest.ingest([text_doc("d1", "x")], SCOPE)
    report = await ingest.ingest([text_doc("d1", "x")], SCOPE)
    assert report.documents[0].possible_redelivery_of is None
    assert report.warnings == ()


async def test_version_conflicts_are_retried(
    store: SQLiteMetadataStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls = fail_transaction(
        monkeypatch, "put_documents", lambda: VersionConflictError("busy"), times=2
    )
    [result] = (await service(store).ingest([text_doc("d1", "x")], SCOPE)).documents
    assert calls[0] == 3
    assert (result.outcome, result.counter) == (IngestOutcome.CREATED, 1)


async def test_exhausted_version_conflicts_fail_the_document(
    store: SQLiteMetadataStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    fail_transaction(monkeypatch, "put_documents", lambda: VersionConflictError("busy"))
    logger = RecordingLogger()
    ingest = IngestionService(store, IngestionConfig(), clock=FakeClock(), logger=logger)
    [result] = (await ingest.ingest([text_doc("d1", "x")], SCOPE)).documents
    assert result.outcome == IngestOutcome.FAILED
    assert result.error is not None
    assert result.error.code == "version_conflict"
    assert result.error.retryable
    assert result.version is None
    assert result.counter is None
    assert ("warning", "ingest_failed") in [(lvl, ev) for lvl, ev, _ in logger.records]
    monkeypatch.undo()
    assert await document(store, "d1") is None  # nothing was committed


async def test_unexpected_errors_propagate_and_roll_back(
    store: SQLiteMetadataStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    fail_transaction(monkeypatch, "put_events", lambda: RuntimeError("disk on fire"))
    with pytest.raises(ExceptionGroup) as caught:
        await service(store).ingest([text_doc("d1", "x")], SCOPE)
    assert caught.group_contains(RuntimeError)
    monkeypatch.undo()
    assert await document(store, "d1") is None
    assert await objects_of(store, "d1") == []


async def test_documents_are_written_in_chunk_transactions(
    store: SQLiteMetadataStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(ingestion, "_WRITE_CHUNK", 2)
    writes = fail_transaction(monkeypatch, "put_documents", RuntimeError, times=0)
    ingest = service(store)
    first = await ingest.ingest(
        [text_doc(f"d{n}", "x", appearance_id=f"a{n}") for n in range(5)], SCOPE
    )
    assert writes[0] == 3
    assert {result.outcome for result in first.documents} == {IngestOutcome.CREATED}
    again = await ingest.ingest(
        [
            text_doc("d0", "x", appearance_id="a0"),
            text_doc("d1", "x", appearance_id="a1"),
            text_doc("d2", "x", appearance_id="a2"),
            text_doc("d5", "x", appearance_id="a5"),
        ],
        SCOPE,
    )
    # The first chunk only replays, so it writes nothing.
    assert writes[0] == 4
    assert [(result.document_id, result.outcome) for result in again.documents] == [
        ("d0", IngestOutcome.REPLAYED),
        ("d1", IngestOutcome.REPLAYED),
        ("d2", IngestOutcome.REPLAYED),
        ("d5", IngestOutcome.CREATED),
    ]
    assert [result.counter for result in again.documents] == [1, 1, 1, 1]


async def test_failing_document_does_not_fail_its_chunk(
    store: SQLiteMetadataStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    bad = document_uid(SCOPE, "bad")
    original = SQLiteTransaction.put_objects

    async def put_objects(self: SQLiteTransaction, objects: Sequence[DerivedObject]) -> None:
        if any(obj.document_uid == bad for obj in objects):
            raise ValidationError("unstorable object")
        await original(self, objects)

    monkeypatch.setattr(SQLiteTransaction, "put_objects", put_objects)
    logger = RecordingLogger()
    ingest = IngestionService(store, IngestionConfig(), clock=FakeClock(), logger=logger)
    report = await ingest.ingest(
        [text_doc("good-1", "x"), text_doc("bad", "y"), text_doc("good-2", "z")], SCOPE
    )
    assert [(result.document_id, result.outcome) for result in report.documents] == [
        ("good-1", IngestOutcome.CREATED),
        ("bad", IngestOutcome.FAILED),
        ("good-2", IngestOutcome.CREATED),
    ]
    events = [(level, name) for level, name, _ in logger.records]
    assert ("debug", "ingest_chunk_split") in events
    assert ("warning", "ingest_failed") in events
    assert await document(store, "bad") is None
    assert [len(await objects_of(store, name)) for name in ("good-1", "good-2")] == [1, 1]


async def test_chunk_conflicts_are_retried_per_document(
    store: SQLiteMetadataStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls = fail_transaction(
        monkeypatch, "put_documents", lambda: VersionConflictError("busy"), times=1
    )
    report = await service(store).ingest([text_doc("d1", "x"), text_doc("d2", "y")], SCOPE)
    assert calls[0] == 3
    assert [result.outcome for result in report.documents] == [IngestOutcome.CREATED] * 2


async def test_unexpected_chunk_errors_roll_back_the_whole_chunk(
    store: SQLiteMetadataStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    fail_transaction(monkeypatch, "put_events", lambda: RuntimeError("disk on fire"))
    with pytest.raises(ExceptionGroup) as caught:
        await service(store).ingest([text_doc("d1", "x"), text_doc("d2", "y")], SCOPE)
    assert caught.group_contains(RuntimeError, match="disk on fire")
    monkeypatch.undo()
    assert [await document(store, name) for name in ("d1", "d2")] == [None, None]


# delete ---------------------------------------------------------------------------------
async def test_delete_and_revive(store: SQLiteMetadataStore) -> None:
    ingest = service(store)
    await ingest.ingest([text_doc("d1", "x"), text_doc("d2", "y")], SCOPE)
    uid = document_uid(SCOPE, "d1")
    async with store.transaction(SCOPE, write=True) as transaction:
        await transaction.upsert_coverage(
            [
                CoverageRecord(
                    scope_key=SCOPE.key,
                    document_uid=uid,
                    channel="index",
                    method="sparse",
                    target_version=1,
                    indexed_version=1,
                    state=CoverageState.READY,
                    updated_at=FakeClock()(),
                )
            ]
        )
    report = await ingest.delete(["d1", "d1", "missing"], SCOPE)
    assert (report.deleted, report.already_deleted, report.not_found) == (("d1",), (), ("missing",))
    deleted = await document(store, "d1")
    assert deleted is not None
    assert deleted.state == DocumentState.DELETED
    assert deleted.deleted_at is not None
    assert [obj.state for obj in await objects_of(store, "d1")] == [ObjectState.DELETED]
    async with store.transaction(SCOPE, write=False) as transaction:
        coverage = await transaction.get_coverage(
            UnitKey(scope_key=SCOPE.key, channel="index", method="sparse"), [uid]
        )
    assert coverage[uid].state == CoverageState.PENDING
    assert coverage[uid].target_version == 1
    again = await ingest.delete(["d1"], SCOPE)
    assert again.already_deleted == ("d1",)

    revived = (await ingest.ingest([text_doc("d1", "x")], SCOPE)).documents[0]
    assert (revived.outcome, revived.version, revived.counter) == (IngestOutcome.REVIVED, 1, 2)
    assert [obj.state for obj in await objects_of(store, "d1")] == [ObjectState.LIVE]
    await ingest.delete(["d1"], SCOPE)
    changed = (await ingest.ingest([text_doc("d1", "new")], SCOPE)).documents[0]
    assert (changed.outcome, changed.version) == (IngestOutcome.REVIVED, 2)


async def test_delete_retries_and_reraises_version_conflicts(
    store: SQLiteMetadataStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    ingest = service(store)
    await ingest.ingest([text_doc("d1", "x")], SCOPE)
    calls = fail_transaction(
        monkeypatch, "close_visibility_many", lambda: VersionConflictError("busy"), times=1
    )
    report = await ingest.delete(["d1"], SCOPE)
    assert calls[0] == 2
    assert report.deleted == ("d1",)

    await ingest.ingest([text_doc("d2", "y")], SCOPE)
    monkeypatch.undo()
    fail_transaction(monkeypatch, "close_visibility_many", lambda: VersionConflictError("busy"))
    with pytest.raises(VersionConflictError):
        await ingest.delete(["d2"], SCOPE)
    monkeypatch.undo()
    current = await document(store, "d2")
    assert current is not None
    assert current.state == DocumentState.ACTIVE  # rolled back


# graph ----------------------------------------------------------------------------------
class RecordingExtractor(GraphExtractorABC):
    """Wraps :class:`CapitalizedExtractor`, recording versions; can be told to fail."""

    def __init__(self) -> None:
        self.inner = CapitalizedExtractor()
        self.versions: list[int] = []
        self.error: BaseException | None = None

    @property
    def info(self) -> ComponentInfo:
        return self.inner.info

    async def extract(self, source: ExtractionSource) -> GraphExtraction:
        if self.error is not None:
            raise self.error
        self.versions.append(source.version)
        return await self.inner.extract(source)


class FailingNormalizer(GraphNormalizerABC):
    @property
    def info(self) -> ComponentInfo:
        return make_info(component="graph_normalizer", implementation="failing", version="1")

    async def normalize(self, extraction: GraphExtraction, scope: Scope) -> NormalizedGraph:
        raise BackendUnavailableError("normalizer down")


async def graph_objects(store: SQLiteMetadataStore, document_id: str) -> dict[str, DerivedObject]:
    """Aggregates the document has ever contributed to, by ``kind:name``."""
    uid = document_uid(SCOPE, document_id)
    async with store.transaction(SCOPE, write=False) as transaction:
        contributions = await transaction.list_document_contributions(SCOPE.key, uid)
        objects = await transaction.get_objects(
            SCOPE.key, sorted({c.object_id for c in contributions})
        )
    return {f"{obj.kind}:{obj.text}": obj for obj in objects.values()}


async def live_contributions(store: SQLiteMetadataStore, document_id: str) -> int:
    uid = document_uid(SCOPE, document_id)
    async with store.transaction(SCOPE, write=False) as transaction:
        objects = [
            c.object_id for c in await transaction.list_document_contributions(SCOPE.key, uid)
        ]
        current = await transaction.get_contributions(SCOPE.key, objects)
    return len([c for c in current if c.document_uid == uid and not is_graph_marker(c.object_id)])


def graph_doc(document_id: str, *names: str, text: str = "body") -> DocumentInput:
    return DocumentInput.model_validate(
        {
            "_id": document_id,
            "representations": (
                TextRepresentation(text=text),
                GraphRepresentation(
                    representation_id="kg",
                    entities=tuple(GraphEntityInput(name=name) for name in names),
                    relationships=(
                        GraphRelationshipInput(
                            source_key=names[0], target_key=names[-1], relation_type="knows"
                        ),
                    ),
                ),
            ),
        }
    )


async def test_provided_graph_is_aggregated_and_retracted_on_delete(
    store: SQLiteMetadataStore,
) -> None:
    ingest = service(store, graph=GraphDerivation(IdentityGraphNormalizer()))
    await ingest.ingest([graph_doc("d1", "Alice", "Bob"), graph_doc("d2", "Alice", "Carol")], SCOPE)
    objects = await graph_objects(store, "d1")
    entities = {k: v for k, v in objects.items() if v.kind == ObjectKind.GRAPH_ENTITY}
    assert len(entities) == 2
    assert all(obj.state == ObjectState.LIVE for obj in objects.values())
    assert len([o for o in objects.values() if o.kind == ObjectKind.GRAPH_RELATIONSHIP]) == 1
    alice = next(obj for obj in entities.values() if "Alice" in (obj.text or ""))
    assert {source.document_uid for source in alice.sources} == {
        document_uid(SCOPE, "d1"),
        document_uid(SCOPE, "d2"),
    }
    async with store.transaction(SCOPE, write=False) as transaction:
        lineage = await transaction.get_lineage(SCOPE.key, alice.object_id)
    assert {(e.to_id, e.relation) for e in lineage} >= {
        (document_uid(SCOPE, "d1"), LineageRelation.SUPPORTED_BY)
    }

    await ingest.delete(["d1"], SCOPE)
    async with store.transaction(SCOPE, write=False) as transaction:
        after = dict(
            await transaction.get_objects(SCOPE.key, [obj.object_id for obj in objects.values()])
        )
    assert after[alice.object_id].state == ObjectState.LIVE  # still supported by d2
    assert [s.document_uid for s in after[alice.object_id].sources] == [document_uid(SCOPE, "d2")]
    orphaned = [obj for oid, obj in after.items() if oid != alice.object_id]
    assert orphaned
    assert all(obj.state != ObjectState.LIVE for obj in orphaned)
    assert await live_contributions(store, "d1") == 0

    graph_only = DocumentInput.model_validate(
        {
            "_id": "g",
            "representations": (GraphRepresentation(entities=(GraphEntityInput(name="Zed"),)),),
        }
    )
    [created] = (await ingest.ingest([graph_only], SCOPE)).documents
    assert created.outcome == IngestOutcome.CREATED
    assert await objects_of(store, "g") == []
    assert await live_contributions(store, "g") > 0


async def test_extractor_sees_the_version_being_written(store: SQLiteMetadataStore) -> None:
    extractor = RecordingExtractor()
    ingest = service(
        store,
        IngestionConfig(transient_metadata_keys=("k",)),
        graph=GraphDerivation(IdentityGraphNormalizer(), extractor),
    )
    await ingest.ingest([text_doc("d1", "Alice met Bob")], SCOPE)
    assert extractor.versions == [1]
    contributed = await live_contributions(store, "d1")
    assert contributed > 0

    # Unchanged content (even with new metadata) is not re-extracted; contributions stay.
    await ingest.ingest([text_doc("d1", "Alice met Bob")], SCOPE)
    await ingest.ingest([text_doc("d1", "Alice met Bob", metadata={"k": 1})], SCOPE)
    assert extractor.versions == [1]
    contributed = await live_contributions(store, "d1")
    assert contributed > 0

    await ingest.ingest([chunk_doc("d1", "Alice met Carol", "Dave")], SCOPE)
    assert extractor.versions == [1, 2]
    await ingest.delete(["d1"], SCOPE)
    assert await live_contributions(store, "d1") == 0
    revived = (await ingest.ingest([chunk_doc("d1", "Alice met Carol", "Dave")], SCOPE)).documents[
        0
    ]
    assert (revived.outcome, revived.version) == (IngestOutcome.REVIVED, 2)
    assert extractor.versions == [1, 2, 2]  # revival keeps the version
    assert await live_contributions(store, "d1") > 0
    await ingest.delete(["d1"], SCOPE)
    await ingest.ingest([text_doc("d1", "Erin")], SCOPE)
    assert extractor.versions == [1, 2, 2, 3]


async def test_documents_without_graph_input_contribute_nothing(
    store: SQLiteMetadataStore,
) -> None:
    extractor = RecordingExtractor()
    tree = DocumentInput.model_validate(
        {
            "_id": "tree",
            "representations": (StructureRepresentation(root=StructureNodeInput(text="Alice")),),
        }
    )
    with_extractor = service(store, graph=GraphDerivation(IdentityGraphNormalizer(), extractor))
    [result] = (await with_extractor.ingest([tree], SCOPE)).documents
    assert result.outcome == IngestOutcome.CREATED
    assert extractor.versions == []  # nothing to extract from
    no_extractor = service(store, graph=GraphDerivation(IdentityGraphNormalizer()))
    await no_extractor.ingest([text_doc("plain", "Alice met Bob")], SCOPE)
    assert await live_contributions(store, "plain") == 0
    assert with_extractor._graph_representation_id(tree) == "default"
    assert with_extractor._graph_representation_id(text_doc("x", "y")) == "default"


async def test_graph_failures(store: SQLiteMetadataStore) -> None:
    extractor = RecordingExtractor()
    extractor.error = BackendUnavailableError("extractor down")
    ingest = service(store, graph=GraphDerivation(IdentityGraphNormalizer(), extractor))
    [failed] = (await ingest.ingest([text_doc("d1", "Alice")], SCOPE)).documents
    assert failed.outcome == IngestOutcome.FAILED
    assert failed.error is not None
    assert failed.error.code == "backend_unavailable"
    assert await document(store, "d1") is None

    broken = service(store, graph=GraphDerivation(FailingNormalizer()))
    [failed] = (await broken.ingest([graph_doc("d2", "Alice", "Bob")], SCOPE)).documents
    assert failed.outcome == IngestOutcome.FAILED

    extractor.error = RuntimeError("bug")
    with pytest.raises(ExceptionGroup) as caught:
        await ingest.ingest([text_doc("d3", "Alice")], SCOPE)
    assert caught.group_contains(RuntimeError, match="bug")


async def test_refresh_aggregates_ignores_unknown_objects(store: SQLiteMetadataStore) -> None:
    async with store.transaction(SCOPE, write=True) as transaction:
        await transaction.ensure_scope(SCOPE)
        await IngestionService.refresh_aggregates(transaction, SCOPE, ["obj_missing"])
        assert await transaction.get_objects(SCOPE.key, ["obj_missing"]) == {}
