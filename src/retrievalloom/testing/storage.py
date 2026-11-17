"""Contract kit for ``DocumentMetadataStoreABC``."""

from __future__ import annotations

from datetime import timedelta
from typing import Any

import pytest

from retrievalloom.domain.base import utc_now
from retrievalloom.domain.hashing import random_id, sha256_digest, stable_id
from retrievalloom.domain.inputs import Scope
from retrievalloom.domain.records import (
    Channel,
    CoverageRecord,
    CoverageState,
    DerivedObject,
    DocumentRecord,
    DocumentState,
    DocumentVersion,
    EventStatus,
    GenerationState,
    IndexGeneration,
    IngestionEvent,
    LifecycleTask,
    LineageEdge,
    LineageRelation,
    ObjectContribution,
    ObjectKind,
    ObjectState,
    RepresentationSnapshot,
    SourceReference,
    TaskKind,
    TaskState,
)
from retrievalloom.errors import LifecycleError, TenantIsolationError, VersionConflictError
from retrievalloom.storage.base import StoredRefreshState, UnitKey
from retrievalloom.testing._base import ComponentContract

SCOPE = Scope(tenant_id="store-a")
OTHER_SCOPE = Scope(tenant_id="store-b")


def document(scope: Scope, document_id: str, version: int = 1, **values: Any) -> DocumentRecord:
    """A document record of the kit."""
    now = utc_now()
    return DocumentRecord.model_validate(
        {
            "_id": document_id,
            "_counter": version,
            "_version": version,
            "_content_hash": sha256_digest(f"{document_id}:{version}"),
            "_created_at": now,
            "_updated_at": now,
            "document_uid": stable_id("doc", scope.key, document_id),
            "scope": scope,
            "hash_profile": "contract",
            **values,
        }
    )


def generation(scope: Scope, state: GenerationState = GenerationState.BUILDING) -> IndexGeneration:
    """An index generation of the kit."""
    return IndexGeneration(
        generation_id=random_id("gen"),
        scope_key=scope.key,
        channel=Channel.INDEX,
        method="keyword",
        backend_id="contract",
        dependency_fingerprint=sha256_digest("deps"),
        state=state,
        created_at=utc_now(),
    )


async def _seed_history(tx: Any, record: DocumentRecord, versions: int) -> list[DerivedObject]:
    """Write versions, representations, an event, objects, lineage and visibility."""
    scope_key = record.scope.key
    uid = record.document_uid
    unit = UnitKey(scope_key=scope_key, channel="index", method="keyword")
    source = SourceReference(
        scope=record.scope,
        document_id=record.document_id,
        document_uid=uid,
        document_version=versions,
        representation_id="text",
    )
    for number in range(1, versions + 1):
        await tx.put_version(
            scope_key,
            DocumentVersion(
                document_uid=uid,
                version=number,
                content_hash=sha256_digest(f"{uid}:{number}"),
                hash_profile="contract",
                representation_digests={},
                metadata={},
                created_at=utc_now(),
                event_id=f"evt-{number}",
            ),
        )
        await tx.put_representations(
            scope_key,
            [
                RepresentationSnapshot(
                    document_uid=uid,
                    version=number,
                    representation_id="text",
                    kind="text",
                    payload={"text": str(number)},
                    hash_profile="contract",
                )
            ],
        )
        await tx.open_visibility(unit, uid, number, number)
    await tx.put_event(
        IngestionEvent(
            event_id=random_id("evt"),
            scope=record.scope,
            document_id=record.document_id,
            document_uid=uid,
            received_at=utc_now(),
            status=EventStatus.COMMITTED,
            content_hash=sha256_digest("c"),
            appearance_fingerprint=sha256_digest(uid),
        )
    )
    chunk = DerivedObject(
        object_id=stable_id("obj", uid, "chunk"),
        scope=record.scope,
        kind=ObjectKind.CHUNK,
        document_uid=uid,
        version=versions,
        representation_id="text",
        text="chunk",
        sources=(source,),
        content_hash=sha256_digest("chunk"),
    )
    entity = DerivedObject(
        object_id=stable_id("obj", uid, "entity"),
        scope=record.scope,
        kind=ObjectKind.GRAPH_ENTITY,
        text="entity",
        sources=(source,),
        content_hash=sha256_digest("entity"),
    )
    await tx.put_objects([chunk, entity])
    await tx.replace_contributions(
        scope_key,
        uid,
        [
            ObjectContribution(
                object_id=entity.object_id, document_uid=uid, version=versions, field_path="name"
            )
        ],
    )
    await tx.put_lineage(
        [
            LineageEdge(
                scope_key=scope_key,
                from_id=chunk.object_id,
                to_id=uid,
                relation=LineageRelation.DERIVED_FROM,
            ),
            LineageEdge(
                scope_key=scope_key,
                from_id=entity.object_id,
                to_id=uid,
                relation=LineageRelation.SUPPORTED_BY,
            ),
        ]
    )
    return [chunk, entity]


class MetadataStoreContract(ComponentContract):
    """Kit for ``DocumentMetadataStoreABC``: transactions, CAS, isolation and lifecycle rows."""

    async def prepare(self, component: Any) -> None:
        """Migrate the schema to head."""
        await component.migrate()

    async def test_migrate_is_idempotent(self, component: Any) -> None:
        """Migrating twice keeps the schema at the same revision."""
        await component.migrate()
        version = await component.schema_version()
        await component.migrate()
        assert version is not None
        assert await component.schema_version() == version

    async def test_scopes_and_commit_sequence(self, component: Any) -> None:
        """Scopes register idempotently; the commit sequence is strictly monotonic."""
        async with component.transaction(write=True) as tx:
            await tx.ensure_scope(SCOPE)
            await tx.ensure_scope(SCOPE)
            first = await tx.next_commit_seq()
            second = await tx.next_commit_seq()
        async with component.transaction(write=False) as tx:
            assert SCOPE in await tx.list_scopes()
            assert await tx.current_commit_seq() == second
        assert second > first

    async def test_document_compare_and_set(self, component: Any) -> None:
        """Inserting twice or updating with a stale revision conflicts."""
        record = document(SCOPE, "doc-1")
        async with component.transaction(SCOPE, write=True) as tx:
            await tx.ensure_scope(SCOPE)
            await tx.put_document(record, expected_revision=None)
        with pytest.raises(VersionConflictError):
            async with component.transaction(SCOPE, write=True) as tx:
                await tx.put_document(record, expected_revision=None)
        async with component.transaction(SCOPE, write=False) as tx:
            stored = await tx.get_document(SCOPE, "doc-1")
        assert stored is not None
        assert stored.document_uid == record.document_uid
        updated = stored.model_copy(update={"metadata": {"k": "v"}})
        async with component.transaction(SCOPE, write=True) as tx:
            await tx.put_document(updated, expected_revision=stored.row_revision)
        with pytest.raises(VersionConflictError):
            async with component.transaction(SCOPE, write=True) as tx:
                await tx.put_document(updated, expected_revision=stored.row_revision)
        async with component.transaction(SCOPE, write=False) as tx:
            final = await tx.get_document(SCOPE, "doc-1")
            by_uid = await tx.get_documents_by_uid(SCOPE.key, [record.document_uid])
            listed = await tx.list_documents(SCOPE.key)
            metadata = await tx.list_document_metadata(SCOPE.key, state=DocumentState.ACTIVE)
            deleted = await tx.list_document_metadata(SCOPE.key, state=DocumentState.DELETED)
            after = await tx.list_document_metadata(SCOPE.key, after_id="doc-1", limit=1)
        assert final is not None
        assert final.metadata == {"k": "v"}
        assert final.row_revision > stored.row_revision
        assert set(by_uid) == {record.document_uid}
        assert [item.document_id for item in listed] == ["doc-1"]
        assert [(key, dict(value)) for key, value in metadata] == [("doc-1", {"k": "v"})]
        assert list(deleted) == list(after) == []

    async def test_rollback_on_exception(self, component: Any) -> None:
        """A failing transaction leaves no trace."""
        message = "boom"
        with pytest.raises(RuntimeError, match=message):  # noqa: PT012
            async with component.transaction(SCOPE, write=True) as tx:
                await tx.ensure_scope(SCOPE)
                await tx.put_document(document(SCOPE, "doc-r"), expected_revision=None)
                raise RuntimeError(message)
        async with component.transaction(SCOPE, write=False) as tx:
            assert await tx.get_document(SCOPE, "doc-r") is None

    async def test_tenant_isolation(self, component: Any) -> None:
        """A scope-bound transaction rejects another scope and never sees its rows."""
        async with component.transaction(write=True) as tx:
            await tx.ensure_scope(SCOPE)
            await tx.ensure_scope(OTHER_SCOPE)
            foreign = document(OTHER_SCOPE, "doc-x")
            await tx.put_document(foreign, expected_revision=None)
            assert await tx.get_documents_by_uid(SCOPE.key, [foreign.document_uid]) == {}
        with pytest.raises(TenantIsolationError):
            async with component.transaction(SCOPE, write=True) as tx:
                await tx.put_document(document(OTHER_SCOPE, "doc-y"), expected_revision=None)
        async with component.transaction(SCOPE, write=False) as tx:
            assert await tx.get_document(SCOPE, "doc-x") is None
            assert list(await tx.list_documents(SCOPE.key)) == []

    async def test_visibility_windows(self, component: Any) -> None:
        """Opening a version closes the previous one at that sequence; closing hides all."""
        unit = UnitKey(scope_key=SCOPE.key, channel="index", method="keyword")
        uid = stable_id("doc", "visibility")
        async with component.transaction(SCOPE, write=True) as tx:
            await tx.ensure_scope(SCOPE)
            await tx.open_visibility(unit, uid, 1, 10)
            await tx.open_visibility(unit, uid, 2, 20)
            await tx.close_visibility(SCOPE.key, uid, 30)
        async with component.transaction(SCOPE, write=False) as tx:
            assert dict(await tx.visible_versions(unit, [uid], 5)) == {}
            assert dict(await tx.visible_versions(unit, [uid], 15)) == {uid: frozenset({1})}
            assert dict(await tx.visible_versions(unit, [uid], 25)) == {uid: frozenset({2})}
            assert dict(await tx.visible_versions(unit, [uid], 30)) == {}

    async def test_batched_documents_and_versions(self, component: Any) -> None:
        """Batched document writes are all-or-nothing and compare-and-set every update."""
        first, second, third = (document(SCOPE, f"batch-{n}") for n in range(3))
        async with component.transaction(SCOPE, write=True) as tx:
            await tx.ensure_scope(SCOPE)
            await tx.put_documents([(first, None), (second, None)])
            await tx.put_versions(
                SCOPE.key,
                [
                    DocumentVersion(
                        document_uid=record.document_uid,
                        version=1,
                        content_hash=record.content_hash,
                        hash_profile="contract",
                        representation_digests={},
                        metadata={},
                        created_at=utc_now(),
                        event_id=f"evt-{record.document_id}",
                    )
                    for record in (first, second)
                ],
            )
        with pytest.raises(VersionConflictError):
            async with component.transaction(SCOPE, write=True) as tx:
                await tx.put_documents([(third, None), (first, None)])
        async with component.transaction(SCOPE, write=False) as tx:
            stored = await tx.get_documents(SCOPE, ["batch-0", "batch-1", "batch-2", "batch-0"])
            versions = await tx.get_versions(second.document_uid)
        assert sorted(stored) == ["batch-0", "batch-1"]
        assert [version.version for version in versions] == [1]
        one, two = stored["batch-0"], stored["batch-1"]
        updates = [
            (one.model_copy(update={"metadata": {"n": 1}}), one.row_revision),
            (two.model_copy(update={"metadata": {"n": 2}}), two.row_revision),
        ]
        with pytest.raises(VersionConflictError):
            async with component.transaction(SCOPE, write=True) as tx:
                await tx.put_documents([updates[0], (updates[1][0], two.row_revision + 7)])
        async with component.transaction(SCOPE, write=True) as tx:
            await tx.put_documents([*updates, (third, None)])
        async with component.transaction(SCOPE, write=False) as tx:
            final = await tx.get_documents(SCOPE, ["batch-0", "batch-1", "batch-2"])
        assert {key: record.metadata for key, record in final.items()} == {
            "batch-0": {"n": 1},
            "batch-1": {"n": 2},
            "batch-2": {},
        }
        assert final["batch-0"].row_revision == one.row_revision + 1

    async def test_batched_events(self, component: Any) -> None:
        """Events upsert in batches and fingerprint probes find the latest other event."""
        now = utc_now()

        def event(event_id: str, fingerprint: str, age: int, **values: Any) -> IngestionEvent:
            return IngestionEvent(
                event_id=event_id,
                scope=SCOPE,
                document_id="doc",
                document_uid=stable_id("doc", "events"),
                received_at=now - timedelta(seconds=age),
                status=values.pop("status", EventStatus.COMMITTED),
                content_hash=sha256_digest("c"),
                appearance_fingerprint=sha256_digest(fingerprint),
                **values,
            )

        async with component.transaction(SCOPE, write=True) as tx:
            await tx.ensure_scope(SCOPE)
            await tx.put_events(
                [
                    event("e-old", "a", 300),
                    event("e-new", "a", 10),
                    event("e-pending", "a", 1, status=EventStatus.PENDING),
                    event("e-b", "b", 10),
                    event("e-b", "b", 5, counter_after=2),
                ]
            )
            await tx.put_events([])
        async with component.transaction(SCOPE, write=True) as tx:
            await tx.put_events([event("e-old", "a", 300, counter_after=9)])
        async with component.transaction(SCOPE, write=False) as tx:
            loaded = await tx.get_events(SCOPE.key, ["e-old", "e-b", "e-missing"])
            found = await tx.find_events_by_fingerprints(
                SCOPE.key,
                {
                    sha256_digest("a"): "e-x",
                    sha256_digest("b"): "e-b",
                    sha256_digest("c"): "e-y",
                },
                since=now - timedelta(seconds=60),
            )
            excluded = await tx.find_events_by_fingerprints(
                SCOPE.key, {sha256_digest("a"): "e-new"}, since=now - timedelta(seconds=600)
            )
        assert sorted(loaded) == ["e-b", "e-old"]
        assert loaded["e-old"].counter_after == 9
        assert loaded["e-b"].counter_after == 2
        assert {key: item.event_id for key, item in found.items()} == {sha256_digest("a"): "e-new"}
        assert {key: item.event_id for key, item in excluded.items()} == {
            sha256_digest("a"): "e-old"
        }

    async def test_batched_objects_and_visibility(self, component: Any) -> None:
        """Objects list per document in batches; visibility opens in batches like one by one."""
        unit = UnitKey(scope_key=SCOPE.key, channel="index", method="keyword")
        records = [document(SCOPE, f"objs-{n}") for n in range(3)]
        uids = [record.document_uid for record in records]
        async with component.transaction(SCOPE, write=True) as tx:
            await tx.ensure_scope(SCOPE)
            for record in records[:2]:
                await tx.put_document(record, expected_revision=None)
                await _seed_history(tx, record, 2)
            await tx.open_visibility_many(unit, [], 3)
            # Version 2 of uids[0] is already open; uids[1] goes back to 1; uids[2] is new.
            await tx.open_visibility_many(unit, [(uids[0], 2), (uids[1], 1), (uids[2], 1)], 3)
            await tx.close_visibility(SCOPE.key, uids[2], 3)
            await tx.open_visibility_many(unit, [(uids[2], 5), (uids[2], 1)], 3)
        async with component.transaction(SCOPE, write=False) as tx:
            visible = await tx.visible_versions(unit, uids, 3)
            listed = await tx.list_objects_for_documents(
                SCOPE.key, [(uids[0], 2), (uids[1], 1), (uids[2], None), (uids[0], None)]
            )
            pinned = await tx.list_objects_for_documents(
                SCOPE.key, [(uids[0], 2), (uids[1], 2), (uids[1], 1)], [ObjectKind.CHUNK]
            )
            empty = await tx.list_objects_for_documents(SCOPE.key, [])
        assert dict(visible) == {
            uids[0]: frozenset({2}),
            uids[1]: frozenset({1}),
            uids[2]: frozenset({1}),
        }
        assert [obj.kind for obj in listed[uids[0]]] == [ObjectKind.CHUNK]
        assert listed[uids[1]] == []
        assert listed[uids[2]] == []
        assert {uid: [obj.version for obj in objs] for uid, objs in pinned.items()} == {
            uids[0]: [2],
            uids[1]: [2],
        }
        assert dict(empty) == {}

    async def test_batched_coverage_state(self, component: Any) -> None:
        """Coverage state changes for many documents at once."""
        unit = UnitKey(scope_key=SCOPE.key, channel="index", method="keyword")
        uids = [stable_id("doc", "coverage", str(n)) for n in range(3)]
        async with component.transaction(SCOPE, write=True) as tx:
            await tx.ensure_scope(SCOPE)
            await tx.upsert_coverage(
                [
                    CoverageRecord(
                        scope_key=SCOPE.key,
                        document_uid=uid,
                        channel=Channel.INDEX,
                        method="keyword",
                        target_version=1,
                        state=CoverageState.READY,
                        updated_at=utc_now(),
                    )
                    for uid in uids
                ]
            )
            await tx.set_documents_coverage_state(
                SCOPE.key, {uids[0]: 4, uids[1]: 5}, CoverageState.PENDING
            )
            await tx.set_documents_coverage_state(SCOPE.key, {}, CoverageState.PENDING)
        async with component.transaction(SCOPE, write=False) as tx:
            rows = await tx.get_coverage(unit, uids)
        assert {uid: (row.state, row.target_version) for uid, row in rows.items()} == {
            uids[0]: (CoverageState.PENDING, 4),
            uids[1]: (CoverageState.PENDING, 5),
            uids[2]: (CoverageState.READY, 1),
        }
        assert rows[uids[0]].row_revision == rows[uids[2]].row_revision + 1

    async def test_batched_document_retirement(self, component: Any) -> None:
        """Object states, contributions, lineage and visibility change for many documents."""
        unit = UnitKey(scope_key=SCOPE.key, channel="index", method="keyword")
        records = [document(SCOPE, f"retire-{n}", change_seq=n + 7) for n in range(2)]
        uids = [record.document_uid for record in records]
        foreign = document(OTHER_SCOPE, "retire-x")
        async with component.transaction(OTHER_SCOPE, write=True) as tx:
            await tx.ensure_scope(OTHER_SCOPE)
            await tx.put_document(foreign, expected_revision=None)
        async with component.transaction(SCOPE, write=True) as tx:
            await tx.ensure_scope(SCOPE)
            seeded = []
            for record in records:
                await tx.put_document(record, expected_revision=None)
                seeded.append(await _seed_history(tx, record, 2))
            await tx.set_documents_objects_state(
                SCOPE.key, {uids[0]: 2, uids[1]: None}, ObjectState.SUPERSEDED
            )
            await tx.set_documents_objects_state(SCOPE.key, {}, ObjectState.SUPERSEDED)
            retracted = await tx.retract_contributions(SCOPE.key, [uids[1], uids[0], uids[1]])
            await tx.close_visibility_many(SCOPE.key, [uids[0], uids[1], uids[0]], 5)
            await tx.close_visibility_many(SCOPE.key, [], 5)
        chunks = [objects[0].object_id for objects in seeded]
        entities = [objects[1].object_id for objects in seeded]
        async with component.transaction(SCOPE, write=False) as tx:
            listed = await tx.list_objects_for_documents(SCOPE.key, [(uid, None) for uid in uids])
            contributions = await tx.get_contributions(SCOPE.key, entities)
            edges = await tx.get_lineage_many(SCOPE.key, [chunks[0], uids[0], uids[1]])
            before = await tx.visible_versions(unit, uids, 4)
            after = await tx.visible_versions(unit, uids, 5)
            seqs = await tx.get_change_seqs(SCOPE.key, [*uids, uids[0], foreign.document_uid])
            assert await tx.get_lineage_many(SCOPE.key, []) == []
        assert {uid: [obj.state for obj in listed[uid]] for uid in uids} == {
            uids[0]: [ObjectState.LIVE],
            uids[1]: [ObjectState.SUPERSEDED],
        }
        assert list(retracted) == sorted(entities)
        assert list(contributions) == []
        assert [(edge.from_id, edge.to_id) for edge in edges] == sorted(
            [
                (chunks[0], uids[0]),
                (entities[0], uids[0]),
                (chunks[1], uids[1]),
                (entities[1], uids[1]),
            ]
        )
        assert dict(before) == {uid: frozenset({2}) for uid in uids}
        assert dict(after) == {}
        assert dict(seqs) == {uids[0]: 7, uids[1]: 8}

    async def test_snapshot_marker(self, component: Any) -> None:
        """The marker carries the commit sequence; a token, if any, moves with writes."""
        unit = UnitKey(scope_key=SCOPE.key, channel="index", method="keyword")
        built = generation(SCOPE)
        writes = [
            lambda tx: tx.put_documents([(document(SCOPE, "marker"), None)]),
            lambda tx: tx.put_generation(built, expected_revision=None),
            lambda tx: tx.set_pointer(unit, built.generation_id, expected_revision=None),
            lambda tx: tx.upsert_coverage(
                [
                    CoverageRecord(
                        scope_key=SCOPE.key,
                        document_uid=stable_id("doc", "marker"),
                        channel=Channel.INDEX,
                        method="keyword",
                        target_version=1,
                        state=CoverageState.PENDING,
                        updated_at=utc_now(),
                    )
                ]
            ),
        ]
        async with component.transaction(SCOPE, write=True) as tx:
            await tx.ensure_scope(SCOPE)
        tokens = []
        for write in writes:
            async with component.transaction(SCOPE, write=True) as tx:
                await write(tx)
            async with component.transaction(SCOPE, write=False) as tx:
                seq, token = await tx.snapshot_marker(SCOPE.key)
                assert seq == await tx.current_commit_seq()
                assert await tx.snapshot_marker(SCOPE.key) == (seq, token)
            tokens.append(token)
        assert tokens == [None] * len(writes) or (
            None not in tokens and len(set(tokens)) == len(writes)
        )

    async def test_changed_documents_and_coverage_watermarks(self, component: Any) -> None:
        """Changed documents page by ``change_seq`` groups; watermarks are per unit."""
        seqs = {"c-a": 1, "c-b": 2, "c-c": 2, "c-d": 3}
        shared = UnitKey(scope_key=SCOPE.key, channel="index", method="keyword")
        local = UnitKey(scope_key=SCOPE.key, channel="index", method="keyword", instance="inst-w")
        async with component.transaction(SCOPE, write=True) as tx:
            await tx.ensure_scope(SCOPE)
            await tx.put_documents(
                [(document(SCOPE, name, change_seq=seq), None) for name, seq in seqs.items()]
            )
            assert await tx.get_coverage_watermark(shared) is None
            await tx.set_coverage_watermark(shared, 2)
            await tx.set_coverage_watermark(shared, 3)
            await tx.set_coverage_watermark(local, 1)
        async with component.transaction(SCOPE, write=False) as tx:
            group = await tx.list_changed_documents(SCOPE.key, after_seq=0, limit=2)
            first = await tx.list_changed_documents(SCOPE.key, after_seq=0, limit=1)
            rest = await tx.list_changed_documents(SCOPE.key, after_seq=2)
            done = await tx.list_changed_documents(SCOPE.key, after_seq=3)
            marks = (
                await tx.get_coverage_watermark(shared),
                await tx.get_coverage_watermark(local),
            )
        ties = sorted(("c-b", "c-c"), key=lambda name: stable_id("doc", SCOPE.key, name))
        assert [item.document_id for item in group] == ["c-a", *ties]
        assert [item.document_id for item in first] == ["c-a"]
        assert [(item.document_id, item.change_seq) for item in rest] == [("c-d", 3)]
        assert list(done) == []
        assert marks == (3, 1)
        async with component.transaction(write=True) as tx:
            await tx.purge_instance("inst-w")
        async with component.transaction(SCOPE, write=False) as tx:
            assert await tx.get_coverage_watermark(local) is None
            assert await tx.get_coverage_watermark(shared) == 3

    async def test_keyword_dictionaries(self, component: Any) -> None:
        """Term counts adjust in place; the revision moves only when terms come or go."""
        async with component.transaction(SCOPE, write=True) as tx:
            await tx.ensure_scope(SCOPE)
            assert list(await tx.list_keyword_dictionaries(SCOPE.key)) == []
            await tx.create_keyword_dictionary(
                SCOPE.key, "dict", ["keywords"], {"beta": 1, "Alpha": 2, "zero": 0}
            )
            with pytest.raises(VersionConflictError):
                await tx.create_keyword_dictionary(SCOPE.key, "dict", ["keywords"], {})
        async with component.transaction(SCOPE, write=True) as tx:
            await tx.adjust_keyword_terms(SCOPE.key, "dict", {"Alpha": -1, "beta": 2})
            await tx.adjust_keyword_terms(SCOPE.key, "dict", {"beta": 0})
            await tx.adjust_keyword_terms(SCOPE.key, "missing", {"beta": 1})
            assert await tx.list_keyword_terms(SCOPE.key, "missing") == []
            unchanged = await tx.list_keyword_dictionaries(SCOPE.key)
            await tx.adjust_keyword_terms(SCOPE.key, "dict", {"Alpha": -1, "gamma": 1, "delta": -1})
        async with component.transaction(SCOPE, write=False) as tx:
            states = await tx.list_keyword_dictionaries(SCOPE.key)
            terms = await tx.list_keyword_terms(SCOPE.key, "dict")
        assert [(s.dictionary_id, s.fields, s.revision) for s in unchanged] == [
            ("dict", ("keywords",), 1)
        ]
        assert [state.revision for state in states] == [2]
        assert list(terms) == ["beta", "gamma"]

    async def test_generations_and_pointer_cas(self, component: Any) -> None:
        """Generations CAS by revision; pointers publish with compare-and-set."""
        built = generation(SCOPE)
        unit = UnitKey(scope_key=SCOPE.key, channel="index", method="keyword")
        async with component.transaction(SCOPE, write=True) as tx:
            await tx.ensure_scope(SCOPE)
            await tx.put_generation(built, expected_revision=None)
            await tx.set_pointer(unit, built.generation_id, expected_revision=None)
        async with component.transaction(SCOPE, write=False) as tx:
            stored = await tx.get_generation(built.generation_id)
            pointer = await tx.get_pointer(unit)
        assert stored is not None
        assert pointer is not None
        assert pointer.generation_id == built.generation_id
        with pytest.raises(VersionConflictError):
            async with component.transaction(SCOPE, write=True) as tx:
                await tx.set_pointer(unit, built.generation_id, expected_revision=None)
        active = stored.model_copy(update={"state": GenerationState.ACTIVE})
        async with component.transaction(SCOPE, write=True) as tx:
            await tx.put_generation(active, expected_revision=stored.row_revision)
            await tx.set_pointer(unit, built.generation_id, expected_revision=pointer.row_revision)
        with pytest.raises(VersionConflictError):
            async with component.transaction(SCOPE, write=True) as tx:
                await tx.put_generation(active, expected_revision=stored.row_revision)
        async with component.transaction(SCOPE, write=False) as tx:
            listed = await tx.list_generations(scope_key=SCOPE.key, states=[GenerationState.ACTIVE])
            pointers = await tx.list_pointers(SCOPE.key, [""])
        assert [item.generation_id for item in listed] == [built.generation_id]
        assert [row.generation_id for row in pointers] == [built.generation_id]

    async def test_task_fencing(self, component: Any) -> None:
        """Task updates with a stale revision are rejected."""
        now = utc_now()
        task = LifecycleTask(
            task_id=random_id("task"),
            scope_key=SCOPE.key,
            kind=TaskKind.REBUILD,
            state=TaskState.QUEUED,
            created_at=now,
            updated_at=now,
        )
        async with component.transaction(SCOPE, write=True) as tx:
            await tx.ensure_scope(SCOPE)
            await tx.put_task(task, expected_revision=None)
        async with component.transaction(SCOPE, write=False) as tx:
            stored = await tx.get_task(task.task_id)
        assert stored is not None
        running = stored.model_copy(update={"state": TaskState.RUNNING})
        async with component.transaction(SCOPE, write=True) as tx:
            await tx.put_task(running, expected_revision=stored.row_revision)
        with pytest.raises(VersionConflictError):
            async with component.transaction(SCOPE, write=True) as tx:
                await tx.put_task(running, expected_revision=stored.row_revision)
        async with component.transaction(SCOPE, write=False) as tx:
            listed = await tx.list_tasks(scope_key=SCOPE.key, states=[TaskState.RUNNING])
        assert [item.task_id for item in listed] == [task.task_id]

    async def test_reader_leases_and_instances(self, component: Any) -> None:
        """Leases count until expiry; expired instances are reported and purgeable."""
        now = utc_now()
        later = now + timedelta(minutes=5)
        async with component.transaction(write=True) as tx:
            await tx.put_reader_lease("gen", "engine-1", expires_at=later, now=now)
            await tx.put_reader_lease("gen", "engine-2", expires_at=now, now=now)
            await tx.put_reader_lease("gen", "inst-1", expires_at=later, now=now)
            await tx.heartbeat_instance("inst-1", "engine-1", started_at=now, expires_at=now)
        async with component.transaction(write=False) as tx:
            assert await tx.count_reader_leases("gen", now=now) == 2
            assert "inst-1" in await tx.expired_instances(now=later)
        async with component.transaction(write=True) as tx:
            assert await tx.purge_expired_leases(now=now) == 1
            await tx.delete_reader_lease("gen", "engine-1")
            await tx.purge_instance("inst-1")
        async with component.transaction(write=False) as tx:
            assert await tx.count_reader_leases("gen", now=now) == 0
            assert "inst-1" not in await tx.expired_instances(now=later)

    async def test_refresh_state_persistence(self, component: Any) -> None:
        """Refresh-engine state round-trips per namespace with puts and deletes."""
        a = StoredRefreshState(resource_id="a", state={"n": 1})
        b = StoredRefreshState(resource_id="b", state={"n": 2})
        async with component.transaction(write=True) as tx:
            await tx.apply_refresh_states("ns", [a, b], [])
        async with component.transaction(write=True) as tx:
            await tx.apply_refresh_states("ns", [a.model_copy(update={"state": {"n": 3}})], ["b"])
        async with component.transaction(write=False) as tx:
            states = await tx.load_refresh_states("ns")
            assert list(await tx.load_refresh_states("other")) == []
        assert [(item.resource_id, item.state) for item in states] == [("a", {"n": 3})]

    async def test_purge_document_erases_every_row(self, component: Any) -> None:
        """Only tombstones with fully deleted coverage are purgeable; purge is idempotent."""
        record = document(SCOPE, "doc-purge", 2)
        other = document(OTHER_SCOPE, "doc-purge", 2)
        unit = UnitKey(scope_key=SCOPE.key, channel="index", method="keyword")
        async with component.transaction(write=True) as tx:
            await tx.ensure_scope(SCOPE)
            await tx.ensure_scope(OTHER_SCOPE)
            await tx.put_document(other, expected_revision=None)
            await _seed_history(tx, other, 2)
        async with component.transaction(SCOPE, write=True) as tx:
            await tx.put_document(record, expected_revision=None)
            objects = await _seed_history(tx, record, 2)
            await tx.upsert_coverage(
                [
                    CoverageRecord(
                        scope_key=SCOPE.key,
                        document_uid=record.document_uid,
                        channel=Channel.INDEX,
                        method="keyword",
                        state=CoverageState.READY,
                        updated_at=utc_now(),
                    )
                ]
            )
        with pytest.raises(LifecycleError):
            async with component.transaction(SCOPE, write=True) as tx:
                await tx.purge_document(SCOPE.key, record.document_uid)
        deleted_at = utc_now()
        later = deleted_at + timedelta(seconds=1)
        async with component.transaction(SCOPE, write=True) as tx:
            stored = await tx.get_document(SCOPE, "doc-purge")
            tombstone = stored.model_copy(
                update={"state": DocumentState.DELETED, "deleted_at": deleted_at}
            )
            await tx.put_document(tombstone, expected_revision=stored.row_revision)
            await tx.set_document_coverage_state(
                SCOPE.key, record.document_uid, CoverageState.PENDING, target_version=2
            )
            assert list(await tx.list_purgeable_documents(SCOPE.key, deleted_before=later)) == []
            await tx.set_document_coverage_state(
                SCOPE.key, record.document_uid, CoverageState.DELETED, target_version=2
            )
            assert await tx.list_purgeable_documents(SCOPE.key, deleted_before=deleted_at) == []
            listed = await tx.list_purgeable_documents(SCOPE.key, deleted_before=later)
            assert [item.document_uid for item in listed] == [record.document_uid]
            assert await tx.purge_document(
                SCOPE.key, record.document_uid, object_ids=[objects[1].object_id]
            )
            assert await tx.purge_document(SCOPE.key, record.document_uid) == 0
        async with component.transaction(SCOPE, write=False) as tx:
            assert await tx.get_document(SCOPE, "doc-purge") is None
            assert list(await tx.get_versions(record.document_uid)) == []
            assert list(await tx.get_representations(record.document_uid, 1)) == []
            assert dict(await tx.get_objects(SCOPE.key, [o.object_id for o in objects])) == {}
            assert list(await tx.list_document_contributions(SCOPE.key, record.document_uid)) == []
            assert list(await tx.get_lineage(SCOPE.key, objects[0].object_id)) == []
            assert dict(await tx.get_coverage(unit, [record.document_uid])) == {}
            assert dict(await tx.visible_versions(unit, [record.document_uid], 10**6)) == {}
        async with component.transaction(OTHER_SCOPE, write=False) as tx:
            assert await tx.get_document(OTHER_SCOPE, "doc-purge") is not None
            assert len(await tx.get_versions(other.document_uid)) == 2

    async def test_purge_documents_appends_to_the_purge_log(self, component: Any) -> None:
        """Batch purges log one dense, per-scope entry per erased document."""
        records = [
            document(SCOPE, f"doc-batch-{i}", 1, state=DocumentState.DELETED) for i in (1, 2)
        ]
        async with component.transaction(write=True) as tx:
            await tx.ensure_scope(SCOPE)
            await tx.ensure_scope(OTHER_SCOPE)
        async with component.transaction(SCOPE, write=True) as tx:
            start = (await tx.purge_log_heads()).get(SCOPE.key, 0)
            objects = []
            for record in records:
                await tx.put_document(record, expected_revision=None)
                objects.append(await _seed_history(tx, record, 1))
        purged_at = utc_now()
        async with component.transaction(SCOPE, write=True) as tx:
            uids = {record.document_uid: [] for record in records}
            assert await tx.purge_documents(SCOPE.key, uids, purged_at=purged_at) > 0
            assert await tx.purge_documents(SCOPE.key, uids, purged_at=purged_at) == 0
        async with component.transaction(SCOPE, write=False) as tx:
            entries = await tx.list_purge_log(SCOPE.key, after_seq=start)
            assert (await tx.purge_log_heads()) == {SCOPE.key: start + 2}
            for record in records:
                assert await tx.get_document(SCOPE, record.document_id) is None
        assert [entry.seq for entry in entries] == [start + 1, start + 2]
        assert sorted(entry.document_uid for entry in entries) == sorted(uids)
        for entry in entries:
            assert entry.scope_key == SCOPE.key
            assert entry.purged_at == purged_at
            assert entry.object_ids
        later = purged_at + timedelta(seconds=1)
        async with component.transaction(SCOPE, write=True) as tx:
            assert await tx.prune_purge_log(SCOPE.key, before=later) >= 1
            remaining = await tx.list_purge_log(SCOPE.key, after_seq=0)
        assert [entry.seq for entry in remaining] == [start + 2]
        async with component.transaction(OTHER_SCOPE, write=False) as tx:
            assert SCOPE.key not in await tx.purge_log_heads()

    async def test_retention_keeps_versions_referenced_by_events(self, component: Any) -> None:
        """A version stays while an ingestion event names it as ``version_after``."""
        record = document(SCOPE, "doc-evented", 3)
        async with component.transaction(SCOPE, write=True) as tx:
            await tx.ensure_scope(SCOPE)
            await tx.put_document(record, expected_revision=None)
            await _seed_history(tx, record, 3)
            await tx.put_event(
                IngestionEvent(
                    event_id=random_id("evt"),
                    scope=SCOPE,
                    document_id=record.document_id,
                    document_uid=record.document_uid,
                    received_at=utc_now() - timedelta(days=2),
                    status=EventStatus.COMMITTED,
                    content_hash=sha256_digest("c"),
                    appearance_fingerprint=sha256_digest("evented"),
                    version_after=1,
                )
            )
            assert await tx.prune_versions(SCOPE.key, keep=1) == 1
            assert [item.version for item in await tx.get_versions(record.document_uid)] == [1, 3]
            assert await tx.prune_events(SCOPE.key, before=utc_now() - timedelta(days=1)) == 1
            assert await tx.prune_versions(SCOPE.key, keep=1) == 1
            assert [item.version for item in await tx.get_versions(record.document_uid)] == [3]

    async def test_retention_prunes_versions_and_events(self, component: Any) -> None:
        """Old unprotected versions and old terminal events are pruned; the rest stay."""
        record = document(SCOPE, "doc-retained", 4)
        async with component.transaction(SCOPE, write=True) as tx:
            await tx.ensure_scope(SCOPE)
            await tx.put_document(record, expected_revision=None)
            await _seed_history(tx, record, 4)
            assert await tx.prune_versions(SCOPE.key, keep=1) == 3
            assert await tx.prune_versions(SCOPE.key, keep=1) == 0
            assert [item.version for item in await tx.get_versions(record.document_uid)] == [4]
            assert list(await tx.get_representations(record.document_uid, 1)) == []
            future = utc_now() + timedelta(days=1)
            assert await tx.prune_events(SCOPE.key, before=utc_now() - timedelta(days=1)) == 0
            assert await tx.prune_events(SCOPE.key, before=future) == 1
            assert await tx.prune_events(SCOPE.key, before=future) == 0
