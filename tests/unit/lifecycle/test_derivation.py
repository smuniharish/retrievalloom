"""Derived objects: chunks, structure nodes, lineage and graph aggregates."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from retrievalloom.contracts.models import (
    NormalizedEntity,
    NormalizedGraph,
    NormalizedRelationship,
)
from retrievalloom.domain.inputs import (
    ChunkInput,
    ChunksRepresentation,
    SourceDescriptor,
    StructureNodeInput,
    StructureRepresentation,
    TextRepresentation,
    TextSpan,
)
from retrievalloom.domain.records import (
    DocumentRecord,
    DocumentState,
    LineageRelation,
    ObjectKind,
    ObjectState,
)
from retrievalloom.lifecycle.canonical import document_uid
from retrievalloom.lifecycle.derivation import (
    derive_chunks,
    derive_structure,
    document_filter_record,
    entity_id,
    graph_contributions,
    lineage_for,
    recompute_aggregates,
    relationship_id,
    text_hash,
)
from tests.unit.lifecycle.helpers import SCOPE

NOW = datetime(2026, 1, 1, tzinfo=UTC)


def record(document_id: str = "d1", version: int = 1, **extra: Any) -> DocumentRecord:
    return DocumentRecord.model_validate(
        {
            "_id": document_id,
            "_counter": version,
            "_version": version,
            "_content_hash": "sha256:" + "0" * 64,
            "_created_at": NOW,
            "_updated_at": NOW,
            "document_uid": document_uid(SCOPE, document_id),
            "scope": SCOPE,
            "hash_profile": "rl-v1",
            **extra,
        }
    )


def test_filter_record_exposes_reserved_fields() -> None:
    plain = document_filter_record(record(metadata={"lang": "en"}))
    assert plain == {"lang": "en", "_id": "d1"}
    assert document_filter_record(record(dataset="ds"))["_dataset"] == "ds"


def test_text_representation_yields_one_chunk_with_source() -> None:
    source = SourceDescriptor(uri="file://a", title="A")
    doc = record(metadata={"lang": "en"}, source=source, version=3)
    [chunk] = derive_chunks(doc, TextRepresentation(text="Hello", metadata={"part": 1}))
    assert chunk.kind == ObjectKind.CHUNK
    assert chunk.text == "Hello"
    assert chunk.payload == {"ordinal": 0}
    assert chunk.metadata == {"lang": "en", "part": 1, "_id": "d1", "_kind": "chunk", "_version": 3}
    assert chunk.content_hash == text_hash("Hello")
    assert chunk.sources[0].source_uri == "file://a"
    assert chunk.sources[0].title == "A"
    assert chunk.sources[0].document_version == 3
    [unsourced] = derive_chunks(record(), TextRepresentation(text="Hello"))
    assert unsourced.sources[0].source_uri is None
    assert unsourced.object_id != chunk.object_id  # ids are per version


def test_chunks_are_ordered_and_linked_to_parents() -> None:
    representation = ChunksRepresentation(
        metadata={"rep": 1, "shared": "rep"},
        chunks=(
            ChunkInput(local_id="c", text="third", order=3, parent_local_id="a"),
            ChunkInput(local_id="a", text="first", order=1, metadata={"shared": "chunk"}),
            ChunkInput(text="second", order=2, parent_local_id="missing"),
            ChunkInput(local_id="self", text="fourth", order=4, parent_local_id="self",
                       span=TextSpan(start=0, end=6)),
        ),
    )  # fmt: skip
    objects = derive_chunks(record(), representation)
    assert [obj.text for obj in objects] == ["first", "second", "third", "fourth"]
    assert [obj.payload["ordinal"] for obj in objects] == [0, 1, 2, 3]
    first, second, third, fourth = objects
    assert third.payload["parent_object_id"] == first.object_id
    assert "parent_object_id" not in second.payload
    assert "parent_object_id" not in fourth.payload
    assert first.metadata["shared"] == "chunk"
    assert second.metadata["shared"] == "rep"
    assert fourth.sources[0].span == TextSpan(start=0, end=6)
    # Same input derives the same ids (idempotent re-derivation).
    assert [obj.object_id for obj in derive_chunks(record(), representation)] == [
        obj.object_id for obj in objects
    ]


def test_chunks_without_order_keep_submission_order() -> None:
    representation = ChunksRepresentation(chunks=(ChunkInput(text="b"), ChunkInput(text="a")))
    assert [obj.text for obj in derive_chunks(record(), representation)] == ["b", "a"]


def test_structure_nodes_preorder_with_paths_and_levels() -> None:
    root = StructureNodeInput(
        title="Guide",
        children=(
            StructureNodeInput(
                title="Intro",
                text="Welcome",
                level=7,
                children=(StructureNodeInput(text="untitled leaf"),),
            ),
            StructureNodeInput(local_id="loops", title="Loops", text="   "),
        ),
    )
    objects = derive_structure(record(), StructureRepresentation(root=root, metadata={"r": 1}))
    assert [obj.text for obj in objects] == ["Guide", "Welcome", "untitled leaf", "Loops"]
    guide, intro, leaf, loops = objects
    assert guide.payload["child_object_ids"] == [intro.object_id, loops.object_id]
    assert "parent_object_id" not in guide.payload
    assert intro.payload["parent_object_id"] == guide.object_id
    assert leaf.payload["parent_object_id"] == intro.object_id
    assert (guide.payload["level"], intro.payload["level"], leaf.payload["level"]) == (0, 7, 2)
    assert leaf.payload["path"] == ["Guide", "Intro"]
    assert leaf.sources[0].section_path == ("Guide", "Intro")
    assert loops.payload["ordinal"] == 1
    assert all(obj.kind == ObjectKind.STRUCTURE_NODE for obj in objects)
    assert guide.metadata["r"] == 1


def test_lineage_edges() -> None:
    representation = ChunksRepresentation(
        chunks=(
            ChunkInput(local_id="p", text="parent"),
            ChunkInput(text="kid", parent_local_id="p"),
        )
    )
    parent, child = derive_chunks(record(), representation)
    edges = lineage_for([parent, child])
    relations = {(edge.from_id, edge.to_id, edge.relation) for edge in edges}
    uid = document_uid(SCOPE, "d1")
    assert relations == {
        (parent.object_id, uid, LineageRelation.DERIVED_FROM),
        (child.object_id, uid, LineageRelation.DERIVED_FROM),
        (child.object_id, parent.object_id, LineageRelation.CHILD_OF),
    }
    aggregate = child.model_copy(update={"document_uid": None, "payload": {}})
    assert lineage_for([aggregate]) == []


GRAPH = NormalizedGraph(
    entities=(
        NormalizedEntity(
            canonical_key="python",
            entity_type="language",
            name="Python",
            aliases=("Py", "python"),
            description="A language",
            properties={"b": 2, "a": 1},
        ),
        NormalizedEntity(canonical_key="asyncio", entity_type="library", name="asyncio"),
    ),
    relationships=(
        NormalizedRelationship(
            source_key="asyncio",
            source_type="library",
            target_key="python",
            target_type="language",
            relation_type="part_of",
            description="stdlib",
            properties={"since": "3.4"},
        ),
        NormalizedRelationship(
            source_key="ghost",
            source_type="thing",
            target_key="python",
            target_type="language",
            relation_type="haunts",
            qualifier="q",
        ),
    ),
)


def test_graph_contributions_are_source_attributed() -> None:
    doc = record()
    contributions = graph_contributions(doc, "g", GRAPH)
    python = entity_id(SCOPE, "language", "python")
    by_path = {(c.object_id, c.field_path, c.ordinal): c.value for c in contributions}
    assert by_path[(python, "name", 0)] == "Python"
    assert by_path[(python, "alias", 1)] == "python"
    assert by_path[(python, "description", 0)] == "A language"
    assert [c.field_path for c in contributions if c.object_id == python][-2:] == [
        "property.a",
        "property.b",
    ]
    asyncio_id = entity_id(SCOPE, "library", "asyncio")
    assert (asyncio_id, "description", 0) not in by_path
    ghost = relationship_id(SCOPE, entity_id(SCOPE, "thing", "ghost"), "haunts", python, "q")
    identity = by_path[(ghost, "identity", 0)]
    assert identity["source_name"] == "ghost"  # unknown endpoint falls back to its key
    assert identity["target_name"] == "Python"
    assert all(c.document_uid == doc.document_uid and c.version == 1 for c in contributions)


def test_aggregates_merge_live_support_and_orphan_unsupported() -> None:
    first = record("d1")
    second = record("d2", metadata={})
    other = NormalizedGraph(
        entities=(
            NormalizedEntity(
                canonical_key="python", entity_type="language", name="CPython",
                aliases=("PYTHON",), properties={"a": "other"},
            ),
        )
    )  # fmt: skip
    contributions = [
        *graph_contributions(first, "g1", GRAPH),
        *graph_contributions(second, "g2", other),
    ]
    python = entity_id(SCOPE, "language", "python")
    part_of = relationship_id(
        SCOPE, entity_id(SCOPE, "library", "asyncio"), "part_of", python, None
    )
    documents = {first.document_uid: first, second.document_uid: second}
    aggregates = {
        obj.object_id: obj
        for obj in recompute_aggregates(SCOPE, [python, part_of], contributions, documents, {})
    }
    entity = aggregates[python]
    assert entity.text == "Python: A language"
    # Names of other documents become aliases; case-insensitive duplicates of the name drop.
    assert entity.payload["aliases"] == ["CPython", "Py"]
    assert entity.payload["properties"] == {"a": 1, "b": 2}
    assert [source.document_id for source in entity.sources] == ["d1", "d2"]
    assert [source.representation_id for source in entity.sources] == ["g1", "g2"]
    assert entity.metadata == {"_kind": "graph_entity"}
    relation = aggregates[part_of]
    assert relation.text == "asyncio part_of Python: stdlib"
    assert relation.payload["properties"] == {"since": "3.4"}
    assert relation.payload["relation_type"] == "part_of"

    # Deleted documents and superseded versions no longer support aggregates.
    deleted = first.model_copy(update={"state": DocumentState.DELETED})
    moved_on = second.model_copy(update={"version": 2})
    gone = recompute_aggregates(
        SCOPE,
        [python, part_of, "obj_" + "0" * 32],
        contributions,
        {first.document_uid: deleted, second.document_uid: moved_on},
        {python: entity, part_of: relation.model_copy(update={"state": ObjectState.ORPHANED})},
    )
    assert [(obj.object_id, obj.state) for obj in gone] == [(python, ObjectState.ORPHANED)]


def test_relationship_without_description_has_plain_text() -> None:
    doc = record()
    contributions = graph_contributions(doc, "g", GRAPH)
    ghost = relationship_id(
        SCOPE,
        entity_id(SCOPE, "thing", "ghost"),
        "haunts",
        entity_id(SCOPE, "language", "python"),
        "q",
    )
    [aggregate] = recompute_aggregates(SCOPE, [ghost], contributions, {doc.document_uid: doc}, {})
    assert aggregate.text == "ghost haunts Python"
    assert aggregate.payload["qualifier"] == "q"
    assert "description" not in aggregate.payload
