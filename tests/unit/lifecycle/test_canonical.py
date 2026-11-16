"""Canonicalisation: limits, content hashes, event identity and representation selection."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import pytest

from retrievalloom.config import IngestionConfig, IngestionLimits
from retrievalloom.domain.inputs import (
    ChunkInput,
    ChunksRepresentation,
    DeliveryProvenance,
    DocumentInput,
    GraphEntityInput,
    GraphRelationshipInput,
    GraphRepresentation,
    SourceDescriptor,
    StructureNodeInput,
    StructureRepresentation,
    TextRepresentation,
    TransformationRef,
)
from retrievalloom.errors import ValidationError
from retrievalloom.lifecycle.canonical import (
    canonicalize,
    check_limits,
    document_uid,
    graph_representation,
    index_representation,
    structure_representation,
)
from tests.unit.lifecycle.helpers import OTHER, SCOPE

CONFIG = IngestionConfig()


def doc(*representations: Any, **extra: Any) -> DocumentInput:
    return DocumentInput.model_validate({"_id": "d1", "representations": representations, **extra})


def limited(**limits: int) -> IngestionConfig:
    return IngestionConfig(limits=IngestionLimits(**limits))


def tree(depth: int, fanout: int = 1) -> StructureNodeInput:
    node = StructureNodeInput(title="leaf")
    for _ in range(depth - 1):
        node = StructureNodeInput(title="n", children=(node,) * fanout)
    return node


GRAPH = GraphRepresentation(
    representation_id="g",
    entities=(GraphEntityInput(key="a", name="A"), GraphEntityInput(key="b", name="B")),
    relationships=(GraphRelationshipInput(source_key="a", target_key="b", relation_type="r"),),
)


@pytest.mark.parametrize(
    ("document", "config", "limit", "value"),
    [
        (
            doc(TextRepresentation(representation_id="a", text="x"),
                TextRepresentation(representation_id="b", text="y")),
            limited(max_representations=1), "max_representations", 2,
        ),
        (doc(TextRepresentation(text="secret text")), limited(max_text_chars=5),
         "max_text_chars", 11),
        (doc(ChunksRepresentation(chunks=(ChunkInput(text="a"), ChunkInput(text="b")))),
         limited(max_chunks=1), "max_chunks", 2),
        (doc(ChunksRepresentation(chunks=(ChunkInput(text="abc"), ChunkInput(text="def")))),
         limited(max_text_chars=5), "max_text_chars", 6),
        (doc(StructureRepresentation(root=tree(3))), limited(max_tree_depth=2),
         "max_tree_depth", 3),
        (doc(StructureRepresentation(root=tree(2, fanout=3))), limited(max_tree_nodes=3),
         "max_tree_nodes", 4),
        (doc(GRAPH), limited(max_graph_items=2), "max_graph_items", 3),
    ],
)  # fmt: skip
def test_limits_reject_without_echoing_content(
    document: DocumentInput, config: IngestionConfig, limit: str, value: int
) -> None:
    with pytest.raises(ValidationError) as caught:
        check_limits(document, config)
    details = caught.value.details
    assert details["limit"] == limit
    assert details["value"] == value
    assert details["document_id"] == "d1"
    assert "secret" not in str(caught.value)
    assert "secret" not in str(details)


def test_documents_within_limits_pass() -> None:
    document = doc(
        TextRepresentation(representation_id="t", text="hello"),
        ChunksRepresentation(representation_id="c", chunks=(ChunkInput(text="a"),)),
        StructureRepresentation(representation_id="s", root=tree(3, fanout=2)),
        GRAPH,
    )
    check_limits(
        document,
        limited(max_text_chars=5, max_tree_depth=3, max_tree_nodes=7, max_graph_items=3),
    )


def test_scope_mismatch_is_rejected_and_matching_scope_accepted() -> None:
    with pytest.raises(ValidationError, match="scope"):
        canonicalize(doc(TextRepresentation(text="x"), scope=OTHER), SCOPE, CONFIG)
    same = canonicalize(doc(TextRepresentation(text="x"), scope=SCOPE), SCOPE, CONFIG)
    assert same.scope == SCOPE


def test_content_hash_ignores_order_transformation_and_transient_metadata() -> None:
    config = IngestionConfig(transient_metadata_keys=("seen",))
    a = TextRepresentation(representation_id="a", text="alpha")
    b = TextRepresentation(representation_id="b", text="beta")
    produced = b.model_copy(update={"transformation": TransformationRef(name="p", version="2")})
    first = canonicalize(doc(a, b, metadata={"k": 1, "seen": 1}), SCOPE, config)
    second = canonicalize(doc(produced, a, metadata={"k": 1, "seen": 2}), SCOPE, config)
    assert first.content_hash == second.content_hash
    assert list(first.payloads) == ["a", "b"]
    assert first.representation_kinds == {"a": "text", "b": "text"}
    assert first.representation_digests == second.representation_digests


@pytest.mark.parametrize(
    "change",
    [{"metadata": {"k": 2}}, {"dataset": "ds"}],
)
def test_significant_changes_alter_the_content_hash(change: dict[str, Any]) -> None:
    base = canonicalize(doc(TextRepresentation(text="x"), metadata={"k": 1}), SCOPE, CONFIG)
    changed = canonicalize(
        doc(TextRepresentation(text="x"), **{"metadata": {"k": 1}, **change}), SCOPE, CONFIG
    )
    assert base.content_hash != changed.content_hash
    text_changed = canonicalize(doc(TextRepresentation(text="y"), metadata={"k": 1}), SCOPE, CONFIG)
    assert text_changed.content_hash != base.content_hash


def test_event_identity_tiers() -> None:
    text = TextRepresentation(text="x")
    tier1 = canonicalize(doc(text, appearance_id="a1"), SCOPE, CONFIG)
    assert tier1.event_tier == 1
    assert tier1.event_id == canonicalize(doc(text, appearance_id="a1"), SCOPE, CONFIG).event_id
    assert tier1.event_id != canonicalize(doc(text, appearance_id="a1"), OTHER, CONFIG).event_id

    delivery = DeliveryProvenance(source_system="kafka", delivery_id="7")
    tier2 = canonicalize(doc(text, delivery=delivery), SCOPE, CONFIG)
    assert tier2.event_tier == 2
    assert tier2.event_id == canonicalize(doc(text, delivery=delivery), SCOPE, CONFIG).event_id

    for partial in (DeliveryProvenance(source_system="kafka"), DeliveryProvenance(delivery_id="7")):
        tier3 = canonicalize(doc(text, delivery=partial), SCOPE, CONFIG)
        assert tier3.event_tier == 3
    plain = [canonicalize(doc(text), SCOPE, CONFIG) for _ in range(2)]
    assert plain[0].event_tier == 3
    assert plain[0].event_id != plain[1].event_id
    assert plain[0].appearance_fingerprint == plain[1].appearance_fingerprint


def test_fingerprint_tracks_external_version_and_observation_time() -> None:
    text = TextRepresentation(text="x")
    base = canonicalize(doc(text), SCOPE, CONFIG).appearance_fingerprint
    versioned = canonicalize(
        doc(text, source=SourceDescriptor(external_version="v2")), SCOPE, CONFIG
    )
    observed = canonicalize(
        doc(text, delivery=DeliveryProvenance(observed_at=datetime(2026, 1, 1, tzinfo=UTC))),
        SCOPE,
        CONFIG,
    )
    assert len({base, versioned.appearance_fingerprint, observed.appearance_fingerprint}) == 3
    # The source descriptor is descriptive: it does not create a new version.
    assert versioned.content_hash == canonicalize(doc(text), SCOPE, CONFIG).content_hash


def test_document_uid_is_stable_per_scope_and_id() -> None:
    assert document_uid(SCOPE, "d1") == document_uid(SCOPE, "d1")
    assert document_uid(SCOPE, "d1") != document_uid(OTHER, "d1")
    assert canonicalize(doc(TextRepresentation(text="x")), SCOPE, CONFIG).document_uid == (
        document_uid(SCOPE, "d1")
    )


def test_representation_selection() -> None:
    text = TextRepresentation(representation_id="t", text="x")
    chunks = ChunksRepresentation(representation_id="c", chunks=(ChunkInput(text="y"),))
    structure = StructureRepresentation(representation_id="s", root=tree(1))
    assert index_representation(doc(text, chunks)) is chunks
    assert index_representation(doc(text, structure)) is text
    only_tree = doc(structure)
    assert index_representation(only_tree) is None
    assert structure_representation(only_tree) is structure
    assert structure_representation(doc(text)) is None
    assert graph_representation(doc(text, GRAPH)) is GRAPH
    assert graph_representation(doc(text)) is None
