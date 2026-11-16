"""Document input validation: scopes, spans, representations and documents."""

from __future__ import annotations

from typing import Any

import pytest
from pydantic import ValidationError as PydanticValidationError

from retrievalloom.domain.inputs import (
    ChunkInput,
    ChunksRepresentation,
    DocumentInput,
    GraphEntityInput,
    GraphRelationshipInput,
    GraphRepresentation,
    LineRange,
    PageRange,
    Scope,
    StructureNodeInput,
    StructureRepresentation,
    TextRepresentation,
    TextSpan,
    TransformationRef,
)


def test_scope_defaults_and_key() -> None:
    scope = Scope(tenant_id="t1")
    assert (scope.namespace, scope.collection) == ("default", "default")
    assert scope.key == Scope(tenant_id="t1", namespace="default", collection="default").key
    assert scope.key != Scope(tenant_id="t1", namespace="other").key
    assert scope.key != Scope(tenant_id="t2").key
    assert scope.key.startswith("sk_")
    # the key is length-prefixed, so shifting characters between parts changes it
    assert Scope(tenant_id="ab", namespace="c").key != Scope(tenant_id="a", namespace="bc").key


def test_scope_rejects_bad_identifiers_and_is_frozen() -> None:
    with pytest.raises(PydanticValidationError):
        Scope(tenant_id=" t")
    scope = Scope(tenant_id="t")
    with pytest.raises(PydanticValidationError):
        scope.tenant_id = "u"  # type: ignore[misc]


def test_text_span_order() -> None:
    span = TextSpan(start=0, end=1)
    assert (span.unit, span.basis) == ("char", "representation")
    with pytest.raises(PydanticValidationError, match="greater than start"):
        TextSpan(start=3, end=3)
    with pytest.raises(PydanticValidationError):
        TextSpan(start=-1, end=3)
    with pytest.raises(PydanticValidationError):
        TextSpan.model_validate({"start": 0, "end": 2, "unit": "byte"})


@pytest.mark.parametrize("model", [PageRange, LineRange])
def test_inclusive_ranges(model: Any) -> None:
    assert model(start=2, end=2).end == 2
    with pytest.raises(PydanticValidationError, match="must not precede"):
        model(start=3, end=2)
    with pytest.raises(PydanticValidationError):
        model(start=0, end=2)


def test_transformation_ref_validation() -> None:
    TransformationRef.model_validate(
        {
            "name": "p",
            "version": "1",
            "config_fingerprint": "sha256:" + "0" * 64,
            "produced_at": "2024-01-01T00:00:00Z",
        }
    )
    with pytest.raises(PydanticValidationError):
        TransformationRef(name="", version="1")
    with pytest.raises(PydanticValidationError):
        TransformationRef.model_validate(
            {"name": "p", "version": "1", "produced_at": "2024-01-01T00:00:00"}
        )


def test_chunk_input_validation() -> None:
    assert ChunkInput(text=" x ").text == " x "
    with pytest.raises(PydanticValidationError, match="blank"):
        ChunkInput(text="   ")
    with pytest.raises(PydanticValidationError, match="reserved"):
        ChunkInput(text="x", metadata={"_id": 1})


def test_structure_node_needs_title_or_text() -> None:
    assert StructureNodeInput(title="T").text == ""
    assert StructureNodeInput(text="body").title is None
    for kwargs in ({}, {"title": "  ", "text": " "}):
        with pytest.raises(PydanticValidationError, match="title or text"):
            StructureNodeInput.model_validate(kwargs)
    with pytest.raises(PydanticValidationError, match="reserved"):
        StructureNodeInput(title="T", metadata={"_x": 1})
    tree = StructureNodeInput.model_validate(
        {"title": "root", "children": [{"text": "a"}, {"title": "b", "children": [{"text": "c"}]}]}
    )
    assert tree.children[1].children[0].text == "c"


def test_graph_inputs() -> None:
    entity = GraphEntityInput(name="Ada")
    assert entity.entity_type == "entity"
    with pytest.raises(PydanticValidationError):
        GraphEntityInput(name="")
    relationship = GraphRelationshipInput(source_key="a", target_key="b", relation_type="knows")
    assert relationship.directed
    with pytest.raises(PydanticValidationError):
        GraphRelationshipInput(source_key="a", target_key="", relation_type="knows")


def test_representation_metadata_keys_checked() -> None:
    with pytest.raises(PydanticValidationError, match="reserved"):
        TextRepresentation(text="x", metadata={"_r": 1})


def test_chunks_representation_unique_local_ids() -> None:
    ok = ChunksRepresentation(
        chunks=(ChunkInput(text="a", local_id="1"), ChunkInput(text="b"), ChunkInput(text="c"))
    )
    assert len(ok.chunks) == 3
    with pytest.raises(PydanticValidationError, match="unique"):
        ChunksRepresentation(
            chunks=(ChunkInput(text="a", local_id="1"), ChunkInput(text="b", local_id="1"))
        )
    with pytest.raises(PydanticValidationError):
        ChunksRepresentation(chunks=())


def test_graph_representation_needs_content() -> None:
    assert GraphRepresentation(entities=(GraphEntityInput(name="x"),)).kind == "graph"
    rel = GraphRelationshipInput(source_key="a", target_key="b", relation_type="r")
    assert GraphRepresentation(relationships=(rel,)).relationships == (rel,)
    with pytest.raises(PydanticValidationError, match="entities or relationships"):
        GraphRepresentation()


def _document(**overrides: Any) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "_id": "doc-1",
        "representations": [
            {"kind": "text", "text": "hello"},
            {"kind": "chunks", "representation_id": "chunks", "chunks": [{"text": "c"}]},
            {"kind": "structure", "representation_id": "tree", "root": {"title": "T"}},
        ],
    }
    payload.update(overrides)
    return payload


def test_document_input_aliases_and_discriminator() -> None:
    document = DocumentInput.model_validate(_document())
    assert document.id == "doc-1"
    assert document.schema_version == "1.0.0"
    assert isinstance(document.representation("default"), TextRepresentation)
    assert isinstance(document.representation("chunks"), ChunksRepresentation)
    assert isinstance(document.representation("tree"), StructureRepresentation)
    assert document.representation("missing") is None
    assert document.model_dump(by_alias=True)["_id"] == "doc-1"
    # validation by field name is accepted as well as by alias
    assert (
        DocumentInput.model_validate(
            {"id": "doc-2", "representations": [{"kind": "text", "text": "x"}]}
        ).id
        == "doc-2"
    )


@pytest.mark.parametrize(
    ("overrides", "match"),
    [
        ({"metadata": {"_secret": 1}}, "reserved"),
        ({"metadata": {"": 1}}, "non-empty"),
        (
            {"representations": [{"kind": "text", "text": "a"}, {"kind": "text", "text": "b"}]},
            "unique",
        ),
        ({"representations": []}, "at least 1"),
        ({"representations": [{"kind": "audio", "text": "a"}]}, "kind"),
        ({"_id": ""}, "length"),
        ({"schema_version": "0.9"}, "schema_version"),
        ({"unknown": 1}, "Extra inputs"),
    ],
)
def test_document_input_rejections(overrides: dict[str, Any], match: str) -> None:
    with pytest.raises(PydanticValidationError, match=match):
        DocumentInput.model_validate(_document(**overrides))
