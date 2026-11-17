"""Derived objects: chunks, structure nodes and source-attributed graph aggregates."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from retrievalloom.domain.hashing import digest_json, normalize_text, stable_id
from retrievalloom.domain.inputs import ChunksRepresentation, StructureNodeInput
from retrievalloom.domain.records import (
    DerivedObject,
    DocumentState,
    LineageEdge,
    LineageRelation,
    ObjectContribution,
    ObjectKind,
    ObjectState,
    SourceReference,
)

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping, Sequence

    from retrievalloom.contracts.models import NormalizedGraph
    from retrievalloom.domain.inputs import (
        Scope,
        StructureRepresentation,
        TextRepresentation,
    )
    from retrievalloom.domain.records import DocumentRecord

KIND_FIELD = "_kind"
DOCUMENT_ID_FIELD = "_id"
DATASET_FIELD = "_dataset"
VERSION_FIELD = "_version"
# Prefix of graph-derivation marker ids (``graph_marker_id``); aggregates use ``obj``.
_GRAPH_MARKER = "gmark"


def document_filter_record(record: DocumentRecord) -> dict[str, Any]:
    """Filterable view of a document: user metadata plus reserved ``_id``/``_dataset``."""
    view: dict[str, Any] = dict(record.metadata)
    view[DOCUMENT_ID_FIELD] = record.document_id
    if record.dataset is not None:
        view[DATASET_FIELD] = record.dataset
    return view


def _object_metadata(record: DocumentRecord, kind: ObjectKind, extra: Mapping[str, Any]) -> dict:
    metadata = document_filter_record(record)
    metadata.update(extra)
    metadata[KIND_FIELD] = kind.value
    metadata[VERSION_FIELD] = record.version
    return metadata


def text_hash(text: str) -> str:
    """Exact-duplicate digest of an object's evidence text."""
    return digest_json(["text", normalize_text(text)])


def _source(
    record: DocumentRecord, representation_id: str, object_id: str, **location: Any
) -> SourceReference:
    source = record.source
    return SourceReference(
        scope=record.scope,
        document_id=record.document_id,
        document_uid=record.document_uid,
        document_version=record.version,
        representation_id=representation_id,
        object_id=object_id,
        source_uri=source.uri if source else None,
        title=source.title if source else None,
        **location,
    )


def derive_chunks(
    record: DocumentRecord, representation: ChunksRepresentation | TextRepresentation
) -> list[DerivedObject]:
    """Chunk objects of one document version (one chunk for a ``text`` representation)."""
    scope = record.scope
    rep_id = representation.representation_id

    def object_id(local_key: str) -> str:
        return stable_id(
            "obj", scope.key, ObjectKind.CHUNK.value, record.document_uid, record.version,
            rep_id, local_key,
        )  # fmt: skip

    if not isinstance(representation, ChunksRepresentation):
        oid = object_id("0")
        return [
            DerivedObject(
                object_id=oid,
                scope=scope,
                kind=ObjectKind.CHUNK,
                document_uid=record.document_uid,
                version=record.version,
                representation_id=rep_id,
                text=representation.text,
                sources=(_source(record, rep_id, oid),),
                transformation=representation.transformation,
                metadata=_object_metadata(record, ObjectKind.CHUNK, representation.metadata),
                payload={"ordinal": 0},
                content_hash=text_hash(representation.text),
            )
        ]
    positioned = sorted(
        enumerate(representation.chunks),
        key=lambda pair: (pair[1].order if pair[1].order is not None else pair[0], pair[0]),
    )
    ids = {
        position: object_id(chunk.local_id if chunk.local_id is not None else str(position))
        for position, chunk in positioned
    }
    by_local = {chunk.local_id: ids[position] for position, chunk in positioned if chunk.local_id}
    objects: list[DerivedObject] = []
    for ordinal, (position, chunk) in enumerate(positioned):
        oid = ids[position]
        payload: dict[str, Any] = {"ordinal": ordinal}
        parent = by_local.get(chunk.parent_local_id) if chunk.parent_local_id else None
        if parent is not None and parent != oid:
            payload["parent_object_id"] = parent
        metadata = {**representation.metadata, **chunk.metadata}
        objects.append(
            DerivedObject(
                object_id=oid,
                scope=scope,
                kind=ObjectKind.CHUNK,
                document_uid=record.document_uid,
                version=record.version,
                representation_id=rep_id,
                text=chunk.text,
                sources=(
                    _source(
                        record,
                        rep_id,
                        oid,
                        span=chunk.span,
                        pages=chunk.pages,
                        section_path=chunk.section_path,
                    ),
                ),
                transformation=representation.transformation,
                metadata=_object_metadata(record, ObjectKind.CHUNK, metadata),
                payload=payload,
                content_hash=text_hash(chunk.text),
            )
        )
    return objects


def derive_structure(
    record: DocumentRecord, representation: StructureRepresentation
) -> list[DerivedObject]:
    """Structure-node objects (pre-order) of one document version."""
    scope = record.scope
    rep_id = representation.representation_id
    slots: list[DerivedObject | None] = []

    def visit(
        node: StructureNodeInput,
        key: str,
        depth: int,
        ordinal: int,
        parent: str | None,
        path: tuple[str, ...],
    ) -> str:
        local_key = node.local_id if node.local_id is not None else key
        oid = stable_id(
            "obj", scope.key, ObjectKind.STRUCTURE_NODE.value, record.document_uid,
            record.version, rep_id, local_key,
        )  # fmt: skip
        node_path = (*path, node.title) if node.title else path
        text = node.text if node.text.strip() else str(node.title)
        index = len(slots)
        slots.append(None)
        children = [
            visit(child, f"{key}.{position}", depth + 1, position, oid, node_path)
            for position, child in enumerate(node.children)
        ]
        payload: dict[str, Any] = {
            "ordinal": ordinal,
            "level": node.level if node.level is not None else depth,
            "title": node.title,
            "path": list(node_path),
            "child_object_ids": children,
        }
        if parent is not None:
            payload["parent_object_id"] = parent
        slots[index] = DerivedObject(
            object_id=oid,
            scope=scope,
            kind=ObjectKind.STRUCTURE_NODE,
            document_uid=record.document_uid,
            version=record.version,
            representation_id=rep_id,
            text=text,
            sources=(
                _source(
                    record,
                    rep_id,
                    oid,
                    span=node.span,
                    pages=node.pages,
                    lines=node.lines,
                    section_path=node_path,
                ),
            ),
            transformation=representation.transformation,
            metadata=_object_metadata(
                record, ObjectKind.STRUCTURE_NODE, {**representation.metadata, **node.metadata}
            ),
            payload=payload,
            content_hash=text_hash(text),
        )
        return oid

    visit(representation.root, "0", 0, 0, None, ())
    return [obj for obj in slots if obj is not None]


def lineage_for(objects: Iterable[DerivedObject]) -> list[LineageEdge]:
    """``derived_from`` edges to the document and ``child_of`` edges between nodes."""
    edges: list[LineageEdge] = []
    for obj in objects:
        if obj.document_uid is not None:
            edges.append(
                LineageEdge(
                    scope_key=obj.scope.key,
                    from_id=obj.object_id,
                    to_id=obj.document_uid,
                    relation=LineageRelation.DERIVED_FROM,
                )
            )
        parent = obj.payload.get("parent_object_id")
        if parent:
            edges.append(
                LineageEdge(
                    scope_key=obj.scope.key,
                    from_id=obj.object_id,
                    to_id=str(parent),
                    relation=LineageRelation.CHILD_OF,
                )
            )
    return edges


def entity_id(scope: Scope, entity_type: str, canonical_key: str) -> str:
    """Engine id of a graph entity aggregate."""
    return stable_id("obj", scope.key, ObjectKind.GRAPH_ENTITY.value, entity_type, canonical_key)


def relationship_id(
    scope: Scope, source_id: str, relation_type: str, target_id: str, qualifier: str | None
) -> str:
    """Engine id of a graph relationship aggregate."""
    return stable_id(
        "obj", scope.key, ObjectKind.GRAPH_RELATIONSHIP.value, source_id, relation_type,
        target_id, qualifier or "",
    )  # fmt: skip


def graph_marker_id(document_uid: str) -> str:
    """Object id of the graph-derivation marker of a document.

    The marker is a contribution row (never an aggregate) whose version records the
    document version the graph contributions were derived from.
    """
    return stable_id(_GRAPH_MARKER, document_uid)


def is_graph_marker(object_id: str) -> bool:
    """Whether ``object_id`` is a graph-derivation marker rather than an aggregate."""
    return object_id.startswith(f"{_GRAPH_MARKER}_")


def graph_marker(record: DocumentRecord) -> ObjectContribution:
    """Marker recording that the graph of ``record``'s version has been derived."""
    return ObjectContribution(
        object_id=graph_marker_id(record.document_uid),
        document_uid=record.document_uid,
        version=record.version,
        field_path="derived",
        ordinal=0,
        value=None,
    )


def graph_contributions(
    record: DocumentRecord, representation_id: str, graph: NormalizedGraph
) -> list[ObjectContribution]:
    """Contributions of one document version to graph aggregates."""
    scope = record.scope
    contributions: list[ObjectContribution] = []
    names: dict[tuple[str, str], str] = {}

    def add(object_id: str, field_path: str, value: Any, ordinal: int = 0) -> None:
        contributions.append(
            ObjectContribution(
                object_id=object_id,
                document_uid=record.document_uid,
                version=record.version,
                field_path=field_path,
                ordinal=ordinal,
                value=value,
            )
        )

    for entity in graph.entities:
        oid = entity_id(scope, entity.entity_type, entity.canonical_key)
        names[(entity.entity_type, entity.canonical_key)] = entity.name
        add(
            oid,
            "identity",
            {
                "kind": ObjectKind.GRAPH_ENTITY.value,
                "entity_type": entity.entity_type,
                "canonical_key": entity.canonical_key,
                "representation_id": representation_id,
            },
        )
        add(oid, "name", entity.name)
        for position, alias in enumerate(entity.aliases):
            add(oid, "alias", alias, position)
        if entity.description:
            add(oid, "description", entity.description)
        for key, value in sorted(entity.properties.items()):
            add(oid, f"property.{key}", value)
    for relation in graph.relationships:
        source_id = entity_id(scope, relation.source_type, relation.source_key)
        target_id = entity_id(scope, relation.target_type, relation.target_key)
        oid = relationship_id(
            scope, source_id, relation.relation_type, target_id, relation.qualifier
        )
        add(
            oid,
            "identity",
            {
                "kind": ObjectKind.GRAPH_RELATIONSHIP.value,
                "source_entity_id": source_id,
                "target_entity_id": target_id,
                "relation_type": relation.relation_type,
                "qualifier": relation.qualifier,
                "directed": relation.directed,
                "source_name": names.get(
                    (relation.source_type, relation.source_key), relation.source_key
                ),
                "target_name": names.get(
                    (relation.target_type, relation.target_key), relation.target_key
                ),
                "representation_id": representation_id,
            },
        )
        if relation.description:
            add(oid, "description", relation.description)
        for key, value in sorted(relation.properties.items()):
            add(oid, f"property.{key}", value)
    return contributions


def _first(contributions: Sequence[ObjectContribution], field_path: str) -> Any:
    for contribution in contributions:
        if contribution.field_path == field_path:
            return contribution.value
    return None


def _properties(contributions: Sequence[ObjectContribution]) -> dict[str, Any]:
    properties: dict[str, Any] = {}
    for contribution in contributions:
        if contribution.field_path.startswith("property."):
            properties.setdefault(
                contribution.field_path.removeprefix("property."), contribution.value
            )
    return properties


def _unique_names(values: Iterable[str], exclude: str) -> list[str]:
    seen = {exclude.casefold()}
    unique: list[str] = []
    for value in values:
        if value.casefold() not in seen:
            seen.add(value.casefold())
            unique.append(value)
    return unique


def recompute_aggregates(
    scope: Scope,
    object_ids: Iterable[str],
    contributions: Sequence[ObjectContribution],
    documents: Mapping[str, DocumentRecord],
    existing: Mapping[str, DerivedObject],
) -> list[DerivedObject]:
    """Rebuild graph aggregates from their live contributions.

    Merge order is version descending, then ``document_uid`` ascending (the store's
    ordering). Aggregates without live support become ``orphaned``.
    """
    grouped: dict[str, list[ObjectContribution]] = {}
    for contribution in contributions:
        document = documents.get(contribution.document_uid)
        if (
            document is not None
            and document.state == DocumentState.ACTIVE
            and document.version == contribution.version
        ):
            grouped.setdefault(contribution.object_id, []).append(contribution)
    result: list[DerivedObject] = []
    for object_id in sorted(set(object_ids)):
        support = grouped.get(object_id)
        if not support:
            previous = existing.get(object_id)
            if previous is not None and previous.state == ObjectState.LIVE:
                result.append(previous.model_copy(update={"state": ObjectState.ORPHANED}))
            continue
        identity = _first(support, "identity")
        kind = ObjectKind(identity["kind"])
        description = _first(support, "description")
        properties = _properties(support)
        if kind == ObjectKind.GRAPH_ENTITY:
            names = [str(c.value) for c in support if c.field_path == "name"]
            aliases = [str(c.value) for c in support if c.field_path == "alias"]
            name = names[0]
            payload: dict[str, Any] = {
                "entity_type": identity["entity_type"],
                "canonical_key": identity["canonical_key"],
                "name": name,
                "aliases": _unique_names([*names[1:], *aliases], name),
            }
            text = name
        else:
            payload = {
                key: identity[key]
                for key in (
                    "source_entity_id", "target_entity_id", "relation_type", "qualifier",
                    "directed", "source_name", "target_name",
                )
            }  # fmt: skip
            text = (
                f"{identity['source_name']} {identity['relation_type']} {identity['target_name']}"
            )
        if description:
            payload["description"] = description
            text = f"{text}: {description}"
        payload["properties"] = properties
        seen: set[tuple[str, int]] = set()
        sources: list[SourceReference] = []
        for contribution in support:
            key = (contribution.document_uid, contribution.version)
            if key in seen:
                continue
            seen.add(key)
            document = documents[contribution.document_uid]
            rep_id = next(
                c.value["representation_id"]
                for c in support
                if c.field_path == "identity" and c.document_uid == contribution.document_uid
            )
            sources.append(_source(document, rep_id, object_id))
        result.append(
            DerivedObject(
                object_id=object_id,
                scope=scope,
                kind=kind,
                text=text,
                sources=tuple(sources),
                metadata={KIND_FIELD: kind.value},
                payload=payload,
                content_hash=digest_json([kind.value, payload]),
            )
        )
    return result
