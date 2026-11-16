"""Canonicalisation of document inputs: limits, content hashes and event identity."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from retrievalloom.domain.hashing import digest_json, normalize_payload, random_id, stable_id
from retrievalloom.domain.inputs import (
    ChunksRepresentation,
    GraphRepresentation,
    StructureNodeInput,
    StructureRepresentation,
    TextRepresentation,
)
from retrievalloom.errors import ValidationError

if TYPE_CHECKING:
    from retrievalloom.config import IngestionConfig
    from retrievalloom.domain.inputs import DocumentInput, Representation, Scope


@dataclass(frozen=True, slots=True)
class CanonicalDocument:
    """A validated input with its canonical payload and identities.

    Attributes:
        document: The validated input (scope already resolved).
        scope: Scope the document belongs to.
        document_uid: Engine id of ``(scope, document_id)``.
        payloads: Canonical, normalised payload per representation id.
        representation_digests: Digest per representation id.
        representation_kinds: Kind per representation id.
        content_hash: Digest of the version-significant canonical payload.
        event_id: Idempotency key of this appearance.
        event_tier: 1 (``appearance_id``), 2 (delivery id) or 3 (engine-generated).
        appearance_fingerprint: Advisory redelivery fingerprint.
    """

    document: DocumentInput
    scope: Scope
    document_uid: str
    payloads: dict[str, dict[str, Any]]
    representation_digests: dict[str, str]
    representation_kinds: dict[str, str]
    content_hash: str
    event_id: str
    event_tier: int
    appearance_fingerprint: str


def document_uid(scope: Scope, document_id: str) -> str:
    """Engine id of a document key; stable across delete and re-create."""
    return stable_id("doc", scope.key, document_id)


def _tree_size(node: StructureNodeInput, depth: int = 1) -> tuple[int, int]:
    nodes, deepest = 1, depth
    for child in node.children:
        child_nodes, child_depth = _tree_size(child, depth + 1)
        nodes += child_nodes
        deepest = max(deepest, child_depth)
    return nodes, deepest


def check_limits(document: DocumentInput, config: IngestionConfig) -> None:
    """Enforce ``IngestionConfig.limits``.

    Raises:
        ValidationError: When a limit is exceeded (content is never echoed).
    """
    limits = config.limits

    def fail(limit: str, value: int, maximum: int) -> None:
        raise ValidationError(
            "document exceeds an ingestion limit",
            details={"document_id": document.id, "limit": limit, "value": value, "max": maximum},
        )

    if len(document.representations) > limits.max_representations:
        fail("max_representations", len(document.representations), limits.max_representations)
    for representation in document.representations:
        if isinstance(representation, TextRepresentation):
            if len(representation.text) > limits.max_text_chars:
                fail("max_text_chars", len(representation.text), limits.max_text_chars)
        elif isinstance(representation, ChunksRepresentation):
            if len(representation.chunks) > limits.max_chunks:
                fail("max_chunks", len(representation.chunks), limits.max_chunks)
            chars = sum(len(chunk.text) for chunk in representation.chunks)
            if chars > limits.max_text_chars:
                fail("max_text_chars", chars, limits.max_text_chars)
        elif isinstance(representation, StructureRepresentation):
            nodes, depth = _tree_size(representation.root)
            if depth > limits.max_tree_depth:
                fail("max_tree_depth", depth, limits.max_tree_depth)
            if nodes > limits.max_tree_nodes:
                fail("max_tree_nodes", nodes, limits.max_tree_nodes)
        else:
            items = len(representation.entities) + len(representation.relationships)
            if items > limits.max_graph_items:
                fail("max_graph_items", items, limits.max_graph_items)


def representation_payload(representation: Representation, profile: str) -> dict[str, Any]:
    """Canonical payload of a representation (producer identity excluded)."""
    raw = representation.model_dump(mode="json", exclude={"transformation"}, exclude_none=True)
    return normalize_payload(raw, profile)


def significant_metadata(metadata: dict[str, Any], config: IngestionConfig) -> dict[str, Any]:
    """Metadata that participates in the content hash."""
    transient = set(config.transient_metadata_keys)
    return {key: value for key, value in metadata.items() if key not in transient}


def canonicalize(
    document: DocumentInput, scope: Scope, config: IngestionConfig
) -> CanonicalDocument:
    """Validate limits, hash the version-significant payload and derive the event id.

    Raises:
        ValidationError: When the document scope differs from ``scope`` or a limit is hit.
    """
    if document.scope is not None and document.scope != scope:
        raise ValidationError(
            "document scope does not match the ingest scope",
            details={"document_id": document.id},
        )
    check_limits(document, config)
    profile = config.hash_profile
    payloads: dict[str, dict[str, Any]] = {}
    digests: dict[str, str] = {}
    kinds: dict[str, str] = {}
    for representation in sorted(document.representations, key=lambda r: r.representation_id):
        payload = representation_payload(representation, profile)
        payloads[representation.representation_id] = payload
        digests[representation.representation_id] = digest_json(payload)
        kinds[representation.representation_id] = representation.kind
    content_hash = digest_json(
        {
            "representations": [payloads[key] for key in sorted(payloads)],
            "metadata": normalize_payload(significant_metadata(document.metadata, config), profile),
            "dataset": document.dataset,
        }
    )
    uid = document_uid(scope, document.id)
    delivery = document.delivery
    if document.appearance_id is not None:
        event_id, tier = stable_id("evt", scope.key, document.id, "a", document.appearance_id), 1
    elif delivery is not None and delivery.source_system and delivery.delivery_id:
        event_id = stable_id(
            "evt", scope.key, document.id, "d", delivery.source_system, delivery.delivery_id
        )
        tier = 2
    else:
        event_id, tier = random_id("evt"), 3
    external_version = document.source.external_version if document.source else None
    observed_at = delivery.observed_at.isoformat() if delivery and delivery.observed_at else None
    fingerprint = digest_json([scope.key, document.id, content_hash, external_version, observed_at])
    return CanonicalDocument(
        document=document,
        scope=scope,
        document_uid=uid,
        payloads=payloads,
        representation_digests=digests,
        representation_kinds=kinds,
        content_hash=content_hash,
        event_id=event_id,
        event_tier=tier,
        appearance_fingerprint=fingerprint,
    )


def index_representation(
    document: DocumentInput,
) -> ChunksRepresentation | TextRepresentation | None:
    """Representation feeding the index channel: first ``chunks``, else first ``text``."""
    chunks = [r for r in document.representations if isinstance(r, ChunksRepresentation)]
    if chunks:
        return chunks[0]
    texts = [r for r in document.representations if isinstance(r, TextRepresentation)]
    return texts[0] if texts else None


def structure_representation(document: DocumentInput) -> StructureRepresentation | None:
    """First ``structure`` representation, if any."""
    for representation in document.representations:
        if isinstance(representation, StructureRepresentation):
            return representation
    return None


def graph_representation(document: DocumentInput) -> GraphRepresentation | None:
    """First ``graph`` representation, if any."""
    for representation in document.representations:
        if isinstance(representation, GraphRepresentation):
            return representation
    return None
