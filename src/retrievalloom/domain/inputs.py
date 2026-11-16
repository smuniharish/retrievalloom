"""Scope, location value objects and application-provided document inputs."""

from __future__ import annotations

from functools import cached_property
from typing import Annotated, Literal

from pydantic import AwareDatetime, Field, model_validator

from retrievalloom.domain.base import (
    Digest,
    FrozenModel,
    Identifier,
    Metadata,
    NonNegativeInt,
    PositiveInt,
    VersionedModel,
    check_metadata_keys,
)
from retrievalloom.domain.hashing import stable_id


class Scope(FrozenModel):
    """Isolation scope: tenant, namespace and collection."""

    tenant_id: Identifier
    namespace: Identifier = "default"
    collection: Identifier = "default"

    @cached_property
    def key(self) -> str:
        """Deterministic scope key used in storage and cache keys."""
        return stable_id("sk", self.tenant_id, self.namespace, self.collection)


class TextSpan(FrozenModel):
    """Character span in Unicode code points."""

    start: NonNegativeInt
    end: PositiveInt
    unit: Literal["char"] = "char"
    basis: Literal["original", "representation"] = "representation"

    @model_validator(mode="after")
    def _check_order(self) -> TextSpan:
        if self.end <= self.start:
            raise ValueError("span end must be greater than start")
        return self


class PageRange(FrozenModel):
    """Inclusive 1-based page range."""

    start: PositiveInt
    end: PositiveInt
    label: str | None = None

    @model_validator(mode="after")
    def _check_order(self) -> PageRange:
        if self.end < self.start:
            raise ValueError("page range end must not precede start")
        return self


class LineRange(FrozenModel):
    """Inclusive 1-based line range."""

    start: PositiveInt
    end: PositiveInt

    @model_validator(mode="after")
    def _check_order(self) -> LineRange:
        if self.end < self.start:
            raise ValueError("line range end must not precede start")
        return self


class TransformationRef(FrozenModel):
    """Identity of the application's parser, chunker or extractor."""

    name: str = Field(min_length=1)
    version: str = Field(min_length=1)
    config_fingerprint: Digest | None = None
    produced_at: AwareDatetime | None = None


class SourceDescriptor(FrozenModel):
    """Descriptive source information; never interpreted by the engine."""

    uri: str | None = None
    media_type: str | None = None
    title: str | None = None
    external_version: str | None = None


class DeliveryProvenance(FrozenModel):
    """How an appearance was delivered; used for idempotency and redelivery detection."""

    source_system: Identifier | None = None
    delivery_id: Identifier | None = None
    observed_at: AwareDatetime | None = None


class ChunkInput(FrozenModel):
    """Application-produced chunk."""

    local_id: Identifier | None = None
    text: str = Field(min_length=1)
    span: TextSpan | None = None
    pages: PageRange | None = None
    section_path: tuple[str, ...] = ()
    parent_local_id: Identifier | None = None
    order: int | None = None
    metadata: Metadata = Field(default_factory=dict)

    @model_validator(mode="after")
    def _check(self) -> ChunkInput:
        if not self.text.strip():
            raise ValueError("chunk text must not be blank")
        check_metadata_keys(self.metadata)
        return self


class StructureNodeInput(FrozenModel):
    """Application-produced document tree node."""

    local_id: Identifier | None = None
    title: str | None = None
    text: str = ""
    level: int | None = None
    span: TextSpan | None = None
    pages: PageRange | None = None
    lines: LineRange | None = None
    children: tuple[StructureNodeInput, ...] = ()
    metadata: Metadata = Field(default_factory=dict)

    @model_validator(mode="after")
    def _check(self) -> StructureNodeInput:
        if not (self.text.strip() or (self.title and self.title.strip())):
            raise ValueError("structure node needs a title or text")
        check_metadata_keys(self.metadata)
        return self


class GraphEntityInput(FrozenModel):
    """Pre-extracted or extractor-produced graph entity."""

    key: str | None = None
    name: str = Field(min_length=1)
    entity_type: str = Field(default="entity", min_length=1)
    aliases: tuple[str, ...] = ()
    description: str | None = None
    properties: Metadata = Field(default_factory=dict)
    span: TextSpan | None = None
    pages: PageRange | None = None


class GraphRelationshipInput(FrozenModel):
    """Pre-extracted or extractor-produced graph relationship between entity keys."""

    source_key: str = Field(min_length=1)
    target_key: str = Field(min_length=1)
    relation_type: str = Field(min_length=1)
    qualifier: str | None = None
    description: str | None = None
    properties: Metadata = Field(default_factory=dict)
    directed: bool = True
    span: TextSpan | None = None
    pages: PageRange | None = None


class _RepresentationBase(FrozenModel):
    representation_id: Identifier = "default"
    transformation: TransformationRef | None = None
    metadata: Metadata = Field(default_factory=dict)

    @model_validator(mode="after")
    def _check_metadata(self) -> _RepresentationBase:
        check_metadata_keys(self.metadata)
        return self


class TextRepresentation(_RepresentationBase):
    """Full document text."""

    kind: Literal["text"] = "text"
    text: str = Field(min_length=1)


class ChunksRepresentation(_RepresentationBase):
    """Application-chunked document."""

    kind: Literal["chunks"] = "chunks"
    chunks: tuple[ChunkInput, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def _check_local_ids(self) -> ChunksRepresentation:
        ids = [chunk.local_id for chunk in self.chunks if chunk.local_id is not None]
        if len(ids) != len(set(ids)):
            raise ValueError("chunk local_id values must be unique")
        return self


class StructureRepresentation(_RepresentationBase):
    """Application-provided document tree."""

    kind: Literal["structure"] = "structure"
    root: StructureNodeInput


class GraphRepresentation(_RepresentationBase):
    """Pre-extracted graph."""

    kind: Literal["graph"] = "graph"
    entities: tuple[GraphEntityInput, ...] = ()
    relationships: tuple[GraphRelationshipInput, ...] = ()

    @model_validator(mode="after")
    def _check_nonempty(self) -> GraphRepresentation:
        if not self.entities and not self.relationships:
            raise ValueError("graph representation needs entities or relationships")
        return self


Representation = Annotated[
    TextRepresentation | ChunksRepresentation | StructureRepresentation | GraphRepresentation,
    Field(discriminator="kind"),
]


class DocumentInput(VersionedModel):
    """One document appearance submitted by the application."""

    id: Identifier = Field(alias="_id")
    scope: Scope | None = None
    metadata: Metadata = Field(default_factory=dict)
    dataset: Identifier | None = None
    source: SourceDescriptor | None = None
    representations: tuple[Representation, ...] = Field(min_length=1)
    appearance_id: Identifier | None = None
    delivery: DeliveryProvenance | None = None

    @model_validator(mode="after")
    def _check(self) -> DocumentInput:
        check_metadata_keys(self.metadata)
        ids = [representation.representation_id for representation in self.representations]
        if len(ids) != len(set(ids)):
            raise ValueError("representation_id values must be unique within a document")
        return self

    def representation(self, representation_id: str) -> Representation | None:
        """Return the representation with ``representation_id`` if present."""
        for representation in self.representations:
            if representation.representation_id == representation_id:
                return representation
        return None
