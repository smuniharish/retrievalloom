"""Default graph normaliser: canonical keys by Unicode folding, alias and duplicate merge."""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

from retrievalloom.contracts.component import ComponentInfo, make_info
from retrievalloom.contracts.models import (
    GraphNormalizerABC,
    NormalizedEntity,
    NormalizedGraph,
    NormalizedRelationship,
)
from retrievalloom.index.text import fold

if TYPE_CHECKING:
    from collections.abc import Iterable

    from retrievalloom.contracts.models import GraphExtraction
    from retrievalloom.domain.inputs import Scope

_SPACES = re.compile(r"\s+")
DEFAULT_ENTITY_TYPE = "entity"


def canonical_key(value: str) -> str:
    """NFC, casefold and collapse whitespace."""
    return _SPACES.sub(" ", fold(value)).strip()


class IdentityGraphNormalizer(GraphNormalizerABC):
    """Keys entities by ``fold(key or name)`` and merges duplicates of one type.

    Duplicates (same type and canonical key) are merged: aliases are unioned, the first
    name, description and properties win. Relationship endpoints are resolved through the
    extraction's entity keys; unknown endpoints become entities of the default type.
    Duplicate relationships (same endpoints, type and qualifier) are merged.
    """

    def __init__(self) -> None:
        self._info = make_info(component="graph_normalizer", implementation="identity", version="1")

    @property
    def info(self) -> ComponentInfo:
        """Component identity."""
        return self._info

    async def normalize(self, extraction: GraphExtraction, scope: Scope) -> NormalizedGraph:
        """Normalise ``extraction``."""
        del scope
        entities: dict[tuple[str, str], NormalizedEntity] = {}
        resolve: dict[str, tuple[str, str]] = {}
        for entity in extraction.entities:
            raw_key = entity.key or entity.name
            key = canonical_key(raw_key)
            if not key:
                continue
            identity = (entity.entity_type, key)
            resolve.setdefault(raw_key, identity)
            resolve.setdefault(key, identity)
            existing = entities.get(identity)
            aliases = [entity.name, *entity.aliases]
            if existing is None:
                entities[identity] = NormalizedEntity(
                    canonical_key=key,
                    entity_type=entity.entity_type,
                    name=entity.name,
                    aliases=_aliases(entity.name, aliases),
                    description=entity.description,
                    properties=dict(entity.properties),
                    source_keys=(raw_key,),
                )
                continue
            merged_aliases = _aliases(existing.name, (*existing.aliases, *aliases))
            entities[identity] = existing.model_copy(
                update={
                    "aliases": merged_aliases,
                    "source_keys": _unique((*existing.source_keys, raw_key)),
                }
            )
        relationships: dict[tuple[str, ...], NormalizedRelationship] = {}
        for relationship in extraction.relationships:
            ends = []
            for raw in (relationship.source_key, relationship.target_key):
                identity = resolve.get(raw) or resolve.get(canonical_key(raw))
                if identity is None:
                    identity = (DEFAULT_ENTITY_TYPE, canonical_key(raw))
                    if not identity[1]:
                        break
                    entities.setdefault(
                        identity,
                        NormalizedEntity(
                            canonical_key=identity[1],
                            entity_type=DEFAULT_ENTITY_TYPE,
                            name=raw.strip(),
                            source_keys=(raw,),
                        ),
                    )
                    resolve[raw] = identity
                ends.append(identity)
            if len(ends) != 2:
                continue
            (source_type, source_key), (target_type, target_key) = ends
            if not relationship.directed and (target_type, target_key) < (source_type, source_key):
                (source_type, source_key), (target_type, target_key) = ends[1], ends[0]
            identity_key = (
                source_type,
                source_key,
                target_type,
                target_key,
                relationship.relation_type,
                relationship.qualifier or "",
            )
            relationships.setdefault(
                identity_key,
                NormalizedRelationship(
                    source_key=source_key,
                    source_type=source_type,
                    target_key=target_key,
                    target_type=target_type,
                    relation_type=relationship.relation_type,
                    qualifier=relationship.qualifier,
                    description=relationship.description,
                    properties=dict(relationship.properties),
                    directed=relationship.directed,
                ),
            )
        return NormalizedGraph(
            entities=tuple(entities[key] for key in sorted(entities)),
            relationships=tuple(relationships[key] for key in sorted(relationships)),
        )


def _aliases(name: str, values: Iterable[str]) -> tuple[str, ...]:
    """Aliases unique by canonical form, excluding forms equal to ``name``."""
    seen = {canonical_key(name)}
    kept: list[str] = []
    for value in values:
        key = canonical_key(value)
        if key and key not in seen:
            seen.add(key)
            kept.append(value.strip())
    return tuple(kept)


def _unique(values: Iterable[str]) -> tuple[str, ...]:
    seen: dict[str, None] = {}
    for value in values:
        if value.strip():  # pragma: no branch - callers pass keys with a non-empty canonical form
            seen.setdefault(value, None)
    return tuple(seen)
