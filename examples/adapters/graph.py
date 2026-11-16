"""Custom graph components: a rule-based entity/relation extractor and an alias normaliser."""

from __future__ import annotations

import itertools
import re
from typing import TYPE_CHECKING

from retrievalloom.contracts.component import ComponentInfo, make_info
from retrievalloom.contracts.models import (
    GraphExtraction,
    GraphExtractorABC,
    GraphNormalizerABC,
    NormalizedGraph,
)
from retrievalloom.domain.inputs import GraphEntityInput, GraphRelationshipInput
from retrievalloom.graph.normalizer import IdentityGraphNormalizer, canonical_key
from retrievalloom.runtime.offloader import WorkloadKind

if TYPE_CHECKING:
    from collections.abc import Mapping

    from retrievalloom.contracts.component import ComponentContext
    from retrievalloom.contracts.models import ExtractionSource
    from retrievalloom.domain.inputs import Scope
    from retrievalloom.runtime.offloader import Offloader

_SENTENCE = re.compile(r"(?<=[.!?])\s+")
_PARTICLE = r"(?:van|von|de|der|da|del|di|du|le|la|of)"
_NAME_WORD = r"[A-Z][\w+#&-]*"
_ENTITY = re.compile(rf"\b{_NAME_WORD}(?:\s+(?:{_PARTICLE}\s+)?{_NAME_WORD})*")
_RELATION = re.compile(r"^\s*,?\s*([a-z]+(?:\s+[a-z]+){0,3})\s*,?\s*$")
_CONNECTORS = frozenset({"and", "or", "but", "with", "nor", "than", "as"})
_STOP = frozenset(
    re.findall(
        r"\S+",
        "A An The This That These Those It Its In On At For From If When While After Before "
        "Each Every Some All Our Their Your We They He She I Since Because However",
    )
)
_OFFLOAD_CHARS = 20_000


def _clean_name(raw: str) -> str | None:
    words = raw.split()
    while words and words[0] in _STOP:
        words = words[1:]
    return " ".join(words) or None


def extract_graph(texts: tuple[str, ...], types: Mapping[str, str]) -> GraphExtraction:
    """Capitalised names become entities; "<Entity> lowercase verb phrase <Entity>" in one
    sentence becomes a relationship typed by the upper-cased phrase (``RUNS_ON``)."""
    entities: dict[str, GraphEntityInput] = {}
    relationships: dict[tuple[str, str, str], GraphRelationshipInput] = {}
    for text in texts:
        for sentence in _SENTENCE.split(text):
            found: list[tuple[int, int, str]] = []
            for match in _ENTITY.finditer(sentence):
                name = _clean_name(match.group())
                if name is None:
                    continue
                found.append((match.end() - len(name), match.end(), name))
                key = canonical_key(name)
                entities.setdefault(
                    key,
                    GraphEntityInput(name=name, entity_type=types.get(key, "entity")),
                )
            for (_, left_end, left), (right_start, _, right) in itertools.pairwise(found):
                phrase = _RELATION.match(sentence[left_end:right_start])
                if phrase is None or phrase.group(1) in _CONNECTORS:
                    continue
                relation = "_".join(phrase.group(1).upper().split())
                relationships.setdefault(
                    (left, relation, right),
                    GraphRelationshipInput(
                        source_key=left, target_key=right, relation_type=relation
                    ),
                )
    return GraphExtraction(
        entities=tuple(entities[key] for key in sorted(entities)),
        relationships=tuple(relationships[key] for key in sorted(relationships)),
    )


class CapitalizedEntityExtractor(GraphExtractorABC):
    """Deterministic, dependency-free extractor for well-edited English prose.

    Suited to catalogues, org charts and runbooks where entities are proper names
    ("Northwind Analytics", "Ana de Souza") and facts read "X owns Y". Swap in an LLM or
    spaCy extractor for free text - the engine only sees the ABC.

    Args:
        entity_types: Canonical name -> entity type (``{"python": "language"}``).
    """

    def __init__(self, *, entity_types: Mapping[str, str] | None = None) -> None:
        self._types = {canonical_key(k): v for k, v in (entity_types or {}).items()}
        self._offloader: Offloader | None = None
        self._info = make_info(
            component="graph_extractor",
            implementation="capitalized-rules",
            version="1",
            config={"types": dict(sorted(self._types.items()))},
        )

    @property
    def info(self) -> ComponentInfo:
        """Component identity (the type map is fingerprinted)."""
        return self._info

    async def initialize(self, context: ComponentContext) -> None:
        """Keep the offloader for long documents."""
        self._offloader = context.offloader

    async def extract(self, source: ExtractionSource) -> GraphExtraction:
        """Extract entities and relationships (offloaded for long documents)."""
        if self._offloader is not None and sum(map(len, source.texts)) > _OFFLOAD_CHARS:
            return await self._offloader.run(
                WorkloadKind.KEYWORD_BUILD,
                extract_graph,
                source.texts,
                self._types,
                tenant=source.scope.tenant_id,
            )
        return extract_graph(source.texts, self._types)


class AliasGraphNormalizer(GraphNormalizerABC):
    """Resolves known aliases ("PG", "Postgres" -> "PostgreSQL") before identity merging.

    Without it, "Postgres" and "PostgreSQL" would be two graph nodes and a question about
    one would miss facts stored under the other. Alias surface forms are kept as entity
    aliases, so ``find_entities`` still matches them.

    Args:
        aliases: Alias -> canonical name (case-insensitive).
        entity_types: Canonical name -> entity type override.
    """

    def __init__(
        self, aliases: Mapping[str, str], *, entity_types: Mapping[str, str] | None = None
    ) -> None:
        self._aliases = {canonical_key(alias): name for alias, name in aliases.items()}
        self._types = {canonical_key(k): v for k, v in (entity_types or {}).items()}
        self._inner = IdentityGraphNormalizer()
        self._info = make_info(
            component="graph_normalizer",
            implementation="alias-map",
            version="1",
            config={"aliases": dict(sorted(self._aliases.items())), "types": self._types},
        )

    @property
    def info(self) -> ComponentInfo:
        """Component identity (the alias map is fingerprinted)."""
        return self._info

    def _resolve(self, name: str) -> str:
        return self._aliases.get(canonical_key(name), name)

    async def normalize(self, extraction: GraphExtraction, scope: Scope) -> NormalizedGraph:
        """Rewrite aliases to canonical names, then merge by canonical key."""
        entities = []
        for entity in extraction.entities:
            canonical = self._resolve(entity.key or entity.name)
            renamed = canonical_key(canonical) != canonical_key(entity.key or entity.name)
            entity_type = self._types.get(canonical_key(canonical), entity.entity_type)
            entities.append(
                entity.model_copy(
                    update={
                        "key": canonical,
                        "name": canonical if renamed else entity.name,
                        "entity_type": entity_type,
                        "aliases": (*entity.aliases, entity.name) if renamed else entity.aliases,
                    }
                )
            )
        relationships = [
            relationship.model_copy(
                update={
                    "source_key": self._resolve(relationship.source_key),
                    "target_key": self._resolve(relationship.target_key),
                }
            )
            for relationship in extraction.relationships
        ]
        return await self._inner.normalize(
            GraphExtraction(entities=tuple(entities), relationships=tuple(relationships)), scope
        )
