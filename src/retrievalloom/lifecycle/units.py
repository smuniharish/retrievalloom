"""Index units: one (channel, method) backend binding and the preparation of its items."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from retrievalloom.contracts.indexes import GenerationSpec, IndexItem
from retrievalloom.domain.hashing import digest_json
from retrievalloom.domain.records import Channel, IndexMethod, ObjectKind
from retrievalloom.errors import ComponentContractError
from retrievalloom.storage.base import UnitKey

if TYPE_CHECKING:
    from collections.abc import Sequence

    from retrievalloom.contracts.component import ComponentABC
    from retrievalloom.contracts.indexes import GenerationalIndexABC
    from retrievalloom.contracts.models import KeywordDictionaryABC, TokenizerABC
    from retrievalloom.domain.inputs import Scope
    from retrievalloom.domain.records import DerivedObject
    from retrievalloom.index.embedding import EmbeddingValidator

DOCUMENT_KINDS = (ObjectKind.CHUNK,)
STRUCTURE_KINDS = (ObjectKind.STRUCTURE_NODE,)
GRAPH_KINDS = (ObjectKind.GRAPH_ENTITY, ObjectKind.GRAPH_RELATIONSHIP)
DICTIONARY_DIGEST = "keyword_entries_digest"
"""Generation stats key holding :func:`dictionary_digest` of the generation's spec."""
DICTIONARY_REVISION = "keyword_dictionary_revision"
"""Generation stats key holding the keyword dictionary revision the generation was built at."""


def dictionary_digest(spec: GenerationSpec) -> str | None:
    """Digest of the keyword dictionary a generation is built with; ``None`` without one."""
    if not spec.keyword_entries:
        return None
    return digest_json([entry.model_dump(mode="json") for entry in spec.keyword_entries])


@dataclass(frozen=True, slots=True)
class IndexUnit:
    """A backend serving one retrieval method.

    Attributes:
        channel: Retrieval channel.
        method: Method name (``keyword``, ``fuzzy``, ``sparse``, ``dense``, ``graph``,
            ``structure``).
        backend: The generational backend.
        kinds: Object kinds the unit indexes, in write order.
        instance: Engine-local instance id for ephemeral backends, ``""`` for durable ones.
        incremental: Changes are applied in place (otherwise every change rebuilds).
        dependencies: Components whose fingerprints define the unit's dependency fingerprint.
    """

    channel: Channel
    method: str
    backend: GenerationalIndexABC
    kinds: tuple[ObjectKind, ...]
    instance: str
    incremental: bool
    dependencies: tuple[ComponentABC, ...] = ()

    @property
    def name(self) -> str:
        """``<channel>.<method>``."""
        return f"{self.channel.value}.{self.method}"

    def key(self, scope: Scope) -> UnitKey:
        """Store address of this unit in ``scope``."""
        return UnitKey(
            scope_key=scope.key,
            channel=self.channel.value,
            method=self.method,
            instance=self.instance,
        )

    @property
    def fingerprint(self) -> str:
        """Digest of the backend and dependency fingerprints (configuration identity)."""
        parts = [self.backend.info.fingerprint.digest]
        parts.extend(component.info.fingerprint.digest for component in self.dependencies)
        return digest_json([self.name, parts])

    @property
    def per_document(self) -> bool:
        """Objects belong to exactly one document version (not graph aggregates)."""
        return self.channel != Channel.GRAPH


def make_unit(
    channel: Channel,
    method: str,
    backend: GenerationalIndexABC,
    *,
    instance_id: str,
    dependencies: Sequence[ComponentABC] = (),
) -> IndexUnit:
    """Build a unit; the instance is ``""`` for durable backends."""
    capabilities = backend.capabilities
    if channel == Channel.GRAPH:
        kinds = GRAPH_KINDS
    elif channel == Channel.STRUCTURE:
        kinds = STRUCTURE_KINDS
    else:
        kinds = DOCUMENT_KINDS
    return IndexUnit(
        channel=channel,
        method=method,
        backend=backend,
        kinds=kinds,
        instance="" if capabilities.durable else instance_id,
        # Graph aggregates change whenever any supporting document changes, so the graph
        # unit is always rebuilt from the metadata store rather than patched in place.
        incremental=capabilities.mutable_in_place and channel != Channel.GRAPH,
        dependencies=tuple(dependencies),
    )


class ItemPreparer:
    """Turns derived objects into backend items (tokens, vectors, filter metadata)."""

    def __init__(
        self,
        *,
        tokenizer: TokenizerABC | None,
        embedder: EmbeddingValidator | None,
        dictionary: KeywordDictionaryABC | None,
    ) -> None:
        self._tokenizer = tokenizer
        self._embedder = embedder
        self._dictionary = dictionary

    def _dictionary_for(self, unit: IndexUnit) -> KeywordDictionaryABC | None:
        if unit.method in {IndexMethod.KEYWORD.value, IndexMethod.FUZZY.value}:
            return self._dictionary
        return None

    async def dictionary_revision(self, unit: IndexUnit, scope: Scope) -> str | None:
        """Revision of the keyword dictionary ``unit`` is built with; ``None`` without one."""
        dictionary = self._dictionary_for(unit)
        return None if dictionary is None else await dictionary.revision(scope)

    async def spec(self, unit: IndexUnit, scope: Scope, generation_id: str) -> GenerationSpec:
        """Build parameters of a new generation of ``unit``."""
        dictionary = self._dictionary_for(unit)
        entries = () if dictionary is None else tuple(await dictionary.entries(scope))
        dimensions = None
        if unit.method == IndexMethod.DENSE.value and self._embedder is not None:
            dimensions = self._embedder.model.dimensions
        return GenerationSpec(
            generation_id=generation_id,
            method=unit.method,
            dependency_fingerprint=unit.fingerprint,
            dimensions=dimensions,
            keyword_entries=entries,
        )

    async def items(
        self, unit: IndexUnit, objects: Sequence[DerivedObject], *, tenant: str
    ) -> list[IndexItem]:
        """Items for ``objects`` with the extra data ``unit`` needs."""
        if not objects:
            return []
        texts = [obj.text or "" for obj in objects]
        tokens: Sequence[Sequence[str]] | None = None
        vectors: Sequence[tuple[float, ...]] | None = None
        if (
            unit.method in {IndexMethod.SPARSE.value, IndexMethod.FUZZY.value}
            and self._tokenizer is not None
        ):
            tokens = await self._tokenizer.tokenize(texts)
            if len(tokens) != len(texts):
                raise ComponentContractError(
                    "tokenizer returned a wrong number of token lists",
                    details={"expected": len(texts), "actual": len(tokens)},
                )
        if unit.method == IndexMethod.DENSE.value and self._embedder is not None:
            vectors = await self._embedder.embed_documents(texts, tenant=tenant)
        items: list[IndexItem] = []
        for position, obj in enumerate(objects):
            items.append(
                IndexItem(
                    object_id=obj.object_id,
                    document_uid=obj.document_uid,
                    version=obj.version,
                    kind=obj.kind.value,
                    text=texts[position],
                    metadata=dict(obj.metadata),
                    tokens=None if tokens is None else tuple(tokens[position]),
                    vector=None if vectors is None else vectors[position],
                    payload=dict(obj.payload),
                )
            )
        return items
