"""Index units and item preparation."""

from __future__ import annotations

import dataclasses
from typing import TYPE_CHECKING

import pytest

from retrievalloom.contracts.component import ComponentInfo, make_info
from retrievalloom.contracts.models import KeywordEntry, TokenizerABC
from retrievalloom.domain.inputs import ChunkInput, ChunksRepresentation
from retrievalloom.domain.records import Channel, ObjectKind
from retrievalloom.errors import ComponentContractError
from retrievalloom.index.embedding import EmbeddingValidator
from retrievalloom.index.text import SimpleTokenizer, StaticKeywordDictionary
from retrievalloom.lifecycle.derivation import derive_chunks
from retrievalloom.lifecycle.units import ItemPreparer, make_unit
from retrievalloom.runtime.offloader import Offloader
from tests.fakes import HashEmbedding
from tests.support import shared_manager
from tests.unit.lifecycle.helpers import SCOPE, Dependency, DictIndex
from tests.unit.lifecycle.test_derivation import record

if TYPE_CHECKING:
    from collections.abc import Sequence


def test_make_unit_kinds_instances_and_incrementality() -> None:
    mutable = DictIndex(mutable=True)
    durable = DictIndex(mutable=True, durable=True)
    index = make_unit(Channel.INDEX, "sparse", mutable, instance_id="inst_a")
    assert index.kinds == (ObjectKind.CHUNK,)
    assert index.instance == "inst_a"
    assert index.incremental
    assert index.per_document
    assert index.name == "index.sparse"
    key = index.key(SCOPE)
    assert (key.scope_key, key.channel, key.method, key.instance) == (
        SCOPE.key,
        "index",
        "sparse",
        "inst_a",
    )
    structure = make_unit(Channel.STRUCTURE, "structure", durable, instance_id="inst_a")
    assert structure.kinds == (ObjectKind.STRUCTURE_NODE,)
    assert structure.instance == ""
    graph = make_unit(Channel.GRAPH, "graph", durable, instance_id="inst_a")
    assert graph.kinds == (ObjectKind.GRAPH_ENTITY, ObjectKind.GRAPH_RELATIONSHIP)
    assert not graph.incremental  # graph aggregates are always rebuilt
    assert not graph.per_document
    immutable = make_unit(Channel.INDEX, "keyword", DictIndex(mutable=False), instance_id="i")
    assert not immutable.incremental


def test_fingerprint_follows_backend_and_dependencies() -> None:
    backend = DictIndex()
    unit = make_unit(Channel.INDEX, "dense", backend, instance_id="i", dependencies=[Dependency(1)])
    same = make_unit(Channel.INDEX, "dense", backend, instance_id="j", dependencies=[Dependency(1)])
    assert unit.fingerprint == same.fingerprint
    assert unit.fingerprint != dataclasses.replace(unit, dependencies=(Dependency(2),)).fingerprint
    reconfigured = make_unit(Channel.INDEX, "dense", DictIndex(config={"k": 1}), instance_id="i")
    assert reconfigured.fingerprint != dataclasses.replace(unit, dependencies=()).fingerprint


async def test_spec_includes_dictionary_and_dimensions() -> None:
    dictionary = StaticKeywordDictionary([KeywordEntry(canonical="asyncio")])
    embedder = EmbeddingValidator(
        HashEmbedding(dimensions=8), Offloader(resource_manager=shared_manager())
    )
    preparer = ItemPreparer(tokenizer=None, embedder=embedder, dictionary=dictionary)
    backend = DictIndex()
    keyword = make_unit(Channel.INDEX, "keyword", backend, instance_id="i")
    fuzzy = make_unit(Channel.INDEX, "fuzzy", backend, instance_id="i")
    dense = make_unit(Channel.INDEX, "dense", backend, instance_id="i")
    sparse = make_unit(Channel.INDEX, "sparse", backend, instance_id="i")
    spec = await preparer.spec(keyword, SCOPE, "gen_1")
    assert [entry.canonical for entry in spec.keyword_entries] == ["asyncio"]
    assert spec.dependency_fingerprint == keyword.fingerprint
    assert spec.dimensions is None
    assert (await preparer.spec(fuzzy, SCOPE, "g")).keyword_entries == spec.keyword_entries
    dense_spec = await preparer.spec(dense, SCOPE, "g")
    assert dense_spec.dimensions == 8
    assert dense_spec.keyword_entries == ()
    assert (await preparer.spec(sparse, SCOPE, "g")).method == "sparse"
    bare = ItemPreparer(tokenizer=None, embedder=None, dictionary=None)
    assert (await bare.spec(keyword, SCOPE, "g")).keyword_entries == ()
    assert (await bare.spec(dense, SCOPE, "g")).dimensions is None


def objects() -> list:
    representation = ChunksRepresentation(
        chunks=(ChunkInput(text="Alpha beta", metadata={"m": 1}), ChunkInput(text="Gamma"))
    )
    return derive_chunks(record(), representation)


async def test_items_carry_tokens_vectors_and_metadata() -> None:
    model = HashEmbedding(dimensions=8)
    preparer = ItemPreparer(
        tokenizer=SimpleTokenizer(),
        embedder=EmbeddingValidator(model, Offloader(resource_manager=shared_manager())),
        dictionary=None,
    )
    backend = DictIndex()
    derived = objects()
    sparse = await preparer.items(
        make_unit(Channel.INDEX, "sparse", backend, instance_id="i"), derived, tenant="acme"
    )
    assert [item.tokens for item in sparse] == [("alpha", "beta"), ("gamma",)]
    assert all(item.vector is None for item in sparse)
    assert sparse[0].metadata["m"] == 1
    assert sparse[0].payload == {"ordinal": 0}
    assert sparse[0].kind == "chunk"
    assert sparse[0].version == 1
    dense = await preparer.items(
        make_unit(Channel.INDEX, "dense", backend, instance_id="i"), derived, tenant="acme"
    )
    assert [item.vector for item in dense] == [
        tuple(model.vector(t)) for t in ("Alpha beta", "Gamma")
    ]
    assert all(item.tokens is None for item in dense)
    keyword = await preparer.items(
        make_unit(Channel.INDEX, "keyword", backend, instance_id="i"), derived, tenant="acme"
    )
    assert all(item.tokens is None and item.vector is None for item in keyword)
    assert (
        await preparer.items(
            make_unit(Channel.INDEX, "sparse", backend, instance_id="i"), [], tenant="acme"
        )
        == []
    )
    textless = derived[0].model_copy(update={"text": None})
    [item] = await preparer.items(
        make_unit(Channel.INDEX, "keyword", backend, instance_id="i"), [textless], tenant="a"
    )
    assert item.text == ""


class ShortTokenizer(TokenizerABC):
    _INFO = make_info(component="tokenizer", implementation="short", version="1")

    @property
    def info(self) -> ComponentInfo:
        return self._INFO

    async def tokenize(self, texts: Sequence[str]) -> Sequence[Sequence[str]]:
        return [["x"]]


async def test_tokenizer_contract_violation_is_reported() -> None:
    preparer = ItemPreparer(tokenizer=ShortTokenizer(), embedder=None, dictionary=None)
    unit = make_unit(Channel.INDEX, "fuzzy", DictIndex(), instance_id="i")
    with pytest.raises(ComponentContractError) as caught:
        await preparer.items(unit, objects(), tenant="acme")
    assert caught.value.details == {"expected": 2, "actual": 1}
