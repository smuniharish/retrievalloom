"""Embedding validation/batching, tokenizer, keyword dictionaries and the phrase matcher."""

from __future__ import annotations

import contextlib
import dataclasses
import math
from typing import TYPE_CHECKING, Any

import pytest

from retrievalloom.contracts.models import KeywordEntry
from retrievalloom.domain.inputs import Scope
from retrievalloom.errors import ConfigurationError, EmbeddingValidationError
from retrievalloom.index.embedding import EmbeddingValidator, check_vectors
from retrievalloom.index.text import (
    MetadataKeywordDictionary,
    PhraseMatcher,
    SimpleTokenizer,
    StaticKeywordDictionary,
    fold,
)
from tests.fakes import HashEmbedding

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Callable, Sequence

    from retrievalloom.contracts.component import ComponentContext
    from retrievalloom.runtime.offloader import Offloader

SCOPE = Scope(tenant_id="tenant-a")


class ScriptedEmbedding(HashEmbedding):
    """Hash embedding whose outputs can be corrupted and whose batches are recorded."""

    def __init__(
        self,
        dimensions: int = 4,
        batch_size: int = 3,
        corrupt: Callable[[list[list[float]]], list[list[float]]] | None = None,
    ) -> None:
        super().__init__(dimensions=dimensions, batch_size=batch_size)
        self.batches: list[list[str]] = []
        self.corrupt = corrupt or (lambda vectors: vectors)

    async def embed_documents(self, texts: Sequence[str]) -> Sequence[Sequence[float]]:
        self.batches.append(list(texts))
        return self.corrupt([self.vector(text) for text in texts])

    async def embed_query(self, text: str) -> Sequence[float]:
        return self.corrupt([self.vector(text)])[0]


# check_vectors -----------------------------------------------------------------------------
def test_check_vectors_converts_to_float_tuples() -> None:
    assert check_vectors([[1, 0], [0.5, 2]], 2, 2) == [(1.0, 0.0), (0.5, 2.0)]


@pytest.mark.parametrize(
    ("vectors", "expected", "message", "details"),
    [
        ([[1.0, 0.0]], 2, "count", {"expected": 2, "got": 1}),
        ([[1.0, 0.0], [1.0]], 2, "dimensions", {"position": 1, "expected": 2, "got": 1}),
        ([[1.0, math.nan]], 1, "non-finite", {"position": 0}),
        ([[math.inf, 1.0]], 1, "non-finite", {"position": 0}),
        ([[1.0, 1.0], [0.0, 0.0]], 2, "zero norm", {"position": 1}),
    ],
)
def test_check_vectors_rejects_invalid_output(
    vectors: list[list[float]], expected: int, message: str, details: dict[str, Any]
) -> None:
    with pytest.raises(EmbeddingValidationError, match=message) as caught:
        check_vectors(vectors, expected, 2)
    assert caught.value.details == details


# EmbeddingValidator ------------------------------------------------------------------------
async def test_documents_are_embedded_in_max_batch_size_batches(offloader: Offloader) -> None:
    model = ScriptedEmbedding(batch_size=3)
    validator = EmbeddingValidator(model, offloader)
    assert validator.model is model
    texts = [f"text {index}" for index in range(7)]
    vectors = await validator.embed_documents(texts, tenant="tenant-a")
    assert [len(batch) for batch in model.batches] == [3, 3, 1]
    assert [text for batch in model.batches for text in batch] == texts
    assert vectors == [tuple(model.vector(text)) for text in texts]
    assert await validator.embed_documents([]) == []


async def test_non_positive_batch_size_falls_back_to_single_items(offloader: Offloader) -> None:
    model = ScriptedEmbedding(batch_size=0)
    await EmbeddingValidator(model, offloader).embed_documents(["a", "b"])
    assert model.batches == [["a"], ["b"]]


async def test_large_outputs_are_validated_in_the_offloader(offloader: Offloader) -> None:
    model = ScriptedEmbedding(dimensions=1024, batch_size=8)
    validator = EmbeddingValidator(model, offloader)
    vectors = await validator.embed_documents([f"doc {index}" for index in range(5)])
    assert len(vectors) == 5
    assert all(len(vector) == 1024 for vector in vectors)

    bad = ScriptedEmbedding(
        dimensions=1024, batch_size=8, corrupt=lambda vectors: [*vectors[:-1], [math.nan] * 1024]
    )
    with pytest.raises(EmbeddingValidationError, match="non-finite") as caught:
        await EmbeddingValidator(bad, offloader).embed_documents(["a", "b", "c", "d", "e"])
    assert caught.value.details == {"position": 4}


@pytest.mark.parametrize(
    ("corrupt", "message"),
    [
        (lambda vectors: vectors[:-1], "count"),
        (lambda vectors: [vector[:-1] for vector in vectors], "dimensions"),
        (lambda vectors: [[math.inf] * len(vector) for vector in vectors], "non-finite"),
        (lambda vectors: [[0.0] * len(vector) for vector in vectors], "zero norm"),
    ],
)
async def test_invalid_model_output_raises_structured_errors(
    offloader: Offloader, corrupt: Callable[[list[list[float]]], list[list[float]]], message: str
) -> None:
    validator = EmbeddingValidator(ScriptedEmbedding(corrupt=corrupt), offloader)
    with pytest.raises(EmbeddingValidationError, match=message):
        await validator.embed_documents(["one", "two"])


async def test_embed_query_validates(offloader: Offloader) -> None:
    model = ScriptedEmbedding()
    assert await EmbeddingValidator(model, offloader).embed_query("hello") == tuple(
        model.vector("hello")
    )
    broken = ScriptedEmbedding(corrupt=lambda vectors: [[1.0] for _ in vectors])
    with pytest.raises(EmbeddingValidationError, match="dimensions"):
        await EmbeddingValidator(broken, offloader).embed_query("hello")


# tokenizer ---------------------------------------------------------------------------------
async def test_simple_tokenizer_folds_filters_and_is_unicode_aware() -> None:
    tokenizer = SimpleTokenizer(stopwords=["THE", "Und"], min_length=2)
    assert tokenizer.tokenize_one("The Straße und a Café_Bar, x 42") == [
        "strasse",
        "café_bar",
        "42",
    ]
    assert await tokenizer.tokenize(["The end", ""]) == [["end"], []]
    assert tokenizer.info.implementation == "simple"
    assert tokenizer.info != SimpleTokenizer().info


def test_fold_composes_and_casefolds() -> None:
    assert fold("Cafe\u0301") == "café"
    assert fold("STRASSE") == fold("straße")


def test_tokenizer_rejects_non_positive_min_length() -> None:
    with pytest.raises(ConfigurationError):
        SimpleTokenizer(min_length=0)


# keyword dictionaries ----------------------------------------------------------------------
async def test_static_dictionary_returns_entries_for_every_scope() -> None:
    entries = (KeywordEntry(canonical="Python", aliases=("py",)),)
    dictionary = StaticKeywordDictionary(entries)
    assert await dictionary.entries(SCOPE) == entries
    assert await dictionary.entries(Scope(tenant_id="other")) == entries
    assert dictionary.info.implementation == "static"
    assert dictionary.info != StaticKeywordDictionary().info


class _Document:
    def __init__(self, document_id: str, metadata: dict[str, Any]) -> None:
        self.document_id = document_id
        self.metadata = metadata


class _Transaction:
    def __init__(self, store: FakeStore) -> None:
        self._store = store

    async def list_document_metadata(
        self, scope_key: str, *, state: Any, after_id: str | None, limit: int
    ) -> list[tuple[str, dict[str, Any]]]:
        self._store.calls.append((scope_key, str(state), after_id, limit))
        rows = sorted(self._store.documents, key=lambda document: document.document_id)
        rows = [row for row in rows if after_id is None or row.document_id > after_id]
        return [(row.document_id, row.metadata) for row in rows[:limit]]


class FakeStore:
    """Paged ``list_document_metadata`` over an in-memory list of active documents."""

    def __init__(self, documents: list[_Document]) -> None:
        self.documents = documents
        self.calls: list[tuple[str, str, str | None, int]] = []

    @contextlib.asynccontextmanager
    async def transaction(self, scope: Scope, *, write: bool) -> AsyncIterator[_Transaction]:
        assert write is False
        yield _Transaction(self)


def with_store(context: ComponentContext, store: FakeStore) -> ComponentContext:
    return dataclasses.replace(context, metadata_store=store)


async def test_metadata_dictionary_collects_strings_and_lists(context: ComponentContext) -> None:
    store = FakeStore(
        [
            _Document("d1", {"keywords": ["Python", "  asyncio ", "", 3], "tags": "Rust"}),
            _Document("d2", {"keywords": "PYTHON", "tags": [None, "uv"]}),
            _Document("d3", {"other": "ignored"}),
        ]
    )
    dictionary = MetadataKeywordDictionary(
        fields=("keywords", "tags"),
        static_entries=[KeywordEntry(canonical="Python", aliases=("py",))],
    )
    await dictionary.initialize(with_store(context, store))
    entries = await dictionary.entries(SCOPE)
    # Static entries win on folded collisions; output is sorted by folded form.
    assert [entry.canonical for entry in entries] == ["asyncio", "Python", "Rust", "uv"]
    assert entries[1].aliases == ("py",)
    assert store.calls == [(SCOPE.key, "active", None, 5000)]


async def test_metadata_dictionary_pages_through_documents(context: ComponentContext) -> None:
    documents = [_Document(f"d{index:04d}", {"keywords": f"kw{index}"}) for index in range(5001)]
    store = FakeStore(documents)
    dictionary = MetadataKeywordDictionary()
    await dictionary.initialize(with_store(context, store))
    entries = await dictionary.entries(SCOPE)
    assert len(entries) == 5001
    assert [call[2] for call in store.calls] == [None, "d4999"]


async def test_metadata_dictionary_without_store_or_fields_uses_static_only(
    context: ComponentContext,
) -> None:
    static = [KeywordEntry(canonical="b"), KeywordEntry(canonical="A")]
    unbound = MetadataKeywordDictionary(static_entries=static)
    assert [entry.canonical for entry in await unbound.entries(SCOPE)] == ["A", "b"]
    store = FakeStore([_Document("d1", {"keywords": "x"})])
    no_fields = MetadataKeywordDictionary(fields=(), static_entries=static)
    await no_fields.initialize(with_store(context, store))
    assert [entry.canonical for entry in await no_fields.entries(SCOPE)] == ["A", "b"]
    assert store.calls == []
    assert no_fields.info != unbound.info


# phrase matcher ----------------------------------------------------------------------------
def test_phrase_matcher_respects_word_boundaries_and_longest_match() -> None:
    matcher = PhraseMatcher(
        {"New York": "NYC", "new york city": "NYC-full", "york": "York", "  ": "blank"}
    )
    assert not matcher.empty
    text = "I love New York City; yorkshire is not york."
    assert matcher.find(text) == [
        ("NYC-full", "new york city", 7, 20),
        ("York", "york", 39, 43),
    ]
    # Embedded inside a longer word on either side: no match.
    assert matcher.find("newyork xyork yorkx") == []


def test_phrase_matcher_first_surface_wins_on_fold_collision() -> None:
    matcher = PhraseMatcher({"Python": "first", "PYTHON": "second"})
    assert matcher.find("python") == [("first", "python", 0, 6)]


def test_empty_phrase_matcher_finds_nothing() -> None:
    matcher = PhraseMatcher({" ": "blank"})
    assert matcher.empty
    assert matcher.find("anything") == []
