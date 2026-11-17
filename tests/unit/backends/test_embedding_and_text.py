"""Embedding validation/batching, tokenizer, keyword dictionaries and the phrase matcher."""

from __future__ import annotations

import dataclasses
import math
from typing import TYPE_CHECKING, Any

import pytest

from retrievalloom.contracts.models import KeywordEntry
from retrievalloom.domain.inputs import Scope
from retrievalloom.domain.records import DocumentState
from retrievalloom.errors import ConfigurationError, EmbeddingValidationError
from retrievalloom.index import text as text_module
from retrievalloom.index.embedding import EmbeddingValidator, check_vectors
from retrievalloom.index.text import (
    MetadataKeywordDictionary,
    PhraseMatcher,
    SimpleTokenizer,
    StaticKeywordDictionary,
    fold,
    track_keyword_changes,
)
from retrievalloom.storage.sqlite import SQLiteMetadataStore
from tests.fakes import HashEmbedding
from tests.unit.storage.factories import document

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Callable, Sequence

    from retrievalloom.contracts.component import ComponentContext
    from retrievalloom.domain.records import DocumentRecord
    from retrievalloom.runtime.offloader import Offloader
    from retrievalloom.storage.base import KeywordDictionaryState

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
    revision = await dictionary.revision(SCOPE)
    assert revision == await StaticKeywordDictionary(entries).revision(SCOPE)
    assert revision != await StaticKeywordDictionary().revision(SCOPE)


@pytest.fixture
async def store() -> AsyncIterator[SQLiteMetadataStore]:
    component = SQLiteMetadataStore()
    await component.initialize()
    await component.migrate()
    async with component.transaction(write=True) as tx:
        await tx.ensure_scope(SCOPE)
    try:
        yield component
    finally:
        await component.close()


def record(document_id: str, metadata: dict[str, Any], **values: Any) -> DocumentRecord:
    return document(SCOPE, document_id, metadata=metadata, **values)


async def put(store: SQLiteMetadataStore, *records: DocumentRecord) -> None:
    async with store.transaction(SCOPE, write=True) as tx:
        await tx.put_documents([(item, None) for item in records])


async def bound(
    context: ComponentContext, store: SQLiteMetadataStore, **options: Any
) -> MetadataKeywordDictionary:
    dictionary = MetadataKeywordDictionary(**options)
    await dictionary.initialize(dataclasses.replace(context, metadata_store=store))
    return dictionary


async def test_metadata_dictionary_collects_strings_and_lists(
    context: ComponentContext, store: SQLiteMetadataStore
) -> None:
    await put(
        store,
        record("d1", {"keywords": ["Python", "  asyncio ", "", 3], "tags": "Rust"}),
        record("d2", {"keywords": "PYTHON", "tags": [None, "uv"]}),
        record("d3", {"other": "ignored"}),
        record("d4", {"keywords": "gone"}, state=DocumentState.DELETED),
    )
    dictionary = await bound(
        context,
        store,
        fields=("keywords", "tags"),
        static_entries=[KeywordEntry(canonical="Python", aliases=("py",))],
    )
    entries = await dictionary.entries(SCOPE)
    # Static entries win on folded collisions; output is sorted by folded form.
    assert [entry.canonical for entry in entries] == ["asyncio", "Python", "Rust", "uv"]
    assert entries[1].aliases == ("py",)
    plain = await bound(context, store)
    # Without a static entry the smallest surface form is canonical.
    assert [entry.canonical for entry in await plain.entries(SCOPE)] == ["asyncio", "PYTHON"]
    revision = await dictionary.revision(SCOPE)
    assert revision.endswith(":1")
    assert revision != await plain.revision(SCOPE)
    async with store.transaction(SCOPE, write=False) as tx:
        assert len(await tx.list_keyword_dictionaries(SCOPE.key)) == 2


async def test_metadata_dictionary_pages_through_documents(
    context: ComponentContext, store: SQLiteMetadataStore
) -> None:
    await put(
        store, *[record(f"d{index:04d}", {"keywords": f"kw{index}"}) for index in range(5001)]
    )
    dictionary = await bound(context, store)
    assert len(await dictionary.entries(SCOPE)) == 5001


async def test_metadata_dictionary_registers_once_under_a_race(
    context: ComponentContext, store: SQLiteMetadataStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    await put(store, record("d1", {"keywords": "a"}))
    original = text_module._find
    calls: list[str] = []

    def stale_first_read(states: Sequence[KeywordDictionaryState], dictionary_id: str) -> Any:
        calls.append(dictionary_id)
        return None if len(calls) == 1 else original(states, dictionary_id)

    first = await bound(context, store)
    await first.revision(SCOPE)
    monkeypatch.setattr(text_module, "_find", stale_first_read)
    assert await (await bound(context, store)).revision(SCOPE) == await first.revision(SCOPE)


async def test_keyword_changes_adjust_terms_and_revision(
    context: ComponentContext, store: SQLiteMetadataStore
) -> None:
    old = record("d1", {"keywords": ["a", "shared"]})
    other = record("d2", {"keywords": "shared"})
    await put(store, old, other)
    dictionary = await bound(context, store)
    await dictionary.entries(SCOPE)
    initial = await dictionary.revision(SCOPE)
    renamed = old.model_copy(update={"metadata": {"keywords": ["b", "shared"]}})
    gone = other.model_copy(update={"state": DocumentState.DELETED})
    async with store.transaction(SCOPE, write=True) as tx:
        await track_keyword_changes(tx, SCOPE.key, [])
        await track_keyword_changes(tx, SCOPE.key, [(old, renamed), (other, gone)])
    assert [entry.canonical for entry in await dictionary.entries(SCOPE)] == ["b", "shared"]
    changed = await dictionary.revision(SCOPE)
    assert changed != initial
    async with store.transaction(SCOPE, write=True) as tx:
        await track_keyword_changes(tx, SCOPE.key, [(renamed, renamed)])
        await track_keyword_changes(tx, SCOPE.key, [(None, record("d3", {"keywords": "b"}))])
    # Count-only changes keep the revision.
    assert await dictionary.revision(SCOPE) == changed
    async with store.transaction(SCOPE, write=True) as tx:
        await track_keyword_changes(tx, SCOPE.key, [(gone, other)])
    assert await dictionary.revision(SCOPE) == changed
    assert [entry.canonical for entry in await dictionary.entries(SCOPE)] == ["b", "shared"]


async def test_metadata_dictionary_without_store_or_fields_uses_static_only(
    context: ComponentContext, store: SQLiteMetadataStore
) -> None:
    static = [KeywordEntry(canonical="b"), KeywordEntry(canonical="A")]
    unbound = MetadataKeywordDictionary(static_entries=static)
    assert [entry.canonical for entry in await unbound.entries(SCOPE)] == ["A", "b"]
    await put(store, record("d1", {"keywords": "x"}))
    no_fields = await bound(context, store, fields=(), static_entries=static)
    assert [entry.canonical for entry in await no_fields.entries(SCOPE)] == ["A", "b"]
    assert await no_fields.revision(SCOPE) == await unbound.revision(SCOPE)
    async with store.transaction(SCOPE, write=False) as tx:
        assert list(await tx.list_keyword_dictionaries(SCOPE.key)) == []
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
