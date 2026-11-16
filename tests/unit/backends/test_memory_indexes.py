"""In-memory generational indexes: generation states, removal, fuzzy and BM25 edges."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pytest

from retrievalloom.contracts.indexes import (
    FuzzyQuery,
    GenerationSpec,
    IndexItem,
    KeywordQuery,
    SparseQuery,
)
from retrievalloom.contracts.models import KeywordEntry
from retrievalloom.domain.filters import Eq
from retrievalloom.domain.hashing import sha256_digest, stable_id
from retrievalloom.domain.inputs import Scope
from retrievalloom.domain.records import ObjectKind
from retrievalloom.errors import (
    ComponentContractError,
    ConfigurationError,
    GenerationError,
    LifecycleError,
)
from retrievalloom.index.fuzzy import RapidFuzzIndex
from retrievalloom.index.keyword import AhoCorasickKeywordIndex
from retrievalloom.index.sparse import BM25sIndex

if TYPE_CHECKING:
    from collections.abc import Sequence

    from retrievalloom.contracts.component import ComponentContext
    from retrievalloom.domain.retrieval import RetrievalHit

SCOPE = Scope(tenant_id="tenant-a")
OTHER = Scope(tenant_id="tenant-b")
NAMES: dict[str, str] = {}


def spec(generation_id: str = "gen", *entries: KeywordEntry) -> GenerationSpec:
    return GenerationSpec(
        generation_id=generation_id,
        method="test",
        dependency_fingerprint=sha256_digest("test"),
        keyword_entries=entries,
    )


def oid(name: str) -> str:
    identifier = stable_id("obj", name)
    NAMES[identifier] = name
    return identifier


def names(hits: Sequence[RetrievalHit]) -> list[str]:
    return [NAMES[hit.object_ref.object_id] for hit in hits]


def item(
    object_id: str, text: str, metadata: dict[str, Any] | None = None, *, tokens: bool = True
) -> IndexItem:
    return IndexItem(
        object_id=oid(object_id),
        kind=ObjectKind.CHUNK,
        text=text,
        metadata=metadata or {},
        tokens=tuple(text.casefold().split()) if tokens else None,
    )


async def ready[T: AhoCorasickKeywordIndex | RapidFuzzIndex | BM25sIndex](
    index: T, context: ComponentContext
) -> T:
    await index.initialize(context)
    return index


# shared generation machinery (via the keyword index) ---------------------------------------
async def test_uninitialised_index_has_no_offloader() -> None:
    index = AhoCorasickKeywordIndex()
    with pytest.raises(LifecycleError, match="not initialised"):
        _ = index.offloader
    await index.create_generation(SCOPE, spec())
    await index.write(SCOPE, "gen", [item("a", "python")])
    with pytest.raises(LifecycleError):
        await index.seal(SCOPE, "gen")


async def test_generation_lifecycle_errors(context: ComponentContext) -> None:
    index = await ready(AhoCorasickKeywordIndex(), context)
    with pytest.raises(GenerationError, match="not found") as caught:
        await index.write(SCOPE, "missing", [])
    assert caught.value.details == {"generation_id": "missing"}
    with pytest.raises(GenerationError):
        await index.update_metadata(SCOPE, "missing", {})
    with pytest.raises(GenerationError):
        await index.seal(SCOPE, "missing")
    assert await index.remove(SCOPE, "missing", ["a"]) == 0

    await index.create_generation(SCOPE, spec("gen", KeywordEntry(canonical="python")))
    with pytest.raises(GenerationError, match="not sealed"):
        await index.search(SCOPE, "gen", KeywordQuery(text="python"), 10, None)
    # Generations are per scope: another tenant cannot see this one.
    with pytest.raises(GenerationError, match="not found"):
        await index.write(OTHER, "gen", [])
    await index.seal(SCOPE, "gen")
    # Sealed generations stay writable: objects are inserted or replaced in place.
    await index.write(SCOPE, "gen", [item("a", "rust"), item("b", "python")])
    await index.write(SCOPE, "gen", [item("a", "python python")])
    hits = await index.search(SCOPE, "gen", KeywordQuery(text="python"), 10, None)
    assert sorted(names(hits)) == ["a", "b"]
    assert (await index.seal(SCOPE, "gen")).object_count == 2


async def test_staged_remove_and_rewrite_before_seal(context: ComponentContext) -> None:
    index = await ready(AhoCorasickKeywordIndex(), context)
    await index.create_generation(SCOPE, spec("gen", KeywordEntry(canonical="python")))
    await index.write(SCOPE, "gen", [item("a", "python"), item("b", "python too")])
    assert await index.remove(SCOPE, "gen", [oid("a"), oid("unknown")]) == 1
    assert await index.remove(SCOPE, "gen", [oid("a")]) == 0
    stats = await index.seal(SCOPE, "gen")
    assert stats.object_count == 1
    assert await index.seal(SCOPE, "gen") == stats  # idempotent, no rebuild
    hits = await index.search(SCOPE, "gen", KeywordQuery(text="python"), 10, None)
    assert names(hits) == ["b"]


async def test_remove_deletes_sealed_objects(context: ComponentContext) -> None:
    index = await ready(AhoCorasickKeywordIndex(), context)
    await index.create_generation(SCOPE, spec("gen", KeywordEntry(canonical="python")))
    await index.write(SCOPE, "gen", [item("a", "python", {"v": 1}), item("b", "python")])
    await index.seal(SCOPE, "gen")
    assert await index.remove(SCOPE, "gen", [oid("a"), oid("a"), oid("zzz")]) == 1
    assert (await index.seal(SCOPE, "gen")).object_count == 1
    # Removed objects can no longer have their metadata updated.
    assert await index.update_metadata(SCOPE, "gen", {oid("a"): {"v": 2}, oid("b"): {"v": 2}}) == 1
    hits = await index.search(SCOPE, "gen", KeywordQuery(text="python"), 10, Eq(field="v", value=2))
    assert names(hits) == ["b"]
    assert await index.list_generations(SCOPE) == ["gen"]
    await index.drop_generation(SCOPE, "gen")
    await index.drop_generation(SCOPE, "gen")
    assert await index.list_generations(SCOPE) == []


async def test_close_drops_every_generation(context: ComponentContext) -> None:
    index = await ready(AhoCorasickKeywordIndex(), context)
    await index.create_generation(SCOPE, spec("g1"))
    await index.create_generation(OTHER, spec("g2"))
    await index.close()
    assert await index.list_generations(SCOPE) == []
    assert await index.list_generations(OTHER) == []


async def test_keyword_spans_are_dropped_when_folding_changes_length(
    context: ComponentContext,
) -> None:
    index = await ready(AhoCorasickKeywordIndex(), context)
    entries = (KeywordEntry(canonical="strasse"), KeywordEntry(canonical="python"))
    await index.create_generation(SCOPE, spec("gen", *entries))
    await index.write(SCOPE, "gen", [item("a", "strasse python"), item("b", "rust")])
    await index.seal(SCOPE, "gen")
    folded = await index.search(SCOPE, "gen", KeywordQuery(text="Straße"), 10, None)
    assert folded[0].matched[0].query_span is None
    plain = await index.search(SCOPE, "gen", KeywordQuery(text="python!"), 10, None)
    assert plain[0].matched[0].query_span is not None
    # A matched keyword whose only objects are filtered out contributes nothing.
    assert (
        await index.search(SCOPE, "gen", KeywordQuery(text="python"), 10, Eq(field="x", value=1))
        == []
    )


# fuzzy -------------------------------------------------------------------------------------
@pytest.mark.parametrize(
    "kwargs",
    [{"score_cutoff": -1.0}, {"score_cutoff": 101.0}, {"max_ngram": 0}, {"min_token_length": 0}],
)
def test_fuzzy_rejects_invalid_configuration(kwargs: dict[str, Any]) -> None:
    with pytest.raises(ConfigurationError):
        RapidFuzzIndex(**kwargs)


async def test_fuzzy_dictionary_only_vocabulary(context: ComponentContext) -> None:
    index = await ready(RapidFuzzIndex(corpus_vocabulary=False, score_cutoff=80), context)
    await index.create_generation(
        SCOPE, spec("gen", KeywordEntry(canonical="kubernetes", aliases=("k8s cluster",)))
    )
    await index.write(
        SCOPE,
        "gen",
        [item("a", "deploy on kubernetes today"), item("b", "a k8s cluster"), item("c", "nothing")],
    )
    await index.seal(SCOPE, "gen")
    hits = await index.search(SCOPE, "gen", FuzzyQuery(tokens=("kubernets",)), 10, None)
    assert names(hits) == ["a"]
    assert hits[0].matched[0].matched == "kubernetes"
    # Corpus-only words are not in the vocabulary.
    assert await index.search(SCOPE, "gen", FuzzyQuery(tokens=("deplooy",)), 10, None) == []


async def test_fuzzy_short_tokens_and_empty_inputs(context: ComponentContext) -> None:
    index = await ready(RapidFuzzIndex(min_token_length=4, max_ngram=2), context)
    await index.create_generation(SCOPE, spec("gen"))
    await index.write(SCOPE, "gen", [item("a", "go rust tokio"), item("b", "rust")])
    await index.seal(SCOPE, "gen")
    # Only tokens/grams of >= 4 chars are considered: "go" alone yields nothing.
    assert await index.search(SCOPE, "gen", FuzzyQuery(tokens=("go",)), 10, None) == []
    assert await index.search(SCOPE, "gen", FuzzyQuery(tokens=()), 10, None) == []
    # Repeated tokens produce duplicate grams that are matched only once.
    hits = await index.search(SCOPE, "gen", FuzzyQuery(tokens=("Rust", "rust", "rust")), 10, None)
    assert names(hits) == ["a", "b"]
    assert await index.search(SCOPE, "gen", FuzzyQuery(tokens=("rust",)), 1, None) == hits[:1]

    await index.create_generation(SCOPE, spec("empty"))
    await index.seal(SCOPE, "empty")
    assert await index.search(SCOPE, "empty", FuzzyQuery(tokens=("rust",)), 10, None) == []


async def test_fuzzy_keeps_best_similarity_per_term(context: ComponentContext) -> None:
    index = await ready(RapidFuzzIndex(score_cutoff=70, max_ngram=1), context)
    await index.create_generation(SCOPE, spec("gen"))
    await index.write(SCOPE, "gen", [item("a", "asyncio"), item("b", "other")])
    await index.seal(SCOPE, "gen")
    exact = await index.search(SCOPE, "gen", FuzzyQuery(tokens=("asyncio",)), 10, None)
    both = await index.search(SCOPE, "gen", FuzzyQuery(tokens=("asyncio", "asyncoi")), 10, None)
    reversed_order = await index.search(
        SCOPE, "gen", FuzzyQuery(tokens=("asyncoi", "asyncio")), 10, None
    )
    assert exact[0].raw_score == pytest.approx(1.0)
    # The best similarity (exact gram) wins regardless of gram order.
    assert both[0].raw_score == reversed_order[0].raw_score == exact[0].raw_score
    assert both[0].matched[0].term == reversed_order[0].matched[0].term == "asyncio"
    await index.remove(SCOPE, "gen", [oid("a")])
    assert await index.search(SCOPE, "gen", FuzzyQuery(tokens=("asyncio",)), 10, None) == []


async def test_fuzzy_keeps_five_best_terms_per_gram(context: ComponentContext) -> None:
    index = await ready(RapidFuzzIndex(score_cutoff=80, max_ngram=1), context)
    await index.create_generation(SCOPE, spec("gen"))
    words = ["parsers", "parsera", "parserb", "parserc", "parserd", "parsere", "parser"]
    await index.write(SCOPE, "gen", [item(word, word) for word in words])
    await index.seal(SCOPE, "gen")
    hits = await index.search(SCOPE, "gen", FuzzyQuery(tokens=("parser",)), 10, None)
    # The exact term ranks first; equal-score ties keep vocabulary order up to the cap.
    assert names(hits)[0] == "parser"
    assert set(names(hits)) == {"parser", "parsera", "parserb", "parserc", "parserd"}


# sparse ------------------------------------------------------------------------------------
@pytest.mark.parametrize("kwargs", [{"k1": 0.0}, {"b": -0.1}, {"b": 1.5}, {"method": "bm99"}])
def test_sparse_rejects_invalid_parameters(kwargs: dict[str, Any]) -> None:
    with pytest.raises(ConfigurationError):
        BM25sIndex(**kwargs)


async def test_sparse_requires_tokens(context: ComponentContext) -> None:
    index = await ready(BM25sIndex(), context)
    await index.create_generation(SCOPE, spec())
    await index.write(SCOPE, "gen", [item("a", "python", tokens=False)])
    with pytest.raises(ComponentContractError, match="tokens") as caught:
        await index.seal(SCOPE, "gen")
    assert caught.value.details == {"object_id": oid("a")}


async def test_sparse_empty_generations(context: ComponentContext) -> None:
    index = await ready(BM25sIndex(), context)
    await index.create_generation(SCOPE, spec("none"))
    assert (await index.seal(SCOPE, "none")).object_count == 0
    assert await index.search(SCOPE, "none", SparseQuery(tokens=("python",)), 10, None) == []

    await index.create_generation(SCOPE, spec("blank"))
    await index.write(SCOPE, "blank", [item("a", ""), item("b", "   ")])
    assert (await index.seal(SCOPE, "blank")).object_count == 2
    assert await index.search(SCOPE, "blank", SparseQuery(tokens=("python",)), 10, None) == []


async def test_sparse_unknown_terms_and_fully_masked_queries(context: ComponentContext) -> None:
    index = await ready(BM25sIndex(k1=1.2, b=0.5), context)
    await index.create_generation(SCOPE, spec())
    await index.write(
        SCOPE,
        "gen",
        [item("b", "python python asyncio", {"lang": "en"}), item("a", "python", {"lang": "de"})],
    )
    await index.seal(SCOPE, "gen")
    assert await index.search(SCOPE, "gen", SparseQuery(tokens=("haskell",)), 10, None) == []
    hits = await index.search(SCOPE, "gen", SparseQuery(tokens=("python", "haskell")), 1, None)
    assert names(hits) == ["b"]
    german = Eq(field="lang", value="de")
    filtered = await index.search(SCOPE, "gen", SparseQuery(tokens=("python",)), 10, german)
    assert names(filtered) == ["a"]
    await index.remove(SCOPE, "gen", [oid("a")])
    assert await index.search(SCOPE, "gen", SparseQuery(tokens=("python",)), 10, german) == []
    # Objects without any query term score zero and are never returned.
    only_b = await index.search(SCOPE, "gen", SparseQuery(tokens=("asyncio",)), 10, None)
    assert names(only_b) == ["b"]
