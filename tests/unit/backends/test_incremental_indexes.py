"""In-place updates of the in-memory lexical indexes equal a full rebuild of the live objects."""

from __future__ import annotations

import itertools
import math
from typing import TYPE_CHECKING, Any

import bm25s
import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from retrievalloom.contracts.indexes import FuzzyQuery, KeywordQuery, SparseQuery
from retrievalloom.contracts.models import KeywordEntry
from retrievalloom.domain.filters import Eq
from retrievalloom.index.fuzzy import RapidFuzzIndex
from retrievalloom.index.keyword import AhoCorasickKeywordIndex
from retrievalloom.index.sparse import BM25sIndex
from tests.unit.backends.test_memory_indexes import SCOPE, item, names, oid, ready, spec

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

    from retrievalloom.contracts.component import ComponentContext
    from retrievalloom.contracts.indexes import IndexItem
    from retrievalloom.domain.retrieval import RetrievalHit
    from retrievalloom.index.memory import InMemoryGenerationalIndex

ENTRIES = (
    KeywordEntry(canonical="python"),
    KeywordEntry(canonical="kubernetes", aliases=("k8s cluster",)),
    KeywordEntry(canonical="rust"),
)
WORDS = ("python", "rust", "asyncio", "tokio", "parser", "parsers", "go", "kubernetes", "k8s")
OBJECTS = tuple("abcdefgh")
EVEN = Eq(field="even", value=True)

type Factory = Callable[[], AhoCorasickKeywordIndex | RapidFuzzIndex | BM25sIndex]
type Query = KeywordQuery | FuzzyQuery | SparseQuery

FACTORIES: dict[str, tuple[Factory, tuple[Query, ...]]] = {
    "keyword": (
        AhoCorasickKeywordIndex,
        tuple(
            KeywordQuery(text=text)
            for text in ("python", "Kubernetes and rust", "k8s cluster python", "nothing")
        ),
    ),
    "fuzzy": (
        lambda: RapidFuzzIndex(score_cutoff=80),
        tuple(
            FuzzyQuery(tokens=tokens)
            for tokens in (("pythn",), ("kubernets",), ("parser",), ("tokio", "rust"))
        ),
    ),
    "sparse": (
        BM25sIndex,
        tuple(
            SparseQuery(tokens=tokens)
            for tokens in (("python",), ("rust", "go", "python"), ("parsers", "asyncio"))
        ),
    ),
    "sparse-bm25+": (
        lambda: BM25sIndex(method="bm25+"),
        (SparseQuery(tokens=("python", "tokio")), SparseQuery(tokens=("k8s",))),
    ),
}
_generations = itertools.count()


def obj(name: str, text: str) -> IndexItem:
    return item(name, text, {"even": len(text) % 2 == 0})


def summary(hits: Sequence[RetrievalHit]) -> tuple[list[Any], list[float | None]]:
    return (
        [(hit.object_ref.object_id, hit.rank, hit.matched) for hit in hits],
        [hit.raw_score for hit in hits],
    )


async def assert_equals_rebuild(
    index: InMemoryGenerationalIndex,
    queries: Sequence[Query],
    generation_id: str,
    live: dict[str, str],
) -> None:
    """Search results of ``generation_id`` equal those of a fresh build over ``live``."""
    rebuilt = f"rebuilt-{next(_generations)}"
    searcher: Any = index
    await index.create_generation(SCOPE, spec(rebuilt, *ENTRIES))
    await index.write(SCOPE, rebuilt, [obj(name, text) for name, text in live.items()])
    await index.seal(SCOPE, rebuilt)
    for query, limit, filters in itertools.product(queries, (2, 10), (None, EVEN)):
        got = summary(await searcher.search(SCOPE, generation_id, query, limit, filters))
        want = summary(await searcher.search(SCOPE, rebuilt, query, limit, filters))
        assert got[0] == want[0]
        assert got[1] == pytest.approx(want[1], rel=1e-12)
    assert (await index.seal(SCOPE, generation_id)).object_count == len(live)
    await index.drop_generation(SCOPE, rebuilt)


texts = st.lists(st.sampled_from(WORDS), max_size=6).map(" ".join)
operations = st.lists(st.tuples(st.sampled_from(OBJECTS), st.one_of(st.none(), texts)), max_size=6)


@pytest.mark.parametrize("kind", sorted(FACTORIES))
@settings(
    max_examples=40, deadline=None, suppress_health_check=[HealthCheck.function_scoped_fixture]
)
@given(staged=operations, batches=st.lists(operations, max_size=5))
async def test_in_place_updates_equal_a_full_rebuild(
    context: ComponentContext,
    kind: str,
    staged: list[tuple[str, str | None]],
    batches: list[list[tuple[str, str | None]]],
) -> None:
    """Mixed add/update/delete batches, before and after sealing, then every query."""
    factory, queries = FACTORIES[kind]
    index = await ready(factory(), context)
    generation_id = f"inc-{next(_generations)}"
    await index.create_generation(SCOPE, spec(generation_id, *ENTRIES))
    live: dict[str, str] = {}
    for position, batch in enumerate([staged, *batches]):
        writes = {name: text for name, text in batch if text is not None}
        deletes = [name for name, text in batch if text is None and name not in writes]
        await index.write(SCOPE, generation_id, [obj(n, t) for n, t in writes.items()])
        await index.remove(SCOPE, generation_id, [oid(name) for name in deletes])
        live.update(writes)
        for name in deletes:
            live.pop(name, None)
        if position == 0:
            await index.seal(SCOPE, generation_id)
    await assert_equals_rebuild(index, queries, generation_id, live)
    await index.close()


async def test_fuzzy_vocabulary_follows_posting_changes(context: ComponentContext) -> None:
    index = await ready(RapidFuzzIndex(max_ngram=1), context)
    await index.create_generation(SCOPE, spec("gen"))
    await index.write(SCOPE, "gen", [obj("a", "python rust"), obj("b", "python")])
    await index.seal(SCOPE, "gen")
    await index.write(SCOPE, "gen", [obj("c", "python")])  # existing posting
    await index.remove(SCOPE, "gen", [oid("a")])  # "rust" leaves the vocabulary
    await index.write(SCOPE, "gen", [obj("a", "rust")])  # ... and comes back
    await index.write(SCOPE, "gen", [obj("d", "tokio")])  # new term
    await index.remove(SCOPE, "gen", [oid("d")])  # ... removed before any search
    rust = await index.search(SCOPE, "gen", FuzzyQuery(tokens=("rust",)), 10, None)
    assert names(rust) == ["a"]
    assert await index.search(SCOPE, "gen", FuzzyQuery(tokens=("tokio",)), 10, None) == []
    await index.remove(SCOPE, "gen", [oid("b")])
    queries = [FuzzyQuery(tokens=(word,)) for word in ("python", "rust", "tokio")]
    await assert_equals_rebuild(index, queries, "gen", {"a": "rust", "c": "python"})


async def test_fuzzy_matches_are_not_cached_across_a_vocabulary_change(
    context: ComponentContext, monkeypatch: pytest.MonkeyPatch
) -> None:
    index = await ready(RapidFuzzIndex(max_ngram=1, score_cutoff=80), context)
    await index.create_generation(SCOPE, spec("gen"))
    await index.write(SCOPE, "gen", [obj("a", "python")])
    await index.seal(SCOPE, "gen")
    run = index.offloader.run

    async def write_while_matching(*args: Any, **kwargs: Any) -> Any:
        monkeypatch.setattr(index.offloader, "run", run)
        found = await run(*args, **kwargs)
        # A concurrent search refreshes the vocabulary before this match returns.
        await index.write(SCOPE, "gen", [obj("b", "pythons")])
        await index.search(SCOPE, "gen", FuzzyQuery(tokens=("tokio",)), 10, None)
        return found

    monkeypatch.setattr(index.offloader, "run", write_while_matching)
    query = FuzzyQuery(tokens=("pythons",))
    assert names(await index.search(SCOPE, "gen", query, 10, None)) == ["a"]
    assert sorted(names(await index.search(SCOPE, "gen", query, 10, None))) == ["a", "b"]


async def test_keyword_postings_follow_replacements(context: ComponentContext) -> None:
    index = await ready(AhoCorasickKeywordIndex(), context)
    await index.create_generation(SCOPE, spec("gen", *ENTRIES))
    await index.write(SCOPE, "gen", [obj("a", "python rust"), obj("b", "python")])
    await index.seal(SCOPE, "gen")
    await index.write(SCOPE, "gen", [obj("a", "kubernetes")])  # "rust" posting emptied
    assert await index.search(SCOPE, "gen", KeywordQuery(text="rust"), 10, None) == []
    hits = await index.search(SCOPE, "gen", KeywordQuery(text="python kubernetes"), 10, None)
    # idf counts live objects: N=2, df=1 for both keywords.
    assert sorted(names(hits)) == ["a", "b"]
    assert [hit.raw_score for hit in hits] == pytest.approx([math.log(3.0)] * 2)


@pytest.mark.parametrize("method", ["lucene", "robertson", "atire", "bm25l", "bm25+"])
async def test_sparse_scores_match_bm25s(context: ComponentContext, method: str) -> None:
    corpus = {
        "a": "python asyncio python",
        "b": "rust tokio",
        "c": "python rust go",
        "d": "go go go parser",
        "e": "asyncio event loop python",
    }
    index = await ready(BM25sIndex(k1=1.2, b=0.7, method=method), context)
    await index.create_generation(SCOPE, spec())
    await index.write(SCOPE, "gen", [obj("x", "python"), obj("a", "stale")])
    await index.seal(SCOPE, "gen")
    await index.write(SCOPE, "gen", [obj(name, text) for name, text in corpus.items()])
    await index.remove(SCOPE, "gen", [oid("x")])
    reference = bm25s.BM25(k1=1.2, b=0.7, method=method)
    reference.index([text.split() for text in corpus.values()], show_progress=False)
    for tokens in (["python"], ["python", "rust"], ["go", "asyncio", "loop"]):
        expected = dict(zip(corpus, reference.get_scores(tokens).tolist(), strict=True))
        hits = await index.search(SCOPE, "gen", SparseQuery(tokens=tuple(tokens)), 10, None)
        got = {name: hit.raw_score for name, hit in zip(names(hits), hits, strict=True)}
        assert got == pytest.approx({n: s for n, s in expected.items() if s > 0}, rel=1e-5)


async def test_sparse_compacts_dead_slots(context: ComponentContext) -> None:
    index = await ready(BM25sIndex(), context)
    await index.create_generation(SCOPE, spec())
    await index.write(SCOPE, "gen", [obj(f"o{i}", f"common w{i}") for i in range(1500)])
    await index.seal(SCOPE, "gen")
    structure = index.generation(SCOPE, "gen").built
    await index.remove(SCOPE, "gen", [oid(f"o{i}") for i in range(1000)])
    assert index.generation(SCOPE, "gen").built is structure  # 1000 dead: not yet
    await index.remove(SCOPE, "gen", [oid(f"o{i}") for i in range(1000, 1100)])
    compacted = index.generation(SCOPE, "gen").built
    assert compacted is not structure
    assert (len(compacted.ids), compacted.dead) == (400, 0)
    assert "w0" not in compacted.postings
    await index.write(SCOPE, "gen", [obj("o1100", "common replaced"), obj("new", "common")])
    live = {f"o{i}": f"common w{i}" for i in range(1101, 1500)}
    live |= {"o1100": "common replaced", "new": "common"}
    queries = [SparseQuery(tokens=tokens) for tokens in (("common",), ("w1200", "replaced"))]
    await assert_equals_rebuild(index, queries, "gen", live)


async def test_sparse_terms_of_removed_objects_score_nothing(context: ComponentContext) -> None:
    index = await ready(BM25sIndex(method="bm25+"), context)
    await index.create_generation(SCOPE, spec())
    await index.write(SCOPE, "gen", [obj("a", "haskell"), obj("b", "python")])
    await index.seal(SCOPE, "gen")
    await index.remove(SCOPE, "gen", [oid("a")])
    # "haskell" is still posted (dead slot) but has no live object: no score, no base.
    assert await index.search(SCOPE, "gen", SparseQuery(tokens=("haskell",)), 10, None) == []


async def test_sparse_limit_keeps_lowest_ids_among_ties(context: ComponentContext) -> None:
    index = await ready(BM25sIndex(), context)
    await index.create_generation(SCOPE, spec())
    await index.write(
        SCOPE, "gen", [obj(name, "python") for name in "cab"] + [obj("d", "python rust")]
    )
    await index.seal(SCOPE, "gen")
    query = SparseQuery(tokens=("python",))
    everything = await index.search(SCOPE, "gen", query, 10, None)
    tied = sorted(
        hit.object_ref.object_id for hit in everything if hit.raw_score == everything[0].raw_score
    )
    first = await index.search(SCOPE, "gen", query, 1, None)
    assert [hit.object_ref.object_id for hit in first] == tied[:1]
    assert await index.search(SCOPE, "gen", query, 0, None) == []


async def test_sparse_skips_objects_removed_while_scoring(
    context: ComponentContext, monkeypatch: pytest.MonkeyPatch
) -> None:
    index = await ready(BM25sIndex(), context)
    await index.create_generation(SCOPE, spec())
    await index.write(SCOPE, "gen", [obj("a", "python"), obj("b", "python rust")])
    await index.seal(SCOPE, "gen")
    generation = index.generation(SCOPE, "gen")
    score = index._score_sync

    def racing(*args: Any) -> Any:
        result = score(*args)
        generation.metadata.pop(oid("a"))  # as if removed before the results are read
        return result

    monkeypatch.setattr(index, "_score_sync", racing)
    hits = await index.search(SCOPE, "gen", SparseQuery(tokens=("python",)), 10, None)
    assert names(hits) == ["b"]
