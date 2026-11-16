"""Candidate assembly: visibility at the snapshot, deletions, staleness, filters, aggregates."""

from __future__ import annotations

import contextlib
from typing import TYPE_CHECKING, Any, cast

import pytest

from retrievalloom import (
    ChannelsConfig,
    DocumentInput,
    Eq,
    GraphChannelConfig,
    GraphEntityInput,
    GraphRepresentation,
    IndexWait,
    IngestionConfig,
    RetrievalConfig,
    Scope,
    TenantIsolationError,
    TextRepresentation,
)
from retrievalloom.domain.records import Channel, ObjectKind, ObjectState
from retrievalloom.domain.retrieval import MethodRef, ObjectRef, RetrievalHit
from retrievalloom.retrieval.candidates import CandidateBuilder, candidate_id
from tests.unit.retrieval.helpers import ACME, GLOBEX, opened, runtime, text_doc

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Callable, Mapping, Sequence

    from retrievalloom import RetrievalEngine
    from retrievalloom.domain.records import DerivedObject

KEYWORD = "index.keyword"
GRAPH = "graph.graph"
GRAPH_CONFIG = RetrievalConfig(channels=ChannelsConfig(graph=GraphChannelConfig(enabled=True)))


def _hit(obj: DerivedObject, unit: str = KEYWORD, rank: int = 1, scope: Scope = ACME) -> Any:
    channel, method = unit.split(".")
    return RetrievalHit(
        object_ref=ObjectRef(scope_key=scope.key, object_id=obj.object_id, generation_id="g"),
        method=MethodRef(channel=Channel(channel), method=method),
        backend_id="test",
        raw_score=1.0 / rank,
        rank=rank,
    )


async def _seq(engine: RetrievalEngine, scope: Scope = ACME) -> int:
    async with runtime(engine).store.transaction(scope, write=False) as tx:
        return await tx.current_commit_seq()


async def _objects(
    engine: RetrievalEngine, document_id: str, scope: Scope = ACME, kinds: Any = None
) -> list[DerivedObject]:
    async with runtime(engine).store.transaction(scope, write=False) as tx:
        document = await tx.get_document(scope, document_id)
        assert document is not None
        return list(
            await tx.list_document_objects(
                scope.key, document.document_uid, document.version, kinds
            )
        )


async def _graph_objects(engine: RetrievalEngine, scope: Scope = ACME) -> list[DerivedObject]:
    async with runtime(engine).store.transaction(scope, write=False) as tx:
        return list(await tx.list_current_objects(scope.key, [ObjectKind.GRAPH_ENTITY]))


async def _build(
    engine: RetrievalEngine,
    hits: Mapping[str, Sequence[Any]],
    *,
    seq: int,
    filters: Any = None,
    allow_stale: bool = False,
    store: Any = None,
    normalized: dict[str, dict[str, float]] | None = None,
) -> Any:
    rt = runtime(engine)
    builder = CandidateBuilder(store or rt.store)
    return await builder.build(
        ACME,
        hits,
        rt.units,
        snapshot_seq=seq,
        filters=filters,
        allow_stale=allow_stale,
        normalized=normalized or {},
    )


async def test_empty_hits_and_foreign_hits() -> None:
    async with opened() as engine:
        await engine.ingest([text_doc("d1", "python asyncio")], scope=ACME)
        seq = await _seq(engine)
        empty = await _build(engine, {KEYWORD: []}, seq=seq)
        assert empty.candidates == []
        [obj] = await _objects(engine, "d1")
        with pytest.raises(TenantIsolationError):
            await _build(engine, {KEYWORD: [_hit(obj, scope=GLOBEX)]}, seq=seq)


async def test_candidate_groups_hits_and_carries_scores_and_provenance() -> None:
    async with opened() as engine:
        await engine.ingest([text_doc("d1", "python asyncio")], scope=ACME)
        seq = await _seq(engine)
        [obj] = await _objects(engine, "d1")
        hits = {KEYWORD: [_hit(obj)], "index.sparse": [_hit(obj, "index.sparse")]}
        batch = await _build(
            engine, hits, seq=seq, normalized={KEYWORD: {obj.object_id: 0.7}, "index.sparse": {}}
        )
    [candidate] = batch.candidates
    assert candidate.candidate_id == candidate_id(ACME, obj.object_id)
    assert candidate.scores.normalized == {KEYWORD: 0.7}
    assert [h.method.method for h in candidate.hits] == ["keyword", "sparse"]
    assert candidate.sources == obj.sources
    assert not candidate.flags.stale_version


async def test_unknown_invisible_deleted_and_filtered_objects() -> None:
    async with opened() as engine:
        await engine.ingest(
            [text_doc("d1", "python asyncio", lang="en"), text_doc("d2", "python loops")],
            scope=ACME,
        )
        [one] = await _objects(engine, "d1")
        [two] = await _objects(engine, "d2")
        ghost = one.model_copy(update={"object_id": "obj_" + "0" * 32})
        hits = {KEYWORD: [_hit(ghost), _hit(one, rank=2), _hit(two, rank=3)]}

        invisible = await _build(engine, hits, seq=0)
        assert invisible.candidates == []
        assert invisible.hidden_not_visible == 3

        filtered = await _build(
            engine, hits, seq=await _seq(engine), filters=Eq(field="lang", value="en")
        )
        assert [c.object_id for c in filtered.candidates] == [one.object_id]
        assert filtered.filtered_out == 1

        await engine.delete(["d1"], scope=ACME)
        deleted = await _build(engine, hits, seq=await _seq(engine))
        assert deleted.filtered_deleted == 1
        assert [c.object_id for c in deleted.candidates] == [two.object_id]


async def test_superseded_versions_are_dropped_or_flagged() -> None:
    async with opened(ingestion=IngestionConfig(index_wait=IndexWait.NONE)) as engine:
        await engine.ingest([text_doc("d1", "python asyncio")], scope=ACME)
        await engine.run_lifecycle_once(ACME)
        seq = await _seq(engine)
        [old] = await _objects(engine, "d1")
        await engine.ingest([text_doc("d1", "python asyncio rewritten")], scope=ACME)
        hits = {KEYWORD: [_hit(old)]}
        dropped = await _build(engine, hits, seq=seq)
        kept = await _build(engine, hits, seq=seq, allow_stale=True)
    assert dropped.candidates == []
    assert dropped.stale_dropped == 1
    assert kept.stale_kept == 1
    assert kept.candidates[0].flags.stale_version


class _Tx:
    def __init__(self, inner: Any, rewrite: Callable[[DerivedObject], DerivedObject]) -> None:
        self._inner = inner
        self._rewrite = rewrite

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)

    async def get_objects(self, scope_key: str, object_ids: Sequence[str]) -> Any:
        found = await self._inner.get_objects(scope_key, object_ids)
        return {key: self._rewrite(value) for key, value in found.items()}


class _Store:
    def __init__(self, inner: Any, rewrite: Callable[[DerivedObject], DerivedObject]) -> None:
        self._inner = inner
        self._rewrite = rewrite

    @contextlib.asynccontextmanager
    async def transaction(self, scope: Scope, *, write: bool) -> AsyncGenerator[_Tx]:
        async with self._inner.transaction(scope, write=write) as tx:
            yield _Tx(tx, self._rewrite)


async def test_object_of_another_scope_is_an_isolation_error() -> None:
    async with opened() as engine:
        await engine.ingest([text_doc("d1", "python asyncio")], scope=ACME)
        [obj] = await _objects(engine, "d1")
        store = _Store(runtime(engine).store, lambda o: o.model_copy(update={"scope": GLOBEX}))
        with pytest.raises(TenantIsolationError) as caught:
            await _build(engine, {KEYWORD: [_hit(obj)]}, seq=await _seq(engine), store=store)
    assert caught.value.details == {"object_id": obj.object_id}


def _entity_doc(doc_id: str, text: str, team: str, *entities: str) -> DocumentInput:
    return DocumentInput(
        _id=doc_id,
        metadata={"team": team},
        representations=(
            TextRepresentation(text=text),
            GraphRepresentation(
                representation_id="g",
                entities=tuple(
                    GraphEntityInput(key=name.lower(), name=name, entity_type="thing")
                    for name in entities
                ),
            ),
        ),
    )


GRAPH_DOCS = [
    _entity_doc("d1", "python one", "red", "Python", "Rust"),
    _entity_doc("d2", "python two", "blue", "Python"),
]


@pytest.mark.graph
async def test_graph_aggregates_are_recomputed_from_permitted_sources() -> None:
    async with opened(retrieval=GRAPH_CONFIG) as engine:
        await engine.ingest(GRAPH_DOCS, scope=ACME)
        seq = await _seq(engine)
        entities = {o.payload["name"]: o for o in await _graph_objects(engine)}
        python, rust = entities["Python"], entities["Rust"]
        assert len(python.sources) == 2
        hits = {GRAPH: [_hit(python, GRAPH), _hit(rust, GRAPH, rank=2)]}

        full = await _build(engine, hits, seq=seq)
        assert [c.object_id for c in full.candidates] == [python.object_id, rust.object_id]
        assert len(full.candidates[0].sources) == 2

        blue = await _build(engine, hits, seq=seq, filters=Eq(field="team", value="blue"))
        [candidate] = blue.candidates
        assert candidate.object_id == python.object_id
        assert [s.document_id for s in candidate.sources] == ["d2"]
        assert blue.hidden_not_visible == 1

        hidden = await _build(engine, hits, seq=0)
        assert hidden.candidates == []
        assert hidden.hidden_not_visible == 2

        retired = _Store(
            runtime(engine).store, lambda o: o.model_copy(update={"state": ObjectState.ORPHANED})
        )
        gone = await _build(engine, hits, seq=seq, store=retired)
        assert gone.filtered_deleted == 2


@pytest.mark.graph
async def test_graph_sources_of_deleted_and_superseded_documents() -> None:
    async with opened(retrieval=GRAPH_CONFIG) as engine:
        await engine.ingest(GRAPH_DOCS, scope=ACME)
        seq = await _seq(engine)
        entities = {o.payload["name"]: o for o in await _graph_objects(engine)}
        python = entities["Python"]
        await engine.ingest([_entity_doc("d1", "python one again", "red", "Python")], scope=ACME)
        await engine.delete(["d2"], scope=ACME)
        old_sources = python.sources
        store = _Store(
            runtime(engine).store,
            lambda o: (
                o.model_copy(update={"sources": old_sources})
                if o.object_id == python.object_id
                else o
            ),
        )
        hits = {GRAPH: [_hit(python, GRAPH)]}
        # d1 v1 is superseded and d2 is deleted: nothing is permitted
        dropped = await _build(engine, hits, seq=seq, store=store)
        assert dropped.candidates == []
        # with stale versions allowed only the superseded d1 source is permitted, and the
        # aggregate recomputed from current permitted contributions has no support left
        orphaned = await _build(engine, hits, seq=seq, store=store, allow_stale=True)
        assert orphaned.candidates == []
        assert orphaned.filtered_out == 1


@pytest.mark.graph
async def test_graph_stale_source_is_kept_when_whole_support_is_permitted() -> None:
    async with opened(retrieval=GRAPH_CONFIG) as engine:
        await engine.ingest(GRAPH_DOCS[:1], scope=ACME)
        seq = await _seq(engine)
        rust = next(o for o in await _graph_objects(engine) if o.payload["name"] == "Rust")
        await engine.ingest([_entity_doc("d1", "python one again", "red", "Rust")], scope=ACME)
        old = rust.sources
        store = _Store(
            runtime(engine).store,
            lambda o: o.model_copy(update={"sources": old}) if o.object_id == rust.object_id else o,
        )
        batch = await _build(
            engine, {GRAPH: [_hit(rust, GRAPH)]}, seq=seq, store=store, allow_stale=True
        )
    [candidate] = batch.candidates
    assert candidate.flags.stale_version
    assert batch.stale_kept == 1
    assert cast("Any", candidate.payload)["name"] == "Rust"
