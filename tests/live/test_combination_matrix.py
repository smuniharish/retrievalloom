"""Live backend combination matrix: one scenario, every real backend combination.

Axes: metadata store (SQLite file | PostgreSQL), vector store (in-memory Qdrant | Qdrant
server | pgvector example adapter), graph store (none | Ladybug | Neo4j example adapter)
and the structure channel (off | TreeSearch). Every cell ingests the same two-tenant corpus
and checks retrieval per channel, tenant isolation, versioned updates, delete, physical
purge (metadata rows and backend objects), a restart on the same durable stores, health
and coverage. A smaller ``models`` slice repeats the scenario with the real bge-m3 dense
and learned-sparse adapters and the bge-reranker-v2-m3 cross-encoder.

Services are configured through ``RL_TEST_POSTGRES_DSN``, ``RL_TEST_QDRANT_URL``,
``RL_TEST_PGVECTOR_DSN``, ``RL_TEST_NEO4J_URL`` (plus ``RL_TEST_NEO4J_USER`` and
``RL_TEST_NEO4J_PASSWORD``) and ``RL_TEST_MODELS_DIR``; cells whose services are not
configured are skipped with the missing variables in the reason.
"""

from __future__ import annotations

import itertools
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import pytest

from retrievalloom import (
    ChunkInput,
    ChunksRepresentation,
    Components,
    DocumentInput,
    GraphEntityInput,
    GraphRelationshipInput,
    GraphRepresentation,
    LifecycleConfig,
    RetrievalConfig,
    Scope,
    StructureNodeInput,
    StructureRepresentation,
)
from retrievalloom.config import ChannelsConfig, GraphChannelConfig, StructureChannelConfig
from retrievalloom.domain.records import Channel, GenerationState, TaskState
from retrievalloom.domain.reports import HealthStatus, LifecycleTaskRef
from retrievalloom.domain.retrieval import ResultStatus
from retrievalloom.lifecycle.canonical import document_uid
from tests.fakes import HashEmbedding
from tests.live.combo_backends import Combo, ComboRun, Graph, Meta, Vector
from tests.live.conftest import QDRANT_URL, running_engine

if TYPE_CHECKING:
    from pathlib import Path

    from retrievalloom import RetrievalEngine, RetrievalResult
    from retrievalloom.contracts.models import EmbeddingModelABC


# corpus ---------------------------------------------------------------------------------
@dataclass(frozen=True)
class Doc:
    """A document with chunks, a structure tree and pre-extracted graph facts."""

    id: str
    chunks: tuple[str, ...]
    keywords: tuple[str, ...]
    entities: tuple[tuple[str, str, str], ...]  # key, name, type
    relationships: tuple[tuple[str, str, str], ...]  # source key, target key, type
    title: str
    sections: tuple[tuple[str, str], ...]  # title, text

    def to_input(self) -> DocumentInput:
        return DocumentInput(
            _id=self.id,
            metadata={"keywords": list(self.keywords)},
            representations=(
                ChunksRepresentation(
                    representation_id="chunks",
                    chunks=tuple(ChunkInput(text=text) for text in self.chunks),
                ),
                StructureRepresentation(
                    representation_id="tree",
                    root=StructureNodeInput(
                        title=self.title,
                        children=tuple(
                            StructureNodeInput(title=title, text=text)
                            for title, text in self.sections
                        ),
                    ),
                ),
                GraphRepresentation(
                    representation_id="graph",
                    entities=tuple(
                        GraphEntityInput(key=key, name=name, entity_type=kind)
                        for key, name, kind in self.entities
                    ),
                    relationships=tuple(
                        GraphRelationshipInput(source_key=s, target_key=t, relation_type=r)
                        for s, t, r in self.relationships
                    ),
                ),
            ),
        )


ORBIT = Doc(
    id="orbit",
    chunks=(
        "The Kepler telescope measures exoplanet transits around distant stars.",
        "Kepler photometry records tiny dips in stellar brightness.",
    ),
    keywords=("Kepler",),
    entities=(("kepler", "Kepler", "instrument"), ("exo", "Exoplanet", "object")),
    relationships=(("kepler", "exo", "detects"),),
    title="Kepler handbook",
    sections=(("Kepler transit photometry", "Kepler exoplanet transits dim their stars."),),
)
ORBIT_V2 = Doc(
    id="orbit",
    chunks=(
        "The TESS satellite surveys bright nearby stars for transiting planets.",
        "TESS sectors cover the sky in long observing strips.",
    ),
    keywords=("TESS",),
    entities=(("tess", "TESS", "instrument"), ("planet", "Planet", "object")),
    relationships=(("tess", "planet", "surveys"),),
    title="TESS handbook",
    sections=(("TESS sector survey", "TESS satellite sectors survey transiting planets."),),
)
BREWING = Doc(
    id="brewing",
    chunks=(
        "Espresso extraction depends on grind size and water temperature.",
        "A burr grinder keeps espresso grind size consistent.",
    ),
    keywords=("espresso",),
    entities=(("espresso", "Espresso", "drink"), ("grinder", "Grinder", "tool")),
    relationships=(("grinder", "espresso", "prepares"),),
    title="Espresso manual",
    sections=(("Espresso extraction", "Espresso grind size controls extraction."),),
)
RUST = Doc(
    id="rust",
    chunks=(
        "Rust ownership rules prevent data races at compile time.",
        "The borrow checker enforces Rust ownership and lifetimes.",
    ),
    keywords=("Rust",),
    entities=(("rust", "Rust", "language"), ("borrowck", "Borrowck", "tool")),
    relationships=(("borrowck", "rust", "checks"),),
    title="Rust guide",
    sections=(("Rust ownership", "Rust ownership rules and the borrow checker."),),
)
LEDGER = Doc(
    id="ledger",
    chunks=(
        "Ledger reconciliation matches invoices against bank statements.",
        "Auditors review ledger reconciliation every quarter.",
    ),
    keywords=("ledger",),
    entities=(("ledger", "Ledger", "record"), ("invoice", "Invoice", "record")),
    relationships=(("invoice", "ledger", "posted_to"),),
    title="Ledger procedures",
    sections=(("Ledger reconciliation", "Ledger reconciliation matches invoices."),),
)
# Same document id as tenant A's ``orbit`` but different content: ids must never collide.
GLOBEX_ORBIT = Doc(
    id="orbit",
    chunks=("Glacier ice cores preserve ancient climate records.",),
    keywords=("glacier",),
    entities=(("glacier", "Glacier", "landform"),),
    relationships=(),
    title="Glacier field notes",
    sections=(("Glacier ice cores", "Glacier ice cores hold climate records."),),
)
TENANT_A = (ORBIT, BREWING, RUST)
TENANT_B = (LEDGER, GLOBEX_ORBIT)
# Query -> expected top document; every query names a keyword and a graph entity.
QUERIES_A = {
    "Kepler telescope exoplanet transits": "orbit",
    "espresso grind size extraction": "brewing",
    "Rust ownership borrow checker": "rust",
}
QUERIES_B = {
    "ledger reconciliation invoices": "ledger",
    "glacier ice cores climate": "orbit",
}
A_ONLY_WORDS = ("Kepler", "Espresso", "espresso", "Rust", "TESS")
B_ONLY_WORDS = ("Ledger", "ledger", "Glacier", "glacier")


# matrix ---------------------------------------------------------------------------------
def _matrix() -> list[Any]:
    return [
        pytest.param(combo, id=combo.id, marks=combo.marks())
        for combo in (
            Combo(meta, vector, graph, structure)
            for meta, vector, graph, structure in itertools.product(
                Meta, Vector, Graph, (False, True)
            )
        )
    ]


MODEL_COMBOS = (
    Combo(Meta.SQLITE, Vector.QDRANT_MEMORY, Graph.NONE, structure=False),
    Combo(Meta.SQLITE, Vector.QDRANT_SERVER, Graph.LADYBUG, structure=True),
    Combo(Meta.POSTGRES, Vector.PGVECTOR, Graph.NEO4J, structure=True),
)


def _model_matrix() -> list[Any]:
    return [
        pytest.param(combo, id=combo.id, marks=combo.marks(models=True)) for combo in MODEL_COMBOS
    ]


# engine wiring --------------------------------------------------------------------------
@dataclass
class Parts:
    """The application-owned components of one engine incarnation."""

    components: Components
    embedding: EmbeddingModelABC


class Models:
    """Factory for the real-model components of the models slice."""

    def __init__(self, models_dir: Path, qdrant_url: str) -> None:
        self.models_dir = models_dir
        self.qdrant_url = qdrant_url

    def components(self, run: ComboRun) -> dict[str, Any]:
        from examples.adapters import BgeCrossEncoderReranker, BgeM3Embedding, BgeM3SparseIndex

        bge = self.models_dir / "bge-m3"
        return {
            "embedding": BgeM3Embedding(bge),
            # Under the run's prefix, so ComboRun drops it with the other collections.
            "sparse_index": BgeM3SparseIndex(
                bge, url=self.qdrant_url, collection_prefix=f"{run.qdrant_prefix}_sparse"
            ),
            "reranker": BgeCrossEncoderReranker(self.models_dir / "bge-reranker-v2-m3"),
        }


def _parts(run: ComboRun, models: Models | None) -> Parts:
    extra: dict[str, Any] = (
        models.components(run) if models else {"embedding": HashEmbedding(dimensions=256)}
    )
    components = Components(
        metadata_store=run.metadata_store(),
        vector_store=run.vector_store(),
        graph_store=run.graph_store(),
        structure_engine=run.structure_engine(),
        **extra,
    )
    embedding = components.embedding
    assert embedding is not None
    return Parts(components=components, embedding=embedding)


def _retrieval(combo: Combo) -> RetrievalConfig:
    return RetrievalConfig(
        channels=ChannelsConfig(
            graph=GraphChannelConfig(enabled=combo.graph is not Graph.NONE),
            structure=StructureChannelConfig(enabled=combo.structure),
        )
    )


LIFECYCLE = LifecycleConfig(background=True, wait_for_rebuild=True, poll_interval=0.5)


# assertions -----------------------------------------------------------------------------
def _sources(result: RetrievalResult) -> list[set[str]]:
    return [{source.document_id for source in item.sources} for item in result.evidence]


def _text(result: RetrievalResult) -> str:
    return " ".join(
        segment.text or "" for item in result.evidence for segment in item.segments
    ) + " ".join(item.display_text or "" for item in result.evidence)


def _methods_for(result: RetrievalResult, document_id: str) -> set[tuple[Channel, str]]:
    return {
        (hit.method.channel, hit.method.method)
        for item in result.evidence
        if document_id in {source.document_id for source in item.sources}
        for hit in item.provenance.hits
    }


async def _retrieve(engine: RetrievalEngine, query: str, scope: Scope) -> RetrievalResult:
    result = await engine.retrieve(query, scope=scope, top_k=10)
    assert result.status is ResultStatus.COMPLETE, (query, result.warnings)
    assert result.warnings == (), (query, result.warnings)
    for item in result.evidence:
        assert item.scope == scope
        assert all(source.scope == scope for source in item.sources)
    return result


async def _assert_top(
    engine: RetrievalEngine, combo: Combo, queries: dict[str, str], scope: Scope
) -> None:
    """Known queries rank their document first and every enabled channel contributes."""
    for query, expected in queries.items():
        result = await _retrieve(engine, query, scope)
        assert result.evidence, query
        assert expected in _sources(result)[0], (query, _sources(result))
        methods = _methods_for(result, expected)
        index_methods = {method for channel, method in methods if channel is Channel.INDEX}
        assert {"dense", "sparse", "keyword"} <= index_methods, (query, methods)
        channels = {channel for channel, _ in methods}
        assert (Channel.GRAPH in channels) == (combo.graph is not Graph.NONE), (query, methods)
        assert (Channel.STRUCTURE in channels) == combo.structure, (query, methods)


async def _assert_isolated(engine: RetrievalEngine, a: Scope, b: Scope) -> None:
    """Neither tenant ever sees the other's words, whatever it asks."""
    queries = [*QUERIES_A, *QUERIES_B, "TESS satellite transiting planets"]
    for scope, foreign in ((a, B_ONLY_WORDS), (b, A_ONLY_WORDS)):
        for query in queries:
            text = _text(await _retrieve(engine, query, scope))
            assert not [word for word in foreign if word in text], (scope, query, text)


async def _active(engine: RetrievalEngine, scope: Scope, method: str) -> str:
    status = await engine.status(scope=scope)
    (generation,) = [
        g.generation_id
        for g in status.generations
        if g.method == method and g.state is GenerationState.ACTIVE
    ]
    return generation


async def _assert_ready(
    engine: RetrievalEngine, combo: Combo, scope: Scope, docs: int, deleted: int = 0
) -> None:
    health = await engine.health()
    assert health.status is HealthStatus.HEALTHY, health
    status = await engine.status(scope=scope)
    assert (status.documents_active, status.documents_deleted) == (docs, deleted)
    expected = {"index.keyword", "index.fuzzy", "index.sparse", "index.dense"}
    if combo.graph is not Graph.NONE:
        expected.add("graph.graph")
    if combo.structure:
        expected.add("structure.structure")
    active = {g.method for g in status.generations if g.state is GenerationState.ACTIVE}
    assert expected <= active, status.generations
    assert status.coverage, status
    # Every unit covers each active document; tombstones stay "deleted" until purged.
    expected_states = {"ready": docs} | ({"deleted": deleted} if deleted else {})
    for unit, states in status.coverage.items():
        assert states == expected_states, (unit, status.coverage)


async def _purge_and_verify(
    engine: RetrievalEngine, parts: Parts, combo: Combo, scope: Scope
) -> None:
    """Purge ``rust``: metadata rows and backend objects are physically gone."""
    store = parts.components.metadata_store
    vectors = parts.components.vector_store
    graph = parts.components.graph_store
    structure = parts.components.structure_engine
    assert store is not None
    assert vectors is not None
    uid = document_uid(scope, RUST.id)
    async with store.transaction(scope, write=False) as tx:
        objects = {obj.object_id for obj in await tx.list_document_objects(scope.key, uid)}
    assert objects
    query = "Rust ownership borrow checker"
    vector = await parts.embedding.embed_query(query)

    async def vector_ids() -> set[str]:
        generation = await _active(engine, scope, "index.dense")
        hits = await vectors.search(scope, generation, vector, 100, None)
        return {hit.object_ref.object_id for hit in hits}

    async def entity_ids() -> set[str]:
        assert graph is not None
        generation = await _active(engine, scope, "graph.graph")
        hits = await graph.find_entities(scope, generation, ["Rust", "Borrowck"], 10)
        return {hit.object_ref.object_id for hit in hits}

    async def structure_ids() -> set[str]:
        assert structure is not None
        generation = await _active(engine, scope, "structure.structure")
        hits = await structure.search(scope, generation, query, 100, None)
        return {hit.object_ref.object_id for hit in hits}

    assert await vector_ids() & objects
    if graph is not None:
        assert len(await entity_ids()) == 2
    if structure is not None:
        assert await structure_ids() & objects

    report = await engine.purge([RUST.id], scope=scope)
    assert report.purged == (RUST.id,)
    for task_id in report.task_ids:
        async with store.transaction(scope, write=False) as tx:
            task = await tx.get_task(task_id)
        assert task is not None
        ref = LifecycleTaskRef(task_id=task_id, scope=scope, kind=task.kind)
        assert (await engine.wait_for(ref, timeout=120)).state is TaskState.SUCCEEDED

    async with store.transaction(scope, write=False) as tx:
        assert await tx.get_document(scope, RUST.id) is None
        assert list(await tx.get_versions(uid)) == []
        assert list(await tx.list_document_objects(scope.key, uid)) == []
        assert list(await tx.list_document_contributions(scope.key, uid)) == []
        assert list(await tx.get_lineage(scope.key, uid)) == []
    assert not (await vector_ids() & objects)
    if graph is not None:
        assert await entity_ids() == set()
    if structure is not None:
        assert not (await structure_ids() & objects)
    result = await _retrieve(engine, query, scope)
    assert all(RUST.id not in sources for sources in _sources(result))
    assert "Rust" not in _text(result)
    assert (await engine.purge([RUST.id], scope=scope)).not_found == (RUST.id,)


# scenario -------------------------------------------------------------------------------
async def _scenario(combo: Combo, tmp_path: Path, models: Models | None = None) -> None:
    async with ComboRun(combo, tmp_path) as run:
        tenant_a = Scope(tenant_id=f"acme-{run.suffix}", namespace="matrix")
        tenant_b = Scope(tenant_id=f"globex-{run.suffix}", namespace="matrix")
        retrieval = _retrieval(combo)

        parts = _parts(run, models)
        async with running_engine(
            parts.components, tmp_path, retrieval=retrieval, lifecycle=LIFECYCLE
        ) as engine:
            # 1. ingest
            for scope, docs in ((tenant_a, TENANT_A), (tenant_b, TENANT_B)):
                report = await engine.ingest([doc.to_input() for doc in docs], scope=scope)
                assert not report.warnings, report.warnings
                assert [item.outcome.value for item in report.documents] == ["created"] * len(docs)
            # 2. retrieval per channel
            await _assert_top(engine, combo, QUERIES_A, tenant_a)
            await _assert_top(engine, combo, QUERIES_B, tenant_b)
            # 3. tenant isolation
            await _assert_isolated(engine, tenant_a, tenant_b)
            # 4. update -> new version, new text only
            report = await engine.ingest([ORBIT_V2.to_input()], scope=tenant_a)
            assert not report.warnings
            (updated,) = report.documents
            assert (updated.outcome.value, updated.version) == ("updated", 2)
            await _assert_updated(engine, tenant_a)
            # 5. delete hides, purge erases
            deleted = await engine.delete([BREWING.id], scope=tenant_a)
            assert deleted.deleted == (BREWING.id,)
            await _assert_gone(engine, tenant_a, BREWING.id, "espresso grind size extraction")
            await _purge_and_verify(engine, parts, combo, tenant_a)
            # 7. health and coverage
            await _assert_ready(engine, combo, tenant_a, docs=1, deleted=1)
            await _assert_ready(engine, combo, tenant_b, docs=2)

        # 6. restart on the same durable stores
        restarted_parts = _parts(run, models)
        async with running_engine(
            restarted_parts.components,
            tmp_path,
            retrieval=retrieval,
            lifecycle=LIFECYCLE,
        ) as restarted:
            await _assert_ready(restarted, combo, tenant_a, docs=1, deleted=1)
            await _assert_ready(restarted, combo, tenant_b, docs=2)
            await _assert_top(restarted, combo, QUERIES_B, tenant_b)
            await _assert_updated(restarted, tenant_a)
            await _assert_gone(restarted, tenant_a, BREWING.id, "espresso grind size extraction")
            await _assert_gone(restarted, tenant_a, RUST.id, "Rust ownership borrow checker")
            await _assert_isolated(restarted, tenant_a, tenant_b)
            if models is not None:
                result = await _retrieve(restarted, "TESS satellite transiting planets", tenant_a)
                assert all(item.scores.rerank is not None for item in result.evidence)


async def _assert_updated(engine: RetrievalEngine, scope: Scope) -> None:
    result = await _retrieve(engine, "TESS satellite transiting planets", scope)
    assert "orbit" in _sources(result)[0]
    assert "TESS" in _text(result)
    for query in ("Kepler telescope exoplanet transits", "TESS satellite transiting planets"):
        assert "Kepler" not in _text(await _retrieve(engine, query, scope)), query


async def _assert_gone(engine: RetrievalEngine, scope: Scope, document_id: str, query: str) -> None:
    result = await _retrieve(engine, query, scope)
    assert all(document_id not in sources for sources in _sources(result)), _sources(result)


# tests ----------------------------------------------------------------------------------
@pytest.mark.parametrize("combo", _matrix())
async def test_combination(combo: Combo, tmp_path: Path) -> None:
    """The full scenario on one backend combination with the hashing embedding."""
    await _scenario(combo, tmp_path)


@pytest.mark.parametrize("combo", _model_matrix())
@pytest.mark.filterwarnings("ignore::DeprecationWarning")
@pytest.mark.filterwarnings("ignore::FutureWarning")
@pytest.mark.filterwarnings("ignore::UserWarning")
async def test_combination_with_models(combo: Combo, models_dir: Path, tmp_path: Path) -> None:
    """The scenario with bge-m3 dense + learned sparse and the bge cross-encoder reranker."""
    assert QDRANT_URL is not None
    await _scenario(combo, tmp_path, Models(models_dir, QDRANT_URL))
