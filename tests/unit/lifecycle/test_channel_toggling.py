"""Units enabled, disabled and re-enabled across restarts on one metadata store.

Each "restart" is a new harness on the same store; durable backends (``durable=True``)
are shared between harnesses like a Qdrant collection or graph database would be.
"""

from __future__ import annotations

from datetime import timedelta
from typing import TYPE_CHECKING, Any

import pytest

from retrievalloom.config import LifecycleConfig
from retrievalloom.contracts.models import GraphExtraction, GraphExtractorABC
from retrievalloom.domain.inputs import (
    DocumentInput,
    GraphEntityInput,
    GraphRepresentation,
    StructureNodeInput,
    StructureRepresentation,
    TextRepresentation,
)
from retrievalloom.domain.records import Channel, CoverageState, ObjectKind, ObjectState
from retrievalloom.errors import BackendUnavailableError
from retrievalloom.graph.normalizer import IdentityGraphNormalizer
from retrievalloom.lifecycle.canonical import document_uid
from retrievalloom.lifecycle.derivation import is_graph_marker
from retrievalloom.lifecycle.indexer import PURGE_LOG_SEQ, _purge_log_seq
from retrievalloom.lifecycle.ingestion import GraphDerivation
from tests.fakes import CapitalizedExtractor
from tests.unit.lifecycle.helpers import (
    SCOPE,
    DictIndex,
    Harness,
    build,
    index_unit,
    text_doc,
)

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from retrievalloom.contracts.component import ComponentInfo
    from retrievalloom.contracts.models import ExtractionSource
    from retrievalloom.lifecycle.units import IndexUnit
    from retrievalloom.storage.sqlite import SQLiteMetadataStore


def uid(document_id: str) -> str:
    return document_uid(SCOPE, document_id)


def lifecycle(**overrides: Any) -> LifecycleConfig:
    return LifecycleConfig(background=False, **overrides)


async def published(h: Harness, unit: IndexUnit) -> set[str]:
    """Texts of the published generation of ``unit``."""
    async with h.store.transaction(SCOPE, write=False) as tx:
        pointer = await tx.get_pointer(unit.key(SCOPE))
    assert pointer is not None
    backend = unit.backend
    assert isinstance(backend, DictIndex)
    return {item.text for item in backend.live(SCOPE, pointer.generation_id).values()}


async def coverage(h: Harness, unit: IndexUnit, document_id: str) -> CoverageState:
    async with h.store.transaction(SCOPE, write=False) as tx:
        return (await tx.get_coverage(unit.key(SCOPE), [uid(document_id)]))[uid(document_id)].state


class ScriptedExtractor(GraphExtractorABC):
    """:class:`CapitalizedExtractor` that can fail or run a hook first."""

    def __init__(self) -> None:
        self.inner = CapitalizedExtractor()
        self.error: BaseException | None = None
        self.hook: Callable[[], Awaitable[None]] | None = None
        self.documents: list[str] = []

    @property
    def info(self) -> ComponentInfo:
        return self.inner.info

    async def extract(self, source: ExtractionSource) -> GraphExtraction:
        self.documents.append(source.document_id)
        if self.hook is not None:
            hook, self.hook = self.hook, None
            await hook()
        if self.error is not None:
            raise self.error
        return await self.inner.extract(source)


def with_graph(
    store: SQLiteMetadataStore,
    graph: DictIndex,
    extractor: GraphExtractorABC | None = None,
    **config: Any,
) -> Harness:
    """Engine with an index unit and the graph unit (graph channel enabled)."""
    units = [index_unit(DictIndex()), index_unit(graph, method="graph", channel=Channel.GRAPH)]
    derivation = GraphDerivation(IdentityGraphNormalizer(), extractor or CapitalizedExtractor())
    return build(store, units, lifecycle=lifecycle(**config), graph=derivation)


def index_only(store: SQLiteMetadataStore, *durable: DictIndex, **config: Any) -> Harness:
    """Engine with the graph channel disabled (and optional durable dense units)."""
    units = [index_unit(DictIndex())]
    units.extend(index_unit(backend, method="dense") for backend in durable)
    return build(store, units, lifecycle=lifecycle(**config))


async def run_all(h: Harness) -> None:
    for unit in h.units:
        await h.indexer.run(SCOPE, unit)


# graph catch-up -----------------------------------------------------------------------------
async def test_enabling_graph_derives_documents_ingested_while_it_was_off(
    store: SQLiteMetadataStore,
) -> None:
    off = index_only(store)
    provided = DocumentInput.model_validate(
        {
            "_id": "kg",
            "representations": (
                TextRepresentation(text="no capitals here"),
                GraphRepresentation(
                    representation_id="kg", entities=(GraphEntityInput(name="Carol"),)
                ),
            ),
        }
    )
    tree = DocumentInput.model_validate(
        {
            "_id": "tree",
            "representations": (StructureRepresentation(root=StructureNodeInput(text="Dave")),),
        }
    )
    await off.ingestion.ingest(
        [text_doc("d1", "Alice met Bob"), text_doc("d2", "Bob saw Eve"), provided, tree], SCOPE
    )
    extractor = ScriptedExtractor()
    on = with_graph(store, DictIndex(durable=True), extractor, batch_size=2)
    graph = on.units[1]
    await on.indexer.run(SCOPE, graph)
    assert await published(on, graph) == {
        "Alice",
        "Bob",
        "Eve",
        "Carol",
        "Alice mentions Bob",
        "Bob mentions Eve",
    }
    # The provided graph is replayed; documents without an index representation have none.
    assert sorted(extractor.documents) == ["d1", "d2"]
    assert await coverage(on, graph, "d1") == CoverageState.READY
    assert await coverage(on, graph, "tree") == CoverageState.NOT_APPLICABLE
    assert [f["documents"] for _, e, f in on.logger.records if e == "graph_caught_up"] == [2, 2]
    # Already derived: nothing to do on the next pass.
    assert await on.ingestion.derive_graph(SCOPE, [uid("d1"), uid("kg")]) == 0
    assert await on.ingestion.derive_graph(SCOPE, []) == 0
    assert await off.ingestion.derive_graph(SCOPE, [uid("d1")]) == 0  # graph not configured
    assert not await on.indexer.needs_work(SCOPE, graph)


async def test_updates_made_while_graph_was_off_replace_the_old_graph(
    store: SQLiteMetadataStore,
) -> None:
    graph = DictIndex(durable=True)
    first = with_graph(store, graph)
    await first.ingestion.ingest([text_doc("d1", "Alice met Bob")], SCOPE)
    await run_all(first)
    off = index_only(store)
    await off.ingestion.ingest([text_doc("d1", "Zed met Yan")], SCOPE)
    again = with_graph(store, graph)
    await again.indexer.run(SCOPE, again.units[1])
    assert await published(again, again.units[1]) == {"Zed", "Yan", "Zed mentions Yan"}


async def test_a_non_retryable_extraction_failure_is_recorded_as_an_empty_graph(
    store: SQLiteMetadataStore,
) -> None:
    await index_only(store).ingestion.ingest(
        [text_doc("d1", "Alice met Bob"), text_doc("d2", "Carol")], SCOPE
    )
    extractor = ScriptedExtractor()
    extractor.error = ValueError("bad model output")
    on = with_graph(store, DictIndex(durable=True), extractor)
    graph = on.units[1]
    await on.indexer.run(SCOPE, graph)
    assert await published(on, graph) == set()
    failures = [f for _, e, f in on.logger.records if e == "graph_derivation_failed"]
    assert {f["error_code"] for f in failures} == {"unexpected_error"}
    assert len(failures) == 2
    assert await coverage(on, graph, "d1") == CoverageState.NOT_APPLICABLE
    extractor.error = None
    assert await on.ingestion.derive_graph(SCOPE, [uid("d1")]) == 0  # not retried


async def test_a_retryable_extraction_failure_leaves_the_graph_pending(
    store: SQLiteMetadataStore,
) -> None:
    await index_only(store).ingestion.ingest([text_doc("d1", "Alice met Bob")], SCOPE)
    extractor = ScriptedExtractor()
    extractor.error = BackendUnavailableError("extractor down")
    on = with_graph(store, DictIndex(durable=True), extractor)
    graph = on.units[1]
    with pytest.raises(BackendUnavailableError):
        await on.indexer.run(SCOPE, graph)
    assert await coverage(on, graph, "d1") == CoverageState.PENDING
    extractor.error = None
    await on.indexer.run(SCOPE, graph)
    assert await published(on, graph) == {"Alice", "Bob", "Alice mentions Bob"}


async def test_documents_changed_during_derivation_are_derived_by_the_next_pass(
    store: SQLiteMetadataStore,
) -> None:
    off = index_only(store)
    await off.ingestion.ingest([text_doc("d1", "Alice met Bob")], SCOPE)
    extractor = ScriptedExtractor()

    async def change() -> None:
        await off.ingestion.ingest([text_doc("d1", "Zed met Yan")], SCOPE)

    extractor.hook = change
    on = with_graph(store, DictIndex(durable=True), extractor)
    assert await on.ingestion.derive_graph(SCOPE, [uid("d1")]) == 0
    assert await on.ingestion.derive_graph(SCOPE, [uid("d1")]) == 1
    await on.indexer.run(SCOPE, on.units[1])
    assert await published(on, on.units[1]) == {"Zed", "Yan", "Zed mentions Yan"}


async def test_delete_while_graph_is_off_retracts_graph_contributions(
    store: SQLiteMetadataStore,
) -> None:
    graph = DictIndex(durable=True)
    on = with_graph(store, graph)
    await on.ingestion.ingest([text_doc("d1", "Alice met Bob")], SCOPE)
    await run_all(on)
    off = index_only(store)
    await off.ingestion.delete(["d1"], SCOPE)
    async with store.transaction(SCOPE, write=False) as tx:
        assert list(await tx.list_document_contributions(SCOPE.key, uid("d1"))) == []
        entities = await tx.list_current_objects(SCOPE.key, [ObjectKind.GRAPH_ENTITY])
    assert {obj.state for obj in entities} <= {ObjectState.SUPERSEDED, ObjectState.DELETED}
    again = with_graph(store, graph)
    await again.indexer.run(SCOPE, again.units[1])
    assert await published(again, again.units[1]) == set()


async def test_graph_markers_are_not_aggregated(store: SQLiteMetadataStore) -> None:
    h = with_graph(store, DictIndex(durable=True))
    await h.ingestion.ingest([text_doc("d1", "Alice")], SCOPE)
    async with store.transaction(SCOPE, write=False) as tx:
        ids = [c.object_id for c in await tx.list_document_contributions(SCOPE.key, uid("d1"))]
        objects = await tx.get_objects(SCOPE.key, ids)
    assert any(is_graph_marker(oid) for oid in ids)
    assert not any(is_graph_marker(oid) for oid in objects)


# purges while a durable unit is unconfigured --------------------------------------------------
async def test_a_durable_unit_applies_purges_made_while_it_was_unconfigured(
    store: SQLiteMetadataStore,
) -> None:
    dense = DictIndex(durable=True)
    first = index_only(store, dense)
    await first.ingestion.ingest([text_doc("a", "alpha"), text_doc("b", "beta")], SCOPE)
    await run_all(first)
    off = index_only(store, batch_size=1)
    await off.ingestion.delete(["b", "a"], SCOPE)
    await off.eraser.purge(["b", "a"], SCOPE)
    await off.ingestion.ingest([text_doc("c", "gamma")], SCOPE)
    again = index_only(store, dense, batch_size=1)
    unit = again.units[1]
    assert await again.indexer.needs_work(SCOPE, unit)
    await again.indexer.run(SCOPE, unit)
    assert await published(again, unit) == {"gamma"}
    assert ("info", "purge_log_applied") in [(lvl, e) for lvl, e, _ in again.logger.records]
    assert not await again.indexer.needs_work(SCOPE, unit)
    async with store.transaction(SCOPE, write=False) as tx:
        pointer = await tx.get_pointer(unit.key(SCOPE))
        assert pointer is not None
        generation = await tx.get_generation(pointer.generation_id)
    assert generation is not None
    assert generation.stats[PURGE_LOG_SEQ] == 2
    assert _purge_log_seq(generation.model_copy(update={"stats": {}})) == 0


async def test_a_durable_unit_rebuilds_when_the_purge_log_was_pruned(
    store: SQLiteMetadataStore,
) -> None:
    dense = DictIndex(durable=True)
    first = index_only(store, dense)
    await first.ingestion.ingest([text_doc("a", "alpha"), text_doc("b", "beta")], SCOPE)
    await run_all(first)
    off = index_only(store)
    for document_id in ("a", "b"):
        await off.ingestion.delete([document_id], SCOPE)
        await off.eraser.purge([document_id], SCOPE)
    async with store.transaction(SCOPE, write=True) as tx:
        await tx.prune_purge_log(SCOPE.key, before=off.clock() + timedelta(days=1))
    again = index_only(store, dense)
    unit = again.units[1]
    await again.indexer.run(SCOPE, unit)
    assert await published(again, unit) == set()
    assert "generation_build_started" in again.logger.events()


async def test_a_non_incremental_durable_unit_rebuilds_after_purges(
    store: SQLiteMetadataStore,
) -> None:
    graph = DictIndex(durable=True)
    first = with_graph(store, graph)
    await first.ingestion.ingest([text_doc("a", "Alice"), text_doc("b", "Bob")], SCOPE)
    await run_all(first)
    off = index_only(store)
    await off.ingestion.delete(["b"], SCOPE)
    await off.eraser.purge(["b"], SCOPE)
    again = with_graph(store, graph)
    unit = again.units[1]
    assert await again.indexer.needs_work(SCOPE, unit)
    await again.indexer.run(SCOPE, unit)
    assert await published(again, unit) == {"Alice"}
    assert not await again.indexer.needs_work(SCOPE, unit)


async def test_purge_log_progress_recorded_meanwhile_is_kept(store: SQLiteMetadataStore) -> None:
    dense = DictIndex(durable=True)
    first = index_only(store, dense)
    await first.ingestion.ingest([text_doc("a", "alpha"), text_doc("b", "beta")], SCOPE)
    await run_all(first)
    off = index_only(store)
    await off.ingestion.delete(["b"], SCOPE)
    await off.eraser.purge(["b"], SCOPE)
    again = index_only(store, dense)
    unit = again.units[1]

    async def advance() -> None:
        # Another engine applied the log (further) in the meantime.
        async with store.transaction(SCOPE, write=True) as tx:
            pointer = await tx.get_pointer(unit.key(SCOPE))
            assert pointer is not None
            current = await tx.get_generation(pointer.generation_id)
            assert current is not None
            await tx.put_generation(
                current.model_copy(update={"stats": {**current.stats, PURGE_LOG_SEQ: 5}}),
                expected_revision=current.row_revision,
            )

    dense.hooks["remove"] = advance
    await again.indexer.run(SCOPE, unit)
    async with store.transaction(SCOPE, write=False) as tx:
        pointer = await tx.get_pointer(unit.key(SCOPE))
        assert pointer is not None
        generation = await tx.get_generation(pointer.generation_id)
    assert generation is not None
    assert generation.stats[PURGE_LOG_SEQ] == 5
    assert await published(again, unit) == {"alpha"}


# automatic tombstone purge -------------------------------------------------------------------
async def test_auto_purge_ignores_coverage_of_unconfigured_units(
    store: SQLiteMetadataStore,
) -> None:
    dense = DictIndex(durable=True)
    first = index_only(store, dense)
    await first.ingestion.ingest([text_doc("a", "alpha")], SCOPE)
    await run_all(first)
    off = index_only(store)
    await off.ingestion.delete(["a"], SCOPE)
    await run_all(off)  # the dense row stays pending: its unit is not configured
    purged = await off.eraser.purge_expired(
        SCOPE, deleted_before=off.clock() + timedelta(seconds=1), limit=10
    )
    assert purged == 1
    again = index_only(store, dense)
    await again.indexer.run(SCOPE, again.units[1])
    assert await published(again, again.units[1]) == set()
