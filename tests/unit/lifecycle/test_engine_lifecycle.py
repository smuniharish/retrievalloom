"""Lifecycle behaviour through the public engine, including a restart on a durable data dir."""

from __future__ import annotations

from typing import TYPE_CHECKING

from retrievalloom import (
    DocumentInput,
    LifecycleConfig,
    PurgeReport,
    ResultStatus,
    Scope,
    TextRepresentation,
    WarningCode,
    create_retrieval_engine,
)
from retrievalloom.config import EngineResources
from retrievalloom.domain.records import TaskState
from tests.support import shared_manager

if TYPE_CHECKING:
    from pathlib import Path

    from retrievalloom.engine import RetrievalEngine

SCOPE = Scope(tenant_id="acme")


def engine(data_dir: Path) -> RetrievalEngine:
    return create_retrieval_engine(
        lifecycle=LifecycleConfig(background=False),
        resources=EngineResources(resource_manager=shared_manager(), data_dir=data_dir),
    )


def doc(doc_id: str, text: str) -> DocumentInput:
    return DocumentInput(_id=doc_id, representations=(TextRepresentation(text=text),))


async def test_ingest_delete_and_tasks_survive_a_restart(tmp_path: Path) -> None:
    async with engine(tmp_path) as first:
        await first.ingest(
            [doc("d1", "asyncio event loops"), doc("d2", "qdrant vectors")], scope=SCOPE
        )
        await first.delete(["d2"], scope=SCOPE)
        before = await first.status(scope=SCOPE)
    assert before.documents_active == 1
    assert before.documents_deleted == 1

    async with engine(tmp_path) as second:
        after = await second.status(scope=SCOPE)
        assert after.documents_active == 1
        assert after.documents_deleted == 1
        backfill = await second.wait_for(await second.backfill(scope=SCOPE), timeout=30)
        assert backfill.state == TaskState.SUCCEEDED
        rebuild = await second.wait_for(
            await second.rebuild(scope=SCOPE, channel="index", method="sparse"), timeout=30
        )
        assert rebuild.state == TaskState.SUCCEEDED
        assert rebuild.error is None
        result = await second.retrieve("asyncio event loops", scope=SCOPE)
        assert {s.document_id for e in result.evidence for s in e.sources} == {"d1"}


async def test_purge_erases_documents_through_the_engine(tmp_path: Path) -> None:
    async with engine(tmp_path) as opened:
        await opened.ingest(
            [doc("d1", "asyncio event loops"), doc("d2", "asyncio tasks")], scope=SCOPE
        )
        report = await opened.purge(["d1", "nope"], scope=SCOPE)
        assert isinstance(report, PurgeReport)
        assert report.purged == ("d1",)
        assert report.not_found == ("nope",)
        status = await opened.status(scope=SCOPE)
        assert status.documents_active == 1
        assert status.documents_deleted == 0
        result = await opened.retrieve("asyncio event loops", scope=SCOPE)
        assert {s.document_id for e in result.evidence for s in e.sources} == {"d2"}


async def test_wait_for_rebuild_serves_complete_results_right_after_a_restart(
    tmp_path: Path,
) -> None:
    lifecycle = LifecycleConfig(background=True, wait_for_rebuild=True, poll_interval=60)

    def background_engine() -> RetrievalEngine:
        return create_retrieval_engine(
            lifecycle=lifecycle,
            resources=EngineResources(resource_manager=shared_manager(), data_dir=tmp_path),
        )

    async with background_engine() as first:
        await first.ingest([doc("d1", "asyncio event loops")], scope=SCOPE)
        await first.wait_for(await first.backfill(scope=SCOPE), timeout=30)
    async with background_engine() as second:
        result = await second.retrieve("asyncio event loops", scope=SCOPE)
        assert result.status == ResultStatus.COMPLETE
        assert not [w for w in result.warnings if w.code == WarningCode.INDEX_MISSING]
        assert {s.document_id for e in result.evidence for s in e.sources} == {"d1"}
