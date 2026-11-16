"""Lifecycle behaviour through the public engine, including a restart on a durable data dir."""

from __future__ import annotations

from typing import TYPE_CHECKING

from retrievalloom import (
    DocumentInput,
    LifecycleConfig,
    Scope,
    TextRepresentation,
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
