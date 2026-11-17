"""Behavior tests for an application's RetrievalLoom engine.

Copy this file into your tests and point ``make_engine`` at the factory your
application uses, so the tests exercise your real configuration and components.
Keep ``LifecycleConfig(background=False)`` (or call ``run_lifecycle_once``) so
indexing finishes before each assertion. Each test proves one promise:

- a matching query returns ``complete`` evidence that cites its source;
- one tenant never sees another tenant's documents;
- metadata filters and the reserved ``_id`` field restrict results;
- deleted documents disappear at once, and purged ones are erased.

Never weaken an assertion to make a test pass: a ``partial`` status names its
cause in ``result.warnings``.

Requires ``pytest`` and ``pytest-asyncio`` (``pip install "retrievalloom[testing]"``).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
import pytest_asyncio

from retrievalloom import (
    DocumentInput,
    Eq,
    LifecycleConfig,
    RerankingConfig,
    ResultStatus,
    RetrievalConfig,
    RetrievalEngine,
    Scope,
    SourceDescriptor,
    TextRepresentation,
    create_retrieval_engine,
)

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from retrievalloom import RetrievalResult

pytestmark = pytest.mark.asyncio(loop_scope="function")

ACME = Scope(tenant_id="acme")
GLOBEX = Scope(tenant_id="globex")
DOCUMENTS = {
    "vpn": ("Configure the VPN client before you connect to the office network.", "network"),
    "payroll": ("Payroll runs on the last business day of each month.", "finance"),
    "laptop": ("Request a replacement laptop through the IT service portal.", "it"),
}


def make_engine() -> RetrievalEngine:
    """Replace with your application's factory; keep indexing in the foreground."""
    return create_retrieval_engine(
        retrieval=RetrievalConfig(reranking=RerankingConfig(enabled=False)),
        lifecycle=LifecycleConfig(background=False),
    )


def documents() -> list[DocumentInput]:
    return [
        DocumentInput(
            id=doc_id,
            metadata={"team": team},
            source=SourceDescriptor(uri=f"kb://{doc_id}", title=doc_id.title()),
            representations=(TextRepresentation(text=text),),
        )
        for doc_id, (text, team) in DOCUMENTS.items()
    ]


def document_ids(result: RetrievalResult) -> list[str]:
    return [item.sources[0].document_id for item in result.evidence]


@pytest_asyncio.fixture(loop_scope="function")
async def engine() -> AsyncIterator[RetrievalEngine]:
    async with make_engine() as engine:
        report = await engine.ingest(documents(), scope=ACME)
        assert not report.failed, report.failed
        yield engine


async def test_matching_query_returns_complete_cited_evidence(engine: RetrievalEngine) -> None:
    result = await engine.retrieve("connect to the VPN", scope=ACME)
    assert result.status is ResultStatus.COMPLETE, [w.code for w in result.warnings]
    assert document_ids(result)[0] == "vpn"
    source = result.evidence[0].sources[0]
    assert source.source_uri == "kb://vpn"
    assert source.document_version == 1


async def test_tenants_are_isolated(engine: RetrievalEngine) -> None:
    other = await engine.retrieve("connect to the VPN", scope=GLOBEX)
    assert other.status is ResultStatus.EMPTY
    assert document_ids(other) == []


async def test_filters_restrict_results(engine: RetrievalEngine) -> None:
    finance = await engine.retrieve(
        "payroll month network", scope=ACME, filters=Eq(field="team", value="finance")
    )
    assert document_ids(finance) == ["payroll"]
    by_id = await engine.retrieve("VPN laptop", scope=ACME, filters=Eq(field="_id", value="laptop"))
    assert document_ids(by_id) == ["laptop"]


async def test_deleted_documents_disappear(engine: RetrievalEngine) -> None:
    report = await engine.delete(["vpn"], scope=ACME)
    assert report.deleted == ("vpn",)
    result = await engine.retrieve("connect to the VPN", scope=ACME)
    assert "vpn" not in document_ids(result)


async def test_purge_erases_documents(engine: RetrievalEngine) -> None:
    report = await engine.purge(["payroll", "unknown"], scope=ACME)
    assert report.purged == ("payroll",)
    assert report.not_found == ("unknown",)
    status = await engine.status(scope=ACME)
    assert status.documents_active == len(DOCUMENTS) - 1
