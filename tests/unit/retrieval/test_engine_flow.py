"""End-to-end retrieval through the engine with default in-memory components."""

from __future__ import annotations

from retrievalloom import Eq, ResultStatus, RetrievalExecutionConfig, WarningCode
from tests.unit.retrieval.helpers import ACME, GLOBEX, chunks_doc, opened, text_doc

DOCS = [
    text_doc("d1", "Python asyncio event loops schedule coroutines.", lang="en"),
    chunks_doc(
        "d2", "Qdrant stores dense vectors.", "BM25 ranks sparse tokens for asyncio.", lang="de"
    ),
]


async def test_retrieve_returns_evidence_with_provenance() -> None:
    async with opened() as engine:
        await engine.ingest(DOCS, scope=ACME)
        result = await engine.retrieve(
            "asyncio event loops",
            scope=ACME,
            config=RetrievalExecutionConfig(include_candidates=True),
        )
    assert result.status == ResultStatus.COMPLETE
    assert result.evidence
    top = result.evidence[0]
    assert "event loops" in (top.segments[0].text or "")
    assert top.provenance.hits
    stages = [s.stage for s in top.provenance.stage_trail]
    assert "method_fusion" in stages
    assert "dedup" in stages
    assert result.candidates is not None
    assert set(result.snapshot.generations) == {"index.keyword", "index.fuzzy", "index.sparse"}
    assert result.timings["total"] >= 0


async def test_filter_restricts_documents_and_tenant_isolation() -> None:
    async with opened() as engine:
        await engine.ingest(DOCS, scope=ACME)
        await engine.ingest([text_doc("b1", "asyncio secrets of globex")], scope=GLOBEX)
        result = await engine.retrieve("asyncio", scope=ACME, filters=Eq(field="lang", value="de"))
        other = await engine.retrieve("asyncio", scope=GLOBEX)
    docs = {s.document_id for e in result.evidence for s in e.sources}
    assert docs == {"d2"}
    assert {s.document_id for e in other.evidence for s in e.sources} == {"b1"}
    assert all(w.code != WarningCode.FILTER_POST_APPLIED for w in result.warnings)
