"""Run every script under ``examples/`` and check what it prints.

Offline examples run in the default suite. Live examples are also marked ``live`` and are
skipped unless their services are configured through ``RL_EXAMPLES_<NAME>`` or the test
suite's ``RL_TEST_<NAME>`` variables (``MODELS_DIR``, ``QDRANT_URL``, ``PGVECTOR_DSN``,
``NEO4J_URL``). The scripts run with only the settings a test passes, so an offline run
never touches a configured service.
"""

from __future__ import annotations

import importlib
import importlib.util
import os
import re
import subprocess
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Any

import httpx
import pytest

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from fastapi import FastAPI

ROOT = Path(__file__).resolve().parents[2]
EXAMPLES = ROOT / "examples"


def setting(name: str) -> str | None:
    return os.environ.get(f"RL_EXAMPLES_{name}") or os.environ.get(f"RL_TEST_{name}")


MODELS_DIR = setting("MODELS_DIR")
QDRANT_URL = setting("QDRANT_URL")
PGVECTOR_DSN = setting("PGVECTOR_DSN")
NEO4J_URL = setting("NEO4J_URL")

pytestmark = pytest.mark.examples

requires_graph = pytest.mark.skipif(
    importlib.util.find_spec("ladybug") is None, reason="requires the graph extra"
)
requires_structure = pytest.mark.skipif(
    importlib.util.find_spec("treesearch") is None, reason="requires the structure extra"
)
requires_service = pytest.mark.skipif(
    importlib.util.find_spec("fastapi") is None or importlib.util.find_spec("uvicorn") is None,
    reason="requires the examples dependency group (fastapi, uvicorn)",
)
requires_neo4j_driver = pytest.mark.skipif(
    importlib.util.find_spec("neo4j") is None,
    reason="requires the examples dependency group (neo4j)",
)
requires_opentelemetry = pytest.mark.skipif(
    importlib.util.find_spec("opentelemetry") is None,
    reason="requires the examples dependency group (opentelemetry-sdk)",
)


def needs(*names: str) -> pytest.MarkDecorator:
    missing = [name for name in names if not setting(name)]
    return pytest.mark.skipif(
        bool(missing), reason=f"set RL_TEST_{'/'.join(missing)} to run this example"
    )


def run_example(name: str, timeout: float = 180, **settings: str | None) -> list[str]:
    """Run ``examples/<name>.py`` with only ``settings``; return its stdout lines."""
    env = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith(("RL_EXAMPLES_", "RL_TEST_"))
    }
    env["PYTHONIOENCODING"] = "utf-8"
    env.update({f"RL_EXAMPLES_{key}": value for key, value in settings.items() if value})
    completed = subprocess.run(  # noqa: S603 - fixed interpreter and script path
        [sys.executable, str(EXAMPLES / f"{name}.py")],
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
        encoding="utf-8",
        timeout=timeout,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr
    return completed.stdout.splitlines()


def after(lines: list[str], header: str, count: int) -> list[str]:
    """The ``count`` lines that follow ``header``."""
    start = lines.index(header) + 1
    return lines[start : start + count]


def test_quickstart() -> None:
    lines = run_example("quickstart")
    assert lines[0] == "ingested: {'created': 4}"
    assert lines[1] == "status: complete"
    assert lines[3].startswith("#1 asyncio v1 score=")
    assert "   source:  kb://articles/asyncio (Asyncio)" in lines
    assert "   methods: index.fuzzy, index.keyword, index.sparse" in lines
    assert any(line.startswith("   stages:  ") and "'consolidation'" in line for line in lines)
    assert lines[-1] == "lang=de filter: ['eventloop']"


def test_multi_tenant_saas() -> None:
    lines = run_example("multi_tenant_saas")
    assert after(
        lines, "role-based row filters (acme/handbook, 'holidays expenses salary'):", 3
    ) == [
        "  viewer sees ['holidays']",
        "  editor sees ['expenses', 'holidays']",
        "  admin  sees ['expenses', 'holidays', 'salary-bands']",
    ]
    isolation = after(
        lines, "collection and tenant isolation ('holiday' as acme:admin / globex:admin):", 3
    )
    assert isolation[0].startswith("  acme/handbook: Acme employees")
    assert isolation[1].startswith("  acme/engineering: Engineers on the on-call rota")
    assert isolation[2].startswith("  globex/handbook: Globex staff")
    assert after(lines, "denied operations:", 4) == [
        "  globex admin reads acme: denied retrieve "
        "(principal of tenant 'globex' cannot access this tenant)",
        "  acme viewer ingests: denied ingest (role 'viewer' may not ingest)",
        "  acme editor rebuilds: denied rebuild (role 'editor' may not rebuild)",
        "  anonymous retrieval: denied retrieve (anonymous access is disabled)",
    ]
    assert after(lines, "globex quotas:", 2) == [
        "  ingest 3 documents: rejected {'count': 3, 'max': 2}",
        "  retrieve top_k=5: rejected {'limit': 3}",
    ]
    assert "acme/handbook status (viewer may read it): active=3" in lines
    assert "audit log: 9 record(s) {'ingest': 3, 'retrieval': 6}" in lines
    assert (
        lines[-1] == "  last: event=retrieval tenant=globex principal=globex:admin query='holiday'"
    )


@pytest.mark.structure
@requires_structure
def test_policy_manual() -> None:
    lines = run_example("policy_manual")
    receipt = after(lines, "Q: When do I have to submit my receipt?", 1)[0]
    assert receipt.startswith("  #1 ['index', 'structure'] expense-faq: (whole text)")
    assert "travel-policy: Travel and Expense Policy > Expenses > Submitting receipts" in receipt
    hotel = after(lines, "Q: hotel price limit per night in Europe", 2)
    assert hotel[0] == (
        "  #1 ['structure'] travel-policy: Travel and Expense Policy > Booking travel > Hotels"
    )
    assert "180 euros per night in Europe" in hotel[1]
    flights = after(lines, "Q: Can I fly business class?", 1)[0]
    assert flights.endswith("Travel and Expense Policy > Booking travel > Flights")
    risky = after(lines, "Q: security briefing for risky trips", 1)[0]
    assert risky.endswith("Travel and Expense Policy > Safety > High-risk destinations")
    caps = [line for line in lines if line.startswith("  items from the manual:")]
    assert len(caps) == 4
    assert all(int(line.split()[4]) <= 2 for line in caps)


@pytest.mark.graph
@pytest.mark.structure
@requires_graph
@requires_structure
def test_channel_combinations() -> None:
    lines = run_example("channel_combinations")
    assert after(lines, "1. index only", 1) == ["   complete ['db-failover[index]']"]
    assert after(lines, "2. structure turned on later (caught up while the engine opens)", 3) == [
        "   index.keyword: {'ready': 3}",
        "   structure.structure: {'ready': 3}",
        "   complete ['db-failover[structure]', 'db-failover[index]', 'db-failover[structure]']",
    ]
    (before,) = after(lines, "   before backfill:", 1)
    assert before.startswith("   partial ['db-failover[structure]'"), before
    assert "'index_missing:graph'" in before
    assert after(lines, "   after backfill:", 2) == [
        "   graph.graph: {'ready': 3}",
        "   complete ['db-failover[graph]', 'db-failover[structure]', 'db-failover[index]', "
        "'db-failover[graph]']",
    ]
    combos = after(lines, "4. every combination on the same store", 8)
    for line in combos[:7]:
        enabled, answered = line.split(" answered by ")
        expected = sorted(enabled.strip().split(" + "))
        assert answered == repr(expected), line
    assert combos[7].split() == ["nothing", "enabled", "rejected"]
    for channel in ("index", "structure", "graph"):
        (answer,) = after(lines, f"   {channel}:", 1)
        assert answer.startswith(f'   complete ["db-failover[{channel}] '), answer
    disabled = after(lines, "6. graph off; vpn-access deleted meanwhile", 1)[0]
    assert "graph" not in disabled
    assert "vpn-access" not in disabled
    reenabled = after(lines, "   graph on again (the startup round catches up)", 2)
    assert reenabled[0] == "   graph.graph: {'deleted': 1, 'ready': 2}"
    assert "cache-flush[graph]" in reenabled[1]
    assert "vpn-access" not in reenabled[1]


GRAPH_FIRST_QUESTION = "Q: Who operates Postgres and where is it hosted? (graph channel only)"


def check_knowledge_graph(lines: list[str], store: str) -> None:
    assert lines[0] == f"graph store: {store}"
    assert lines[1] == "graph coverage: {'ready': 4}"
    assert after(lines, GRAPH_FIRST_QUESTION, 1) == [
        "  hops=0 'Postgres' <- ['checkout-architecture', 'incident-2041', 'team-directory']"
    ]
    assert "     path: Postgres -> Team Harbor OPERATES Postgres -> Team Harbor" in lines
    assert "  hops=1 'Postgres IS_HOSTED_IN Frankfurt Region' <- ['checkout-architecture']" in lines
    fused_header = next(line for line in lines if line.endswith("(graph + index fused)"))
    fused = after(lines, fused_header, 6)
    assert any("['graph']" in line for line in fused)
    assert any("['index']" in line and "Frankfurt Region" in line for line in fused)


@pytest.mark.graph
@requires_graph
@requires_neo4j_driver
def test_knowledge_graph_ladybug() -> None:
    check_knowledge_graph(run_example("knowledge_graph"), "ladybug (embedded)")


@pytest.mark.structure
@requires_structure
def test_prechunked_and_multi_representation() -> None:
    lines = run_example("prechunked_and_multi_representation")
    assert (
        "     [body] Manual > Maintenance (chars 43-86, p. 2): "
        "Replace the coolant filter every 500 hours."
    ) in lines
    assert any(
        "channels=['index'] relations=['complementary', 'same_section']" in line for line in lines
    )
    assert "     [outline] Pump manual > Warranty: Coverage period and exclusions." in lines
    assert any("[body] Manual > Warranty (chars 134-185, p. 3)" in line for line in lines)


def test_agxai_provenance() -> None:
    lines = run_example("agxai_provenance")
    assert lines[0] == "evidence record: retrievalloom ingestion {'created': 3}"
    assert lines[1].startswith("evidence record: retrievalloom deletion {'deleted': 1,")
    assert lines[2] == "retrieval: retriever=retrievalloom:support status=complete"
    assert "  query reference = request id: True" in lines
    assert any(line.startswith("  rank 1: reset ") for line in lines)
    assert lines.count("    linked to source uid: True") == 2


@requires_opentelemetry
def test_observability_in_memory() -> None:
    lines = run_example("observability_jaeger")
    marker = next(i for i, line in enumerate(lines) if line.startswith("trace_id="))
    trace_id = re.fullmatch(r"trace_id=([0-9a-f]{32}) request=rl_trace_\w+", lines[marker])
    assert trace_id
    assert lines[marker + 1 :] == [
        "  support.request",
        "    execution.started",
        "    retrievalloom.ingest",
        "    retrievalloom.retrieve",
        "      retrieval.execution",
        "    execution.completed",
        "6 spans kept in memory (set RL_EXAMPLES_OTLP_URL to export)",
    ]
    # Every structlog line emitted inside the request carries the request's trace id.
    answered = next(line for line in lines[:marker] if '"event": "answered"' in line)
    assert f'"trace_id": "{trace_id.group(1)}"' in answered
    assert '"top": "reset"' in answered


@requires_service
def test_http_service_demo() -> None:
    lines = run_example("http_service")
    assert lines[0] == "POST /ingest -> 200 10 documents ['created']"
    assert lines[1] == "POST /retrieve -> 200 status=complete"
    assert lines[2].startswith("    pay-refund v1 ")
    assert "POST /ingest as viewer -> 403 access_denied" in lines
    assert "POST /retrieve as another tenant -> 403 access_denied" in lines
    assert "POST /retrieve top_k=500 -> 422" in lines
    assert "DELETE /documents/pay-refund -> 200 {'deleted': ['pay-refund']}" in lines
    assert "DELETE /documents/pay-refund again -> 404" in lines
    assert "GET /status -> 200 active=9 deleted=1" in lines
    assert "GET /health -> 200 healthy" in lines
    assert lines[-1] == "server stopped, engine closed"


@pytest.fixture
def service_app(monkeypatch: pytest.MonkeyPatch) -> FastAPI:
    monkeypatch.syspath_prepend(str(EXAMPLES))
    module: Any = importlib.import_module("http_service")
    return module.create_app()


@pytest.fixture
async def client(service_app: FastAPI) -> AsyncIterator[httpx.AsyncClient]:
    async with service_app.router.lifespan_context(service_app):
        transport = httpx.ASGITransport(app=service_app)
        async with httpx.AsyncClient(transport=transport, base_url="http://service") as client:
            yield client


@requires_service
async def test_http_service_endpoints(client: httpx.AsyncClient) -> None:
    editor, viewer = {"X-Principal": "acme:editor"}, {"X-Principal": "acme:viewer"}
    scope = {"tenant": "acme", "collection": "help"}
    documents = [
        {
            "id": "pay-refund",
            "title": "Refunding a customer payment",
            "uri": "https://help.acme.example/pay-refund",
            "text": "Open the invoice, choose Refund and enter the amount to refund a payment.",
            "metadata": {"language": "en", "visibility": "public"},
        },
        {
            "id": "pay-internal",
            "text": "Internal refund limits for support agents are set by finance.",
            "metadata": {"language": "en", "visibility": "internal"},
        },
        {
            "id": "pay-erstattung",
            "text": "Um eine Zahlung zu erstatten, öffnen Sie die Rechnung.",
            "metadata": {"language": "de", "visibility": "public"},
        },
    ]

    response = await client.get("/health")
    assert response.status_code == 200
    assert response.json()["status"] == "healthy"

    response = await client.post("/ingest", json={**scope, "documents": documents}, headers=editor)
    assert response.status_code == 200
    assert [d["outcome"] for d in response.json()["documents"]] == ["created"] * 3
    response = await client.post("/ingest", json={**scope, "documents": documents}, headers=editor)
    assert [d["outcome"] for d in response.json()["documents"]] == ["unchanged"] * 3

    query = {**scope, "query": "refund a payment", "top_k": 5}
    response = await client.post("/retrieve", json=query, headers=editor)
    assert response.status_code == 200
    assert {item["document_id"] for item in response.json()["evidence"]} >= {
        "pay-refund",
        "pay-internal",
    }
    response = await client.post(
        "/retrieve", json={**query, "filters": {"language": "en"}}, headers=viewer
    )
    body = response.json()
    assert [item["document_id"] for item in body["evidence"]] == ["pay-refund"]
    assert body["evidence"][0]["uri"] == "https://help.acme.example/pay-refund"
    assert body["evidence"][0]["title"] == "Refunding a customer payment"
    assert body["request_id"].startswith("req_")

    response = await client.post("/ingest", json={**scope, "documents": documents}, headers=viewer)
    assert response.status_code == 403
    assert response.json()["details"]["action"] == "ingest"
    response = await client.post("/retrieve", json=query, headers={"X-Principal": "globex:admin"})
    assert response.status_code == 403
    response = await client.post("/retrieve", json=query)
    assert response.status_code == 403
    response = await client.post("/retrieve", json={**query, "top_k": 0}, headers=viewer)
    assert response.status_code == 422
    response = await client.post("/ingest", json={**scope, "documents": []}, headers=editor)
    assert response.status_code == 422

    response = await client.delete("/documents/pay-erstattung", params=scope, headers=viewer)
    assert response.status_code == 403
    response = await client.delete("/documents/pay-erstattung", params=scope, headers=editor)
    assert response.status_code == 200
    assert response.json() == {"deleted": ["pay-erstattung"]}
    response = await client.delete("/documents/pay-erstattung", params=scope, headers=editor)
    assert response.status_code == 404

    response = await client.get("/status", params=scope, headers=viewer)
    assert response.status_code == 200
    status = response.json()
    assert (status["documents_active"], status["documents_deleted"]) == (2, 1)
    assert status["coverage"]["index.keyword"] == {"deleted": 1, "ready": 2}
    response = await client.get("/status", params={**scope, "tenant": "globex"}, headers=viewer)
    assert response.status_code == 403


@pytest.mark.parametrize(
    ("name", "missing"),
    [
        ("customer_support", "QDRANT_URL"),
        ("durable_restart", "QDRANT_URL"),
        ("document_lifecycle", "PGVECTOR_DSN"),
    ],
)
def test_live_examples_explain_missing_settings(name: str, missing: str) -> None:
    lines = run_example(name, MODELS_DIR=str(ROOT / "models"))
    assert lines == [
        f"skipped: set RL_EXAMPLES_{missing} (or RL_TEST_{missing}) to run this example"
    ]


@pytest.mark.live
@pytest.mark.models
@pytest.mark.qdrant
@needs("MODELS_DIR", "QDRANT_URL")
def test_customer_support() -> None:
    lines = run_example("customer_support", 900, MODELS_DIR=MODELS_DIR, QDRANT_URL=QDRANT_URL)
    assert lines[0] == "ingested: {'created': 10}"
    assert lines[1].count("{'ready': 10}") == 4

    def top(question: str) -> str:
        return after(lines, f"Q: {question}", 1)[0]

    assert top("how do I get my money back for a card payment").startswith("  #1 pay-refund ")
    typo = top("conflcted copy after editing on two devises")
    assert typo.startswith("  #1 sync-conflicts ")
    assert "index.fuzzy" in typo
    assert "index.keyword" not in typo
    mfa = top("I lost my phone and cannot pass MFA")
    assert mfa.startswith("  #1 sync-2fa ")
    assert "index.keyword" in mfa
    assert top("Konfliktkopie löschen").startswith("  #1 sync-konflikte ")
    assert top("webhook signature").startswith("  #1 pay-webhooks ")
    filtered = after(lines, "Q: webhook signature", 6)[::2]
    assert all(line.split()[1].startswith("pay-") for line in filtered)
    comparison = after(
        lines, "per-method comparison for: 'files disappear from my laptop to save disk space'", 5
    )
    assert comparison[0] == "  keyword -> []"
    assert all("['sync-selective'" in line for line in comparison[1:])
    assert lines[-1] == "cache: hits=1 misses=10"


@pytest.mark.live
@pytest.mark.models
@pytest.mark.qdrant
@needs("MODELS_DIR", "QDRANT_URL")
def test_durable_restart() -> None:
    lines = run_example("durable_restart", 900, MODELS_DIR=MODELS_DIR, QDRANT_URL=QDRANT_URL)
    assert lines[0] == "first process: ingested 10 documents"
    assert "second process: documents_active=10 (no re-ingest)" in lines
    assert "   index.dense: reused" in lines
    assert "   index.sparse: reused" in lines
    answers = [line for line in lines if line.startswith("   answer: ")]
    assert len(answers) == 2
    assert answers[0] == answers[1]
    assert answers[0].startswith("   answer: ['sync-selective'")
    assert "index.sparse" in answers[0]
    assert "index.dense" in answers[0]
    assert lines[-1].startswith("   learned-sparse terms behind the top hit: [")
    assert "'laptop'" in lines[-1]


@pytest.mark.live
@pytest.mark.models
@needs("MODELS_DIR", "PGVECTOR_DSN")
def test_document_lifecycle() -> None:
    lines = run_example("document_lifecycle", 900, MODELS_DIR=MODELS_DIR, PGVECTOR_DSN=PGVECTOR_DSN)
    assert "   ingest: {'created': 5}" in lines
    assert "   re-ingest cert-rotation: updated v2" in lines
    assert "   re-ingest db-failover: unchanged v1" in lines
    assert "   delete: deleted=('legacy-vpn',)" in lines
    assert "   purge:  purged=('contractor-access',) not_found=('never-existed',)" in lines
    assert "   'VPN access' after delete+purge: []" in lines
    assert "   index.dense after startup catch-up: {'deleted': 1, 'ready': 3}" in lines
    assert "   'the server ran out of storage space' -> ['disk-full']" in lines
    assert any("warnings=['partial_coverage']" in line for line in lines)
    assert "   backfill task: succeeded" in lines
    assert "   'hostnames cannot be looked up' -> ['dns-outage']" in lines
    assert "   coverage index.dense: {'deleted': 1, 'ready': 4}" in lines
    assert "   re-embedded on startup (new generation): True" in lines
    assert "   rebuild task: succeeded" in lines
    assert "   rebuild switched generation: True" in lines
    assert "   dense generations: ['active', 'retired', 'retired']" in lines
    assert lines[-1] == "   health: healthy unhealthy=[]"


@pytest.mark.live
@pytest.mark.models
@needs("MODELS_DIR")
def test_fact_checking() -> None:
    lines = run_example("fact_checking", 900, MODELS_DIR=MODELS_DIR)
    verdicts = {
        line.removeprefix("claim: "): lines[index + 5 :]
        for index, line in enumerate(lines)
        if line.startswith("claim: ")
    }
    expected = {
        "AcmeSync supports PAC files for proxy configuration.": "DISPUTED",
        "A card refund takes between five and ten business days.": "SUPPORTED",
        "Deleted files stay in the trash for 30 days.": "SUPPORTED",
        "AcmePay had incidents in March.": "REFUTED",
        "AcmeSync has a native client for Linux.": "NOT ENOUGH EVIDENCE",
    }
    assert set(verdicts) == set(expected)
    for claim, verdict in expected.items():
        assert verdicts[claim][0] == f"  verdict: {verdict}", claim
    assert verdicts["AcmeSync supports PAC files for proxy configuration."][1] == (
        "    supported by ['forum-post'], contradicted by ['proxy-article']"
    )
    assert verdicts["AcmePay had incidents in March."][1] == (
        "    supported by [], contradicted by ['status-page']"
    )


@pytest.mark.live
@pytest.mark.graph
@needs("NEO4J_URL")
def test_knowledge_graph_neo4j() -> None:
    check_knowledge_graph(run_example("knowledge_graph", NEO4J_URL=NEO4J_URL), "neo4j")
