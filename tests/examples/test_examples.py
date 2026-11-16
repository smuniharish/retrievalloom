"""Run every script under ``examples/`` and check the evidence it prints.

Offline examples run in the default suite. Live examples are also marked ``live`` and are
skipped unless their services are configured (``RL_EXAMPLES_QDRANT_URL`` /
``RL_EXAMPLES_MODELS_DIR``, falling back to the ``RL_TEST_*`` variables).
"""

from __future__ import annotations

import importlib.util
import os
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
QDRANT_URL = os.environ.get("RL_EXAMPLES_QDRANT_URL") or os.environ.get("RL_TEST_QDRANT_URL")
MODELS_DIR = os.environ.get("RL_EXAMPLES_MODELS_DIR") or os.environ.get("RL_TEST_MODELS_DIR")

pytestmark = pytest.mark.examples

requires_graph = pytest.mark.skipif(
    importlib.util.find_spec("ladybug") is None, reason="requires the graph extra"
)
requires_structure = pytest.mark.skipif(
    importlib.util.find_spec("treesearch") is None, reason="requires the structure extra"
)
requires_models = pytest.mark.skipif(not MODELS_DIR, reason="set RL_EXAMPLES_MODELS_DIR to run")
requires_qdrant = pytest.mark.skipif(not QDRANT_URL, reason="set RL_EXAMPLES_QDRANT_URL to run")


def run_example(name: str, timeout: float = 120) -> list[str]:
    """Run ``examples/<name>.py`` from the repository root; return its stdout lines."""
    env = {**os.environ, "PYTHONIOENCODING": "utf-8"}
    if QDRANT_URL:
        env["RL_EXAMPLES_QDRANT_URL"] = QDRANT_URL
    if MODELS_DIR:
        env["RL_EXAMPLES_MODELS_DIR"] = MODELS_DIR
    completed = subprocess.run(  # noqa: S603 - fixed interpreter and script path
        [sys.executable, str(ROOT / "examples" / f"{name}.py")],
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


def test_quickstart() -> None:
    lines = run_example("quickstart")
    assert lines[0] == "ingested: {'created': 4}"
    assert lines[1] == "status: complete"
    assert lines[3].startswith("#1 asyncio v1 score=")
    assert "   source:  kb://articles/asyncio (Asyncio)" in lines
    assert "   methods: index.fuzzy, index.keyword, index.sparse" in lines
    assert any(line.startswith("   stages:  ") and "'consolidation'" in line for line in lines)
    assert lines[-1] == "lang=de filter: ['eventloop']"


def test_multi_tenant() -> None:
    lines = run_example("multi_tenant")
    start, end = lines.index("acme: 2 evidence item(s)"), lines.index("globex: 1 evidence item(s)")
    assert all(line.endswith("(tenants={'acme'})") for line in lines[start + 1 : end])
    assert (
        "  vacation: Globex employees get 20 vacation days per year. (tenants={'globex'})" in lines
    )
    assert "globex 'work remotely': status=empty" in lines
    assert "delete 'remote' in globex: deleted=() not_found=('remote',)" in lines
    assert "globex batch of 3 rejected: {'count': 3, 'max': 2}" in lines
    assert any(line.startswith("status acme: active=2 deleted=0") for line in lines)
    assert any(line.startswith("status globex: active=1 deleted=0") for line in lines)


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


@pytest.mark.graph
@requires_graph
def test_graph_channel() -> None:
    lines = run_example("graph_channel")
    assert lines[0] == "status=complete evidence=7"
    assert lines[1].endswith("hops=0 'Ada Lovelace' from ['notes']")
    assert any("hops=1 'Analytical Engine'" in line for line in lines)
    assert any("hops=2 'Luigi Menabrea' from ['lecture']" in line for line in lines)
    assert (
        "     path: Ada Lovelace -> Ada Lovelace wrote_about Analytical Engine -> "
        "Analytical Engine -> Charles Babbage designed Analytical Engine -> Charles Babbage"
    ) in lines


@pytest.mark.structure
@requires_structure
def test_structure_channel() -> None:
    lines = run_example("structure_channel")
    first = lines.index("query: 'restore the backup'")
    assert lines[first + 1].startswith("  #1 score=")
    assert lines[first + 2] == (
        "     Admin guide > Backups > Restore: "
        "Stop the service, then copy the backup into the data directory."
    )
    assert "     top node 'Restore': level=2 parent=['Backups'] children=0 siblings=0" in lines
    assert (
        "     top node 'Installation': level=1 parent=['Admin guide'] children=2 siblings=1"
    ) in lines


def test_lifecycle_backfill_rebuild() -> None:
    lines = run_example("lifecycle_backfill_rebuild")
    assert lines[2] == (
        "retrieve before backfill: status=empty "
        "warnings=['index_missing', 'method_unavailable', 'reranker_unavailable']"
    )
    assert "backfill task: succeeded" in lines
    assert "retrieve after backfill: ['ssh', 'tls']" in lines
    assert "rebuild task: succeeded" in lines
    assert "active sparse generation replaced: True" in lines
    assert "retrieve after delete: ['tls']" in lines
    assert any(
        line.startswith("after lifecycle round: active=2 deleted=1")
        and "{'deleted': 1, 'ready': 2}" in line
        for line in lines
    )
    assert lines[-1].startswith("health: healthy")


def test_agxai_provenance() -> None:
    lines = run_example("agxai_provenance")
    assert lines[0] == "evidence record: retrievalloom ingestion {'created': 3}"
    assert lines[1].startswith("evidence record: retrievalloom deletion {'deleted': 1,")
    assert lines[2] == "retrieval: retriever=retrievalloom:support status=complete"
    assert "  query reference = request id: True" in lines
    assert any(line.startswith("  rank 1: reset ") for line in lines)
    assert lines.count("    linked to source uid: True") == 2


@pytest.mark.live
@pytest.mark.models
@requires_models
def test_custom_reranker_nli() -> None:
    lines = run_example("custom_reranker_nli", timeout=900)
    assert lines[0].startswith("fused order:    ['rumour'")
    assert lines[1].startswith("reranked order: ['history', 'rumour'")
    assert any(
        line.startswith("  history:") and "'claim:0': 'entailment'" in line for line in lines
    )
    assert any(
        line.startswith("  rumour:") and "'claim:0': 'contradiction'" in line for line in lines
    )
    assert "top_k=1 returned 2 evidence item(s):" in lines
    assert any(
        line.startswith("  rumour: has_contradiction=True contradicts=['history']")
        for line in lines
    )


@pytest.mark.live
@pytest.mark.models
@pytest.mark.qdrant
@requires_models
@requires_qdrant
def test_byo_embeddings_qdrant() -> None:
    lines = run_example("byo_embeddings_qdrant", timeout=900)
    assert lines[0] == "index.dense coverage: {'ready': 4}"
    assert lines[1].startswith("'famous landmark in France' -> eiffel via ['index.dense'")
    assert lines[3] == "'how do green leaves make sugar' -> photosynthesis via ['index.dense']"
