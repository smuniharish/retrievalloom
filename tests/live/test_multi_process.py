"""Multi-process contention on one Postgres store (opt-in, ``RL_TEST_POSTGRES_DSN``).

Several OS processes, each with its own engine and background lifecycle, write the same
document ids at once (one shared scope, or one tenant scope per process on the same
tables). A fresh engine then checks the invariants: every acknowledged write is stored
at its version, version counters have no duplicates or gaps, no duplicate documents,
coverage converges, and retrieval agrees across processes.

``RL_TEST_MP_SECONDS`` (default 30) and ``RL_TEST_MP_WORKERS`` (default 4) size the run.
"""

from __future__ import annotations

import asyncio
import json
import os
import random
import subprocess
import sys
import time
from collections import Counter, defaultdict
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest

from retrievalloom import Components, LifecycleConfig, Scope
from retrievalloom.domain.records import DocumentState, GenerationState
from tests.fakes import HashEmbedding
from tests.live._reliability_helpers import (
    POSTGRES_DSN,
    WORDS,
    converge,
    drop_schema,
    emit,
    new_schema,
    unsettled_coverage,
)
from tests.live.conftest import running_engine

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from retrievalloom import RetrievalEngine
    from retrievalloom.storage.postgres import PostgresMetadataStore

MP_SECONDS = float(os.environ.get("RL_TEST_MP_SECONDS", "30"))
WORKERS = int(os.environ.get("RL_TEST_MP_WORKERS", "4"))
IDS = 12  # few ids, so processes keep colliding on the same rows
QUERY_WORKERS = 2
TOP_K = 50
REPO_ROOT = Path(__file__).parents[2]

pytestmark = pytest.mark.skipif(
    POSTGRES_DSN is None, reason="set RL_TEST_POSTGRES_DSN to run the multi-process suite"
)


def run_worker(job: dict[str, Any], timeout: float) -> dict[str, Any]:
    """Run one worker process and return its ``RESULT`` payload."""
    completed = subprocess.run(  # noqa: S603 - this interpreter and a fixed module
        [sys.executable, "-m", "tests.live._reliability_worker", json.dumps(job)],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=timeout,
        env={**os.environ, "PYTHONIOENCODING": "utf-8"},
        check=False,
    )
    lines = [line for line in completed.stdout.splitlines() if line.startswith("RESULT ")]
    assert completed.returncode == 0, completed.stderr[-4000:]
    assert lines, f"no RESULT line: {completed.stdout[-2000:]} {completed.stderr[-2000:]}"
    result: dict[str, Any] = json.loads(lines[-1].removeprefix("RESULT "))
    assert "fatal" not in result, result["fatal"]
    return result


async def run_workers(jobs: list[dict[str, Any]], seconds: float) -> list[dict[str, Any]]:
    """Run ``jobs`` as concurrent processes."""
    return list(
        await asyncio.gather(*(asyncio.to_thread(run_worker, job, seconds) for job in jobs))
    )


@pytest.fixture
async def mp_schema() -> AsyncIterator[str]:
    """Fresh schema, dropped afterwards."""
    assert POSTGRES_DSN is not None
    schema = new_schema("rl_mp")
    try:
        yield schema
    finally:
        await drop_schema(POSTGRES_DSN, schema)


async def stored_documents(
    store: PostgresMetadataStore, scope: Scope
) -> dict[str, tuple[Any, dict[int, str | None]]]:
    """``id -> (record, {version: writer tag})`` of every document of ``scope``."""
    documents: dict[str, tuple[Any, dict[int, str | None]]] = {}
    async with store.transaction(scope, write=False) as tx:
        records: list[Any] = []
        after: str | None = None
        while True:
            page = await tx.list_documents(scope.key, after_id=after, limit=500)
            if not page:
                break
            records.extend(page)
            after = page[-1].document_id
        ids = [record.document_id for record in records]
        assert len(ids) == len(set(ids)), f"duplicate documents: {Counter(ids).most_common(3)}"
        for record in records:
            versions = await tx.get_versions(record.document_uid)
            documents[record.document_id] = (
                record,
                {item.version: str(item.metadata.get("writer")) for item in versions},
            )
    return documents


def check_scope(
    scope_name: str,
    results: list[dict[str, Any]],
    documents: dict[str, tuple[Any, dict[int, str | None]]],
    *,
    single_writer: bool,
) -> dict[str, int]:
    """Assert the write invariants of one scope; returns counters for the summary."""
    writers = [result for result in results if result["scope"] == scope_name]
    acked: dict[str, dict[int, str]] = defaultdict(dict)
    for result in writers:
        last: dict[str, int] = {}
        for ack in result["acks"]:
            document_id, version = ack["id"], ack["version"]
            # Two acknowledgements of one version would be a lost update.
            assert version not in acked[document_id], (scope_name, ack, acked[document_id])
            acked[document_id][version] = ack["tag"]
            # One process sees its own writes of an id in increasing version order.
            assert version > last.get(document_id, 0), (scope_name, ack, last)
            last[document_id] = version
            if single_writer:
                assert ack["tag"].startswith(f"w{result['index']}s"), ack
        for private in result["purged"]:
            assert private not in documents, f"purged {private} is still stored"

    unacknowledged = 0
    for document_id, versions in acked.items():
        assert document_id in documents, f"acknowledged {document_id} was lost"
        record, stored = documents[document_id]
        for version, tag in versions.items():
            # No lost committed write: the acknowledged version holds that writer's text.
            assert stored.get(version) == tag, (scope_name, document_id, version, tag, stored)
        assert sorted(stored) == list(range(1, len(stored) + 1)), (document_id, sorted(stored))
        assert record.version == max(stored), (document_id, record.version, max(stored))
        assert record.state in {DocumentState.ACTIVE, DocumentState.DELETED}, record
        unacknowledged += len(set(stored) - set(versions))
    # A version nobody acknowledged may only come from an ingest that raised (its commit
    # outcome is unknown to the caller), never from a successful call.
    ingest_errors = sum(
        1 for result in writers for error in result["errors"] if error["op"] in {"ingest", "purge"}
    )
    assert unacknowledged <= ingest_errors * 3, (unacknowledged, ingest_errors)
    for document_id in documents:
        assert document_id in acked or document_id.startswith("tmp-"), document_id
    return {
        "acked": sum(len(versions) for versions in acked.values()),
        "unacknowledged": unacknowledged,
        "documents": len(documents),
    }


async def answers_of(
    engine: RetrievalEngine, queries: dict[str, list[str]]
) -> dict[str, dict[str, list[str]]]:
    """Retrieval results of ``engine`` in the worker's answer format."""
    answers: dict[str, dict[str, list[str]]] = {}
    for scope_name, texts in queries.items():
        answers[scope_name] = {}
        for text in texts:
            result = await engine.retrieve(text, scope=Scope(tenant_id=scope_name), top_k=TOP_K)
            answers[scope_name][text] = [
                f"{source.scope.tenant_id}/{source.document_id}"
                for item in result.evidence
                for source in item.sources
            ]
    return answers


@pytest.mark.parametrize("layout", ["shared-scope", "tenant-scopes"])
async def test_processes_share_one_postgres_store(layout: str, mp_schema: str) -> None:
    assert POSTGRES_DSN is not None
    from retrievalloom.storage.postgres import PostgresMetadataStore

    shared = layout == "shared-scope"
    scope_names = ["shared"] * WORKERS if shared else [f"tenant{i}" for i in range(WORKERS)]
    start_at = time.time() + 15  # every process is up before the load starts
    deadline = start_at + MP_SECONDS
    base = {"kind": "write", "dsn": POSTGRES_DSN, "schema": mp_schema, "ids": IDS}
    jobs = [
        {**base, "index": index, "seed": index, "scope": scope_names[index]}
        | {"start_at": start_at, "deadline": deadline}
        for index in range(WORKERS)
    ]
    started = time.perf_counter()
    results = await run_workers(jobs, seconds=MP_SECONDS + 180)
    elapsed = time.perf_counter() - started

    errors = [error for result in results for error in result["errors"]]
    unstructured = [error for error in errors if not error["structured"]]
    assert not unstructured, unstructured[:3]
    permanent = [error for error in errors if not error["retryable"]]
    assert not permanent, permanent[:3]
    rejected = [item for result in results for item in result["rejected"]]
    assert all(item["error"] and item["error"].get("retryable") for item in rejected), rejected[:3]

    store = PostgresMetadataStore(POSTGRES_DSN, schema=mp_schema)
    counters: dict[str, dict[str, int]] = {}
    expected_active: dict[str, int] = {}
    queries: dict[str, list[str]] = {}
    owners: dict[str, dict[str, str]] = {}
    rng = random.Random(7)
    async with running_engine(
        Components(metadata_store=store, embedding=HashEmbedding()),
        lifecycle=LifecycleConfig(
            background=False, max_versions=None, purge_after=None, event_retention=None
        ),
    ) as engine:
        for scope_name in sorted(set(scope_names)):
            documents = await stored_documents(store, Scope(tenant_id=scope_name))
            counters[scope_name] = check_scope(
                scope_name, results, documents, single_writer=not shared
            )
            active = {
                document_id: stored[record.version]
                for document_id, (record, stored) in documents.items()
                if record.state is DocumentState.ACTIVE
            }
            expected_active[scope_name] = len(active)
            owners[scope_name] = {tag: key for key, tag in sorted(active.items()) if tag}
            tags = list(owners[scope_name])[:5]
            owners[scope_name] = {tag: owners[scope_name][tag] for tag in tags}
            queries[scope_name] = tags + [" ".join(rng.choices(WORDS, k=3)) for _ in range(3)]

        for scope_name in sorted(set(scope_names)):
            scope = Scope(tenant_id=scope_name)
            status = await converge(engine, scope)
            assert not unsettled_coverage(status), (scope_name, status.coverage)
            assert status.documents_active == expected_active[scope_name]
            active_units = [
                g.method for g in status.generations if g.state is GenerationState.ACTIVE
            ]
            assert len(active_units) == len(set(active_units)), active_units
        local = await answers_of(engine, queries)

    query_job = {
        "kind": "query",
        "dsn": POSTGRES_DSN,
        "schema": mp_schema,
        "queries": queries,
        "top_k": TOP_K,
    }
    remote = await run_workers([query_job] * QUERY_WORKERS, seconds=180)
    for scope_name, texts in queries.items():
        for text in texts:
            mine = local[scope_name][text]
            for other in remote:
                theirs = other["answers"][scope_name][text]
                assert sorted(theirs) == sorted(mine), (scope_name, text, mine, theirs)
            assert all(hit.startswith(f"{scope_name}/") for hit in mine), mine
        for tag, document_id in owners[scope_name].items():
            # A document's unique tag in its current text ranks that document first.
            assert local[scope_name][tag][:1] == [f"{scope_name}/{document_id}"], (
                tag,
                local[scope_name][tag][:3],
            )

    summary = {
        "layout": layout,
        "workers": WORKERS,
        "seconds": round(elapsed, 1),
        "ops": dict(sum((Counter(result["ops"]) for result in results), Counter())),
        "ops_per_second": round(
            sum(sum(result["ops"].values()) for result in results) / MP_SECONDS, 1
        ),
        "mean_ms": [result["mean_ms"] for result in results],
        "deletes": dict(sum((Counter(result["deletes"]) for result in results), Counter())),
        "errors": dict(Counter(f"{error['op']}:{error['code']}" for error in errors)),
        "rejected": len(rejected),
        "scopes": counters,
    }
    emit(f"\nMULTIPROC {summary}")
    assert all(result["acks"] for result in results), "a worker acknowledged nothing"
