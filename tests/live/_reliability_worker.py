"""Worker process of the multi-process suite (``test_multi_process.py``).

Run as ``python -m tests.live._reliability_worker '<json job>'`` from the repository
root. Every worker builds its own engine on the shared Postgres schema and prints one
final ``RESULT {json}`` line:

* ``write`` jobs run a random ingest/update/delete/purge/retrieve mix on overlapping
  document ids until ``deadline`` (a shared wall-clock time) and report every
  acknowledged write and every error;
* ``query`` jobs converge the lifecycle and report what retrieval returns for a list of
  queries, so the test can compare processes.
"""

from __future__ import annotations

import asyncio
import json
import random
import sys
import time
import traceback
from typing import Any

from retrievalloom import (
    Components,
    LifecycleConfig,
    RetrievalEngine,
    Scope,
    create_retrieval_engine,
)
from retrievalloom.domain.records import IngestOutcome
from retrievalloom.errors import RetrievalLoomError
from retrievalloom.storage.postgres import PostgresMetadataStore
from tests.fakes import HashEmbedding
from tests.live._reliability_helpers import WORDS, converge, text_doc

ACKED = {IngestOutcome.CREATED, IngestOutcome.UPDATED, IngestOutcome.REVIVED}


def engine_for(job: dict[str, Any], *, background: bool) -> RetrievalEngine:
    """Engine of this process on the shared schema (versions and events are kept)."""
    store = PostgresMetadataStore(job["dsn"], schema=job["schema"])
    return create_retrieval_engine(
        components=Components(metadata_store=store, embedding=HashEmbedding()),
        lifecycle=LifecycleConfig(
            background=background,
            poll_interval=0.2,
            max_versions=None,
            purge_after=None,
            event_retention=None,
        ),
    )


def scope_of(name: str) -> Scope:
    """Scope from its tenant name."""
    return Scope(tenant_id=name)


def error_entry(op: str, exc: BaseException) -> dict[str, Any]:
    """JSON description of an error raised by an operation."""
    structured = isinstance(exc, RetrievalLoomError)
    return {
        "op": op,
        "type": type(exc).__name__,
        "structured": structured,
        "code": exc.code if isinstance(exc, RetrievalLoomError) else None,
        "retryable": exc.retryable if isinstance(exc, RetrievalLoomError) else None,
        "message": str(exc)[:300],
        "traceback": None if structured else traceback.format_exc()[-2000:],
    }


async def write(job: dict[str, Any]) -> dict[str, Any]:
    """Random writes on overlapping ids until the shared deadline."""
    index: int = job["index"]
    rng = random.Random(job["seed"])
    scope_name: str = job["scope"]
    scope = scope_of(scope_name)
    ids = [f"doc-{n:02d}" for n in range(job["ids"])]
    acks: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []
    deletes: dict[str, int] = {"deleted": 0, "already_deleted": 0, "not_found": 0}
    purged: list[str] = []
    errors: list[dict[str, Any]] = []
    ops: dict[str, int] = {}
    latencies: dict[str, float] = {}
    serial = 0
    async with engine_for(job, background=True) as engine:
        await asyncio.sleep(max(0.0, job["start_at"] - time.time()))
        while time.time() < job["deadline"]:
            roll = rng.random()
            op = (
                "ingest"
                if roll < 0.55
                else "delete"
                if roll < 0.72
                else "purge"
                if roll < 0.8
                else "retrieve"
            )
            started = time.perf_counter()
            try:
                if op == "ingest":
                    batch = []
                    for document_id in rng.sample(ids, rng.randint(1, 3)):
                        serial += 1
                        tag = f"w{index}s{serial}"
                        words = " ".join(rng.choices(WORDS, k=8))
                        batch.append(text_doc(document_id, f"{tag} {words}", writer=tag))
                    report = await engine.ingest(batch, scope=scope)
                    tags = {item.id: item.metadata["writer"] for item in batch}
                    for result in report.documents:
                        if result.outcome in ACKED:
                            acks.append(
                                {
                                    "id": result.document_id,
                                    "version": result.version,
                                    "outcome": result.outcome.value,
                                    "tag": tags[result.document_id],
                                }
                            )
                        else:
                            rejected.append(
                                {
                                    "id": result.document_id,
                                    "outcome": result.outcome.value,
                                    "error": None
                                    if result.error is None
                                    else result.error.model_dump(mode="json"),
                                }
                            )
                elif op == "delete":
                    deleted = await engine.delete([rng.choice(ids)], scope=scope)
                    deletes["deleted"] += len(deleted.deleted)
                    deletes["already_deleted"] += len(deleted.already_deleted)
                    deletes["not_found"] += len(deleted.not_found)
                elif op == "purge":
                    # Private ids: purge must erase exactly what this worker created.
                    serial += 1
                    private = f"tmp-w{index}-{serial}"
                    await engine.ingest([text_doc(private, f"temporary {private}")], scope=scope)
                    report = await engine.purge([private], scope=scope)
                    if private in report.purged:
                        purged.append(private)
                else:
                    await engine.retrieve(" ".join(rng.choices(WORDS, k=3)), scope=scope, top_k=10)
            except Exception as exc:
                errors.append(error_entry(op, exc))
                await asyncio.sleep(0.05)
            ops[op] = ops.get(op, 0) + 1
            latencies[op] = latencies.get(op, 0.0) + time.perf_counter() - started
    return {
        "index": index,
        "scope": scope_name,
        "acks": acks,
        "rejected": rejected,
        "deletes": deletes,
        "purged": purged,
        "errors": errors,
        "ops": ops,
        "mean_ms": {op: round(1000 * latencies[op] / ops[op], 1) for op in ops},
    }


async def query(job: dict[str, Any]) -> dict[str, Any]:
    """Converge the lifecycle, then report retrieval results for every query."""
    answers: dict[str, dict[str, list[str]]] = {}
    async with engine_for(job, background=False) as engine:
        for scope_name, queries in job["queries"].items():
            scope = scope_of(scope_name)
            await converge(engine, scope)
            answers[scope_name] = {}
            for text in queries:
                result = await engine.retrieve(text, scope=scope, top_k=job["top_k"])
                answers[scope_name][text] = [
                    f"{source.scope.tenant_id}/{source.document_id}"
                    for item in result.evidence
                    for source in item.sources
                ]
    return {"answers": answers}


def main() -> None:
    job = json.loads(sys.argv[1])
    try:
        result = asyncio.run(write(job) if job["kind"] == "write" else query(job))
    except Exception as exc:
        result = {"fatal": error_entry(job["kind"], exc)}
    print("RESULT " + json.dumps(result), flush=True)  # noqa: T201 - protocol to the test


if __name__ == "__main__":
    main()
