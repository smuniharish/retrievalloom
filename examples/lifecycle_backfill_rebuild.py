"""Lifecycle: deferred indexing, backfill, rebuild, deletion and status.

Ingestion stores documents durably; indexing is done by lifecycle tasks. This example
ingests without waiting for the indexes (``IndexWait.NONE``), shows the resulting
``index_missing`` warning and pending coverage, then queues a backfill and waits for it.
A rebuild of one method builds a new index generation and switches to it atomically.
Deleted documents leave retrieval results immediately.

``LifecycleConfig(background=False)`` disables the background scheduler, so ``wait_for``
runs the lifecycle rounds itself; with the default background scheduler the same calls
simply wait for the scheduler.

Run from the repository root::

    python examples/lifecycle_backfill_rebuild.py
"""

import asyncio

from _shared import quiet_logs

from retrievalloom import (
    DocumentInput,
    IndexWait,
    IngestionConfig,
    LifecycleConfig,
    Scope,
    ScopeStatus,
    TextRepresentation,
    create_retrieval_engine,
)

SCOPE = Scope(tenant_id="acme")
TEXTS = {
    "tls": "Rotate TLS certificates every 90 days.",
    "ssh": "Rotate SSH host keys once a year.",
    "dns": "DNS records are reviewed every quarter.",
}


def sparse_generation(status: ScopeStatus) -> str:
    return next(
        g.generation_id
        for g in status.generations
        if g.method == "index.sparse" and g.state.value == "active"
    )


def describe(label: str, status: ScopeStatus) -> None:
    states = [g.state.value for g in status.generations if g.method == "index.sparse"]
    print(
        f"{label}: active={status.documents_active} deleted={status.documents_deleted} "
        f"coverage[index.sparse]={status.coverage.get('index.sparse', {})} "
        f"generations[index.sparse]={states}"
    )


async def main() -> None:
    quiet_logs()
    # Ingest returns as soon as documents are stored; indexing is left to lifecycle tasks.
    engine = create_retrieval_engine(
        ingestion=IngestionConfig(index_wait=IndexWait.NONE),
        lifecycle=LifecycleConfig(background=False),
    )
    async with engine:
        docs = [
            DocumentInput(_id=doc_id, representations=(TextRepresentation(text=text),))
            for doc_id, text in TEXTS.items()
        ]
        report = await engine.ingest(docs, scope=SCOPE)
        print(f"ingested: { ({o.value: n for o, n in report.outcomes.items()}) }")
        describe("after ingest", await engine.status(scope=SCOPE))
        result = await engine.retrieve("rotate keys", scope=SCOPE)
        codes = sorted({w.code.value for w in result.warnings})
        print(f"retrieve before backfill: status={result.status.value} warnings={codes}")

        task = await engine.backfill(scope=SCOPE)
        done = await engine.wait_for(task, timeout=30)
        print(f"{done.kind.value} task: {done.state.value}")
        status = await engine.status(scope=SCOPE)
        describe("after backfill", status)
        first_generation = sparse_generation(status)
        result = await engine.retrieve("rotate keys", scope=SCOPE)
        print(f"retrieve after backfill: {[e.sources[0].document_id for e in result.evidence]}")

        # Rebuild one method: a new generation is built and replaces the old one atomically.
        task = await engine.rebuild(scope=SCOPE, channel="index", method="sparse")
        done = await engine.wait_for(task, timeout=30)
        print(f"{done.kind.value} task: {done.state.value}")
        status = await engine.status(scope=SCOPE)
        describe("after rebuild", status)
        print(f"active sparse generation replaced: {sparse_generation(status) != first_generation}")

        # Deleted documents are filtered out at once, before the indexes are cleaned up.
        deleted = await engine.delete(["ssh"], scope=SCOPE)
        print(f"deleted: {deleted.deleted}")
        result = await engine.retrieve("rotate keys", scope=SCOPE)
        print(f"retrieve after delete: {[e.sources[0].document_id for e in result.evidence]}")
        describe("after delete", await engine.status(scope=SCOPE))
        # One lifecycle round removes the deleted document from the indexes.
        await engine.run_lifecycle_once(SCOPE)
        status = await engine.status(scope=SCOPE)
        describe("after lifecycle round", status)
        print(f"tasks: {status.tasks}")

        health = await engine.health()
        print(f"health: {health.status.value} units={health.details['units']}")


if __name__ == "__main__":
    asyncio.run(main())
