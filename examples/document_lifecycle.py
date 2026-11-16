"""Document lifecycle: versions, deletes, erasure, backfill and rebuild on pgvector.

Production corpora change all the time. This example walks one scope of IT runbooks through
three engine processes that share a durable ``data_dir`` (the SQLite metadata store):

1. **Lexical only.** Ingest runbooks, re-ingest an edited one (new version), re-send an
   unchanged one (no-op), ``delete`` a retired runbook (tombstone, leaves results at once)
   and ``purge`` one that must be erased (right to erasure: metadata rows and index
   objects are physically removed).
2. **Add the dense method.** Restart with ``BgeM3Embedding`` and the application's
   ``PgVectorStore`` (Postgres + pgvector). The new method has no coverage yet; with the
   foreground lifecycle (``background=False``) the engine catches up while it opens, so
   existing runbooks are embedded before the first request. A runbook ingested with
   ``IndexWait.NONE`` is stored but not indexed: dense retrieval reports
   ``partial_coverage`` until an operator-triggered ``backfill`` indexes it.
3. **Change the model configuration.** Restart with a different embedding configuration
   (``max_length``). Its fingerprint differs, so vectors from the two configurations are
   never mixed: the engine builds a new pgvector generation on startup and switches
   atomically; ``rebuild`` forces yet another full generation on demand.

``status`` and ``health`` show coverage, generations, tasks and component health.

Requires the local models and a pgvector database (``examples/compose.yaml``)::

    $env:RL_EXAMPLES_MODELS_DIR="models"
    $env:RL_EXAMPLES_PGVECTOR_DSN="postgresql://rl:rl@localhost:55433/rl"
    python examples/document_lifecycle.py
"""

import asyncio
import tempfile
from pathlib import Path

from _shared import doc_ids, quiet_logs, require, run_prefix
from adapters import BgeM3Embedding, PgVectorStore

from retrievalloom import (
    Components,
    DocumentInput,
    EngineResources,
    IndexWait,
    IngestionConfig,
    LifecycleConfig,
    RerankingConfig,
    RetrievalConfig,
    RetrievalEngine,
    RetrievalExecutionConfig,
    Scope,
    ScopeStatus,
    TextRepresentation,
    create_retrieval_engine,
)

SCOPE = Scope(tenant_id="acme", collection="runbooks")
RUNBOOKS = {
    "db-failover": "Promote the standby database with pg_ctl promote, then point the "
    "connection pooler at the new primary.",
    "cert-rotation": "Rotate TLS certificates every 90 days with the cert-manager renewal job.",
    "disk-full": "When a node reports a full disk, delete old journal files and expand the volume.",
    "legacy-vpn": "The legacy VPN concentrator is restarted from the rack console.",
    "contractor-access": "Contractor Jane Roe gets VPN access until the end of the audit.",
}
DENSE_ONLY = RetrievalExecutionConfig(keyword=False, fuzzy=False, sparse=False)
PARAPHRASE = "the server ran out of storage space"
DNS_OUTAGE = "If name resolution fails, flush the resolver cache and fail over to the backup DNS."
DNS_QUERY = "hostnames cannot be looked up"


def runbook(doc_id: str, text: str) -> DocumentInput:
    return DocumentInput(_id=doc_id, representations=(TextRepresentation(text=text),))


def dense_generation(status: ScopeStatus) -> str | None:
    return next(
        (
            g.generation_id
            for g in status.generations
            if g.method == "index.dense" and g.state.value == "active"
        ),
        None,
    )


def describe(status: ScopeStatus) -> None:
    print(f"   documents: active={status.documents_active} deleted={status.documents_deleted}")
    for method, states in sorted(status.coverage.items()):
        print(f"   coverage {method}: {dict(sorted(states.items()))}")


def engine_for(
    data_dir: Path, components: Components | None = None, *, index_wait: IndexWait = IndexWait.ALL
) -> RetrievalEngine:
    return create_retrieval_engine(
        retrieval=RetrievalConfig(
            reranking=RerankingConfig(enabled=False), request_timeout=300.0, method_timeout=240.0
        ),
        ingestion=IngestionConfig(index_wait=index_wait),
        components=components,
        resources=EngineResources(data_dir=data_dir),
        lifecycle=LifecycleConfig(background=False),
    )


async def main() -> None:
    quiet_logs()
    settings = require("MODELS_DIR", "PGVECTOR_DSN")
    if settings is None:
        return
    bge = f"{settings['MODELS_DIR']}/bge-m3"
    prefix = run_prefix("lifecycle")
    with tempfile.TemporaryDirectory() as tmp:
        data_dir = Path(tmp)

        print("phase 1: lexical methods only")
        async with engine_for(data_dir) as engine:
            report = await engine.ingest(
                [runbook(doc_id, text) for doc_id, text in RUNBOOKS.items()], scope=SCOPE
            )
            print(f"   ingest: { {o.value: n for o, n in report.outcomes.items()} }")
            edited = RUNBOOKS["cert-rotation"].replace("90 days", "60 days")
            report = await engine.ingest(
                [runbook("cert-rotation", edited), runbook("db-failover", RUNBOOKS["db-failover"])],
                scope=SCOPE,
            )
            for document in report.documents:
                outcome = f"{document.outcome.value} v{document.version}"
                print(f"   re-ingest {document.document_id}: {outcome}")
            result = await engine.retrieve("rotate certificates", scope=SCOPE, top_k=1)
            print(f"   cert-rotation now says: {result.evidence[0].display_text!r}")

            deleted = await engine.delete(["legacy-vpn"], scope=SCOPE)
            purged = await engine.purge(["contractor-access", "never-existed"], scope=SCOPE)
            print(f"   delete: deleted={deleted.deleted}")
            print(f"   purge:  purged={purged.purged} not_found={purged.not_found}")
            result = await engine.retrieve("VPN access", scope=SCOPE)
            print(f"   'VPN access' after delete+purge: {doc_ids(result.evidence)}")
            describe(await engine.status(scope=SCOPE))

        print("\nphase 2: restart with bge-m3 on pgvector (the dense method is new)")
        store = PgVectorStore(settings["PGVECTOR_DSN"], table_prefix=prefix)
        try:
            components = Components(embedding=BgeM3Embedding(bge), vector_store=store)
            async with engine_for(data_dir, components, index_wait=IndexWait.NONE) as engine:
                status = await engine.status(scope=SCOPE)
                print(f"   index.dense after startup catch-up: {status.coverage['index.dense']}")
                first = dense_generation(status)
                result = await engine.retrieve(PARAPHRASE, scope=SCOPE, top_k=1, config=DENSE_ONLY)
                print(f"   {PARAPHRASE!r} -> {doc_ids(result.evidence)}")

                await engine.ingest([runbook("dns-outage", DNS_OUTAGE)], scope=SCOPE)
                status = await engine.status(scope=SCOPE)
                print(
                    f"   after IndexWait.NONE ingest: active={status.documents_active} "
                    f"index.dense={status.coverage['index.dense']}"
                )
                result = await engine.retrieve(DNS_QUERY, scope=SCOPE, top_k=1, config=DENSE_ONLY)
                warnings = sorted({w.code.value for w in result.warnings})
                print(f"   {DNS_QUERY!r} -> {doc_ids(result.evidence)} warnings={warnings}")
                task = await engine.backfill(scope=SCOPE, methods=["dense"])
                done = await engine.wait_for(task, timeout=600)
                print(f"   backfill task: {done.state.value}")
                describe(await engine.status(scope=SCOPE))
                result = await engine.retrieve(DNS_QUERY, scope=SCOPE, top_k=1, config=DENSE_ONLY)
                print(f"   {DNS_QUERY!r} -> {doc_ids(result.evidence)}")

            print("\nphase 3: restart with a new embedding configuration (max_length=256)")
            components = Components(
                embedding=BgeM3Embedding(bge, max_length=256), vector_store=store
            )
            async with engine_for(data_dir, components) as engine:
                status = await engine.status(scope=SCOPE)
                second = dense_generation(status)
                print(f"   re-embedded on startup (new generation): {second not in (None, first)}")
                task = await engine.rebuild(scope=SCOPE, channel="index", method="dense")
                done = await engine.wait_for(task, timeout=600)
                print(f"   rebuild task: {done.state.value}")
                status = await engine.status(scope=SCOPE)
                third = dense_generation(status)
                print(f"   rebuild switched generation: {third not in (None, second)}")
                states = sorted(
                    g.state.value for g in status.generations if g.method == "index.dense"
                )
                print(f"   dense generations: {states}")
                result = await engine.retrieve(PARAPHRASE, scope=SCOPE, top_k=1, config=DENSE_ONLY)
                print(f"   {PARAPHRASE!r} -> {doc_ids(result.evidence)}")
                print(f"   tasks: {dict(sorted(status.tasks.items()))}")
                health = await engine.health()
                unhealthy = [c.component for c in health.components if c.status.value != "healthy"]
                print(f"   health: {health.status.value} unhealthy={unhealthy}")
        finally:
            await store.drop_tables()
            await store.close()


if __name__ == "__main__":
    asyncio.run(main())
