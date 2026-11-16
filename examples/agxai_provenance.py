"""agxai provenance: record ingestion and retrieval inside an agxai run.

Passing an agxai ``XAIRuntime`` as ``EngineResources(xai_runtime=...)`` makes the engine
use the agxai provenance recorder. Every ingestion, deletion and retrieval performed while
an agxai run is active (``runtime.activate_run(run)``) is recorded on that run:
retrievals as agxai retrieval records (one ranked document per evidence item) plus
``derived_from`` provenance links from each evidence item to its source documents, and
ingestions/deletions as custom evidence records with outcome counts. Outside an active run
nothing is recorded. The runtime's tenant must match the scope's tenant.

Run from the repository root::

    python examples/agxai_provenance.py
"""

import asyncio

from _shared import quiet_logs
from agxai import ProvenanceStore, XAIRuntime

from retrievalloom import (
    DocumentInput,
    EngineResources,
    LifecycleConfig,
    Scope,
    TextRepresentation,
    create_retrieval_engine,
)

SCOPE = Scope(tenant_id="acme", collection="support")


def doc(doc_id: str, text: str) -> DocumentInput:
    return DocumentInput(_id=doc_id, representations=(TextRepresentation(text=text),))


async def main() -> None:
    quiet_logs()
    runtime = XAIRuntime(application_id="support-bot", tenant_id="acme")
    engine = create_retrieval_engine(
        resources=EngineResources(xai_runtime=runtime),
        lifecycle=LifecycleConfig(background=False),
    )
    try:
        async with engine:
            run = await runtime.start_run()
            with runtime.activate_run(run):
                await engine.ingest(
                    [
                        doc("reset", "To reset a password, open Settings and choose Reset."),
                        doc("lockout", "Accounts lock after five failed password attempts."),
                        doc("billing", "Invoices are sent on the first day of each month."),
                    ],
                    scope=SCOPE,
                )
                result = await engine.retrieve("reset password", scope=SCOPE, top_k=2)
                await engine.delete(["billing"], scope=SCOPE)
            await runtime.finish_run(run)

            for record in run.evidence:
                print(f"evidence record: {record.summary} {record.metadata['counts']}")
            (retrieval,) = run.execution.retrievals
            status = retrieval.metadata["status"]
            print(f"retrieval: retriever={retrieval.retriever_id} status={status}")
            print(
                f"  query reference = request id: {retrieval.query_reference == result.request_id}"
            )
            store = runtime.registry.get(ProvenanceStore)
            if store is None:
                raise RuntimeError("the agxai runtime has no provenance store")
            # One ranked document per evidence item, in result order. Its content reference
            # is the evidence id, and the provenance links point at the source document uid.
            for document, item in zip(retrieval.documents, result.evidence, strict=True):
                links = await store.parents(item.evidence_id, context=run.execution.context)
                print(
                    f"  rank {document.rank}: {document.document_id} score={document.score:.4f} "
                    f"links={[link.relation for link in links]}"
                )
                matches = document.content_reference == item.evidence_id
                print(f"    content reference = evidence id: {matches}")
                same = [link.source_id for link in links] == [item.sources[0].document_uid]
                print(f"    linked to source uid: {same}")
    finally:
        await runtime.close()


if __name__ == "__main__":
    asyncio.run(main())
