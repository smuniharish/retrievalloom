"""Multi-tenancy: two tenants share one engine but never see each other's documents.

Every operation takes a ``Scope`` (tenant, namespace, collection). Documents, indexes,
coverage and lifecycle tasks are kept per scope, so the same document id can exist in
several scopes and a retrieval in one scope never returns another scope's evidence.

Run from the repository root::

    python examples/multi_tenant.py
"""

import asyncio

from _shared import quiet_logs

from retrievalloom import (
    DocumentInput,
    LifecycleConfig,
    RetrievalConfig,
    Scope,
    TenantPolicy,
    TextRepresentation,
    ValidationError,
    create_retrieval_engine,
)

ACME = Scope(tenant_id="acme", namespace="hr", collection="policies")
GLOBEX = Scope(tenant_id="globex", namespace="hr", collection="policies")


def doc(doc_id: str, text: str) -> DocumentInput:
    return DocumentInput(_id=doc_id, representations=(TextRepresentation(text=text),))


async def main() -> None:
    quiet_logs()
    # Per-tenant limits: globex may ingest at most two documents per call.
    retrieval = RetrievalConfig(tenant_policies={"globex": TenantPolicy(max_ingest_batch=2)})
    engine = create_retrieval_engine(
        retrieval=retrieval, lifecycle=LifecycleConfig(background=False)
    )
    async with engine:
        # Both tenants use the document id "vacation" with different content.
        await engine.ingest(
            [
                doc("vacation", "Acme employees get 30 vacation days per year."),
                doc("remote", "Acme staff may work remotely three days a week."),
            ],
            scope=ACME,
        )
        await engine.ingest(
            [doc("vacation", "Globex employees get 20 vacation days per year.")], scope=GLOBEX
        )

        for scope in (ACME, GLOBEX):
            result = await engine.retrieve("vacation days per year", scope=scope)
            print(f"{scope.tenant_id}: {len(result.evidence)} evidence item(s)")
            for item in result.evidence:
                tenants = {source.scope.tenant_id for source in item.sources}
                print(f"  {item.sources[0].document_id}: {item.display_text} (tenants={tenants})")

        # Remote work exists only for acme; globex gets nothing back.
        remote = await engine.retrieve("work remotely", scope=GLOBEX)
        print(f"globex 'work remotely': status={remote.status.value}")

        # Deleting acme's document id from the globex scope only affects globex.
        deleted = await engine.delete(["remote"], scope=GLOBEX)
        print(f"delete 'remote' in globex: deleted={deleted.deleted} not_found={deleted.not_found}")

        try:
            await engine.ingest(
                [doc(f"d{n}", f"Policy number {n}.") for n in range(3)], scope=GLOBEX
            )
        except ValidationError as exc:
            print(f"globex batch of 3 rejected: {exc.details}")

        for scope in (ACME, GLOBEX):
            status = await engine.status(scope=scope)
            print(
                f"status {scope.tenant_id}: active={status.documents_active} "
                f"deleted={status.documents_deleted} coverage={status.coverage}"
            )


if __name__ == "__main__":
    asyncio.run(main())
