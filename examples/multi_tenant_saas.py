"""Multi-tenant SaaS: role-based access, tenant quotas and audit logging on one engine.

A SaaS product serves many customers (tenants) from one engine. Each tenant has several
collections, documents carry a ``visibility`` label, and callers authenticate as
``"<tenant>:<role>[:<user>]"``. The example plugs in three application components:

- ``TenantRoleAccessPolicy`` (``AccessPolicyABC``): a principal may only act inside its
  own tenant, roles grant actions (viewer: retrieve/status, editor: + ingest/delete,
  admin: everything) and a row filter hides ``internal``/``restricted`` documents from
  lower roles. Denials raise ``AccessDeniedError`` before any work is done;
- ``JsonlProvenanceRecorder`` (``ProvenanceRecorderABC``): an append-only JSONL audit log
  of every ingestion, deletion and retrieval;
- ``StemmingTokenizer`` (``TokenizerABC``): English stemming and stop-words, so "holidays"
  matches "holiday".

``TenantPolicy`` adds per-tenant quotas (batch size, ``top_k``, allowed channels).

Run from the repository root (no services needed)::

    python examples/multi_tenant_saas.py
"""

import asyncio
import json
import tempfile
from pathlib import Path

from _shared import doc_ids, load, quiet_logs
from adapters import JsonlProvenanceRecorder, StemmingTokenizer, TenantRoleAccessPolicy

from retrievalloom import (
    AccessDeniedError,
    Components,
    DocumentInput,
    LifecycleConfig,
    RerankingConfig,
    RetrievalConfig,
    Scope,
    TenantPolicy,
    TextRepresentation,
    ValidationError,
    create_retrieval_engine,
)


def scope(tenant: str, collection: str) -> Scope:
    return Scope(tenant_id=tenant, collection=collection)


def documents(rows: list[dict[str, str]]) -> list[DocumentInput]:
    return [
        DocumentInput(
            _id=row["id"],
            metadata={"visibility": row["visibility"]},
            representations=(TextRepresentation(text=row["text"]),),
        )
        for row in rows
    ]


async def main() -> None:
    quiet_logs()
    data = load("saas_tenants.json")
    with tempfile.TemporaryDirectory() as tmp:
        audit = JsonlProvenanceRecorder(Path(tmp) / "audit.jsonl")
        engine = create_retrieval_engine(
            retrieval=RetrievalConfig(
                reranking=RerankingConfig(enabled=False),
                tenant_policies={"globex": TenantPolicy(max_ingest_batch=2, max_top_k=3)},
            ),
            components=Components(
                access_policy=TenantRoleAccessPolicy(),
                provenance_recorder=audit,
                tokenizer=StemmingTokenizer(),
            ),
            lifecycle=LifecycleConfig(background=False),
        )
        async with engine:
            for tenant, collections in data.items():
                for collection, rows in collections.items():
                    report = await engine.ingest(
                        documents(rows),
                        scope=scope(tenant, collection),
                        principal=f"{tenant}:admin",
                    )
                    print(f"{tenant}/{collection}: ingested {len(report.documents)} document(s)")

            print("\nrole-based row filters (acme/handbook, 'holidays expenses salary'):")
            handbook = scope("acme", "handbook")
            for role in ("viewer", "editor", "admin"):
                result = await engine.retrieve(
                    "holidays expenses salary", scope=handbook, principal=f"acme:{role}:dana"
                )
                print(f"  {role:<6} sees {sorted(doc_ids(result.evidence))}")

            print("\ncollection and tenant isolation ('holiday' as acme:admin / globex:admin):")
            for tenant, collection in (
                ("acme", "handbook"),
                ("acme", "engineering"),
                ("globex", "handbook"),
            ):
                result = await engine.retrieve(
                    "holiday", scope=scope(tenant, collection), principal=f"{tenant}:admin", top_k=1
                )
                (item,) = result.evidence
                print(f"  {tenant}/{collection}: {(item.display_text or '')[:60]}...")

            print("\ndenied operations:")
            attempts = [
                (
                    "globex admin reads acme",
                    engine.retrieve("holiday", scope=handbook, principal="globex:admin"),
                ),
                (
                    "acme viewer ingests",
                    engine.ingest(
                        documents(data["acme"]["handbook"][:1]),
                        scope=handbook,
                        principal="acme:viewer",
                    ),
                ),
                (
                    "acme editor rebuilds",
                    engine.rebuild(scope=handbook, channel="index", principal="acme:editor"),
                ),
                ("anonymous retrieval", engine.retrieve("holiday", scope=handbook)),
            ]
            for label, call in attempts:
                try:
                    await call
                    print(f"  {label}: allowed (unexpected)")
                except AccessDeniedError as exc:
                    print(f"  {label}: denied {exc.details['action']} ({exc.details['reason']})")

            print("\nglobex quotas:")
            globex = scope("globex", "handbook")
            quota_checks = [
                (
                    "ingest 3 documents",
                    engine.ingest(
                        documents(data["acme"]["handbook"]), scope=globex, principal="globex:admin"
                    ),
                ),
                (
                    "retrieve top_k=5",
                    engine.retrieve("holiday", scope=globex, principal="globex:admin", top_k=5),
                ),
            ]
            for label, call in quota_checks:
                try:
                    await call
                    print(f"  {label}: accepted (unexpected)")
                except ValidationError as exc:
                    print(f"  {label}: rejected {exc.details}")

            status = await engine.status(scope=handbook, principal="acme:viewer")
            print(f"\nacme/handbook status (viewer may read it): active={status.documents_active}")

        records = [json.loads(line) for line in audit.path.read_text(encoding="utf-8").splitlines()]
        events: dict[str, int] = {}
        for record in records:
            events[record["event"]] = events.get(record["event"], 0) + 1
        print(f"audit log: {len(records)} record(s) {dict(sorted(events.items()))}")
        last = records[-1]
        print(
            f"  last: event={last['event']} tenant={last['tenant']} "
            f"principal={last['principal']} query={last['query']!r}"
        )


if __name__ == "__main__":
    asyncio.run(main())
