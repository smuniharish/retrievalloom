"""HTTP service: RetrievalLoom behind a FastAPI app with an engine lifespan.

Most deployments expose retrieval as a service. ``create_app`` builds a FastAPI app whose
lifespan opens one engine at startup and drains it at shutdown (in-flight requests finish,
the lifecycle scheduler stops). Endpoints:

- ``POST /ingest``  — documents for a tenant/collection, returns outcomes per document;
- ``POST /retrieve`` — query, ``top_k`` and equality filters, returns ranked evidence with
  sources and the methods that found it;
- ``DELETE /documents/{document_id}`` — tombstone a document;
- ``GET /status`` — documents, coverage and tasks of a tenant/collection;
- ``GET /health`` — engine and component health (HTTP 503 when unhealthy).

The caller identity comes from the ``X-Principal`` header and is checked by
``TenantRoleAccessPolicy`` ("<tenant>:<role>"), so a viewer cannot ingest and no tenant can
read another's data (HTTP 403). Library errors map to HTTP statuses (validation 422,
access 403, unavailable/timeouts 503).

A Postman collection for these endpoints is in ``examples/postman/``.

Run from the repository root::

    python examples/http_service.py           # demo: serve on a free port, call every endpoint
    python examples/http_service.py --serve   # serve on http://127.0.0.1:8000 until Ctrl+C
"""

import asyncio
import contextlib
import socket
import sys
from collections.abc import AsyncGenerator, Callable
from typing import Annotated, Any

import httpx
import uvicorn
from _shared import load, quiet_logs
from adapters import StemmingTokenizer, TenantRoleAccessPolicy
from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from retrievalloom import (
    AccessDeniedError,
    And,
    Components,
    DocumentInput,
    Eq,
    FilterExpression,
    LifecycleConfig,
    RerankingConfig,
    RetrievalConfig,
    RetrievalEngine,
    RetrievalLoomError,
    Scope,
    SourceDescriptor,
    TextRepresentation,
    ValidationError,
    create_retrieval_engine,
)

Principal = Annotated[str | None, Header(alias="X-Principal")]


class DocumentBody(BaseModel):
    id: str
    text: str = Field(min_length=1)
    title: str | None = None
    uri: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class IngestBody(BaseModel):
    tenant: str
    collection: str = "default"
    documents: list[DocumentBody] = Field(min_length=1)


class RetrieveBody(BaseModel):
    tenant: str
    collection: str = "default"
    query: str = Field(min_length=1)
    top_k: int = Field(default=5, ge=1, le=50)
    filters: dict[str, str | int | float | bool] = Field(default_factory=dict)


def default_engine() -> RetrievalEngine:
    return create_retrieval_engine(
        retrieval=RetrievalConfig(reranking=RerankingConfig(enabled=False)),
        components=Components(
            tokenizer=StemmingTokenizer(), access_policy=TenantRoleAccessPolicy()
        ),
        # The background scheduler indexes, retries and cleans up while the service runs.
        lifecycle=LifecycleConfig(background=True),
    )


def to_filter(filters: dict[str, Any]) -> FilterExpression | None:
    clauses = [Eq(field=field, value=value) for field, value in sorted(filters.items())]
    if not clauses:
        return None
    return clauses[0] if len(clauses) == 1 else And(clauses=tuple(clauses))


def create_app(make_engine: Callable[[], RetrievalEngine] = default_engine) -> FastAPI:
    @contextlib.asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncGenerator[None]:
        async with make_engine() as engine:
            app.state.engine = engine
            yield

    app = FastAPI(title="RetrievalLoom example service", lifespan=lifespan)

    def engine_of(request: Request) -> RetrievalEngine:
        return request.app.state.engine

    @app.exception_handler(RetrievalLoomError)
    async def library_error(request: Request, exc: RetrievalLoomError) -> JSONResponse:
        status = 500
        if isinstance(exc, AccessDeniedError):
            status = 403
        elif isinstance(exc, ValidationError):
            status = 422
        elif exc.retryable:
            status = 503
        body = {"code": exc.code, "message": exc.message, "details": exc.details}
        return JSONResponse(status_code=status, content=body)

    @app.post("/ingest")
    async def ingest(
        body: IngestBody, request: Request, principal: Principal = None
    ) -> dict[str, Any]:
        documents = [
            DocumentInput(
                _id=document.id,
                metadata=document.metadata,
                source=SourceDescriptor(uri=document.uri, title=document.title),
                representations=(TextRepresentation(text=document.text),),
            )
            for document in body.documents
        ]
        scope = Scope(tenant_id=body.tenant, collection=body.collection)
        report = await engine_of(request).ingest(documents, scope=scope, principal=principal)
        return {
            "documents": [
                {"id": d.document_id, "outcome": d.outcome.value, "version": d.version}
                for d in report.documents
            ],
            "warnings": [warning.code.value for warning in report.warnings],
        }

    @app.post("/retrieve")
    async def retrieve(
        body: RetrieveBody, request: Request, principal: Principal = None
    ) -> dict[str, Any]:
        scope = Scope(tenant_id=body.tenant, collection=body.collection)
        result = await engine_of(request).retrieve(
            body.query,
            scope=scope,
            filters=to_filter(body.filters),
            top_k=body.top_k,
            principal=principal,
        )
        return {
            "request_id": result.request_id,
            "status": result.status.value,
            "warnings": [warning.code.value for warning in result.warnings],
            "evidence": [
                {
                    "document_id": item.sources[0].document_id,
                    "version": item.sources[0].document_version,
                    "title": item.sources[0].title,
                    "uri": item.sources[0].source_uri,
                    "score": round(item.score, 6),
                    "text": item.display_text,
                    "methods": sorted({hit.method.key for hit in item.provenance.hits}),
                }
                for item in result.evidence
            ],
        }

    @app.delete("/documents/{document_id}")
    async def delete(
        document_id: str,
        tenant: str,
        request: Request,
        collection: str = "default",
        principal: Principal = None,
    ) -> dict[str, Any]:
        scope = Scope(tenant_id=tenant, collection=collection)
        report = await engine_of(request).delete([document_id], scope=scope, principal=principal)
        if not report.deleted:
            raise HTTPException(status_code=404, detail=f"document {document_id!r} not found")
        return {"deleted": list(report.deleted)}

    @app.get("/status")
    async def status(
        tenant: str, request: Request, collection: str = "default", principal: Principal = None
    ) -> dict[str, Any]:
        scope = Scope(tenant_id=tenant, collection=collection)
        report = await engine_of(request).status(scope=scope, principal=principal)
        return {
            "documents_active": report.documents_active,
            "documents_deleted": report.documents_deleted,
            "coverage": report.coverage,
            "tasks": report.tasks,
        }

    @app.get("/health")
    async def health(request: Request) -> JSONResponse:
        report = await engine_of(request).health()
        body = {
            "status": report.status.value,
            "components": {c.component: c.status.value for c in report.components},
        }
        return JSONResponse(status_code=503 if body["status"] == "unhealthy" else 200, content=body)

    return app


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


async def demo() -> None:
    """Serve on a free local port and call every endpoint like an HTTP client would."""
    port = free_port()
    server = uvicorn.Server(uvicorn.Config(create_app(), port=port, log_level="warning"))
    serving = asyncio.create_task(server.serve())
    while not server.started:  # noqa: ASYNC110 - uvicorn exposes no startup event
        await asyncio.sleep(0.05)
    editor, viewer = {"X-Principal": "acme:editor"}, {"X-Principal": "acme:viewer"}
    articles = [
        {
            "id": a["id"],
            "text": a["text"],
            "title": a["title"],
            "metadata": {
                "product": a["product"],
                "language": a["language"],
                "visibility": "public",
            },
        }
        for a in load("support_kb.json")
    ]
    try:
        async with httpx.AsyncClient(base_url=f"http://127.0.0.1:{port}", timeout=60) as client:
            response = await client.post(
                "/ingest",
                json={"tenant": "acme", "collection": "help", "documents": articles},
                headers=editor,
            )
            outcomes = sorted({d["outcome"] for d in response.json()["documents"]})
            print(f"POST /ingest -> {response.status_code} {len(articles)} documents {outcomes}")

            query = {
                "tenant": "acme",
                "collection": "help",
                "query": "refund a card payment",
                "top_k": 2,
            }
            response = await client.post(
                "/retrieve", json={**query, "filters": {"language": "en"}}, headers=viewer
            )
            body = response.json()
            print(f"POST /retrieve -> {response.status_code} status={body['status']}")
            for item in body["evidence"]:
                print(f"    {item['document_id']} v{item['version']} {item['methods']}")

            response = await client.post(
                "/ingest", json={"tenant": "acme", "documents": articles[:1]}, headers=viewer
            )
            print(f"POST /ingest as viewer -> {response.status_code} {response.json()['code']}")
            response = await client.post(
                "/retrieve", json=query, headers={"X-Principal": "globex:admin"}
            )
            code = response.json()["code"]
            print(f"POST /retrieve as another tenant -> {response.status_code} {code}")
            response = await client.post("/retrieve", json={**query, "top_k": 500}, headers=viewer)
            print(f"POST /retrieve top_k=500 -> {response.status_code}")

            response = await client.delete(
                "/documents/pay-refund",
                params={"tenant": "acme", "collection": "help"},
                headers=editor,
            )
            print(f"DELETE /documents/pay-refund -> {response.status_code} {response.json()}")
            response = await client.delete(
                "/documents/pay-refund",
                params={"tenant": "acme", "collection": "help"},
                headers=editor,
            )
            print(f"DELETE /documents/pay-refund again -> {response.status_code}")

            response = await client.get(
                "/status", params={"tenant": "acme", "collection": "help"}, headers=viewer
            )
            body = response.json()
            print(
                f"GET /status -> {response.status_code} active={body['documents_active']} "
                f"deleted={body['documents_deleted']}"
            )
            response = await client.get("/health")
            print(f"GET /health -> {response.status_code} {response.json()['status']}")
    finally:
        server.should_exit = True
        await serving
    print("server stopped, engine closed")


async def main() -> None:
    quiet_logs()
    if "--serve" in sys.argv:
        config = uvicorn.Config(create_app(), host="127.0.0.1", port=8000, log_level="info")
        await uvicorn.Server(config).serve()
    else:
        await demo()


if __name__ == "__main__":
    asyncio.run(main())
