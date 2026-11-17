# HTTP service

Most deployments run retrieval as a service that other applications call over HTTP.
`examples/http_service.py` is a FastAPI app with one long-lived engine, tenant-aware
authorization, and error mapping from library exceptions to HTTP statuses.

## How it works

- **Lifespan.** `create_app(make_engine)` builds the app. Its lifespan opens one engine at
  startup and closes it at shutdown, so in-flight requests finish and the background
  lifecycle scheduler stops cleanly. The engine runs with `background=True`, so index
  builds happen off the request path.
- **Identity.** Callers send `X-Principal: <tenant>:<role>`. `TenantRoleAccessPolicy`
  turns that into allow/deny decisions and visibility filters (see
  [Multi-tenant SaaS](multi-tenant-saas.md)).
- **Error mapping.** One exception handler maps `RetrievalLoomError`: `AccessDeniedError`
  → 403, `ValidationError` → 422, retryable errors (unavailable, timeouts) → 503, and
  anything else → 500. The body is `{"code", "message", "details"}`.
- **Validation.** Pydantic models check request shapes. `top_k` is bounded to 1–50 and
  filters are equality matches on metadata, translated to `Eq`/`And` expressions.

| Endpoint | Body or parameters | Returns |
| --- | --- | --- |
| `POST /ingest` | `{"tenant", "collection", "documents": [{"id", "text", "title", "uri", "metadata"}]}` | outcome per document and warnings |
| `POST /retrieve` | `{"tenant", "collection", "query", "top_k", "filters"}` | status, ranked evidence with sources and methods, warnings |
| `DELETE /documents/{id}` | `?tenant=&collection=` | deleted ids, or 404 for an unknown document |
| `GET /status` | `?tenant=&collection=` | document counts, index coverage and lifecycle tasks |
| `GET /health` | none | engine and component health; 503 when unhealthy |

`create_app` takes the engine factory as a parameter, so tests and deployments can pass
an engine with real models and stores without changing the app.

## Requirements

The `examples` dependency group (`fastapi`, `uvicorn`, `httpx`).

## Run

```bash
python examples/http_service.py           # demo: serve on a free port and call every endpoint
python examples/http_service.py --serve   # serve on http://127.0.0.1:8000 until Ctrl+C
```

With the server running:

```bash
curl -X POST http://127.0.0.1:8000/ingest -H "X-Principal: acme:editor" \
  -H "Content-Type: application/json" \
  -d '{"tenant": "acme", "collection": "help", "documents": [{"id": "pay-refund", "text": "Refunds go back to the original card within 5 business days.", "metadata": {"language": "en", "visibility": "public"}}]}'
curl -X POST http://127.0.0.1:8000/retrieve -H "X-Principal: acme:viewer" \
  -H "Content-Type: application/json" \
  -d '{"tenant": "acme", "collection": "help", "query": "refund to card", "filters": {"language": "en"}}'
curl "http://127.0.0.1:8000/status?tenant=acme&collection=help" -H "X-Principal: acme:viewer"
curl http://127.0.0.1:8000/health
```

## Postman

Import `examples/postman/retrievalloom.postman_collection.json`. The collection variables
are `baseUrl` (default `http://127.0.0.1:8000`), `tenant` and `collection`. Run the
requests in order against `--serve`:

1. Health
2. Ingest as editor
3. Retrieve as viewer, with a language filter
4. Ingest as viewer (403)
5. Retrieve as another tenant (403)
6. Delete a document
7. Status

Each request has test scripts that check the status code and response body, so the
Collection Runner reports pass/fail.

## Source

<!-- fmt: off -->
```python title="examples/http_service.py"
--8<-- "examples/http_service.py"
```
<!-- fmt: on -->

## Expected output

```text
POST /ingest -> 200 10 documents ['created']
POST /retrieve -> 200 status=complete
    pay-refund v1 ['index.fuzzy', 'index.sparse']
    pay-failed-card v1 ['index.fuzzy', 'index.sparse']
POST /ingest as viewer -> 403 access_denied
POST /retrieve as another tenant -> 403 access_denied
POST /retrieve top_k=500 -> 422
DELETE /documents/pay-refund -> 200 {'deleted': ['pay-refund']}
DELETE /documents/pay-refund again -> 404
GET /status -> 200 active=9 deleted=1
GET /health -> 200 healthy
server stopped, engine closed
```

`tests/examples/test_examples.py` also drives the app in-process through
`httpx.ASGITransport` and checks every endpoint, including the error mapping.
