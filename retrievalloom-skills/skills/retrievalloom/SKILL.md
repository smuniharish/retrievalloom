---
name: retrievalloom
description: >-
  Use this skill when adding RetrievalLoom (the retrievalloom Python package)
  to an application, or when working with it already in a project: hybrid
  retrieval for RAG, search, or agents with keyword, fuzzy, sparse (BM25), and
  dense methods plus optional graph and structure channels; ingesting,
  updating, deleting, purging, and retrieving documents per tenant scope;
  metadata filters and access policies; keeping data on disk with SQLite,
  PostgreSQL, or Qdrant; background or foreground indexing, backfill, and
  rebuild; reranking and NLI claim checks; logging and agxai provenance; and
  results that come back partial or empty, warnings, and errors. Do not use
  it to implement a new embedding model, reranker, index backend, vector
  store, metadata store, cache, or access policy class against RetrievalLoom's
  base classes (use retrievalloom-extend), or for a different retrieval
  library such as LangChain retrievers or LlamaIndex.
license: Apache-2.0
compatibility: >-
  Python 3.12 to 3.14. Installs retrievalloom 0.2.x from PyPI; the graph,
  structure, and PostgreSQL features need the graph, structure, and postgres
  extras. scripts/verify_setup.py runs offline in the project's Python
  environment and never downloads models.
metadata:
  version: "0.2.0"
  documentation: "https://retrievalloom.readthedocs.io"
---

# RetrievalLoom

RetrievalLoom finds the evidence an application needs for a query and shows
where each piece came from. One call runs several retrieval methods side by
side, so it finds the exact error code, the misspelled product name, and the
paraphrased question. Each tenant's documents stay in their own scope, and
every result reports what it could not do.

## Workflow

Copy this checklist and keep it current:

```text
- [ ] 1. verify_setup.py passes
- [ ] 2. One engine per process, opened with async with, configured once
- [ ] 3. Every call passes the caller's Scope (and principal with a policy)
- [ ] 4. Callers check result.status and result.warnings
- [ ] 5. Storage and lifecycle chosen for the deployment
- [ ] 6. Project tests and verify_setup.py pass again
```

1. From the project's Python environment, run this skill's setup check:
   `python scripts/verify_setup.py`. Fix every `FAIL` line first. It ingests
   and retrieves with an in-memory engine. Add `--require graph`,
   `--require structure`, or `--require postgres` for each extra the project
   uses, and `--json` for one JSON object. Set `RETRIEVALLOOM_DSN` or
   `RETRIEVALLOOM_QDRANT_URL` to also check those servers.
2. Install only what is used: `pip install retrievalloom`, plus
   `"retrievalloom[graph]"`, `"retrievalloom[structure]"`, or
   `"retrievalloom[postgres]"`. Create one engine and open it once:

   ```python
   from retrievalloom import DocumentInput, Scope, SourceDescriptor, TextRepresentation
   from retrievalloom import create_retrieval_engine

   engine = create_retrieval_engine()  # in memory: keyword, fuzzy, sparse
   async with engine:
       scope = Scope(tenant_id="acme")
       await engine.ingest(
           [
               DocumentInput(
                   id="vpn",
                   source=SourceDescriptor(uri="kb://vpn"),
                   metadata={"team": "network"},
                   representations=[TextRepresentation(text="Reset the VPN with ...")],
               )
           ],
           scope=scope,
       )
       result = await engine.retrieve("vpn reset", scope=scope, top_k=5)
   ```

   Pass an embedding model as `Components(embedding=...)` to add the dense
   method; the `retrievalloom-extend` skill covers writing one.
3. Derive the `Scope` from the authenticated caller, never from request
   input. With an access policy, pass `principal=` to every call.
4. Read `result.status` before using `result.evidence`. `partial` means a
   method, reranking, or NLI did not run fully; `result.warnings` says which
   and why. Cite `evidence.sources[0].document_id` and `source_uri`.
5. Choose storage and lifecycle from
   [references/storage.md](references/storage.md) and
   [references/lifecycle.md](references/lifecycle.md). Copy
   `assets/test_retrieval_behavior.py` into the project's tests and replace
   `make_engine` with the project's engine factory.
6. Run the project's tests, then `python scripts/verify_setup.py` again. Do
   not report the task done until this second run passes.

## Read more only when needed

| Task | Read |
| --- | --- |
| Exact signatures, inputs, reports, results, and errors | [references/API.md](references/API.md) |
| Channels, methods, fusion, timeouts, tenant limits, enabling a method later | [references/configuration.md](references/configuration.md) |
| Background or foreground indexing, `wait_for`, backfill, rebuild, startup | [references/lifecycle.md](references/lifecycle.md) |
| `data_dir`, SQLite, PostgreSQL, Qdrant, several engines, retention | [references/storage.md](references/storage.md) |
| Scopes, metadata filters, access policies, per-tenant limits | [references/access-and-tenancy.md](references/access-and-tenancy.md) |
| Reranking with FlashRank or your model, NLI claims | [references/rerank-and-nli.md](references/rerank-and-nli.md) |
| Logs, health, status, agxai provenance, tracing | [references/observability.md](references/observability.md) |
| What degrades, what raises, and what never degrades | [references/failure-modes.md](references/failure-modes.md) |
| A warning code, an error, or an unexpected result | [references/troubleshooting.md](references/troubleshooting.md) |

## Rules

- **Scope every call.** Each `ingest`, `delete`, `purge`, `retrieve`, and
  `status` call takes the caller's `Scope`. Never widen it to search across
  tenants, and never put tenant ids in metadata filters instead.
- **Report degradation.** Never treat a `partial` result as complete or hide
  its warnings from the caller. Never catch `TenantIsolationError`.
- **Never download models implicitly.** Keep `RerankingConfig.allow_download`
  off in production; provision model files ahead of time.
- **Keep content out of logs.** Leave `log_content=False` unless the user
  asks for query and document text in logs.
- **Own what you create.** Close components and resources you pass to the
  engine yourself; the engine closes only what it created.

## Gotchas

- `RetrievalEngine(config, components, resources)` takes one `EngineConfig`
  as `config` and has no `ingestion=`, `retrieval=`, or `lifecycle=`
  keyword. Put them in `EngineConfig(...)`, or pass them to
  `create_retrieval_engine(...)`.
- Configuration models are frozen and reject unknown fields. Build them once.
- The keyword method matches only terms listed under the `keywords` metadata
  field. Without them it returns nothing, which is expected.
- Dense needs an embedding model. Without one, results carry an `info`
  `method_unavailable` warning and stay `complete`.
- With the default background lifecycle and a `data_dir`, the first requests
  after a restart are `partial` with `index_missing` until the in-memory
  indexes are rebuilt. Use `LifecycleConfig(wait_for_rebuild=True)` or call
  `await engine.run_lifecycle_once(scope)` after opening.
- `ingest` waits for one indexing round by default (`index_wait="all"`). With
  `index_wait="none"`, documents become searchable later.
- `delete` hides documents at once and keeps their history. `purge` erases
  them physically; use it for erasure requests.
- Reranking is on by default but skipped silently when the FlashRank model
  files are missing. Results keep the fused order.
- Disabling every channel fails when `ChannelsConfig` is built. Enabling only
  methods that have no backend, such as `dense` without an embedding model,
  raises `ConfigurationError` when the engine is built.
- A Qdrant local-mode directory or a LadybugDB graph file opens in one
  process only. Use servers for several processes.

## Files

- `scripts/verify_setup.py`: offline environment and behavior check, with
  optional PostgreSQL and Qdrant probes. Run it with `--help` for options.
- `assets/test_retrieval_behavior.py`: pytest tests for scope isolation,
  filters, citations, deletion, and erasure. Copy it into the project.
