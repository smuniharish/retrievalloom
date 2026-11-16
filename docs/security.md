# Security

RetrievalLoom stores your documents, their derived indexes, and the evidence
it returns, so its stores are as sensitive as the source content. This page
lists what the library enforces by itself and what a deployment must still
decide.

## Built in

- **Scope isolation.** Every operation names a `Scope` (tenant, namespace, and
  collection). The metadata store binds each transaction to one scope and
  rejects rows from any other scope. Vector, keyword, fuzzy, sparse, graph, and
  structure queries are constrained by scope key and generation. Qdrant results
  are checked against the scope again after the search. On-disk paths use the
  hashed scope key, never raw tenant text.
- **Filters cannot widen a query.** Caller filters are a typed AST, never query
  text. In Qdrant they address only the document's `meta.*` payload, and they
  are always combined with the scope and generation conditions, never
  substituted for them.
- **Parameterized queries.** SQL is built with SQLAlchemy expressions, and graph
  (Cypher) queries pass values as bound parameters. The only interpolated
  values are validated integers and enumerations.
- **Access policy hook.** Every engine operation (`ingest`, `retrieve`,
  `delete`, `backfill`, `rebuild`, `status`) is first authorized by the
  configured `AccessPolicyABC`. A denial raises `AccessDeniedError`. The
  policy's `row_filter` is combined with the request's own filters before
  retrieval, and it is part of the result cache key, so principals with
  different row filters never share cached results.
- **No content in logs by default.** Structured logs carry identifiers, counts,
  states, and exception *types*, not document text or queries. Query text is
  logged only when you set `log_content=True`.
- **No ambient configuration.** The library reads no environment variables. A
  Qdrant API key is passed explicitly and goes only to the Qdrant client.
- **No silent model downloads.** `FlashRankReranker` accepts only known
  pairwise model names and loads from a local directory. It downloads only when
  you set `allow_download=True`. A missing model makes the reranker unavailable,
  and results carry a degradation warning.
- **Provenance is never fabricated.** Evidence keeps its source document,
  version, and representation. A failed provenance recording is reported as a
  warning, never hidden.

## Your responsibilities

| Area | What to do |
| --- | --- |
| Authorization | The default `AllowAllAccessPolicy` is **not** a security control: any caller that can reach the engine can use any scope. Authenticate callers in your application, pass their identity as `principal`, and supply an `AccessPolicyABC` that enforces your rules. |
| Scope choice | Derive the `Scope` from the authenticated identity, never from unvalidated request input. |
| Storage | Restrict access to the SQLite file, the Qdrant collections, and the graph and structure directories. Encrypt them at rest, and apply your retention period. `delete` hides a document from retrieval immediately, and the index lifecycle then removes its index entries. The metadata store keeps the document's versions and representations as a tombstone, and there is no hard-erase operation yet, so meet erasure obligations at the storage layer. |
| Qdrant | Run Qdrant with an API key and TLS when it is reachable over a network. Load the key from a secret store, never from source control. |
| Models | Embedding, reranker, and NLI models run in your process. Use weights from sources you trust, pin their revisions, and provision them ahead of time. |
| Logs | Keep `log_content` off in production unless your log pipeline is approved for document and query content. |
| Dependencies | Install only the extras you use, and pin versions in your application. |

## Reporting a vulnerability

Report vulnerabilities privately, as described in the repository's
[security policy](https://github.com/smuniharish/retrievalloom/blob/main/SECURITY.md).
Do not open a public issue.
