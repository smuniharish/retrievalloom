# Security

Deploy RetrievalLoom with confidence that one customer never sees another's
data, and know exactly which decisions remain yours. RetrievalLoom stores your
documents and the indexes built from them, so its stores are as sensitive as
the source content. This page lists what the library enforces by itself, what
your deployment must still decide, and how stored data is erased.

## What RetrievalLoom enforces

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
  `delete`, `purge`, `backfill`, `rebuild`, `status`) is first authorized by the
  configured `AccessPolicyABC` (`purge` uses the `delete` action). A denial
  raises `AccessDeniedError`. The
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

The library cannot know who your callers are or where your data lives. Decide
these in your deployment:

| Area | What to do |
| --- | --- |
| Authorization | The default `AllowAllAccessPolicy` is **not** a security control: any caller that can reach the engine can use any scope. Authenticate callers in your application, pass their identity as `principal`, and supply an `AccessPolicyABC` that enforces your rules. |
| Scope choice | Derive the `Scope` from the authenticated identity, never from unvalidated request input. |
| Storage | Restrict access to the SQLite file, the Qdrant collections, and the graph and structure directories. Encrypt them at rest. See [Erasure and retention](#erasure-and-retention) for how stored data is removed. |
| Qdrant | Run Qdrant with an API key and TLS when it is reachable over a network. Load the key from a secret store, never from source control. |
| Models | Embedding, reranker, and NLI models run in your process. Use weights from sources you trust, pin their revisions, and provision them ahead of time. |
| Logs | Keep `log_content` off in production unless your log pipeline is approved for document and query content. |
| Dependencies | Install only the extras you use, and pin versions in your application. |

## Erasure and retention

You can honor erasure requests completely: `purge` removes a document from
every index and from the metadata store.

- **`delete` is a tombstone.** It hides a document from retrieval at once, and
  the index lifecycle then removes its index entries. The metadata store keeps
  its versions, representations, events and objects.
- **`purge` erases.** `await engine.purge(ids, scope=..., principal=...)`
  removes the documents' objects from every generation of this engine's
  indexes, then deletes every metadata row of the documents, in batches of up
  to 500 documents per write transaction. It never reports a document as
  purged while a backend removal failed: on a backend error nothing of the
  batch is deleted from the metadata store and the call raises, so it can be
  retried. It is idempotent.
- **Other engines follow.** The same transaction appends one entry per
  document to a purge log in the metadata store. Every engine sharing the
  store reads the log at the start of each lifecycle round and removes the
  purged objects from its own in-memory indexes (see
  [eviction latency](#eviction-latency) below).
- **Tombstones expire.** Maintenance erases deleted documents the same way
  after `LifecycleConfig.purge_after` (default 7 days), once every index unit
  has removed them.
- **History is bounded.** `max_versions` (default 10) caps each document's
  version history and `event_retention` (default 90 days) caps ingestion
  events. A version named by a retained ingestion event is kept until the
  event expires. Purge-log entries are kept for `purge_log_retention`
  (default 1 day). Set them to match your retention policy.

### Eviction latency

Retrieval never returns purged documents from any engine, because evidence is
hydrated from the metadata store. Their index entries leave the process memory
of other engines as follows:

- With `background=True` (the default), within about `poll_interval`
  (default 2 s) plus the duration of the current lifecycle round. Eviction
  waits for an index build of the same unit to finish.
- With `background=False`, at the engine's next lifecycle call: its own
  `ingest`, `delete` or `purge` (unless `index_wait="none"`), `wait_for`, or
  `run_lifecycle_once()`. Call `run_lifecycle_once()` periodically.
- An engine that misses entries because they were pruned before it applied
  them (it ran no round for `purge_log_retention`) rebuilds its in-memory
  indexes of the scope from the metadata store instead.

Limits to plan for:

- A graph aggregate that other documents still support stays in the active
  graph generation until the queued graph rebuild publishes a new one. Wait
  for the task ids in `PurgeReport.task_ids`.
- Backups, SQLite WAL files and freed database pages can still contain erased
  data. Rotate backups, and run `VACUUM` on the SQLite file when erased bytes
  must leave the disk.
- Erasure covers what RetrievalLoom stores. Provenance recorders, caches you
  supply, and logs with `log_content=True` are outside it.

## Reporting a vulnerability

Report vulnerabilities privately, as described in the repository's
[security policy](https://github.com/smuniharish/retrievalloom/blob/main/SECURITY.md).
Do not open a public issue.
