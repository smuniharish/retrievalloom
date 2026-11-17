# Concurrency and isolation

The engine runs on one event loop, shares it with your application, and may
share its metadata store with other engines. This page describes how it keeps
the loop responsive, which limits bound concurrent work, what cancellation
guarantees, and how scopes and engines stay isolated from each other.

## The event loop

All public methods are coroutines and are meant to run on one event loop. The
engine never blocks it:

- Blocking calls (index builds and queries, local Qdrant, LadybugDB,
  TreeSearch, FlashRank, embedding validation) run in threads through the
  [offloader](../guide/concepts/offloading.md).
- Backends that are not thread-safe (local Qdrant, LadybugDB, TreeSearch's
  SQLite) use a dedicated single-thread executor, still through the offloader
  for admission and fair share.
- Async clients (Qdrant server mode, aiosqlite) are awaited directly.

There are no sync wrappers. To call the engine from synchronous code, run it
on a loop you own (for example in a dedicated thread) and submit coroutines
with `asyncio.run_coroutine_threadsafe`.

## Limits

| Limit | Setting | Effect |
| --- | --- | --- |
| Requests per tenant | `TenantPolicy.max_concurrent_requests` (16) | Extra requests wait for a slot until their deadline, then raise `OperationTimeoutError`. |
| Offloaded jobs per tenant | `TenantPolicy.max_concurrent_cpu_jobs` (4) | Extra jobs wait for a slot. |
| Offloaded jobs overall | `OffloaderConfig.max_workers` (8) and resourcera admission | Jobs wait for capacity up to `admission_wait_timeout`, then raise `ResourceAdmissionError`. |
| CPU-bound offloaded jobs overall | `OffloaderConfig.max_cpu_jobs` (no cap) and `workload_limits` (none) | Jobs over the cap wait on the loop for a slot. Bounds GIL contention from code that holds the GIL. |
| Units per lifecycle round | `LifecycleConfig.max_concurrency` (4) | Bounds parallel indexing. |
| Request duration | `request_timeout`, `method_timeout` | See [Keep the event loop responsive](../guide/how-to/event-loop-latency.md). |

Per-tenant overrides go in `RetrievalConfig.tenant_policies`.

## Event-loop lag under load

Worker threads and the loop thread share the GIL, so offloading alone does not
make the loop's wake-ups free:

- The largest built-in CPU stage, fuzzy matching, releases the GIL while it
  scores (`rapidfuzz.process.cdist`).
- Offloaded code that holds the GIL (pure-Python loops in your components)
  delays the loop more with every thread running it.
  `OffloaderConfig.max_cpu_jobs` bounds that; see
  [Capping CPU-bound jobs](../guide/concepts/offloading.md#capping-cpu-bound-jobs)
  for measurements.
- Full garbage collections run on the loop thread. With 16 concurrent
  retrievals over 2,000 documents they were the largest stalls measured
  (60–80 ms each); `gc.freeze()` after start-up removed most of them. See
  [Keep the event loop responsive](../guide/how-to/event-loop-latency.md).

## Cancellation

Cancelling a `retrieve`, `ingest` or `delete` is safe:

- Offloader permits (tenant, workload-kind and CPU slots, and the resourcera
  lease) are released when the thread finishes, not when the awaiting
  coroutine is cancelled, so capacity is never over-committed.
- Snapshot readers and leases are released in `finally` blocks.
- An indexing round started by a write is not cancelled when the write stops
  waiting (`index_wait_timeout`); `close()` waits for it.

## Several engines

Several `RetrievalEngine` objects can share one metadata store: a SQLite file
for processes on one host, or a PostgreSQL database for engines on any number
of hosts.

- Writes of one scope run one at a time: SQLite has a single writer
  (`BEGIN IMMEDIATE`), and PostgreSQL takes a per-scope advisory lock.
  Optimistic row revisions detect conflicting updates.
- Generation pointers move by compare-and-swap, so two engines building the
  same unit cannot both publish over the same predecessor.
- Every engine registers an instance row with a heartbeat. Rows of instances
  that stop heartbeating for `instance_ttl` are purged, and their unfinished
  builds are abandoned.
- Durable reader leases keep shared generations alive while another engine
  reads them.
- Embedded backend files have one owner process. A second process's
  `LadybugGraphStore` on the same file fails at startup with
  `BackendUnavailableError`; share a graph through one owner process or a
  `GraphStoreABC` on a server database such as Neo4j.

See [Run several engines on one store](../guide/how-to/multiple-engines.md).

## Tenant isolation

- A `Scope` (tenant, namespace, collection) is required on every operation.
- Metadata transactions opened for a scope reject rows of another scope.
- Every index backend call carries the scope; built-in backends store the
  scope key with every item (or use one database per scope and generation).
- Results are validated before they are returned: any object of another scope
  raises `TenantIsolationError`. This error is never degraded into a warning.
- The access policy (`AccessPolicyABC`) authorizes each operation and can add
  a row filter. The default allows everything; replace it in production. See
  [Filter and authorize](../guide/how-to/filters-and-access.md).
