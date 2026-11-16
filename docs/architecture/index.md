# Architecture

This section describes how RetrievalLoom is built. Read it to write
components, operate several engines, or reason about failures. For usage, see
the [User guide](../guide/index.md).

| Page | Covers |
| --- | --- |
| [Engine and components](engine.md) | Engine states, wiring, default components, ownership |
| [Metadata store](storage.md) | Tables, transactions, migrations, the SQLite implementation |
| [Index lifecycle](lifecycle.md) | Coordinator, indexer, generations, reconciliation, leases |
| [Retrieval execution](retrieval.md) | Modules and data flow of one `retrieve` call |
| [Concurrency and isolation](concurrency.md) | Event loop, offloading, limits, tenant isolation |

## Design rules

- **Async only.** Every public operation is a coroutine. Blocking work runs in
  threads through the offloader.
- **Contracts are ABCs.** Every replaceable part is an `abc.ABC` in
  `retrievalloom.contracts` or `retrievalloom.storage.base`. Built-in backends
  implement the same ABCs.
- **The metadata store is the source of truth.** Documents, versions,
  coverage, generations, tasks and leases live there. Indexes are derived and
  can be rebuilt.
- **Scope on every call.** Every store and index call carries a scope. A hit
  of another scope is a `TenantIsolationError`, never a degraded result.
- **Explicit degradation.** A stage that fails or is missing produces a
  warning code; it is never silently dropped.

## Package layout

| Package | Contents |
| --- | --- |
| `retrievalloom` | `RetrievalEngine`, configuration, errors, re-exported domain models |
| `domain` | Pydantic models: inputs, filters, records, reports, retrieval results |
| `contracts` | Component ABCs: models, indexes, graph, structure, infra |
| `storage` | `DocumentMetadataStoreABC`, the SQLite store, schema and Alembic migrations |
| `lifecycle` | Ingestion, derivation, indexer, coordinator, reader tracking |
| `retrieval` | Planning, snapshot, candidates, the pipeline |
| `evidence` | Fusion, normalisers, deduplication, reranking, consolidation, validation |
| `index` | Keyword, fuzzy, sparse and dense backends, tokenizer, embedding validation |
| `graph`, `structure` | LadybugDB and TreeSearch backends (extras) |
| `infra` | Cache, access policy, provenance recorders, metrics |
| `runtime` | The offloader |
| `integrations` | The agxai provenance recorder |
| `testing` | Contract kits |
