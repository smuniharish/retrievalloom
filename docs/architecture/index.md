# Architecture

Learn how RetrievalLoom is built, so you can write components, operate several
engines and reason about failures with confidence. For usage, see
the [User guide](../guide/index.md).

| Page | Covers |
| --- | --- |
| [Engine and components](engine.md) | Engine states, wiring, default components, ownership |
| [Metadata store](storage.md) | Tables, transactions, migrations, the SQLite and PostgreSQL implementations |
| [Index lifecycle](lifecycle.md) | Coordinator, indexer, generations, reconciliation, leases |
| [Retrieval execution](retrieval.md) | Modules and data flow of one `retrieve` call |
| [Concurrency and isolation](concurrency.md) | Event loop, offloading, limits, tenant isolation |

![RetrievalLoom architecture: your application ingests documents and sends queries to the engine, which keeps the metadata store and the index, graph and structure channels in step and returns ranked, cited evidence](../assets/diagrams/architecture-overview.png){ .diagram }

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
- **Explicit degradation.** A stage that fails, or that a request asked for
  and cannot run, produces a warning code instead of disappearing silently.

## Package layout

| Package | Contents |
| --- | --- |
| `retrievalloom` | `RetrievalEngine`, configuration, errors, re-exported domain models |
| `domain` | Pydantic models: inputs, filters, records, reports, retrieval results |
| `contracts` | Component ABCs: models, indexes, graph, structure, infra |
| `storage` | `DocumentMetadataStoreABC`, the SQLite and PostgreSQL stores, schema and Alembic migrations |
| `lifecycle` | Ingestion, derivation, indexer, coordinator, reader tracking |
| `retrieval` | Planning, snapshot, candidates, the pipeline |
| `evidence` | Fusion, normalizers, deduplication, reranking, consolidation, validation |
| `index` | Keyword, fuzzy, sparse and dense backends, tokenizer, embedding validation |
| `graph`, `structure` | LadybugDB and TreeSearch backends (extras) |
| `infra` | Cache, access policy, provenance recorders, metrics |
| `runtime` | The offloader |
| `integrations` | The agxai provenance recorder |
| `testing` | Contract kits |
