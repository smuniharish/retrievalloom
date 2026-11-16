# Metadata store

The metadata store is the source of truth. Indexes are derived from it and can
be rebuilt from it at any time.

## Contract

`DocumentMetadataStoreABC` (`retrievalloom.storage.base`) is a component with
three extra methods:

- `migrate(policy)`: bring the schema to the current revision (`auto`) or check
  that it is current (`verify`).
- `schema_version()`: the current revision, or `None`.
- `transaction(scope=None, *, write)`: open a `MetadataTransactionABC`, used as
  `async with`.

A transaction opened with a scope rejects any row of another scope with
`TenantIsolationError`. Lifecycle maintenance opens unscoped transactions to
work across scopes.

The transaction methods group into: scopes and commit sequence, documents,
versions and representations, ingestion events, derived objects and their
contributions and lineage, visibility, coverage, generations and generation
pointers, tasks, reader leases, engine instances, and refresh-engine state. See
the [Storage API reference](../api/storage.md).

Use `MetadataStoreContract` from `retrievalloom.testing` to check an
implementation.

## Tables

| Table | Holds |
| --- | --- |
| `rl_scopes` | Known scopes. |
| `rl_documents` | Current state of each document: version, appearance counter, state, content hash, metadata, revisions. |
| `rl_document_versions` | Every version with its content hash and metadata. |
| `rl_representations` | Representations of each version. |
| `rl_ingestion_events` | One row per appearance; used for idempotency and redelivery detection. |
| `rl_objects` | Derived objects: chunks, structure nodes, graph entities and relationships. |
| `rl_object_contributions` | Which document versions contribute to each object (graph aggregates are shared). |
| `rl_lineage` | Lineage edges between objects and their sources. |
| `rl_visibility` | Per version and unit, the commit-sequence range in which it is visible. |
| `rl_commit_seq` | The store-wide commit sequence. |
| `rl_generations` | Index generations and their state. |
| `rl_generation_pointers` | The published generation per (scope, unit, instance). |
| `rl_coverage` | Per document and unit: target version, indexed version, state, attempts, last error. |
| `rl_tasks` | Backfill and rebuild tasks. |
| `rl_reader_leases` | Durable reader leases per generation and engine instance. |
| `rl_engine_instances` | Registered engine instances and their heartbeats. |
| `rl_refresh_state` | refresh-engine resource state. |

Rows of ephemeral units carry the engine instance id. Durable units use the
shared instance (empty id).

## SQLite implementation

`SQLiteMetadataStore` (`retrievalloom.storage.sqlite`) is the only built-in
store. It uses SQLAlchemy async Core with aiosqlite, and Alembic migrations
shipped in the package (`storage/migrations`).

```python
SQLiteMetadataStore(path=None, *, synchronous="NORMAL", busy_timeout_ms=30000, engine=None)
```

- `path=None` keeps the database in memory, for one engine only.
- A file database uses WAL journaling. Write transactions start with
  `BEGIN IMMEDIATE`, so there is one writer at a time and readers are not
  blocked.
- `synchronous="FULL"` trades write speed for durability on power loss.
- `busy_timeout_ms` is how long a writer waits for the lock.
- `engine` accepts your own SQLAlchemy `AsyncEngine`.

Several engines, in one or several processes on one host, can share one file
database. See [Run several engines on one store](../guide/how-to/multiple-engines.md).

There is no PostgreSQL or other server-backed store yet. Implement
`DocumentMetadataStoreABC` to use another database.
