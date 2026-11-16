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

## Implementations

Both built-in stores share one implementation of every query,
`SQLAlchemyMetadataStore` (`retrievalloom.storage.sql`), on SQLAlchemy async
Core, and the Alembic migrations shipped in the package (`storage/migrations`).
A dialect subclass supplies the engine, transaction isolation and locking, the
dialect `INSERT ... ON CONFLICT` used for upserts, and the mapping of driver
errors to RetrievalLoom errors.

### SQLite

`SQLiteMetadataStore` (`retrievalloom.storage.sqlite`) is the default store,
on aiosqlite.

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

### PostgreSQL

`PostgresMetadataStore` (`retrievalloom.storage.postgres`, `postgres` extra) is
on asyncpg. Engines on any number of hosts can share it. See
[A shared PostgreSQL store](../guide/how-to/durable-storage.md#a-shared-postgresql-store).

```python
PostgresMetadataStore(
    dsn=None, *, schema=None, pool_size=5, max_overflow=10, pool_timeout=30.0,
    pool_recycle=1800, pool_pre_ping=True, lock_timeout_ms=30000,
    statement_timeout_ms=None, application_name="retrievalloom",
    connect_args=None, engine=None,
)
```

Concurrency:

- Write transactions run at `READ COMMITTED` and take transaction-level
  advisory locks right after `BEGIN`. A write bound to a scope takes a global
  lock in shared mode, then its scope's lock exclusively. Writers of one scope
  therefore run one at a time, which is the guarantee SQLite's single writer
  gives, and different scopes write in parallel. An unscoped write
  (maintenance) takes the global lock exclusively.
- Every check-then-write a transaction does (revision checks, lease and
  generation fencing, task claims, generation publication) runs under its
  scope's lock, so it cannot race another engine. Locks are always taken in
  the same order (global, then scope), so they cannot deadlock each other.
- The commit sequence is one row bumped by an atomic upsert. Its row lock
  orders commit sequences by commit time.
- Read transactions are `REPEATABLE READ READ ONLY` snapshots. They never wait
  for writers.
- Inside one engine, writers of one scope also queue on an `asyncio.Lock`, so
  they do not hold pooled connections while they wait for the database lock.

Errors:

| SQLSTATE | Raised as |
| --- | --- |
| `40001`, `40P01`, class `23` | `VersionConflictError` (retryable; the engine retries the transaction) |
| `55P03`, `57014` (lock or statement timeout) | `BackendTimeoutError` |
| classes `08`, `53`, `57`, `58`, network errors, pool exhausted | `BackendUnavailableError` (retryable) |
| classes `28`, `3D` (authentication, unknown database) | `BackendUnavailableError` (not retryable) |
| class `22` (invalid value) | `ValidationError` |

Types: timestamps are `timestamptz` in UTC, and JSON columns use `json`, which
keeps key order as SQLite does. PostgreSQL is stricter than SQLite in three
ways:

- It rejects NUL characters in text columns. They are still allowed inside
  JSON values such as metadata.
- It enforces `VARCHAR` lengths.
- Integer columns are 32-bit.

`migrate()` creates `schema` if it is missing and applies the migrations under
an exclusive advisory lock, so engines starting together migrate once.

### Other databases

Implement `DocumentMetadataStoreABC`, or subclass `SQLAlchemyMetadataStore`
for another SQLAlchemy dialect, and check it with `MetadataStoreContract`.
