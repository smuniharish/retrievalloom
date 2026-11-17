# Keep data on disk

Restart your service without re-ingesting or re-embedding a single document.
By default everything lives in memory and disappears when the engine closes.
Set `EngineResources(data_dir=...)` and the engine keeps documents and indexes
on disk:

```python
from pathlib import Path

from retrievalloom import EngineResources, RetrievalEngine

engine = RetrievalEngine(resources=EngineResources(data_dir=Path("var/retrievalloom")))
```

With a `data_dir`, the default components store their data under it:

| Component | Location | Created when |
| --- | --- | --- |
| `SQLiteMetadataStore` | `data_dir/metadata.db` | Always |
| `QdrantVectorStore` (local mode) | `data_dir/qdrant/` | An embedding model is configured |
| `LadybugGraphStore` | `data_dir/graph/graph.lbug` | The graph channel is enabled |
| `TreeSearchStructureEngine` | `data_dir/structure/` | The structure channel is enabled |

## Restart quickly

The keyword, fuzzy and sparse indexes are always in memory and are cheap to
rebuild. On restart the engine rebuilds them from the metadata store
(`LifecycleConfig.auto_rebuild_ephemeral`, default `True`). Vectors and other
disk-backed indexes are not rebuilt: the engine picks up the indexes it
published before and serves from them directly, so no document is embedded
again.

When the engine opens:

- With `LifecycleConfig(background=False)`, the rebuild runs before
  `RetrievalEngine.__aenter__` returns.
- With the background scheduler (the default), the rebuild starts in the
  background. Requests that arrive before it finishes skip the in-memory
  methods with `index_missing` warnings and return `partial` results.

To open only once the rebuild is done while keeping the background scheduler,
set `wait_for_rebuild=True`. `rebuild_timeout` (default 300 seconds) bounds
the wait. If the rebuild takes longer, the engine opens anyway, logs
`startup_rebuild_timeout`, and the background rounds finish the rebuild.

```python
from retrievalloom import EngineConfig, LifecycleConfig

engine = RetrievalEngine(
    EngineConfig(lifecycle=LifecycleConfig(wait_for_rebuild=True, rebuild_timeout=120)),
    resources=EngineResources(data_dir=data),
)
```

You can also run a round yourself after opening:

```python
async with RetrievalEngine(resources=EngineResources(data_dir=data)) as engine:
    await engine.run_lifecycle_once(scope)
    ...
```

`wait_for_rebuild` is off by default so that a large rebuild never delays
startup unexpectedly. The rebuild takes about 1.5 ms and 20 KB of memory per
document; see [Memory and restart time](../../benchmarks.md#memory-and-restart-time)
for measurements up to 100,000 documents and sizing guidance.

## Control retention and erasure

The metadata store keeps document history so you can audit changes, and the
background maintenance round trims it to the limits you set (see
[Deletion, erasure and retention](../concepts/lifecycle.md#deletion-erasure-and-retention)):

- `LifecycleConfig.max_versions` (default 10) caps the version history kept
  per document. Versions named by a retained ingestion event are kept.
- `LifecycleConfig.event_retention` (default 90 days) caps how long
  committed and failed ingestion events are kept.
- `LifecycleConfig.purge_after` (default 7 days) erases deleted documents
  physically once they have been removed from the indexes.
- `engine.purge(ids, scope=...)` erases documents immediately, in batches of
  up to 500 documents per transaction. Other engines on the store evict them
  from their in-memory indexes at their next lifecycle round, through the
  purge log.
- `LifecycleConfig.purge_log_retention` (default 1 day) caps how long
  purge-log entries are kept.

## Configure the metadata store

The metadata store holds your documents, their versions and the indexing state,
and every index is rebuilt from it. RetrievalLoom ships two
implementations: `SQLiteMetadataStore` (SQLAlchemy async on `aiosqlite`, the
default) and `PostgresMetadataStore` for engines on several hosts (see
[A shared PostgreSQL store](#a-shared-postgresql-store)).

```python
from retrievalloom import Components
from retrievalloom.storage.sqlite import SQLiteMetadataStore

store = SQLiteMetadataStore("var/metadata.db", synchronous="FULL")
components = Components(metadata_store=store)
```

| Argument | Default | Meaning |
| --- | --- | --- |
| `path` | `None` | Database file. `None` keeps it in memory. |
| `synchronous` | `"NORMAL"` | SQLite `synchronous` pragma: `NORMAL` or `FULL` |
| `busy_timeout_ms` | 30000 | SQLite busy timeout |
| `engine` | `None` | An application-owned SQLAlchemy `AsyncEngine`; not disposed on close |

The store uses WAL journaling, one serialized writer (`BEGIN IMMEDIATE`) and
concurrent readers.

A store you pass in stays yours: close it with `await store.close()` once you
stop using it.

### Schema migrations

The schema is managed with Alembic migrations shipped in the package.
`LifecycleConfig.migration` selects the policy at startup:

- `"auto"` (default): upgrade the schema to the current revision.
- `"verify"`: fail with `MigrationError` if the schema is not current. Use it
  when migrations run as a separate deployment step.

## A shared PostgreSQL store

Use PostgreSQL when several hosts serve the same documents. SQLite allows one
writer at a time and only works for engines on one host.
To run engines on several hosts against one set of documents, keep the
metadata in PostgreSQL. Install the extra first:

```bash
pip install "retrievalloom[postgres]"
```

Then give every engine the same DSN and schema:

```python
import os

from retrievalloom import Components, PostgresMetadataStore, RetrievalEngine
from retrievalloom.index.vector import QdrantVectorStore

metadata_store = PostgresMetadataStore(
    os.environ["RETRIEVALLOOM_DSN"],  # postgresql://user:password@db-host:5432/rag
    schema="retrievalloom",
    pool_size=5,
    lock_timeout_ms=30_000,
)
components = Components(
    metadata_store=metadata_store,
    vector_store=QdrantVectorStore(url="http://qdrant:6333"),
)
async with RetrievalEngine(components=components) as engine:
    ...
await metadata_store.close()
```

| Argument | Default | Meaning |
| --- | --- | --- |
| `dsn` | `None` | `postgresql://`, `postgres://` or `postgresql+asyncpg://` URL |
| `schema` | `None` | Schema for the `rl_` tables, created by the migration. `None` uses the role's `search_path`. |
| `pool_size`, `max_overflow` | 5, 10 | Connections per engine, and extra connections under load |
| `pool_timeout` | 30.0 | Seconds to wait for a pooled connection |
| `pool_recycle`, `pool_pre_ping` | 1800, `True` | Replace old connections, and test connections before use |
| `lock_timeout_ms` | 30000 | How long a writer waits for a lock before `BackendTimeoutError`. `0` waits forever. |
| `statement_timeout_ms` | `None` | Server `statement_timeout`. `None` keeps the server default. |
| `application_name` | `"retrievalloom"` | Shown in `pg_stat_activity` |
| `connect_args` | `None` | Extra `asyncpg.connect` arguments, such as `ssl`. Its `server_settings` are merged over the store's. |
| `engine` | `None` | An application-owned SQLAlchemy `AsyncEngine` instead of `dsn`. It is not disposed on close, and its own connection settings apply. |

How engines share the store:

- Writers of one scope run one at a time, on whichever host they are, and
  different scopes write in parallel. Readers never wait for writers. When
  writes collide, the engine sees a `VersionConflictError` and retries.
- Durable index backends must be shared too: use a Qdrant server rather than
  local mode, and keep the graph store in one owner process (see below). The
  in-memory keyword, fuzzy and sparse indexes belong to each engine, and each
  engine rebuilds them from the shared metadata.
- Engines starting at the same time migrate the schema once. Each migration
  holds an advisory lock that also pauses writers. To migrate as a separate
  deployment step, run `await PostgresMetadataStore(dsn, schema=...).migrate()`
  once and start the engines with `LifecycleConfig(migration="verify")`.
- Every engine opens up to `pool_size + max_overflow` connections. Keep the
  total across engines below the server's `max_connections`.
- Behind PgBouncer, use session pooling. In transaction pooling, asyncpg's
  prepared-statement caches have to be disabled; see the SQLAlchemy asyncpg
  documentation.
- PostgreSQL is stricter than SQLite in three ways. It rejects NUL characters
  in identifiers and other text fields with `ValidationError`, though they
  are fine inside metadata values. It enforces column lengths. And integer
  columns are 32-bit.

See [Run several engines on one store](multiple-engines.md) for what engines
sharing a store coordinate, and [Metadata store](../../architecture/storage.md#postgresql)
for the locking design.

## Qdrant on disk or on a server

Vectors are the most expensive data to rebuild, so keep them in durable
storage. The default vector store in a `data_dir` is Qdrant in local mode. Local mode
locks its directory: only one process can open it. For several processes, or
for production data, use a Qdrant server:

```python
from retrievalloom.index.vector import QdrantVectorStore

vector_store = QdrantVectorStore(url="http://localhost:6333", api_key=None)
```

See [Qdrant](../../integrations/qdrant.md) for all modes.

!!! note "Closing a local-mode store you created"
    If you pass `QdrantVectorStore(path=...)` yourself, the engine does not
    close it. Call `await store.close()` when you no longer need it to release
    the directory lock.

## The graph file has one owner process

`LadybugGraphStore` (`data_dir/graph/graph.lbug`) is an embedded database:
the process that opens it read-write holds a file lock until it closes. A
second process that opens the same file, for example a second engine with the
same `data_dir` and the graph channel enabled, fails at startup with
`BackendUnavailableError` (`retryable=True`, the file in
`details["path"]`) instead of waiting. It opens normally once the first
process closes its engine.

To serve one graph from several processes, either route graph work through a
single owner process, or implement `GraphStoreABC` on a server database such
as Neo4j and pass it as `Components(graph_store=...)`. See
[Write your own backend](custom-backends.md).

## Who closes what

- Components the engine creates are closed by the engine.
- Components and resources you pass in are never closed by the engine. Close
  them yourself once you stop using them.
- `data_dir` itself is never deleted.

## Next steps

- Serve one corpus from several processes in
  [Run several engines on one store](multiple-engines.md).
- See a restart without re-embedding in the
  [durable restart example](../../examples/durable-restart.md).
