# Keep data on disk

By default everything lives in memory and disappears when the engine closes.
Set `EngineResources(data_dir=...)` to keep it.

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

The keyword, fuzzy and sparse indexes are always in memory. On restart the
engine rebuilds them from the metadata store
(`LifecycleConfig.auto_rebuild_ephemeral`, default `True`). Durable backends
are not rebuilt: the engine reconciles with the published generations and
serves from them directly.

When the engine opens:

- With `LifecycleConfig(background=False)`, the rebuild runs before
  `RetrievalEngine.__aenter__` returns.
- With the background scheduler (the default), the rebuild starts in the
  background. Requests that arrive before it finishes skip the in-memory
  methods with `index_missing` warnings and return `partial` results. To wait
  for it, run a round yourself after opening:

```python
async with RetrievalEngine(resources=EngineResources(data_dir=data)) as engine:
    await engine.run_lifecycle_once(scope)
    ...
```

## The metadata store

The metadata store is the source of truth: documents, versions, index
objects, coverage, generations and lifecycle tasks. RetrievalLoom ships one
implementation, `SQLiteMetadataStore` (SQLAlchemy async on `aiosqlite`).

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

The store uses WAL journaling, one serialised writer (`BEGIN IMMEDIATE`) and
concurrent readers.

A store you pass in stays yours: close it with `await store.close()` once you
stop using it.

### Schema migrations

The schema is managed with Alembic migrations shipped in the package.
`LifecycleConfig.migration` selects the policy at startup:

- `"auto"` (default): upgrade the schema to the current revision.
- `"verify"`: fail with `MigrationError` if the schema is not current. Use it
  when migrations run as a separate deployment step.

## Qdrant on disk or on a server

The default vector store in a `data_dir` is Qdrant in local mode. Local mode
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

## Ownership rules

- Components the engine creates are closed by the engine.
- Components and resources you pass in are never closed by the engine. Close
  them yourself once you stop using them.
- `data_dir` itself is never deleted.
