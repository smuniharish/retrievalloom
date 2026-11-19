# Keep data on disk

Use durable storage to keep documents and indexes across process restarts.
By default, the engine uses in-memory components.

## Use a data directory

`EngineResources(data_dir=...)` configures the built-in components to store
their persistent data in one location:

```python
from pathlib import Path

from retrievalloom import EngineResources, RetrievalEngine

engine = RetrievalEngine(resources=EngineResources(data_dir=Path("var/retrievalloom")))
```

| Component | Default location | Created when |
| --- | --- | --- |
| SQLite metadata store | `data_dir/metadata.db` | Always |
| Qdrant local store | `data_dir/qdrant/` | An embedding model and dense search are configured |
| LadybugDB graph store | `data_dir/graph/graph.lbug` | The graph channel is enabled |
| TreeSearch structure engine | `data_dir/structure/` | The structure channel is enabled |

The engine does not delete `data_dir`. Back it up and manage its permissions
as application data.

## Restart behavior

The in-memory keyword, fuzzy and sparse indexes are rebuilt from metadata
when the engine opens. Persistent vector, graph and structure backends can be
reused without re-ingesting their data.

By default, in-memory rebuilds may continue in the background after startup.
Requests made before a method is ready include `index_missing` warnings. Set
`LifecycleConfig.wait_for_rebuild=True` to wait for readiness, bounded by
`rebuild_timeout`, or call `run_lifecycle_once()` when the application
controls scheduling.

The [benchmark results](../../benchmarks.md#memory-and-restart-time) include
measurements up to 100,000 documents. Use them as sizing guidance, not as a
capacity guarantee for a different corpus or machine.

## Retention and erasure

Lifecycle settings control document history and physical erasure:

- `max_versions` limits retained version history; versions referenced by
  retained ingestion events are preserved.
- `event_retention` controls the retention period for ingestion events.
- `purge_after` erases tombstoned documents after the configured delay and
  after indexing has removed them.
- `purge_log_retention` controls how long shared-store erasure updates remain
  available to other engines.

Use `engine.purge(ids, scope=...)` for immediate physical erasure. See
[Generations and coverage](../concepts/lifecycle.md#deletion-erasure-and-retention)
for the lifecycle behavior.

## SQLite metadata

`SQLiteMetadataStore` is the default metadata backend and is suitable for a
single host. Processes on that host can share a file, but SQLite serializes
writes. Use `synchronous="FULL"` when stronger durability on power loss is
more important than write throughput.

```python
from retrievalloom import Components
from retrievalloom.storage.sqlite import SQLiteMetadataStore

store = SQLiteMetadataStore("var/metadata.db", synchronous="FULL")
components = Components(metadata_store=store)
```

The engine does not close a store passed through `Components`; close it after
all engines using it have shut down.

## PostgreSQL metadata

Use `PostgresMetadataStore` when engines on multiple hosts need to share
metadata. Install the optional extra:

```bash
pip install "retrievalloom[postgres]"
```

Configure every engine with the same database and schema:

```python
import os

from retrievalloom import Components, PostgresMetadataStore, RetrievalEngine
from retrievalloom.index.vector import QdrantVectorStore

metadata_store = PostgresMetadataStore(
    os.environ["RETRIEVALLOOM_DSN"],
    schema="retrievalloom",
    pool_size=5,
    max_overflow=10,
)
components = Components(
    metadata_store=metadata_store,
    vector_store=QdrantVectorStore(url="http://localhost:6333"),
)

async with RetrievalEngine(components=components) as engine:
    ...
await metadata_store.close()
```

Choose pool settings with the number of engine processes and the database
connection limit in mind. PostgreSQL applies stricter type and length
validation than SQLite, so validate existing data before migrating.
See the [storage API reference](../../api/storage.md) for all backend
constructor settings.

`LifecycleConfig.migration="auto"` applies schema migrations during engine
initialization. Set it to `"verify"` when migrations run as a separate
deployment step; initialization then fails if the schema is not current.

## Durable search backends

For several processes, use server-backed search stores such as Qdrant rather
than local database files. A local Qdrant directory or embedded graph file may
be owned by only one process at a time. If multiple hosts need the same graph,
use a server-backed graph store or route graph work through one owner process.

See [Qdrant](../../integrations/qdrant.md) and
[Run several engines on one store](multiple-engines.md) for deployment
options.

## Resource ownership

The engine closes components it creates. Components and stores passed in by
the application remain application owned and must be closed by the
application after use.
