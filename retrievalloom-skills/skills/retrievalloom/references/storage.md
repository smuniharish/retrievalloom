# Storage

Use this page to keep documents and indexes across restarts and to share
them between engines. By default everything lives in memory and disappears
when the engine closes.

## Keep data in one directory

```python
from pathlib import Path

from retrievalloom import EngineResources, create_retrieval_engine

engine = create_retrieval_engine(resources=EngineResources(data_dir=Path("var/rl")))
```

| What | Where | Created when |
| --- | --- | --- |
| Metadata store (SQLite) | `data_dir/metadata.db` | Always |
| Dense vectors (Qdrant local mode) | `data_dir/qdrant/` | An embedding model is configured |
| Graph (LadybugDB) | `data_dir/graph/graph.lbug` | The graph channel is enabled |
| Structure (TreeSearch) | `data_dir/structure/` | The structure channel is enabled |
| FlashRank model | `data_dir/models/` | Read only; you provision it |

The keyword, fuzzy, and sparse indexes are always in memory and are rebuilt
from the metadata store on start (see [lifecycle.md](lifecycle.md)). The
engine never deletes `data_dir`.

## Tune SQLite

```python
from retrievalloom import Components
from retrievalloom.storage.sqlite import SQLiteMetadataStore

store = SQLiteMetadataStore("var/metadata.db", synchronous="FULL", busy_timeout_ms=30_000)
engine = create_retrieval_engine(components=Components(metadata_store=store))
async with engine:
    ...
await store.close()
```

SQLite uses WAL, one writer at a time, and concurrent readers. Several engines
on one host can share the file.

## Share a PostgreSQL store across hosts

Install `"retrievalloom[postgres]"`, then give every engine the same DSN and
schema, and a Qdrant server rather than local mode:

```python
import os

from retrievalloom import Components, PostgresMetadataStore, create_retrieval_engine
from retrievalloom.index.vector import QdrantVectorStore

metadata_store = PostgresMetadataStore(os.environ["RETRIEVALLOOM_DSN"], schema="retrievalloom")
vector_store = QdrantVectorStore(url=os.environ["RETRIEVALLOOM_QDRANT_URL"])
engine = create_retrieval_engine(
    components=Components(
        metadata_store=metadata_store,
        embedding=my_embedding,  # your EmbeddingModelABC instance
        vector_store=vector_store,
    )
)
async with engine:
    ...
await metadata_store.close()
await vector_store.close()
```

- The DSN may start with `postgresql://`, `postgres://`, or
  `postgresql+asyncpg://`. Pass TLS options through `connect_args`.
- Engines starting together migrate the schema once. To migrate as a
  deployment step, run `await PostgresMetadataStore(dsn, schema=...).migrate()`
  and start engines with `LifecycleConfig(migration="verify")`.
- Each engine opens up to `pool_size + max_overflow` (5 + 10) connections;
  keep the total below the server's `max_connections`. Behind PgBouncer, use
  session pooling.
- PostgreSQL rejects NUL characters in ids and other text fields with
  `ValidationError`, enforces column lengths, and uses 32-bit integers.

## Run several engines on one store

Engines that share the metadata store and the durable backend location see
each other's writes, generations, and tasks.

| Index | Shared |
| --- | --- |
| Dense on a Qdrant server | Yes, built once |
| Keyword, fuzzy, sparse | No; each engine keeps its own copy |
| Graph, structure | Only when every engine can open the same location |

- Give every engine its own component instances and the same embedding model
  identity and dimensions.
- A Qdrant local-mode directory and a LadybugDB graph file open in one process
  at a time. A second process fails with `BackendUnavailableError`.
- Generations in use by another engine are kept until its reader lease
  (`lease_ttl`, 120 s) expires.

## Own what you pass in

Components and resources you pass to the engine stay yours: close them after
the engine closes. The engine closes only the components it created.
