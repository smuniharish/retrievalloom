# Run several engines on one store

Run multiple engine instances against the same corpus when an application
needs more than one process or host. Each engine must use the same metadata
store and the same durable backend locations and configuration.

For one host, engines can share a SQLite metadata file. For multiple hosts,
use `PostgresMetadataStore` and server-backed search stores such as Qdrant.
Local database files may be restricted to a single process.

## Configure each engine

Create separate component instances for each engine. Share the underlying
database and service locations, and use the same embedding model identity and
dimensions in every process:

```python
from retrievalloom import Components, RetrievalEngine
from retrievalloom.index.vector import QdrantVectorStore
from retrievalloom.storage.sqlite import SQLiteMetadataStore


def make_engine(embedding) -> RetrievalEngine:
    return RetrievalEngine(
        components=Components(
            metadata_store=SQLiteMetadataStore("shared/metadata.db"),
            embedding=embedding,
            vector_store=QdrantVectorStore(
                url="http://localhost:6333",
                collection_prefix="docs",
            ),
        )
    )
```

Use PostgreSQL rather than a shared SQLite file for engines on different
hosts. See [Keep data on disk](durable-storage.md#postgresql-metadata).

## What each engine keeps

Durable backends can be shared by all engines that use the same location.
Keyword, fuzzy and sparse indexes are in memory, so each process keeps its own
copy and refreshes it through lifecycle processing. Plan for the memory and
restart cost in each process; see [Benchmarks](../../benchmarks.md).

Configure background lifecycle processing on every engine, or schedule
`run_lifecycle_once()` yourself. An engine that does not process lifecycle
work will not promptly reflect changes made by another engine in its
in-memory indexes.

Purges are reflected by other engines during lifecycle processing. Retrieval
still validates candidates against current metadata, so erased documents are
not returned while an engine is waiting to refresh its local index.

## Embedded backends

SQLite metadata files can be shared by processes on one host. Embedded graph
and local vector backends may require a single process to own their data
directory. For access from multiple hosts, use a server-backed implementation
or route operations through one owner process.

See [Metadata store](../../architecture/storage.md) for store choices and
[Concurrency and isolation](../../architecture/concurrency.md) for the
deployment guarantees.
