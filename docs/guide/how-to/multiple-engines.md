# Run several engines on one store

Scale out by running several engines, in one process or many, against the same
documents. Durable indexes such as vectors are built once and shared, and every
engine sees the others' writes. The engines must share:

- one metadata store (for example the same SQLite file for engines on one
  host, or a [shared PostgreSQL store](durable-storage.md#a-shared-postgresql-store)
  for engines on several hosts), and
- the same durable backend location (for example one Qdrant server and
  `collection_prefix`).

Configure every engine the same way:

```python
from retrievalloom import Components, RetrievalEngine
from retrievalloom.index.vector import QdrantVectorStore
from retrievalloom.storage.sqlite import SQLiteMetadataStore


def make_engine(embedding) -> RetrievalEngine:
    return RetrievalEngine(
        components=Components(
            metadata_store=SQLiteMetadataStore("shared/metadata.db"),
            embedding=embedding,
            vector_store=QdrantVectorStore(url="http://qdrant:6333", collection_prefix="docs"),
        )
    )
```

Every engine needs its own component instances; only the underlying storage
is shared. Use the same embedding model (same identity and dimensions) in every
engine: a different model fingerprint makes the shared generation stale for
that engine.

## What is shared and what is not

Durable indexes are shared; in-memory indexes are rebuilt by each engine from
the shared metadata:

| Unit | Shared between engines |
| --- | --- |
| Dense (Qdrant server, or another durable store) | Yes. One generation, built once. |
| Keyword, fuzzy, sparse (in memory) | No. Each engine builds and keeps its own generation. |
| Graph, structure | Only if every engine can open the same backend location. LadybugDB files and Qdrant local-mode directories are opened by one process at a time: a second process's `LadybugGraphStore` fails at startup with `BackendUnavailableError`. Share a graph through one owner process or a `GraphStoreABC` on a server database such as Neo4j. |

Rows of an engine's in-memory generations carry its instance id. They are
deleted when the engine closes, or by another engine's maintenance round when
its registration expires (`LifecycleConfig.instance_ttl`, default 120 s,
renewed by a heartbeat every round).

## Reader leases

A search on one engine never loses its index because another engine rebuilt
it. A rebuild publishes a new generation and retires the old one. The old one must
not be dropped while another engine still reads from it. Durable reader leases
guarantee this:

- While an engine reads a durable generation, it holds a reader lease on it in
  the metadata store, per generation and per engine instance.
- A lease lasts `LifecycleConfig.lease_ttl` (default 120 s). The engine renews
  it on a read when less than half the TTL remains.
- Retiring a generation waits until no **other** engine holds an unexpired
  lease on it. The retiring engine's own readers are tracked in process.
- After an engine's last read, its lease can persist for up to `lease_ttl`. If
  the engine closes, it deregisters and its leases are deleted at once.
- Expired leases are purged by the maintenance round.
- `LifecycleConfig.generation_retention` (default 0 s) delays the drop
  further.

In-memory backends need no leases: their generations belong to one engine.

Choose `lease_ttl` longer than your longest request. A crashed engine delays
the drop of a retired generation by at most `lease_ttl`; its registration
and in-memory rows are purged after `instance_ttl`.

## Who runs background indexing

Every engine with `background=True` (the default) runs lifecycle rounds.
Generation pointers are switched with compare-and-swap in the metadata store,
so several schedulers on one store are safe.

An engine with `LifecycleConfig(background=False)` runs rounds only when you
ask: on its own `ingest` and `delete`, `wait_for`, and
`run_lifecycle_once()`. Call `engine.run_lifecycle_once()` periodically, at
least every `instance_ttl`. The round renews the engine's registration, and
indexes documents written by other engines into its in-memory indexes.
Without it, those documents are missing from its keyword, fuzzy and sparse
results, and other engines purge its in-memory rows once its registration
expires. The round also evicts documents other engines purged from its
in-memory indexes (see [Erasure and retention](../../security.md#erasure-and-retention)).

## Next steps

- Choose a shared metadata store in [Keep data on disk](durable-storage.md).
- Learn how leases protect searches in [Generations and coverage](../concepts/lifecycle.md#reader-leases).
