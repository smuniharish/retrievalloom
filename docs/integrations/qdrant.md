# Qdrant

Store the vectors for semantic search in Qdrant, from a quick in-memory test
to a shared production cluster. `QdrantVectorStore` (`retrievalloom.index.vector`)
is the default vector store. The engine creates one when an embedding model is
configured and you do not pass `vector_store`:

- without `data_dir`: in memory (`":memory:"`), rebuilt after each restart;
- with `data_dir`: local mode at `data_dir/qdrant/`.

## Choose a mode

Pass at most one of `location`, `path`, `url` or `client`.

```python
from qdrant_client import AsyncQdrantClient
from retrievalloom.index.vector import QdrantVectorStore

memory = QdrantVectorStore()  # location=":memory:"
local = QdrantVectorStore(path="./data/qdrant")
server = QdrantVectorStore(url="http://localhost:6333")
cloud = QdrantVectorStore(url="https://xyz.cloud.qdrant.io:6333", api_key="...")
shared = QdrantVectorStore(client=AsyncQdrantClient(url="http://localhost:6333"))
```

| Mode | Durable | Notes |
| --- | --- | --- |
| Memory | No | Per engine instance. Rebuilt by background indexing after a restart. |
| Local path | Yes | Locks its directory: one process at a time. Calls run on a dedicated single thread through the offloader (`WorkloadKind.VECTOR`). |
| Server / cloud (`url`) | Yes | Uses `AsyncQdrantClient`; no offloading needed. Several engines can share it. |
| `client` | Yes | Your `AsyncQdrantClient`. The store never closes it. |

## Options

Tune the collection name, distance and timeouts:

| Argument | Default | Meaning |
| --- | --- | --- |
| `collection_prefix` | `"retrievalloom"` | Collection name prefix. Letters, digits and `_`. |
| `distance` | `"cosine"` | `cosine`, `dot` or `euclid`. |
| `timeout` | `None` | Server request timeout in seconds. |
| `overfetch_factor` | `4` | Candidate multiplier when a filter can only be partly pushed down. |

## Data layout

Tenants never see each other's vectors. There is one collection per vector size: `{collection_prefix}_{dimensions}`.
Each point stores `scope_key`, `generation_id`, `object_id` and the object's
filter metadata under `meta`. Every query and delete is constrained to one
scope and one generation. Point ids
are deterministic, so re-indexing an object overwrites its point.

## Filters

Filters are translated to Qdrant conditions where the semantics match exactly:

- `Eq` and `In` on strings or integers;
- `And`, `Or` and `Not` of exact parts.

Other expressions (for example `Range`, `Exists`, floats or booleans in `Eq`)
are pushed down as a broader condition or not at all. The store then
over-fetches by `overfetch_factor` and applies the exact filter afterwards.

Qdrant does not match an integer `Eq`/`In` value against a float payload value
(`1` vs `1.0`). Store numeric metadata with consistent types.

## Run a server locally

```bash
podman run -d --name rl-qdrant -p 6333:6333 -p 6334:6334 qdrant/qdrant
```

`docker` works the same way.

## Next steps

- Keep vectors across restarts in [Keep data on disk](../guide/how-to/durable-storage.md).
- Connect a model in [Use your own embedding model](../guide/how-to/embedding-model.md).
- Run the [customer support](../examples/customer-support.md) and
  [durable restart](../examples/durable-restart.md) examples against a Qdrant server.
