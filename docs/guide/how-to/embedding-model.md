# Use your own embedding model

RetrievalLoom does not ship an embedding model. Dense retrieval is enabled as
soon as you pass one in `Components(embedding=...)`.

## The contract

Subclass `EmbeddingModelABC` and implement:

| Member | Purpose |
| --- | --- |
| `info` | `ComponentInfo` whose fingerprint carries `model_identity` and `dimensions`. Build it with `make_info`. |
| `dimensions` | Vector size. |
| `max_batch_size` | Optional. Largest batch per `embed_documents` call (default 64). The engine splits larger inputs. |
| `embed_documents(texts)` | One vector per text, in order. |
| `embed_query(text)` | One vector. |
| `initialize(context)` | Optional. Keep `context.offloader` and load the model. |

The engine validates every call: vector count, dimensions, finite values and
non-zero norm. A violation raises `EmbeddingValidationError`.

## A complete adapter

The model below is a toy so the example runs anywhere. Replace `_encode` with
your model's blocking call. Everything else stays the same.

```python
import asyncio
import hashlib
import math
from collections.abc import Sequence

from retrievalloom import Components, DocumentInput, RetrievalEngine, Scope, TextRepresentation
from retrievalloom.contracts.component import ComponentContext, ComponentInfo, make_info
from retrievalloom.contracts.models import EmbeddingModelABC
from retrievalloom.runtime.offloader import Offloader, WorkloadKind


class HashEmbedding(EmbeddingModelABC):
    """Toy 64-d embedding. Replace ``_encode`` with your model's blocking call."""

    def __init__(self) -> None:
        self._offloader: Offloader | None = None
        self._info = make_info(
            component="embedding",
            implementation="hash",
            version="1",
            model_identity="example/hash-64",
            dimensions=64,
        )

    @property
    def info(self) -> ComponentInfo:
        return self._info

    @property
    def dimensions(self) -> int:
        return 64

    async def initialize(self, context: ComponentContext) -> None:
        # Load real model weights here, through the offloader.
        self._offloader = context.offloader

    def _encode(self, texts: list[str]) -> list[list[float]]:
        vectors = []
        for text in texts:
            vector = [0.0] * 64
            for word in text.lower().split():
                vector[hashlib.sha256(word.encode()).digest()[0] % 64] += 1.0
            norm = math.sqrt(sum(v * v for v in vector)) or 1.0
            vectors.append([v / norm if any(vector) else 1.0 / 8 for v in vector])
        return vectors

    async def embed_documents(self, texts: Sequence[str]) -> Sequence[Sequence[float]]:
        assert self._offloader is not None
        return await self._offloader.run(WorkloadKind.EMBEDDING, self._encode, list(texts))

    async def embed_query(self, text: str) -> Sequence[float]:
        assert self._offloader is not None
        vectors = await self._offloader.run(WorkloadKind.EMBEDDING, self._encode, [text])
        return vectors[0]


async def main() -> None:
    scope = Scope(tenant_id="acme")
    async with RetrievalEngine(components=Components(embedding=HashEmbedding())) as engine:
        await engine.ingest(
            [
                DocumentInput(
                    id="a",
                    representations=[TextRepresentation(text="the event loop runs coroutines")],
                )
            ],
            scope=scope,
        )
        result = await engine.retrieve("event loop", scope=scope)
        print([hit.method.key for hit in result.evidence[0].provenance.hits])


asyncio.run(main())
```

The vectors go to the default `QdrantVectorStore` (in memory here). The output
lists `index.dense` next to the other methods.

## Why the offloader

`embed_documents` and `embed_query` are coroutines, but the model call inside
them is blocking. Calling it directly would stall every other request on the
event loop. `context.offloader.run(WorkloadKind.EMBEDDING, ...)` runs it in a
worker thread under the tenant fair share and resourcera admission. See
[Offloading and admission](../concepts/offloading.md).

If your model is an async HTTP API, await the client directly. No offloading is
needed.

## Changing the model

The model identity and dimensions are part of the dense unit's dependency
fingerprint. When you restart with a different model, the existing generation
no longer matches. Retrieval skips the dense method with a `stale_coverage`
warning until the lifecycle has built a new generation with the new model.

Each dimensionality uses its own Qdrant collection, so old and new vectors
never mix.

## Query embedding cache

Query vectors are cached per tenant in an LRU cache
(`RetrievalConfig.embedding_cache_size`, default 1024 entries;
`embedding_cache_ttl`, default 600 s). Set the size to `0` to disable it.

## Real models

The [BYO embeddings with Qdrant example](../../examples/byo-embeddings-qdrant.md)
wires BAAI/bge-m3 through `transformers` to a Qdrant server. Use a server or a
persistent path for production data: see [Qdrant](../../integrations/qdrant.md).
