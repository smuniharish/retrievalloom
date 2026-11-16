# Offloading and admission

The public API is async-only. Blocking work never runs on the event loop: it
runs in a thread executor through the engine's **offloader**, under admission
control from [resourcera](../../integrations/resourcera.md).

## What the offloader does

`Offloader.run(kind, fn, *args, tenant=None, timeout=None, executor=None)`
runs the synchronous callable `fn(*args)` in an executor and awaits the result.
Before the job starts:

1. **Fair share.** The job takes a slot of its tenant's semaphore. The limit is
   the tenant's `TenantPolicy.max_concurrent_cpu_jobs` (default 4). Jobs
   without a tenant share one pool.
2. **Admission.** The job reserves capacity from the resourcera
   `ResourceManager`: `memory_per_job` (default `16MB`) and, if set,
   `cpu_per_job`, with an I/O-bound or CPU-bound profile chosen from the
   workload kind. If no capacity frees up within `admission_wait_timeout`
   (default 5 s), the call raises `ResourceAdmissionError`.

Both permits are released when the thread finishes, not when the awaiting
coroutine stops. Cancelling a request or hitting its deadline therefore never
over-commits capacity while the thread is still running. A `timeout` raises
`OperationTimeoutError` but cannot stop the thread.

`run` accepts positional arguments only. Use `functools.partial` for keyword
arguments.

## Workload kinds

`retrievalloom.runtime.offloader.WorkloadKind` labels each job:

| Kind | Built-in users | Profile |
| --- | --- | --- |
| `EMBEDDING` | Your embedding model adapters | CPU |
| `RERANK` | `FlashRankReranker` and your reranker adapters | CPU |
| `NLI` | Your NLI model adapters | CPU |
| `EMBEDDING_VALIDATION` | Validation of large embedding batches | CPU |
| `KEYWORD_BUILD` | Keyword and fuzzy index builds | CPU |
| `FUZZY_MATCH` | Fuzzy queries | CPU |
| `SPARSE_INDEX`, `SPARSE_QUERY` | BM25 builds and queries | CPU |
| `VECTOR` | Local-mode Qdrant calls | CPU |
| `STRUCTURE_INDEX` | TreeSearch calls | CPU |
| `GRAPH_QUERY` | LadybugDB calls | I/O |
| `IO_RETRIEVAL`, `STRUCTURE_QUERY` | None; for your blocking I/O | I/O |
| `KEYWORD_QUERY`, `HASHING` | None | CPU |

## Offloading in your own components

Your component receives the offloader in `initialize(context)` as
`context.offloader`. Keep it and route every blocking call through it:

```python
from retrievalloom.contracts.component import ComponentContext
from retrievalloom.contracts.models import EmbeddingModelABC
from retrievalloom.runtime.offloader import WorkloadKind


class MyEmbedding(EmbeddingModelABC):
    # info and dimensions omitted; see the how-to for a complete adapter.

    async def initialize(self, context: ComponentContext) -> None:
        self._offloader = context.offloader
        self._model = await self._offloader.run(WorkloadKind.EMBEDDING, load_model)

    async def embed_documents(self, texts):
        return await self._offloader.run(WorkloadKind.EMBEDDING, self._model.encode, list(texts))
```

Load models in `initialize` through the offloader too: loading weights blocks
for seconds. Use `WorkloadKind.RERANK` for rerankers and `WorkloadKind.NLI` for
NLI models. Libraries that must stay on one thread (some SQLite or ONNX
bindings) can pass a dedicated single-thread `executor=` to `run`.

Async clients (an HTTP embedding API, `AsyncQdrantClient`) need no offloading.

See [Use your own embedding model](../how-to/embedding-model.md) for a
complete, runnable adapter.

## Sharing an executor or resource manager

By default the engine creates and owns a `ThreadPoolExecutor`
(`OffloaderConfig.max_workers`, default 8) and a resourcera `ResourceManager`.
To share them with the rest of your application, pass them in
`EngineResources`:

```python
from concurrent.futures import ThreadPoolExecutor

from resourcera import ResourceManager
from retrievalloom import EngineResources

resources = EngineResources(
    executor=ThreadPoolExecutor(max_workers=16),
    resource_manager=ResourceManager(),
)
```

Application-owned resources are never closed by the engine. Start the resource
manager yourself and close both after the engine.

## Request concurrency

Separately from the offloader, `retrieve` limits concurrent requests per
tenant with `TenantPolicy.max_concurrent_requests` (default 16). A request that
cannot start before its deadline raises `OperationTimeoutError`.

## Event-loop pauses that offloading cannot prevent

Python's garbage collector runs on whichever thread allocates, including the
event loop thread. A full collection over a large heap, for example many
loaded torch models, can pause the loop. See
[Keep the event loop responsive](../how-to/event-loop-latency.md).
