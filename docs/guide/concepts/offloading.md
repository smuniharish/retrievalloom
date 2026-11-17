# Offloading and admission

Keep your service responsive while models run and indexes build. Every engine
method is async, and blocking work never runs on your event loop. It runs in
worker threads through the engine's **offloader**, which also limits how much
work runs at once so the process does not run out of memory. Admission control
comes from [resourcera](../../integrations/resourcera.md).

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

Between the two, the job also takes a slot of any engine-wide cap that
applies to it: `OffloaderConfig.workload_limits[kind]` and, for CPU-bound
kinds, `OffloaderConfig.max_cpu_jobs` (see
[Capping CPU-bound jobs](#capping-cpu-bound-jobs)). Both are off by default.
Slots are always taken in the same order (tenant, workload kind, CPU, then
admission), and a job waiting for a slot waits on the event loop without
holding an executor thread.

All permits are released when the thread finishes, not when the awaiting
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

If you write your own model or store adapter, route its blocking calls through
the same offloader so they share these limits. Your component receives the offloader in `initialize(context)` as
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

## Capping CPU-bound jobs

Limit CPU-heavy jobs when your own components run pure-Python code. A worker
thread running pure-Python code holds the GIL (the interpreter lock that lets
only one thread run Python code at a time). The event loop thread
then waits for the GIL each time it wakes up, and the more such threads run at
once, the longer it waits. Two `OffloaderConfig` settings bound how many jobs
run at once, across all tenants:

| Setting | Default | Effect |
| --- | --- | --- |
| `max_cpu_jobs` | `None` (no cap) | At most this many jobs of CPU-bound kinds run at once. I/O-bound kinds (`IO_RETRIEVAL`, `GRAPH_QUERY`, `STRUCTURE_QUERY`) are not counted. |
| `workload_limits` | `{}` | Per-kind caps, for example `{WorkloadKind.EMBEDDING: 1}` for a model that should not run twice in parallel. |

```python
from retrievalloom import create_retrieval_engine
from retrievalloom.runtime.offloader import OffloaderConfig, WorkloadKind

engine = create_retrieval_engine(
    offloader=OffloaderConfig(max_cpu_jobs=2, workload_limits={WorkloadKind.EMBEDDING: 1})
)
```

A capped job waits for its slot on the event loop, before admission, and
releases it when its thread finishes, like the tenant slot. Cancellation and
`timeout` behave as described above.

Set a cap when your own components offload code that holds the GIL. Measured
on the machine in [Benchmarks](../../benchmarks.md), 16 concurrent offloaded
jobs of about 40 ms of pure-Python arithmetic each, 3 runs per setting:

| `max_cpu_jobs` | Max event-loop lag | Total time |
| --- | ---: | ---: |
| `None` | 54–67 ms | 643–719 ms |
| `2` | 37–62 ms | 574–630 ms |
| `1` | 24–30 ms | 607–682 ms |

Total time does not suffer, because the GIL serializes such jobs anyway.

Leave the cap off for work that releases the GIL. The built-in fuzzy index
scores with `rapidfuzz.process.cdist`, which releases the GIL while it runs
(`process.extract` does not), and a BM25 query takes about 3 ms. For the default
pipeline (2,000 documents, 16 concurrent retrievals), `max_cpu_jobs` of 1 or 2
did not reduce the loop lag measurably and only cost throughput. The lag
there comes from the garbage collector; see the next section.

## Event-loop pauses that offloading cannot prevent

Python's garbage collector runs on whichever thread allocates, including the
event loop thread. A full collection over a large heap, for example many
loaded torch models, can pause the loop. See
[Keep the event loop responsive](../how-to/event-loop-latency.md).

With the default pipeline at 2,000 documents and 16 concurrent retrievals,
the heap holds about 224,000 tracked objects and every full collection paused
the loop for 60–80 ms; these pauses were the largest stalls measured. Calling
`gc.collect()` and `gc.freeze()` once the engine has loaded its indexes cut the
max loop lag from 122–153 ms to 21–29 ms over three runs.

## Next steps

- Apply these measurements in [Keep the event loop responsive](../how-to/event-loop-latency.md).
- Share one resource budget across libraries with [resourcera](../../integrations/resourcera.md).
