# Offloading and admission

RetrievalLoom keeps blocking model and backend calls off the event loop. Its
offloader also limits concurrent work so that model inference, indexing and
other synchronous operations do not consume unbounded process resources.
Resource admission is integrated with
[resourcera](../../integrations/resourcera.md).

## How work is admitted

`Offloader.run(kind, fn, *args, tenant=None, timeout=None, executor=None)`
runs a synchronous callable in a worker and awaits its result. Before the
call starts, RetrievalLoom applies the configured per-tenant and global
concurrency limits and requests capacity from the resource manager.

Automatic concurrency is the default for worker, ingestion and lifecycle
limits. `AutoConcurrency` can cap its maximum; an integer selects a fixed
limit. Configure `EngineLimits` to raise a supported upper bound. See
[Automatic concurrency](../../getting-started/configuration.md#automatic-concurrency).

If work cannot be admitted within the configured wait, the call raises
`ResourceAdmissionError`. A configured operation timeout raises
`OperationTimeoutError`.

## Custom components

Route blocking calls in custom components through the offloader provided by
`initialize(context)`. Async clients should be awaited directly.

```python
from collections.abc import Sequence

from retrievalloom.contracts.component import ComponentContext
from retrievalloom.contracts.models import EmbeddingModelABC
from retrievalloom.runtime.offloader import WorkloadKind


class MyEmbedding(EmbeddingModelABC):
    async def initialize(self, context: ComponentContext) -> None:
        self._offloader = context.offloader
        self._model = await self._offloader.run(WorkloadKind.EMBEDDING, load_model)

    async def embed_documents(self, texts: Sequence[str]) -> Sequence[Sequence[float]]:
        return await self._offloader.run(
            WorkloadKind.EMBEDDING, self._model.encode, list(texts)
        )
```

See [Use your own embedding model](../how-to/embedding-model.md) for a
complete adapter.

## Cancellation and timeouts

Cancelling the coroutine does not stop synchronous code that is already
running in a worker thread. RetrievalLoom keeps that job's capacity reserved
until the call finishes, preventing the configured limit from being
overcommitted. Design custom operations to be bounded and safe to finish after
their caller has stopped waiting.

## Optional CPU limits

Set `OffloaderConfig.max_cpu_jobs` to cap CPU-bound work across tenants, or
`workload_limits` to cap a particular workload. These settings are useful
when custom components use CPU-heavy code or when a model backend should not
run multiple calls concurrently:

```python
from retrievalloom import create_retrieval_engine
from retrievalloom.runtime.offloader import OffloaderConfig, WorkloadKind

engine = create_retrieval_engine(
    offloader=OffloaderConfig(
        max_cpu_jobs=2,
        workload_limits={WorkloadKind.EMBEDDING: 1},
    )
)
```

Measure event-loop latency under your own workload before adding a cap.
See [Keep the event loop responsive](../how-to/event-loop-latency.md) and the
[configuration API reference](../../api/config.md) for other offloading
settings.
