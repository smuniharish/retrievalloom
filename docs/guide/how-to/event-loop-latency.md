# Keep the event loop responsive

Keep response times low under load. RetrievalLoom never runs blocking work on
the event loop, but your own components and Python's garbage collector can
still stall it. This page lists each cause, its fix, and how to measure the
result.

## Offload blocking calls in your components

Every blocking call inside a component you write (model inference, a sync
database client, file I/O, heavy parsing) must go through
`context.offloader.run(...)`. A call made directly in an `async def` blocks
every request on the loop until it returns. See
[Offloading and admission](../concepts/offloading.md).

## Bound every request

- `RetrievalConfig.request_timeout` (default 30 s), or
  `RetrievalExecutionConfig(timeout=...)` per request, is the deadline for the
  whole request.
- `RetrievalConfig.method_timeout` bounds each retrieval method.
- With `allow_partial_on_deadline=True`, a request that reaches its deadline
  while methods run returns what finished, with status `partial` and a
  `timeout_partial` warning. Otherwise it raises `OperationTimeoutError`.
- `TenantPolicy.max_concurrent_requests` and `max_concurrent_cpu_jobs` keep
  one tenant from taking every slot.

## Avoid garbage-collector pauses on large heaps

Python's cyclic garbage collector runs on the thread that triggers it, often
the event loop thread. A full collection walks every tracked object. With a
large heap, for example several loaded torch models, one full collection can
pause the loop for hundreds of milliseconds. Offloading does not help: the
pause is in the interpreter.

Move long-lived objects out of the collector's view after they are loaded:

```python
import gc

async with RetrievalEngine(components=components) as engine:
    # Models are loaded in the components' initialize().
    gc.collect()
    gc.freeze()
    ...  # serve requests
```

`gc.freeze()` moves every object currently tracked into a permanent
generation that later collections skip. Call it once, after models and other
long-lived state are loaded. Objects created later are collected as usual.
Call `gc.unfreeze()` if you unload the models.

RetrievalLoom's live soak test measures event-loop lag this way: it calls
`gc.collect()` and `gc.freeze()` before it starts probing, so the probe sees
stalls caused by the engine rather than by the models that earlier tests left
on the heap.

## Measure it

A simple probe records the worst scheduling delay of the loop:

```python
import asyncio


async def worst_lag(duration: float, interval: float = 0.01) -> float:
    loop = asyncio.get_running_loop()
    worst = 0.0
    end = loop.time() + duration
    while loop.time() < end:
        start = loop.time()
        await asyncio.sleep(interval)
        worst = max(worst, loop.time() - start - interval)
    return worst
```

Run it as a task next to your workload. A worst lag in the tens of
milliseconds is normal; hundreds of milliseconds point to a blocking call or a
collector pause.

## Next steps

- See the measured effect of each setting in
  [Offloading and admission](../concepts/offloading.md#capping-cpu-bound-jobs).
- Trace slow requests with the [Jaeger example](../../examples/observability-jaeger.md).
