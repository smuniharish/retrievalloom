# Keep the event loop responsive

RetrievalLoom offloads its blocking work, but custom components and application
code share the event loop. Measure latency under realistic load and address
the operation that causes stalls before changing concurrency or garbage
collector settings.

## Offload blocking calls

Do not call a synchronous model, database client or expensive function
directly from an `async` component method. Route blocking work through
`context.offloader.run(...)`. Await async clients directly.

See [Offloading and admission](../concepts/offloading.md) for a component
example and cancellation behavior.

## Bound work

- Set a request deadline with `RetrievalConfig.request_timeout` or a
  per-request timeout.
- Use `method_timeout` to bound individual retrieval methods.
- Configure per-tenant request and offloaded-job limits.
- Use automatic concurrency by default; cap it with `AutoConcurrency` when
  the application needs a stricter resource budget.
- Enable partial results on a deadline only when callers can handle and
  clearly identify incomplete evidence.

## Measure scheduling delay

Run a small probe alongside the actual workload to measure how late the event
loop resumes a scheduled task:

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

Interpret the measurement with request latency, CPU and memory usage, and
backend timings. Investigate synchronous work on the event loop, worker
contention, garbage-collection pauses and slow external services before
applying process-wide tuning.

Use the [Jaeger example](../../examples/observability-jaeger.md) to trace slow
requests and [Benchmarks](../../benchmarks.md) as a reference for the
measurement environment and limitations.
