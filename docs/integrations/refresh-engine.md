# refresh-engine

Send RetrievalLoom's metrics to your monitoring system, and understand how
background indexing is scheduled.
[refresh-engine](https://pypi.org/project/refresh-engine/) runs background
indexing with retries and timeouts. You do not call it directly; the one
contract you may implement is `MetricsSink`.

## How indexing rounds run

The lifecycle coordinator wraps a `RefreshEngine`:

- **Discovery** yields every (scope, index unit) pair that lags behind the
  metadata store.
- **Operation** runs the indexer for one pair: build or update a generation,
  write coverage, publish.
- **State** is kept in the metadata store (table `rl_refresh_state`), so it is
  shared by every engine on that store.

`LifecycleConfig` maps onto refresh-engine settings:

| `LifecycleConfig` field | refresh-engine setting |
| --- | --- |
| `max_concurrency` | Units processed in parallel per round. |
| `max_attempts` | Retry attempts per unit (backoff 0.05 s to 2 s) for retryable errors. |
| `unit_timeout` | Timeout of one unit operation. |
| `poll_interval` | Interval of the background `AsyncScheduler` (when `background=True`). |

A round runs on the scheduler's tick, after a write (`ingest`, `delete`), or
when you call `engine.run_lifecycle_once(scope)`. See
[Generations and coverage](../guide/concepts/lifecycle.md).

## Collect metrics

Pass a refresh-engine `MetricsSink` as `EngineResources(metrics=...)`. The
default discards everything. The engine records:

| Name | Kind | Attributes |
| --- | --- | --- |
| `retrievalloom.retrieval.requests` | counter | `status` |
| `retrievalloom.retrieval.duration_ms` | observation | `status` |
| `retrievalloom.retrieval.method_ms` | observation | `method` |

Components also receive the sink as `context.metrics` in `initialize`.

```python
import asyncio
from collections.abc import Mapping

from refresh_engine import MetricsSink
from retrievalloom import DocumentInput, EngineResources, RetrievalEngine, Scope, TextRepresentation


class PrintMetrics(MetricsSink):
    async def increment(
        self, name: str, value: int = 1, attributes: Mapping[str, str] | None = None, /
    ) -> None:
        print("count", name, value, dict(attributes or {}))

    async def observe(
        self, name: str, value: float, attributes: Mapping[str, str] | None = None, /
    ) -> None:
        print("observe", name, dict(attributes or {}))


async def main() -> None:
    scope = Scope(tenant_id="acme")
    async with RetrievalEngine(resources=EngineResources(metrics=PrintMetrics())) as engine:
        await engine.ingest(
            [DocumentInput(id="a", representations=[TextRepresentation(text="hello world")])],
            scope=scope,
        )
        await engine.retrieve("hello", scope=scope)


asyncio.run(main())
```

Both methods are awaited on the request path; keep them fast or hand the
values to a queue.

## Next steps

- Trace requests end to end with the [Jaeger example](../examples/observability-jaeger.md).
- Learn how rounds keep indexes current in
  [Generations and coverage](../guide/concepts/lifecycle.md).
