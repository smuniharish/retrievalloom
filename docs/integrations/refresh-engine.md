# refresh-engine

Send RetrievalLoom's metrics to your monitoring system, and understand how
background indexing is scheduled.
[refresh-engine](https://pypi.org/project/refresh-engine/) supports
background indexing, retries and timeouts. RetrievalLoom manages it
internally; applications configure lifecycle behavior through
`LifecycleConfig`. Applications may implement `MetricsSink` to receive
metrics.

## How indexing rounds run

Lifecycle settings control how often indexing runs, how many units can be
processed concurrently, and how failed work is retried:

| Setting | Effect |
| --- | --- |
| `max_concurrency` | Maximum concurrent indexing work. |
| `max_attempts` | Attempts for retryable failures. |
| `unit_timeout` | Maximum time allowed for one indexing operation. |
| `poll_interval` | Interval between background indexing rounds. |

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
