# resourcera

Keep a busy host stable: when memory or CPU runs short, new background work
waits instead of piling up threads.
[resourcera](https://pypi.org/project/resourcera/) provides this admission
control. RetrievalLoom uses it in the
[offloader](../guide/concepts/offloading.md): before a blocking job starts in a
thread, the job reserves capacity from a resourcera `ResourceManager`. A job that waits longer than
`OffloaderConfig.admission_wait_timeout` fails with `ResourceAdmissionError`.

## Let the engine manage it

By default the engine creates, starts and closes its own `ResourceManager`
from `OffloaderConfig.resource_config`.

```python
from resourcera import ResourceConfig
from retrievalloom import EngineConfig, OffloaderConfig

config = EngineConfig(
    offloader=OffloaderConfig(
        memory_per_job="64MB",
        admission_wait_timeout=10.0,
        resource_config=ResourceConfig(max_concurrency=32),
    )
)
```

| `OffloaderConfig` field | Default | Meaning |
| --- | --- | --- |
| `memory_per_job` | `"16MB"` | Memory each job declares. |
| `cpu_per_job` | `None` | Optional CPU each job declares. Needs warm resourcera samples. |
| `admission_wait_timeout` | `5.0` | Seconds to wait for admission. |
| `max_workers` | `auto` | Jobs running at once. `auto` sizes CPU-bound and I/O-bound jobs separately from the capacity the manager measures, up to `EngineLimits.max_offloader_workers` (512) and `resource_config.max_concurrency`. An integer fixes the pool size. |
| `max_cpu_jobs` | `None` | Engine-wide cap on CPU-bound jobs running at once. See [Optional CPU limits](../guide/concepts/offloading.md#optional-cpu-limits). |
| `workload_limits` | `{}` | Per-`WorkloadKind` caps on jobs running at once. |
| `max_concurrent_jobs_per_tenant` | `4` | Default per-tenant fair share. `TenantPolicy.max_concurrent_offloaded_jobs` overrides it per tenant. |
| `shutdown_timeout` | `30.0` | Seconds `close` waits for running threads. |
| `resource_config` | `ResourceConfig(cpu_budget_fraction=0.05, max_concurrency=512)` | Settings of the engine-owned manager. |

The manager also supplies capacity information for automatic ingestion and
lifecycle concurrency. See
[Automatic concurrency](../getting-started/configuration.md#automatic-concurrency).

## Share your own manager

To share one manager between the engine and the rest of your application,
start it yourself and pass it in `EngineResources`. The engine never starts or
closes it.

```python
import asyncio

from resourcera import ResourceManager
from retrievalloom import EngineResources, RetrievalEngine


async def main() -> None:
    manager = ResourceManager()
    await manager.start()
    try:
        async with RetrievalEngine(resources=EngineResources(resource_manager=manager)) as engine:
            print((await engine.health()).status)
    finally:
        await manager.close()


asyncio.run(main())
```

`resource_config` is ignored when you pass a manager.

## Next steps

- See how jobs are scheduled in [Offloading and admission](../guide/concepts/offloading.md).
- Resolve `ResourceAdmissionError` with [Troubleshooting](../guide/troubleshooting.md).
