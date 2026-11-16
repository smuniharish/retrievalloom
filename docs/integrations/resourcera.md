# resourcera

[resourcera](https://pypi.org/project/resourcera/) provides admission control.
RetrievalLoom uses it in the [offloader](../guide/concepts/offloading.md):
before a blocking job starts in a thread, the job reserves capacity from a
resourcera `ResourceManager`. When the host is short of memory or CPU, jobs
wait instead of piling up threads. A job that waits longer than
`OffloaderConfig.admission_wait_timeout` fails with `ResourceAdmissionError`.

## Engine-owned manager

By default the engine creates, starts and closes its own `ResourceManager`
from `OffloaderConfig.resource_config`. The default raises resourcera's
monitoring CPU budget to 5 % (`cpu_budget_fraction=0.05`) so sampling spikes on
a busy host do not suspend admission.

```python
from resourcera import ResourceConfig
from retrievalloom import EngineConfig, OffloaderConfig

config = EngineConfig(
    offloader=OffloaderConfig(
        memory_per_job="64MB",
        admission_wait_timeout=10.0,
        resource_config=ResourceConfig(cpu_budget_fraction=0.05, max_concurrency=32),
    )
)
```

| `OffloaderConfig` field | Default | Meaning |
| --- | --- | --- |
| `memory_per_job` | `"16MB"` | Memory each job declares. |
| `cpu_per_job` | `None` | Optional CPU each job declares. Needs warm resourcera samples. |
| `admission_wait_timeout` | `5.0` | Seconds to wait for admission. |
| `max_workers` | `8` | Threads of the engine-owned executor. |
| `max_cpu_jobs` | `None` | Engine-wide cap on CPU-bound jobs running at once. See [Capping CPU-bound jobs](../guide/concepts/offloading.md#capping-cpu-bound-jobs). |
| `workload_limits` | `{}` | Per-`WorkloadKind` caps on jobs running at once. |
| `max_concurrent_jobs_per_tenant` | `4` | Default per-tenant fair share. `TenantPolicy.max_concurrent_cpu_jobs` overrides it per tenant. |
| `shutdown_timeout` | `30.0` | Seconds `close` waits for running threads. |
| `resource_config` | `ResourceConfig(cpu_budget_fraction=0.05)` | Settings of the engine-owned manager. |

## Application-owned manager

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
