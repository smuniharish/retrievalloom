# Lifecycle

Use this page to decide when documents become searchable and who does the
indexing work. Writes land in the metadata store first; the *lifecycle* then
brings every index up to date, either in a background scheduler or during
your calls.

## Pick background or foreground indexing

| Mode | Set | Indexing runs | Use it for |
| --- | --- | --- | --- |
| Background (default) | `LifecycleConfig(background=True)` | Every `poll_interval` (2 s), plus one round per `ingest` and `delete` | Long-running services |
| Foreground | `LifecycleConfig(background=False)` | During `ingest`, `delete`, `purge`, `wait_for`, and `run_lifecycle_once` only | Tests, scripts, serverless, and apps that schedule work themselves |

```python
from retrievalloom import LifecycleConfig, create_retrieval_engine

engine = create_retrieval_engine(lifecycle=LifecycleConfig(background=False))
```

With `IngestionConfig(index_wait="all")`, the default, `ingest` and `delete`
wait for one indexing round of their scope, up to `index_wait_timeout`
(120 s). The documents are searchable when the call returns, in either mode.

A foreground engine that shares a store with other engines must call
`await engine.run_lifecycle_once()` at least every `instance_ttl` (120 s).
The round renews its registration and indexes documents other engines wrote.

## Start without partial results

The keyword, fuzzy, and sparse indexes live in memory. After a restart with a
`data_dir`, the engine rebuilds them from the metadata store
(`auto_rebuild_ephemeral=True`):

- Foreground: the rebuild finishes before `async with engine` enters.
- Background: the rebuild runs in the first round, and earlier requests
  return `partial` with `index_missing`. Set `wait_for_rebuild=True` to open
  only once it is done, bounded by `rebuild_timeout` (300 s), or call
  `await engine.run_lifecycle_once(scope)` after opening.

```python
engine = create_retrieval_engine(
    resources=EngineResources(data_dir=Path("var/rl")),
    lifecycle=LifecycleConfig(wait_for_rebuild=True, rebuild_timeout=120),
)
```

## Backfill and rebuild

Both queue a durable task and return a `LifecycleTaskRef` at once.

```python
ref = await engine.backfill(scope=scope, channels=["index"], methods=["dense"])
result = await engine.wait_for(ref, timeout=300)
if result.state.value != "succeeded":
    raise RuntimeError(result.error)

ref = await engine.rebuild(scope=scope, channel="index", method="dense")
await engine.wait_for(ref, timeout=600)
```

- `backfill` indexes every document the selected units do not cover yet and
  resets `failed` coverage to `pending`. Use it after enabling a method or
  after indexing failures.
- `rebuild` builds a complete new generation of one unit and switches to it
  atomically; running requests finish on the old one. The lifecycle starts
  one on its own after a model or index setting changes.
- `wait_for` returns failed tasks rather than raising; check `state` and
  `error`. A `timeout` raises `OperationTimeoutError` while the task keeps
  running. In foreground mode `wait_for` runs the rounds itself.
- A selection that matches no configured unit raises `ConfigurationError`.
- `RetrievalConfig(missing_index_policy="schedule_backfill")` queues a
  backfill when a request finds a method without an index.

## Watch progress

```python
status = await engine.status(scope=scope)
status.documents_active, status.documents_deleted
status.coverage  # {"index.dense": {"ready": 120, "pending": 3}, ...}
status.generations  # method, generation id, state, backend id
status.tasks  # {"succeeded": 2, "queued": 1}
```

## Delete, purge, and retention

- `delete` hides documents from retrieval at once and keeps their history as
  tombstones. Re-ingesting the id revives the document (`revived`).
- `purge` erases documents physically, in batches of up to 500 per
  transaction, and is idempotent: unknown or already purged ids are in
  `not_found`. If a backend fails, the batch stays in the metadata store and
  the call raises; retry it. Other engines on the store evict purged
  documents from their in-memory indexes at their next round through the
  purge log; retrieval never returns a purged document meanwhile.
- Maintenance erases tombstones older than `purge_after` (7 days), keeps
  `max_versions` (10) versions per document, deletes ingestion events older
  than `event_retention` (90 days), and purge-log entries older than
  `purge_log_retention` (1 day). Set the first three to `None` to disable
  them. Keep `purge_log_retention` well above `instance_ttl`.

## Tune the lifecycle

| `LifecycleConfig` field | Default | Effect |
| --- | --- | --- |
| `batch_size` | 128 | Documents per backend write |
| `max_concurrency` | 4 | Index units processed at once |
| `max_attempts` | 3 | Attempts per unit round for retryable errors |
| `unit_timeout` | 600 s | Time one unit round may take |
| `instance_ttl`, `lease_ttl` | 120 s | Engine registration and durable reader lease lifetimes |
| `generation_retention` | 0 s | Extra time a retired generation is kept |
| `migration` | `auto` | `verify` only checks the metadata schema |
| `shutdown_timeout` | 30 s | Time `close` waits for in-flight work |
