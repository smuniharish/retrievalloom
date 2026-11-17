# Reliability suites

Four opt-in live suites check how RetrievalLoom behaves over time, under outages and
under contention between processes. Each one is skipped unless you set its environment
variable. They are marked `live` like the rest of `tests/live`, so the default run
(`pytest -m "not live"`) never starts them.

| Suite | File | Enabled by | Needs |
| --- | --- | --- | --- |
| Soak | `tests/live/test_soak_workload.py` | `RL_TEST_SOAK_SECONDS` | Nothing. The Postgres variant also needs `RL_TEST_POSTGRES_DSN`. |
| Fault injection | `tests/live/test_faults.py` | `RL_TEST_FAULTS=1` | The `podman` CLI and the Postgres and Qdrant images. |
| Multi-process | `tests/live/test_multi_process.py` | `RL_TEST_POSTGRES_DSN` | A Postgres server. |
| Outage recovery | `tests/live/test_channel_combinations.py` | `RL_TEST_FAULTS=1` | The `podman` CLI and the Qdrant image. |

Shared helpers are in `tests/live/_reliability_helpers.py`. The multi-process worker is
in `tests/live/_reliability_worker.py`.

## Soak

```powershell
$env:RL_TEST_SOAK_SECONDS = "120"
$env:RL_TEST_POSTGRES_DSN = "postgresql://rl:rl@localhost:5432/rl"   # optional
uv run pytest tests/live/test_soak_workload.py -s --no-cov
```

One engine runs a random mix of operations for `RL_TEST_SOAK_SECONDS` seconds:

- ingest and update, 35%;
- retrieve, 45%;
- delete, 10%;
- purge, 5%;
- status, 5%.

Four workers do the work at once, and the lifecycle runs in the background with
retention enabled. The suite runs once with the SQLite store and once with the Postgres
store. The Postgres variant uses a temporary schema that is dropped afterwards.

The text comes from a fixed vocabulary, so the index vocabularies stop growing after
warm-up. If nothing leaks, memory stays flat.

The suite fails if any of these is true:

- Traced Python memory (`tracemalloc`) grows faster than `RL_TEST_SOAK_MAX_SLOPE_KIB`
  KiB/s. The slope is a least-squares fit over the second half of the run, and the
  default threshold is `32`. RSS is also reported when `psutil` is installed, but it
  is not checked. Short runs still include some warm-up of bounded caches, and the
  default in-memory Qdrant index keeps deleted points until the next generation, so
  update churn grows it slowly. Run for 300 s or more for a tighter signal, and set
  `RL_TEST_SOAK_TRACE=25` to print the top allocation growth sites (this slows the
  workload considerably).
- The number of asyncio tasks or threads after the run is more than two above the
  baseline.
- The event loop stalls for 1 s or more.
- An acknowledged write is missing from the metadata store, or the store does not
  match the expected state. Active documents must be at their acknowledged version.
  Deleted documents must be tombstoned or erased by retention. Purged documents must
  be gone.
- Coverage does not converge.
- `health()` is not healthy at the end.

The test prints a `SOAK {...}` line with throughput, latency percentiles, retrieval
statuses and warnings, memory, tasks, threads and loop lag.

## Fault injection

```powershell
$env:RL_TEST_FAULTS = "1"
$env:RL_TEST_POSTGRES_DSN = "postgresql://rl:rl@<podman host>:5432/rl"   # names the host
uv run pytest tests/live/test_faults.py -s --no-cov
```

The suite creates its own containers and removes them when the module finishes:

- `rl-fault-postgres` on port `RL_TEST_FAULTS_PG_PORT` (default `55442`);
- `rl-fault-qdrant` on ports `RL_TEST_FAULTS_QDRANT_PORT` and
  `RL_TEST_FAULTS_QDRANT_GRPC_PORT` (defaults `6343` and `6344`).

It never touches the `rl-postgres` or `rl-qdrant` containers that the other live suites
use, but it takes its images from them when they exist. Otherwise it uses
`postgres:18-alpine` and `qdrant/qdrant:latest`. It never pulls an image.

Ports are published on `RL_TEST_FAULTS_HOST`. If that is not set, the suite uses the
host of `RL_TEST_POSTGRES_DSN` or `RL_TEST_QDRANT_URL`, which is the Podman machine
address on Windows and macOS. If neither is set, it uses `127.0.0.1`.

The store under test uses short driver timeouts (connect 3 s, command 5 s, pool 5 s,
lock 5 s). Each call is bounded at 60 s, and the test fails if a call takes longer.

| Test | Fault | What it checks |
| --- | --- | --- |
| `test_postgres_pause_mid_ingest` | `podman pause` during a 200-document ingest | Ingest, retrieve and delete raise retryable `RetrievalLoomError`s, and `health()` turns unhealthy. After `unpause`, retrying the ingest succeeds and is idempotent: every document is at version 1. The failed delete changed nothing, coverage converges and health recovers. |
| `test_postgres_restart_under_background_lifecycle` | `podman stop` and `start` with the background lifecycle on | Writes, reads and `status()` fail with retryable errors. After the restart, the background scheduler catches up with no manual lifecycle rounds. |
| `test_qdrant_pause_degrades_retrieval` | `podman pause` of the vector database | Retrieval returns `partial` results with a `stage_failed_degraded` warning instead of failing. Ingest commits the metadata and reports the same warning. Keyword retrieval still finds new documents. After `unpause`, the lifecycle restores `complete` results and health. The test runs a `backfill` only if dense coverage is still unsettled. |
| `test_qdrant_restart_during_rebuild` | Qdrant stopped while a rebuild is requested | The rebuild task ends `failed` after its attempts, and the old dense generation stays the only active one. Retrieval is `partial` but not empty. After `start`, a new rebuild succeeds. |

## Outage recovery

```powershell
$env:RL_TEST_FAULTS = "1"
uv run pytest tests/live/test_channel_combinations.py -k recovers -s --no-cov
```

`test_dense_coverage_recovers_after_qdrant_outage` checks that dense coverage
heals by itself after a vector-store outage. It creates its own `rl-chan-qdrant`
container on port `RL_TEST_CHANNELS_QDRANT_PORT` (default `6353`), from the image
of `rl-qdrant` when it exists, and removes it afterwards.

1. Five documents are ingested and indexed by the dense method.
2. The container is stopped and five more documents are ingested. Their dense
   coverage ends `failed`.
3. The container is started again. With no `backfill`, the background lifecycle
   retries the failed coverage (`retry_failed_after=1`, `retry_failed_max_delay=4`)
   until all ten documents are `ready`.
4. A search is `complete`, and the dense method finds the documents ingested
   during the outage.

The other tests in the file run the channel on/off scenarios on live durable
backends. They are not part of this suite: they run with the other live tests
when `RL_TEST_QDRANT_URL` (and, for the Postgres variant, `RL_TEST_POSTGRES_DSN`)
is set.

## Multi-process

```powershell
$env:RL_TEST_POSTGRES_DSN = "postgresql://rl:rl@localhost:5432/rl"
$env:RL_TEST_MP_SECONDS = "30"   # default 30
$env:RL_TEST_MP_WORKERS = "4"    # default 4
uv run pytest tests/live/test_multi_process.py -s --no-cov
```

The suite starts `RL_TEST_MP_WORKERS` OS processes (`python -m
tests.live._reliability_worker`). Each process opens its own engine with a background
lifecycle on one fresh Postgres schema, so the processes also race the migrations. At a
shared start time, every process runs ingest, update, delete, purge and retrieve on the
same 12 document ids. Each write carries a unique writer tag in its text and metadata.

The suite runs twice, with two layouts:

- `shared-scope`: every process writes the same scope, so they contend on the same rows.
- `tenant-scopes`: each process writes its own tenant scope on the same tables.

A fresh engine then checks these invariants:

- Every process exits cleanly. Every error was a structured, retryable
  `RetrievalLoomError`, and none was unstructured, such as a raw deadlock.
- No version is acknowledged twice, and each process sees its own versions of an id
  increase.
- Every acknowledged version is stored with its writer's tag, so no committed write is
  lost.
- Version numbers have no gaps, and the document's current version is the highest one.
- A stored version that nobody acknowledged can only come from a call that raised.
- There are no duplicate documents, and purged documents are gone.
- With `tenant-scopes`, each scope holds only its own process's writes.
- Coverage converges, `documents_active` matches the store, and each unit has one
  active generation.
- Two more processes get the same retrieval results as the checking engine. For each
  document's unique tag, that document ranks first.

The test prints a `MULTIPROC {...}` line with operation counts, throughput, mean
latencies and errors by code.
