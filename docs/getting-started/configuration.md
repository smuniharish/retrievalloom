# Configuration

The defaults run a complete in-memory engine, so you change only what your
workload needs: how long a request may take, which search methods run, how many
results come back, and how much each customer may use. This page lists the
settings you are most likely to change. The [API reference](../api/config.md)
lists every field.

## Create a configured engine

An engine takes three arguments, all optional:

| Argument | Type | Holds |
| --- | --- | --- |
| `config` | `EngineConfig` | Settings: `ingestion`, `retrieval`, `lifecycle`, `offloader`, `limits`, `health_timeout`, `log_content` |
| `components` | `Components` | Your own components. `None` fields select the defaults. |
| `resources` | `EngineResources` | Application-owned infrastructure: `data_dir`, `resource_manager`, `executor`, `xai_runtime`, `metrics` |

Configuration objects are immutable Pydantic models that reject unknown
fields, so a typo fails at once instead of being ignored. Build them once and
pass them in:

```python
from pathlib import Path

from retrievalloom import (
    ChannelsConfig,
    EngineConfig,
    EngineResources,
    FusionConfig,
    IndexChannelConfig,
    IngestionConfig,
    LifecycleConfig,
    MethodConfig,
    RetrievalConfig,
    RetrievalEngine,
    TenantPolicy,
)

config = EngineConfig(
    ingestion=IngestionConfig(index_wait="none"),
    retrieval=RetrievalConfig(
        default_top_k=5,
        request_timeout=10.0,
        channels=ChannelsConfig(
            index=IndexChannelConfig(sparse=MethodConfig(candidate_k=100, weight=2.0)),
        ),
        method_fusion=FusionConfig(method="weighted_rrf"),
        tenant_policies={"free-tier": TenantPolicy(max_concurrent_requests=2, max_top_k=20)},
    ),
    lifecycle=LifecycleConfig(poll_interval=1.0),
)
engine = RetrievalEngine(config, resources=EngineResources(data_dir=Path("rl-data")))
```

If you prefer keyword arguments, `create_retrieval_engine(ingestion=...,
retrieval=..., lifecycle=..., offloader=..., limits=..., health_timeout=...,
components=..., resources=...)` builds the same engine.

`EngineConfig.health_timeout` (default `5` seconds) bounds each component
check that `health()` runs. Raise it when a remote backend answers health
checks slowly.

## Control how documents are ingested

`IngestionConfig` decides whether `ingest` waits until new documents are
searchable, how invalid documents are handled, and how large one call may be.

| Field | Default | Meaning |
| --- | --- | --- |
| `index_wait` | `all` | `all`: `ingest` and `delete` run one indexing round before returning. `none`: they return at once and indexing runs in the background. |
| `index_wait_timeout` | `120` | Seconds `ingest` waits for that round. On timeout it returns with an `index_wait_timeout` warning; indexing continues. |
| `failure_policy` | `per_document` | `per_document` reports invalid documents and stores the rest. `fail_batch` rejects the whole call. |
| `concurrency` | `auto` | Graph extractions running at once across the engine. See [Automatic concurrency](#automatic-concurrency). |
| `transient_metadata_keys` | `()` | Metadata keys whose change never creates a new version. It only bumps the document's `metadata_revision`. |
| `redelivery_window_seconds` | `300` | Window for the `possible_redelivery` warning. `0` disables it. |
| `limits` | `IngestionLimits()` | Per-call limits: documents (1000), representations (16), chunks (10000), text characters (10 000 000), tree depth (64) and nodes (50000), graph items (50000). |

## Control how searches run

`RetrievalConfig` decides which search methods run, how their results are
merged, how many results come back, and how long a request may take.

| Field | Default | Meaning |
| --- | --- | --- |
| `channels` | index on, graph and structure off | Which channels and methods run. See [Channels and methods](../guide/concepts/channels.md). |
| `method_fusion` | `rrf`, `k=60` | Fuses the methods of the index channel. |
| `channel_fusion` | `rrf`, `k=60` | Fuses the channels. |
| `reranking` | `enabled=True`, `rerank_k=50`, `model_dir=None`, `model_name="ms-marco-TinyBERT-L-2-v2"`, `allow_download=False` | Reranks the top candidates with the default FlashRank reranker (when its model files are present) or `Components.reranker`. See [Rerank and verify with NLI](../guide/how-to/rerank-and-nli.md). |
| `nli` | `enabled=False`, `nli_k=10` | Judges claims against candidates when an NLI model is configured. |
| `consolidation` | see below | Groups candidates into evidence. |
| `default_top_k` | `10` | Evidence items returned when `top_k` is not given. |
| `request_timeout` | `30` | Seconds per request. |
| `method_timeout` | `10` | Seconds per retrieval method. |
| `allow_partial_on_deadline` | `True` | When the deadline passes during the methods, continue with the methods that finished and warn `timeout_partial`. With `False`, raise `OperationTimeoutError`. |
| `missing_index_policy` | `skip` | What to do when a method has no index yet: `skip`, `fail` or `schedule_backfill`. |
| `allow_stale_versions` | `False` | Keep hits that point at a superseded document version. |
| `max_query_chars` | `4096` | Longest accepted query. |
| `max_filter_nodes` | `64` | Largest accepted filter expression. |
| `filter_overfetch` | `4` | When a backend cannot apply metadata filters itself, the method fetches `candidate_k` times this factor and filters afterwards. If too few hits survive, the result carries a `filter_underfetch` warning. |
| `embedding_cache_size` | `1024` | Cached query embeddings (`0` disables the cache). |
| `embedding_cache_ttl` | `600` | Seconds a query embedding stays cached. |
| `fail_on_recording_error` | `False` | Raise instead of warning when the provenance recorder fails. |
| `default_tenant_policy` | `TenantPolicy()` | Limits for tenants without their own policy. |
| `tenant_policies` | `{}` | Per-tenant `TenantPolicy`, keyed by tenant id. |

### Tune each search method

Each method of the index channel (`keyword`, `fuzzy`, `sparse`, `dense`) has a
`MethodConfig`:

| Field | Default | Meaning |
| --- | --- | --- |
| `enabled` | `True` | Run the method. |
| `candidate_k` | `50` | Hits requested from the method. |
| `weight` | `1.0` | Weight in weighted fusion. |
| `on_error` | `degrade` | `degrade` turns a failure into a warning. `fail` fails the request. |

`fuzzy` uses `FuzzyConfig`, which adds `score_cutoff` (85), `max_ngram` (3),
`matches_per_ngram` (5), `corpus_vocabulary` (`True`) and `min_token_length`
(3). `matches_per_ngram` is how many similar vocabulary terms each query word
expands to. Raise it when a large vocabulary holds many close spellings and
relevant documents are missed.

### Choose how rankings are merged

Fusion merges the ranked lists of several methods into one. `FusionConfig.method`
is one of `rrf`, `weighted_rrf`, `weighted_score`,
`combsum` or `combmnz`. Score-based methods normalize each list first with
`normalizer`: `minmax` (default), `zscore`, `rank` or `sigmoid`. Reciprocal rank
fusion (`rrf`, the default) uses only ranks, so it needs no normalization.

### Group results into evidence

`ConsolidationConfig` controls how matching passages are grouped into the
evidence items you receive:
`window_multiplier` (3), `group_structural` (`True`), `merge_adjacent` (`True`),
`span_iou_threshold` (0.5), `semantic_relations` (`False`),
`semantic_threshold` (0.9) and `contradiction_overflow`.

### Limit each tenant

`TenantPolicy` caps what one tenant can use, so a busy customer cannot slow
down the others. Set a default for everyone in `default_tenant_policy` and
overrides in `tenant_policies`:

| Field | Default |
| --- | --- |
| `max_concurrent_requests` | `16` |
| `max_concurrent_offloaded_jobs` | `None`: uses `OffloaderConfig.max_concurrent_jobs_per_tenant` (4) |
| `max_top_k` | `100` |
| `max_candidate_k` | `500` |
| `max_ingest_batch` | `1000` |
| `allowed_channels` | all channels |

`max_candidate_k` caps the `candidate_k` of every method for that tenant. When
it reduces a configured value, the result carries a `limit_clamped` warning of
severity `info` naming the setting, the requested value and the applied value.
To use a `candidate_k` above 500, raise `max_candidate_k` in the tenant policy
as well.

!!! note "Renamed in 0.2.0"
    `max_concurrent_offloaded_jobs` was called `max_concurrent_cpu_jobs`. The
    old name is still accepted and emits a `DeprecationWarning`.

## Control background indexing and retention

`LifecycleConfig` decides when indexing runs, how an engine rebuilds its
in-memory indexes after a restart, and how long deleted documents, old versions
and history are kept. For how indexing works, see
[Generations and coverage](../guide/concepts/lifecycle.md).

| Field | Default | Meaning |
| --- | --- | --- |
| `background` | `True` | Run a scheduler that indexes and maintains in the background. With `False`, indexing happens during `ingest`, `wait_for` and `run_lifecycle_once`. |
| `poll_interval` | `2` | Seconds between background rounds. |
| `batch_size` | `128` | Documents per backend write batch. Writes and removals are further split to the backend's own `max_batch`. |
| `max_concurrency` | `auto` | Index units processed concurrently. See [Automatic concurrency](#automatic-concurrency). |
| `max_attempts` | `3` | Attempts per unit round for retryable errors, and attempts per document before its coverage is marked `failed`. |
| `retry_failed_after` | `30` | Seconds before coverage that failed with a retryable error, such as a `BackendUnavailableError` during an outage, is queued again. The delay doubles with each further failure. Non-retryable failures stay `failed` until a `backfill`. `None` disables automatic retries. |
| `retry_failed_max_delay` | `900` | Upper bound, in seconds, of the doubling retry delay. |
| `unit_timeout` | `600` | Seconds one unit round may take, counted from when it starts running rather than from when it is queued. |
| `instance_ttl` | `120` | Seconds an engine instance stays registered without a heartbeat. |
| `lease_ttl` | `120` | Seconds a durable reader lease lasts. See [Run several engines on one store](../guide/how-to/multiple-engines.md). |
| `generation_retention` | `0` | Extra seconds a retired generation is kept before it is dropped. |
| `auto_rebuild_ephemeral` | `True` | Rebuild ephemeral (in-memory) backends from the metadata store at startup. Without `background` (or with `wait_for_rebuild`), the rebuild finishes before the engine opens; otherwise it runs in the first background round. |
| `wait_for_rebuild` | `False` | With `background`, still finish the startup rebuild before the engine opens, so the first requests get no `index_missing` warnings. |
| `rebuild_timeout` | `300` | Seconds a startup rebuild may delay opening when `background` is on. On timeout the engine opens and the rebuild continues in the background. |
| `purge_after` | `604800` (7 days) | Seconds a deleted document stays a tombstone before maintenance erases it physically. `None` disables. |
| `max_versions` | `10` | Version-history entries kept per document. Older versions that nothing references (including the `version_after` of a retained ingestion event) are pruned with their representations. `None` disables. |
| `event_retention` | `7776000` (90 days) | Seconds committed and failed ingestion events are kept. `None` disables. |
| `purge_log_retention` | `86400` (1 day) | Seconds purge-log entries are kept. Other engines read the log each lifecycle round to evict purged documents from their in-memory indexes; an engine that misses pruned entries rebuilds those indexes instead. Keep it well above `instance_ttl`. |
| `housekeeping_interval` | `300` | Seconds between retention and purge passes. |
| `migration` | `auto` | `auto` upgrades the metadata schema on start. `verify` only checks it. |
| `shutdown_timeout` | `30` | Seconds `close` waits for in-flight work. |

## Size the worker pool

Blocking work, such as model inference and index builds, runs in a thread pool
so it never stalls your event loop. `OffloaderConfig` sizes that pool and caps
how much of it one tenant may use. See
[Offloading and admission](../guide/concepts/offloading.md).

| Field | Default |
| --- | --- |
| `max_workers` | `auto` |
| `max_cpu_jobs` | `None` |
| `workload_limits` | `{}` |
| `max_concurrent_jobs_per_tenant` | `4` |
| `admission_wait_timeout` | `5` |
| `memory_per_job` | `"16MB"` |
| `cpu_per_job` | `None` |
| `shutdown_timeout` | `30` |
| `resource_config` | resourcera `ResourceConfig(cpu_budget_fraction=0.05)` |

## Automatic concurrency

Three settings decide how much work runs at once: `IngestionConfig.concurrency`,
`LifecycleConfig.max_concurrency` and `OffloaderConfig.max_workers`. Each
defaults to `AutoConcurrency`, which sizes the limit from the host capacity
that [resourcera](../integrations/resourcera.md) measures. The limit starts at
a floor, grows while work is waiting and the host has room, shrinks when
capacity drops, and halves when a backend times out or becomes unavailable.

| Setting | Floor | Ceiling |
| --- | --- | --- |
| `ingestion.concurrency` | 8 | `limits.max_ingestion_concurrency` (256) |
| `lifecycle.max_concurrency` | 4 | `limits.max_lifecycle_concurrency` (64) |
| `offloader.max_workers` | CPU-bound jobs: `min(8, cpu_count)`, at least 2. I/O-bound jobs: 8 | `limits.max_offloader_workers` (512) |

Narrow the range with `AutoConcurrency(minimum=..., maximum=...)`, or pass an
integer to fix the limit:

```python
from retrievalloom import AutoConcurrency, EngineConfig, IngestionConfig, LifecycleConfig

config = EngineConfig(
    ingestion=IngestionConfig(concurrency=AutoConcurrency(minimum=4, maximum=32)),
    lifecycle=LifecycleConfig(max_concurrency=8),  # fixed
)
```

`health()` reports the current state of every limiter under
`details["concurrency"]`, keyed `ingestion`, `lifecycle`, `offloader.cpu` and
`offloader.io`. Each entry gives the mode, the current limit, its bounds, and
how many permits are in use or waiting.

## Raise the built-in limits

`EngineConfig.limits` holds an `EngineLimits` object: the upper bounds that the
rest of the configuration is checked against. They protect a typical
deployment from a setting that would exhaust memory, threads or a backend.
None of them is fixed; raise any field when your hardware and backends allow
more.

| Field | Default | Bounds |
| --- | --- | --- |
| `max_candidate_k` | `10000` | `candidate_k` of every method and channel |
| `max_rerank_k` | `1000` | `reranking.rerank_k` |
| `max_nli_k` | `200` | `nli.nli_k` |
| `max_fuzzy_ngram` | `6` | `fuzzy.max_ngram` |
| `max_fuzzy_matches_per_ngram` | `100` | `fuzzy.matches_per_ngram` |
| `max_graph_depth` | `4` | `graph.depth` |
| `max_ingestion_concurrency` | `256` | `ingestion.concurrency` |
| `max_lifecycle_batch` | `10000` | `lifecycle.batch_size` |
| `max_lifecycle_concurrency` | `64` | `lifecycle.max_concurrency` |
| `max_lifecycle_attempts` | `20` | `lifecycle.max_attempts` |
| `max_offloader_workers` | `512` | `offloader.max_workers` |
| `max_claims` | `32` | Claims in one retrieval request |
| `max_window_multiplier` | `20` | `consolidation.window_multiplier` |

A setting above its bound fails when the configuration is built, and the
error names the limit to raise:

```text
retrieval.channels.graph.depth=5 exceeds limits.max_graph_depth=4;
raise EngineLimits.max_graph_depth (EngineConfig.limits) to allow it
```

```python
from retrievalloom import (
    ChannelsConfig,
    EngineConfig,
    EngineLimits,
    GraphChannelConfig,
    RetrievalConfig,
)

config = EngineConfig(
    limits=EngineLimits(max_graph_depth=5, max_claims=128),
    retrieval=RetrievalConfig(
        channels=ChannelsConfig(graph=GraphChannelConfig(enabled=True, depth=5)),
    ),
)
```

Some backends have real limits of their own. A graph store reports the
deepest traversal it supports in `IndexCapabilities.max_depth`; the bundled
Ladybug store supports 5. `initialize` raises `ConfigurationError` when the
configured depth exceeds it, so the problem surfaces at startup rather than
on the first request.

## Override settings for one request

Pass a `RetrievalExecutionConfig` to `retrieve` to change the configuration for
that request only, for example to compare methods or to turn on reranking. Its channel and method fields are tri-state: `None` keeps the
configured value, `True` forces the method on, `False` turns it off.

```python
from retrievalloom import RetrievalExecutionConfig

result = await engine.retrieve(
    "event loop",
    scope=scope,
    config=RetrievalExecutionConfig(fuzzy=False, reranking=True, include_candidates=True),
)
```

Forcing a method on that has no backend produces a `method_unavailable` warning
of severity `warning`. Forcing on a channel the tenant policy forbids raises
`ConfigurationError`.

## Next steps

- Learn which [channels and methods](../guide/concepts/channels.md) to enable.
- Add semantic search with [your own embedding model](../guide/how-to/embedding-model.md).
- See every field in the [API reference](../api/config.md).
