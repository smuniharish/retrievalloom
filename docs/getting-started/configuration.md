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
| `config` | `EngineConfig` | Settings: `ingestion`, `retrieval`, `lifecycle`, `offloader`, `log_content` |
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
retrieval=..., lifecycle=..., offloader=..., components=..., resources=...)`
builds the same engine.

## Control how documents are ingested

`IngestionConfig` decides whether `ingest` waits until new documents are
searchable, how invalid documents are handled, and how large one call may be.

| Field | Default | Meaning |
| --- | --- | --- |
| `index_wait` | `all` | `all`: `ingest` and `delete` run one indexing round before returning. `none`: they return at once and indexing runs in the background. |
| `index_wait_timeout` | `120` | Seconds `ingest` waits for that round. On timeout it returns with an `index_wait_timeout` warning; indexing continues. |
| `failure_policy` | `per_document` | `per_document` reports invalid documents and stores the rest. `fail_batch` rejects the whole call. |
| `concurrency` | `8` | Documents prepared concurrently. |
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
`corpus_vocabulary` (`True`) and `min_token_length` (3).

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
| `max_concurrent_cpu_jobs` | `4` |
| `max_top_k` | `100` |
| `max_candidate_k` | `500` |
| `max_ingest_batch` | `1000` |
| `allowed_channels` | all channels |

## Control background indexing and retention

`LifecycleConfig` decides when indexing runs, how an engine rebuilds its
in-memory indexes after a restart, and how long deleted documents, old versions
and history are kept. For how indexing works, see
[Generations and coverage](../guide/concepts/lifecycle.md).

| Field | Default | Meaning |
| --- | --- | --- |
| `background` | `True` | Run a scheduler that indexes and maintains in the background. With `False`, indexing happens during `ingest`, `wait_for` and `run_lifecycle_once`. |
| `poll_interval` | `2` | Seconds between background rounds. |
| `batch_size` | `128` | Documents per backend write batch. |
| `max_concurrency` | `4` | Index units processed concurrently. |
| `max_attempts` | `3` | Attempts per unit round for retryable errors, and attempts per document before its coverage is marked `failed`. |
| `retry_failed_after` | `30` | Seconds before coverage that failed with a retryable error, such as a `BackendUnavailableError` during an outage, is queued again. The delay doubles with each further failure. Non-retryable failures stay `failed` until a `backfill`. `None` disables automatic retries. |
| `retry_failed_max_delay` | `900` | Upper bound, in seconds, of the doubling retry delay. |
| `unit_timeout` | `600` | Seconds one unit round may take. |
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
| `migration` | `auto` | `auto` upgrades the metadata schema on start. `verify` only checks it. |
| `shutdown_timeout` | `30` | Seconds `close` waits for in-flight work. |

## Size the worker pool

Blocking work, such as model inference and index builds, runs in a thread pool
so it never stalls your event loop. `OffloaderConfig` sizes that pool and caps
how much of it one tenant may use. See
[Offloading and admission](../guide/concepts/offloading.md).

| Field | Default |
| --- | --- |
| `max_workers` | `8` |
| `max_cpu_jobs` | `None` |
| `workload_limits` | `{}` |
| `max_concurrent_jobs_per_tenant` | `4` |
| `admission_wait_timeout` | `5` |
| `memory_per_job` | `"16MB"` |
| `cpu_per_job` | `None` |
| `shutdown_timeout` | `30` |
| `resource_config` | resourcera `ResourceConfig(cpu_budget_fraction=0.05)` |

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
