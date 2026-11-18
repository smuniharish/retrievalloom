# Configuration

Use this page to decide which retrieval methods run, how their results
combine, and how much each tenant may use. Configuration models are frozen
pydantic models that reject unknown fields: build them once and pass them to
`create_retrieval_engine`.

```python
from retrievalloom import (
    ChannelsConfig,
    FusionConfig,
    IndexChannelConfig,
    IngestionConfig,
    MethodConfig,
    RetrievalConfig,
    TenantPolicy,
    create_retrieval_engine,
)

engine = create_retrieval_engine(
    ingestion=IngestionConfig(index_wait="all"),
    retrieval=RetrievalConfig(
        default_top_k=5,
        request_timeout=10.0,
        channels=ChannelsConfig(
            index=IndexChannelConfig(sparse=MethodConfig(candidate_k=100, weight=2.0))
        ),
        method_fusion=FusionConfig(method="weighted_rrf"),
        tenant_policies={"free-tier": TenantPolicy(max_concurrent_requests=2, max_top_k=20)},
    ),
)
```

## Choose channels and methods

A *channel* groups *methods*. Method keys in results have the form
`<channel>.<method>`, for example `index.sparse`.

| Channel | Method | Finds | On by default | Needs |
| --- | --- | --- | --- | --- |
| `index` | `keyword` | Terms from the `keywords` metadata field, such as error codes | Yes | Terms in metadata |
| `index` | `fuzzy` | Misspelled words | Yes | Nothing |
| `index` | `sparse` | Shared words, ranked by BM25 | Yes | Nothing |
| `index` | `dense` | Paraphrases | Yes, once a model is supplied | `Components(embedding=...)` |
| `graph` | `graph` | Entities and their neighbors | No | `GraphChannelConfig(enabled=True)` and the `graph` extra |
| `structure` | `structure` | Sections of a document tree | No | `StructureChannelConfig(enabled=True)` and the `structure` extra |

```python
from retrievalloom import (
    ChannelsConfig,
    FuzzyConfig,
    GraphChannelConfig,
    IndexChannelConfig,
    RetrievalConfig,
)

retrieval = RetrievalConfig(
    channels=ChannelsConfig(
        index=IndexChannelConfig(fuzzy=FuzzyConfig(enabled=False)),
        graph=GraphChannelConfig(enabled=True),
    )
)
```

- At least one channel, and for the index channel at least one method, must
  be enabled; otherwise building `ChannelsConfig` fails with a pydantic
  `ValidationError`. An engine whose enabled methods all lack a backend, such
  as `dense` alone without an embedding model, raises `ConfigurationError`.
- A `vector_store` without an `embedding` raises `ConfigurationError`.
- `MethodConfig`: `enabled`, `candidate_k` (50), `weight` (1.0), `on_error`
  (`degrade` or `fail`). `fuzzy` takes `FuzzyConfig` with `score_cutoff` (85),
  `max_ngram` (3), `corpus_vocabulary` (`True`), `min_token_length` (3).
- Use other metadata fields or fixed terms for the keyword method with
  `Components(keyword_dictionary=MetadataKeywordDictionary(fields=(...),
  static_entries=[KeywordEntry(canonical=..., aliases=(...))]))` from
  `retrievalloom.index.text` and `retrievalloom.contracts.models`.

## Enable a method on an existing store

Adding a method, for example an embedding model for `dense`, does not need a
re-ingest. The lifecycle builds the new index from the stored documents. To
start and await that work explicitly:

```python
ref = await engine.backfill(scope=scope, channels=["index"], methods=["dense"])
result = await engine.wait_for(ref, timeout=600)
assert result.state.value == "succeeded", result.error
```

Until it finishes, requests skip the method with an `index_missing` or
`partial_coverage` warning. Changing the embedding model or any setting that
changes index content makes the old index stale (`stale_coverage`); the
lifecycle rebuilds it.

## Combine results

`RetrievalConfig.method_fusion` combines the methods of the index channel and
`channel_fusion` combines channels. `FusionConfig.method` is `rrf` (default,
`k=60`), `weighted_rrf`, `weighted_score`, `combsum`, or `combmnz`.
Score-based methods normalize each list with `normalizer`: `minmax`
(default), `zscore`, `rank`, or `sigmoid`. Weights come from
`MethodConfig.weight` and each channel config's `weight`, and matter only for
weighted methods.

## Bound time and size

| `RetrievalConfig` field | Default | Effect |
| --- | --- | --- |
| `default_top_k` | 10 | Evidence returned without `top_k` |
| `request_timeout` | 30 s | Per request |
| `method_timeout` | 10 s | Per method |
| `allow_partial_on_deadline` | `True` | Continue with finished methods (`timeout_partial`); `False` raises `OperationTimeoutError` |
| `missing_index_policy` | `skip` | `skip`, `fail` (`CoverageError`), or `schedule_backfill` |
| `allow_stale_versions` | `False` | Keep hits of a superseded document version |
| `max_query_chars` | 4096 | Longer queries raise `ValidationError` |
| `max_filter_nodes` | 64 | Larger filters raise `ValidationError` |
| `embedding_cache_size`, `embedding_cache_ttl` | 1024, 600 s | Query embedding cache |
| `fail_on_recording_error` | `False` | Raise instead of warn when provenance recording fails |

## Limit each tenant

`TenantPolicy` sets `max_concurrent_requests` (16),
`max_concurrent_offloaded_jobs` (`None`, falling back to
`OffloaderConfig.max_concurrent_jobs_per_tenant`, 4; the old name
`max_concurrent_cpu_jobs` still works with a `DeprecationWarning`), `max_top_k` (100), `max_candidate_k` (500), `max_ingest_batch` (1000),
and `allowed_channels`. Set `RetrievalConfig.default_tenant_policy` and
per-tenant `tenant_policies={"tenant-id": TenantPolicy(...)}`. A `top_k` or an
ingest batch above its limit raises `ValidationError`, and `candidate_k` is
capped at `max_candidate_k` with an `info` `limit_clamped` warning. Requests beyond `max_concurrent_requests` wait
for a slot within their deadline. Forcing a disallowed channel raises
`ConfigurationError`.

## Control ingestion

| `IngestionConfig` field | Default | Effect |
| --- | --- | --- |
| `index_wait` | `all` | `ingest` and `delete` wait for one indexing round; `none` returns at once |
| `index_wait_timeout` | 120 s | Then return with `index_wait_timeout`; indexing continues |
| `failure_policy` | `per_document` | Store valid documents and report invalid ones; `fail_batch` rejects the call |
| `concurrency` | `auto` | Graph extractions at once across the engine |
| `transient_metadata_keys` | `()` | Metadata keys whose change creates no new version |
| `redelivery_window_seconds` | 300 | Window for `possible_redelivery` |
| `limits` | `IngestionLimits()` | Per-call caps on documents, representations, chunks, and text |

## Concurrency and limits

`IngestionConfig.concurrency`, `LifecycleConfig.max_concurrency` and
`OffloaderConfig.max_workers` default to `AutoConcurrency`: resourcera sizes
them from measured host capacity. Pass an integer to fix one, or
`AutoConcurrency(minimum=..., maximum=...)` to bound it. `health().details["concurrency"]`
shows each limiter.

Every upper bound is a field of `EngineLimits`, passed as
`EngineConfig(limits=...)`. Raise a field when a setting is rejected with
`exceeds limits.<field>`:

```python
from retrievalloom import EngineConfig, EngineLimits

config = EngineConfig(limits=EngineLimits(max_candidate_k=50_000, max_claims=128))
```

Fields and defaults: `max_candidate_k` 10000, `max_rerank_k` 1000,
`max_nli_k` 200, `max_fuzzy_ngram` 6, `max_fuzzy_matches_per_ngram` 100,
`max_graph_depth` 4, `max_ingestion_concurrency` 256, `max_lifecycle_batch`
10000, `max_lifecycle_concurrency` 64, `max_lifecycle_attempts` 20,
`max_offloader_workers` 512, `max_claims` 32, `max_window_multiplier` 20.

## Override one request

```python
from retrievalloom import RetrievalExecutionConfig

result = await engine.retrieve(
    "wpa3 setup",
    scope=scope,
    config=RetrievalExecutionConfig(fuzzy=False, dense=True, include_candidates=True),
)
```

`None` keeps the configured value, `True` forces a method on, `False` turns
it off. Forcing on a method with no backend adds a `method_unavailable`
warning of severity `warning`, so the result is `partial`.
