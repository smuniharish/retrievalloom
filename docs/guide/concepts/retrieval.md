# Retrieval pipeline

`engine.retrieve(query, scope=...)` runs these stages in order:

1. **Authorise.** The access policy authorises the call and may add a
   mandatory row filter.
2. **Plan.** The request is validated: query length (`max_query_chars`),
   `top_k` against the tenant's `max_top_k`, filter size (`max_filter_nodes`)
   and at most 32 claims. The configuration and the per-request
   `RetrievalExecutionConfig` decide which methods, reranking and NLI run.
3. **Snapshot.** For each planned method, the engine picks the published
   generation of the scope and registers itself as a reader. Coverage gaps
   produce warnings here.
4. **Methods.** All methods run concurrently, each limited by `method_timeout`
   and all by the request deadline (`request_timeout`, or `timeout` per
   request).
5. **Method fusion.** Index-channel methods are fused into one list.
6. **Candidates.** Hits are resolved to objects. Deleted documents, invisible
   versions, superseded versions and objects that fail the filter are dropped.
7. **Deduplication and channel fusion.** Candidates of the same object or with
   equal text are merged. Channels are fused into one order.
8. **Rerank.** The top `rerank_k` candidates with text are rescored by the
   reranker.
9. **NLI.** The top `nli_k` candidates are judged against the query and each
   claim.
10. **Consolidation.** The best `top_k × window_multiplier` candidates are
    grouped into evidence items. The top `top_k` items are returned, plus
    items that contradict them, up to `contradiction_overflow` (default
    `ceil(top_k / 2)`).
11. **Record.** The provenance recorder receives the request and result.

The snapshot is released at the end, so the generations it read cannot be
retired while the request runs.

## Fusion

Fusion uses [ranknexus](https://pypi.org/project/ranknexus/). Two levels are
configured separately:

- `RetrievalConfig.method_fusion` fuses the methods inside the index channel,
  weighted by `MethodConfig.weight`.
- `RetrievalConfig.channel_fusion` fuses the channels, weighted by the channel
  `weight`.

Both default to Reciprocal Rank Fusion with `k=60`. The supported methods are
`rrf`, `weighted_rrf`, `weighted_score`, `combsum` and `combmnz`. Score-based
methods normalise scores first with `minmax`, `zscore`, `rank` or `sigmoid`.
Hits without a finite score fall back to rank normalisation. You can change the
fusion method per request:

```python
config = RetrievalExecutionConfig(method_fusion="weighted_rrf", channel_fusion="rrf")
```

To replace fusion entirely, pass a `FusionABC` as `Components.method_fusion`
or `Components.channel_fusion`.

## Reranking and NLI

Reranking is enabled in the configuration (`RerankingConfig.enabled=True`) but
no reranker is wired by default. Without one, results carry a
`reranker_unavailable` warning with `info` severity. Without reranking, the
final score is the channel-fusion score.

NLI is disabled by default. When enabled and a model is configured, each of the
top `nli_k` candidates is judged against the query and against every claim
passed with `retrieve(..., claims=[...])`. Judgements are stored on candidates
and summarised per evidence item (`Evidence.nli_summary`).

See [Rerank and verify with NLI](../how-to/rerank-and-nli.md).

## Failure handling

Each method, reranking and NLI has an `on_error` setting:

- `degrade` (default): the failure is logged, a warning is added and the
  pipeline continues without that stage.
- `fail`: the error is raised.

Warning codes: `stage_failed_degraded` for errors, `timeout_partial` for
timeouts, `reranker_unavailable` when the reranker reports
`BackendUnavailableError`.

`TenantIsolationError` is never degraded. A backend that returns a hit of
another scope stops the request.

## Result status

| Status | When |
| --- | --- |
| `empty` | No evidence. |
| `partial` | Evidence, plus at least one warning with `warning` or `error` severity. |
| `complete` | Evidence and only `info` warnings, or none. |

Always look at `result.warnings` when the status is not `complete`. The
[troubleshooting page](../troubleshooting.md) lists the codes.

## Caching

`Components.cache` caches whole results. The default is `NoCache`. With a cache
such as `InMemoryLRUCache`, only `complete` results are stored. The key covers
the request (except `request_id` and `principal`), the effective filter, the
snapshot and the pipeline configuration, so new data or a new generation
never returns an old result.

Query embeddings have their own LRU cache, set by `embedding_cache_size`
and `embedding_cache_ttl`.

## Inspecting a result

```python
result = await engine.retrieve("rotate api keys", scope=scope, top_k=5)

print(result.status, result.timings["total"])
for warning in result.warnings:
    print(warning.code, warning.severity, warning.message)
for item in result.evidence:
    print(item.score, item.display_text)
```

Pass `RetrievalExecutionConfig(include_candidates=True)` to also get the
ordered candidates in `result.candidates`.
