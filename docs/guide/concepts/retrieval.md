# Retrieval pipeline

Know what happens between a query and its results, so you can tune ranking and
read every warning with confidence. A search runs all enabled methods at the
same time, merges their rankings, optionally reranks the best matches, and
groups them into evidence items. When a method fails or runs out of time, the
search still returns what it found and tells you what was skipped.

## Stages

![The four phases of one retrieve call: prepare (authorize, plan, snapshot, cache lookup), search (all methods concurrently), rank (method fusion, candidates, deduplication, channel fusion) and refine (rerank, NLI, consolidation, recording)](../../assets/diagrams/retrieval-pipeline.png){ .diagram }

`engine.retrieve(query, scope=...)` runs these stages in order:

1. **Authorize.** The access policy authorizes the call and may add a
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

Fusion merges several ranked lists into one. RetrievalLoom uses
[ranknexus](https://pypi.org/project/ranknexus/) at two levels, configured
separately:

- `RetrievalConfig.method_fusion` fuses the methods inside the index channel,
  weighted by `MethodConfig.weight`.
- `RetrievalConfig.channel_fusion` fuses the channels, weighted by the channel
  `weight`.

Both default to Reciprocal Rank Fusion with `k=60`. The supported methods are
`rrf`, `weighted_rrf`, `weighted_score`, `combsum` and `combmnz`. Score-based
methods normalize scores first with `minmax`, `zscore`, `rank` or `sigmoid`.
Hits without a finite score fall back to rank normalization. You can change the
fusion method per request:

```python
config = RetrievalExecutionConfig(method_fusion="weighted_rrf", channel_fusion="rrf")
```

To replace fusion entirely, pass a `FusionABC` as `Components.method_fusion`
or `Components.channel_fusion`.

## Reranking and NLI

A reranker reads the query and each top passage together and reorders them,
which usually improves the first few results. Reranking is enabled by default
(`RerankingConfig.enabled=True`) and uses the
built-in FlashRank reranker when its model files are in
`RerankingConfig.model_dir`; pass `Components(reranker=...)` to use your own.
Models are never downloaded implicitly. Without a model the default reranker is
dropped at startup and the stage is skipped silently; only a request that
explicitly asks for it with `RetrievalExecutionConfig(reranking=True)` gets a
`reranker_unavailable` warning. Fusion (RRF by default) needs no model, and
without reranking the final score is the channel-fusion score.

Natural language inference (NLI) checks whether a passage supports or
contradicts a statement. It is disabled by default. When enabled and a model is
configured, each of the
top `nli_k` candidates is judged against the query and against every claim
passed with `retrieve(..., claims=[...])`. Judgments are stored on candidates
and summarized per evidence item (`Evidence.nli_summary`).

See [Rerank and verify with NLI](../how-to/rerank-and-nli.md).

## Failure handling

A failing model or backend does not have to fail the whole search. Each
method, reranking and NLI has an `on_error` setting:

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

Caching returns repeated searches instantly. `Components.cache` caches whole
results. The default is `NoCache`. With a cache
such as `InMemoryLRUCache`, only `complete` results are stored. The key covers
the request (except `request_id` and `principal`), the effective filter, the
snapshot and the pipeline configuration, so new data or a new generation
never returns an old result.

Query embeddings have their own LRU cache, set by `embedding_cache_size`
and `embedding_cache_ttl`.

## Inspect a result

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

## Next steps

- Read each result's sources and trace in [Evidence and provenance](evidence.md).
- Improve ranking with [a reranker and NLI](../how-to/rerank-and-nli.md).
