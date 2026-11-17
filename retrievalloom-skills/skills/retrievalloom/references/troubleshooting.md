# Troubleshooting

Use this page when a result is `partial` or `empty`, a call raises, or
evidence is missing. Start with `result.status` and `result.warnings`: every
warning has a stable `code`, a `severity`, a `message`, and `details`.

```python
for warning in result.warnings:
    print(
        warning.severity.value, warning.code.value, warning.method, warning.message, warning.details
    )
```

## Fix a common symptom

| Symptom | Cause | Fix |
| --- | --- | --- |
| `partial` with `index_missing` right after startup | In-memory indexes are rebuilt by the first background round | `LifecycleConfig(wait_for_rebuild=True)`, or `await engine.run_lifecycle_once(scope)` after opening |
| Documents not found right after `ingest` | `index_wait="none"`, or an `index_wait_timeout` warning on the report | Wait for coverage in `status()`, or keep `index_wait="all"` |
| Keyword method never matches | No terms under the `keywords` metadata field | Add `metadata={"keywords": [...]}` or configure a keyword dictionary |
| `method_unavailable` for `dense` | No embedding model | Pass `Components(embedding=...)`, or ignore the `info` warning |
| Reranking has no effect | FlashRank model files missing; the default reranker is dropped silently | Provision the model; see [rerank-and-nli.md](rerank-and-nli.md) |
| Dense skipped with `stale_coverage` after a model change | Old vectors were built by another model | Wait for the automatic rebuild, or `rebuild` and `wait_for` it |
| `partial_coverage` | Some documents are not indexed yet, or failed | Wait, or `backfill`; check `status().coverage` for `failed` |
| Fewer results than `top_k` with a filter | Graph or structure filtered after search (`filter_post_applied`) | Expected; raise `candidate_k` for those channels |
| Another tenant's documents missing | Scopes are isolated | Expected; never query across scopes |
| `ResourceAdmissionError` under load | The offloader could not reserve memory or CPU in time | Lower `OffloaderConfig.memory_per_job`, raise `admission_wait_timeout`, or reduce concurrency |
| Qdrant local directory is locked | Local mode opens in one process | Close the other engine, or use a Qdrant server |
| `BackendUnavailableError` opening the graph | Another process holds the LadybugDB file | One owner process, or a server graph store |
| `DLL load failed while importing _lbug` on Windows | The `ladybug` wheel needs OpenSSL 3 DLLs from the interpreter | Use a uv-managed Python, or put OpenSSL 3 DLLs on the DLL path |
| A purged document still in another engine's sparse results | That engine has not run a round since the purge | It is evicted at its next round; foreground engines must call `run_lifecycle_once()` |

## Look up a warning code

| Code | Severity | What to do |
| --- | --- | --- |
| `method_unavailable` | info; warning if requested | Configure the component or disable the method |
| `reranker_unavailable` | warning | Provision the model or supply a reranker |
| `stage_skipped` | info or warning | Configure an NLI model or disable NLI; a generation swap clears on its own |
| `index_missing` | warning | Wait for the lifecycle, or run a round |
| `stale_coverage` | warning | Wait for the rebuild, or `rebuild` |
| `partial_coverage` | warning | Wait, or `backfill` |
| `backfill_scheduled` | info | None |
| `stale_document_version` | info; warning when kept | None; the new version appears once indexed |
| `filter_post_applied` | info | None |
| `timeout_partial` | warning | Raise `request_timeout` or `method_timeout`, or lower `candidate_k` or `rerank_k` |
| `stage_failed_degraded` | warning | Read `details`, `health()`, and the logs |
| `evidence_truncated_contradictions` | info | None |
| `provenance_recording_failed` | warning | Check the recorder backend |
| `possible_redelivery` | info | Set `appearance_id` or `delivery` for exact idempotency |
| `index_wait_timeout` | warning | Raise `index_wait_timeout`, or poll `status()` |

## Look up an error

| Error | Typical cause |
| --- | --- |
| `ConfigurationError` | A vector store without an embedding model; a backfill or rebuild selection that matches no unit; a forced channel the tenant policy forbids |
| `ValidationError` | Invalid input, or beyond `max_query_chars`, `max_top_k`, `max_filter_nodes`, or `max_ingest_batch` |
| `EmbeddingValidationError` | The embedding model returned a wrong count or dimension, non-finite values, or a zero vector |
| `ComponentContractError` | A component broke its contract, such as a reranker skipping an item |
| `AccessDeniedError` | The access policy denied the call; see `details` |
| `TenantIsolationError` | Data of another scope reached a request, or an agxai run of another tenant; never degraded |
| `CoverageError` | `missing_index_policy="fail"` and a method has no usable index |
| `OperationTimeoutError` | A deadline elapsed: request, tenant slot, `wait_for`, or an offloaded job |
| `LifecycleError` | The engine is not open, or a task is unknown |
| `MigrationError` | `migration="verify"` and the schema is not current |

## Get more detail

Turn on logs with `logging.getLogger("retrievalloom").setLevel(logging.INFO)`
after `logging.basicConfig()`, and pass
`RetrievalExecutionConfig(include_candidates=True)` to see every candidate
with its per-method hits. See [observability.md](observability.md).
