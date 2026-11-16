# Troubleshooting

Start with `result.status` and `result.warnings`. Every warning has a stable
`code`, a `severity`, a `message` and `details`. `info` warnings do not change
the status; `warning` and `error` make it `partial`.

## Warning codes

| Code | Severity | Cause | What to do |
| --- | --- | --- | --- |
| `method_unavailable` | info, or warning if the request asked for the method | The method has no backend, e.g. `dense` without an embedding model. | Configure the component, or disable the method in `IndexChannelConfig`. |
| `reranker_unavailable` | warning | A request asked for reranking (`RetrievalExecutionConfig(reranking=True)`) but no reranker is available, or a reranker you supplied raised `BackendUnavailableError` (e.g. FlashRank model missing). The default FlashRank reranker is dropped at startup when its model is missing, so configured reranking is then skipped silently and results keep the RRF-fused order. | Put the FlashRank model in `RerankingConfig.model_dir` ([provision it](how-to/rerank-and-nli.md#provision-the-model-files)), or supply your own reranker. |
| `stage_skipped` | info or warning | NLI is enabled without an NLI model, or a method's generation is being replaced while the request starts. | Configure an NLI model or disable NLI. The replacement case clears on its own. |
| `index_missing` | warning | No published generation for a method yet. Typical just after startup or for a new scope. | Wait for the lifecycle, call `run_lifecycle_once(scope)`, or set `LifecycleConfig(wait_for_rebuild=True)` so startup waits for the rebuild. See [Keep data on disk](how-to/durable-storage.md). |
| `stale_coverage` | warning | The generation was built with another fingerprint (model or config changed). The method is skipped. | Wait for the rebuild the lifecycle starts, or `rebuild` the unit. |
| `partial_coverage` | warning | Some active documents are not yet indexed by a method. The method runs. | Wait, or `backfill`. Check `status().coverage` for `failed` entries. |
| `backfill_scheduled` | info | `missing_index_policy="schedule_backfill"` or failed coverage queued a backfill. | None. |
| `stale_document_version` | info, or warning when kept | Hits of a superseded document version were dropped, or kept with `allow_stale_versions=True`. | None; the new version appears once it is indexed. |
| `filter_post_applied` | info | A method without filter pushdown (graph, structure) was filtered after the search. | None. Expect fewer results with selective filters. |
| `timeout_partial` | warning | The request deadline was reached; finished stages were used. | Raise `request_timeout`/`method_timeout`, or reduce `candidate_k`/`rerank_k`. |
| `stage_failed_degraded` | warning | A method, reranking, NLI or post-write indexing failed and `on_error="degrade"`. | Read `details`; check `health()` and the logs. |
| `evidence_truncated_contradictions` | info | Linked contradicting evidence was added beyond `top_k`. | None. |
| `provenance_recording_failed` | warning | The provenance recorder raised. | Check the recorder backend. Set `fail_on_recording_error=True` to raise instead. |
| `possible_redelivery` | info | An ingest without `appearance_id` or `delivery` repeated a recent appearance within `redelivery_window_seconds`. It was still counted. | Set `appearance_id` or `delivery` on the document for exact idempotency. |
| `index_wait_timeout` | warning | `ingest`/`delete` stopped waiting for indexing after `index_wait_timeout`. Indexing continues. | Raise the timeout, or use `index_wait="none"` and poll `status`. |

## Errors

All errors derive from `RetrievalLoomError` and carry `code`, `retryable` and
`details`.

| Error | Typical cause |
| --- | --- |
| `ConfigurationError` | Invalid configuration, e.g. a `vector_store` without an embedding model, or a selection that matches no unit. |
| `ValidationError` | Invalid input, or a request beyond a limit (`max_query_chars`, `max_top_k`, `max_filter_nodes`, `max_ingest_batch`). |
| `EmbeddingValidationError` | The embedding model returned the wrong count, wrong dimensions, non-finite values or a zero vector. |
| `ComponentContractError` | A component broke its contract, e.g. a reranker skipped an item. |
| `AccessDeniedError` | The access policy denied the operation. |
| `TenantIsolationError` | Data of another scope reached a request. Never degraded. Report it as a bug. |
| `CoverageError` | `missing_index_policy="fail"` and a method has no usable index. |
| `OperationTimeoutError` | A deadline elapsed: request, tenant slot, `wait_for`, or an offloaded job. |
| `ResourceAdmissionError` | resourcera admission did not grant capacity within `admission_wait_timeout`. |
| `BackendUnavailableError`, `BackendTimeoutError`, `BackendError` | A backend (Qdrant, a model) failed. |
| `LifecycleError` | The engine is not open, or a task is unknown. |
| `MigrationError` | `migration="verify"` and the schema is not current, or a migration failed. |

## Common problems

**Results are `partial` right after startup.**
In-memory indexes are rebuilt by the first background round. Call
`await engine.run_lifecycle_once(scope)` after opening, or use
`LifecycleConfig(background=False)`.

**`ResourceAdmissionError` under load.**
The offloader could not reserve memory or CPU capacity in time. Lower
`OffloaderConfig.memory_per_job`, raise `admission_wait_timeout`, give the
engine a larger `ResourceManager`, or reduce concurrency.

**Dense results disappear after changing the embedding model.**
The old generation is stale for the new fingerprint (`stale_coverage`). The
lifecycle builds a new generation; until then dense is skipped.

**A local Qdrant directory is locked.**
Only one process can open a Qdrant local-mode directory. Close the other
engine, or use a Qdrant server. See [Keep data on disk](how-to/durable-storage.md).

**Retired generations are not dropped.**
Another engine holds a reader lease on them. Leases expire `lease_ttl` after
the engine's last read, or immediately when it closes. See
[Run several engines on one store](how-to/multiple-engines.md#reader-leases).

**The event loop stalls.**
See [Keep the event loop responsive](how-to/event-loop-latency.md).

## Logs

RetrievalLoom logs with stable event names, for example
`generation_published`, `retrieval_completed`, `reranker_unavailable` and
`lifecycle_round_errors`. It is silent by default. To see the events, configure
structlog, or add a handler or level for the standard library `retrievalloom`
logger (for example `logging.basicConfig(level=logging.INFO)`). See
[Logging](../getting-started/installation.md#logging). Query text and document
content are not logged unless `EngineConfig(log_content=True)`.
