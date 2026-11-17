# Failure modes

Use this page to decide how the application reacts when part of a request
fails. By default RetrievalLoom keeps serving: a failing method, reranker, or
NLI model becomes a warning and the result becomes `partial`. Security and
input errors always raise.

## Read the result status

```python
from retrievalloom import ResultStatus, Severity

result = await engine.retrieve(query, scope=scope)
if result.status is ResultStatus.EMPTY:
    ...  # nothing matched: say so, do not invent an answer
elif result.status is ResultStatus.PARTIAL:
    problems = [w for w in result.warnings if w.severity is not Severity.INFO]
    ...  # answer from result.evidence, and log or surface the problems
```

| Status | Meaning |
| --- | --- |
| `complete` | Every requested stage ran; only `info` warnings |
| `partial` | At least one `warning` or `error` warning; evidence may be incomplete |
| `empty` | No evidence |

## Choose degrade or fail per stage

| Stage | Setting | Default | On failure with the default |
| --- | --- | --- | --- |
| Each method | `MethodConfig.on_error` | `degrade` | `stage_failed_degraded`; other methods still answer |
| Reranking | `RerankingConfig.on_error` | `degrade` | `reranker_unavailable` or `stage_failed_degraded`; fused order kept |
| NLI | `NLIConfig.on_error` | `degrade` | `stage_failed_degraded`; no NLI summaries |
| Request deadline | `RetrievalConfig.allow_partial_on_deadline` | `True` | `timeout_partial`; finished methods used |
| Method without an index | `RetrievalConfig.missing_index_policy` | `skip` | `index_missing`; `fail` raises `CoverageError`, `schedule_backfill` queues a backfill |
| Provenance recording | `RetrievalConfig.fail_on_recording_error` | `False` | `provenance_recording_failed`; `True` raises `ProvenanceError` |

Set `fail` where a missing stage would make the answer wrong, for example
`on_error="fail"` on the only method a compliance search may rely on.

## Handle ingest failures

- `IngestionConfig(failure_policy="per_document")`, the default, stores the
  valid documents and reports the others as `rejected` or `failed` in
  `report.failed`, each with an `error`. With `fail_batch`, the first invalid
  document raises its error (a `ValidationError`) and nothing is stored.
- Indexing after the write is separate. If it does not finish within
  `index_wait_timeout`, the report has an `index_wait_timeout` warning and
  indexing continues. If it fails, the report has `stage_failed_degraded`,
  the documents stay stored, and later lifecycle rounds retry the indexing.
  Coverage left `failed` is retried by a `backfill`.

## Know what always raises

| Error | Why it never degrades |
| --- | --- |
| `TenantIsolationError` | Data of another scope reached the request. Report it as a bug; never catch and continue. |
| `AccessDeniedError` | The access policy denied the call. |
| `ValidationError` | The input is invalid or beyond a limit. Fix the caller. |
| `ConfigurationError` | The configuration cannot work, for example a vector store without an embedding model. |
| `LifecycleError` | The engine is not open. |
| `MigrationError` | `migration="verify"` and the schema is not current. |

## Retry what is retryable

Every error has `retryable`. `BackendUnavailableError`,
`BackendTimeoutError`, `ResourceAdmissionError`, and `OperationTimeoutError`
are typical transient failures: retry them with backoff. `purge` that raised
can be retried as is; it is idempotent.
