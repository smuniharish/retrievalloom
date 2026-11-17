# Observability

Use this page to see what the engine did for a request: logs with stable
event names, health and status checks, metrics, and an audit trail of
retrievals in agxai.

## Turn on logs

RetrievalLoom logs through structlog under `retrievalloom.*` logger names and
is silent by default. It never configures logging itself.

```python
import logging

logging.basicConfig(level=logging.WARNING)
logging.getLogger("retrievalloom").setLevel(logging.INFO)
```

- If the application configures structlog, events use that configuration;
  otherwise they are rendered as `key=value` text into the standard library
  loggers.
- Event names are stable, for example `generation_published`,
  `retrieval_completed`, `reranker_unavailable`, `startup_rebuild_timeout`,
  and `lifecycle_round_errors`.
- Query and document text are not logged unless
  `create_retrieval_engine(log_content=True)`.

## Check health and progress

```python
report = await engine.health()  # healthy, degraded, or unhealthy, per component
status = await engine.status(scope=scope)
status.coverage  # documents per method and coverage state
status.tasks  # backfill and rebuild tasks by state
```

Expose `health()` on a readiness endpoint. Watch `coverage` for `failed`
entries, then queue a `backfill` to retry them.

## Collect metrics

Pass a refresh-engine `MetricsSink` as `EngineResources(metrics=...)`. The
default discards everything. The engine records
`retrievalloom.retrieval.requests` (counter, by `status`),
`retrievalloom.retrieval.duration_ms` (by `status`), and
`retrievalloom.retrieval.method_ms` (by `method`). Each result also carries
per-stage `timings` in seconds.

## Record provenance with agxai

Pass an agxai `XAIRuntime` and the engine records retrievals, ingestions,
and deletions into the active agxai run:

```python
from agxai import XAIRuntime

from retrievalloom import EngineResources, create_retrieval_engine

runtime = XAIRuntime(application_id="support-bot", tenant_id="acme")
engine = create_retrieval_engine(resources=EngineResources(xai_runtime=runtime))

async with engine:
    run = await runtime.start_run()
    with runtime.activate_run(run):
        result = await engine.retrieve("reset password", scope=scope)
    await runtime.finish_run(run)
await runtime.close()
```

- Nothing is recorded outside an active run.
- `retrieve` records one retrieval with each evidence id, and a
  `derived_from` link from each source document; no text is copied.
  `ingest` and `delete` record outcome counts only.
- The run's tenant must equal the scope's `tenant_id`, or the call raises
  `TenantIsolationError`.
- Other recording failures add `provenance_recording_failed` unless
  `RetrievalConfig(fail_on_recording_error=True)`, which raises
  `ProvenanceError`.
- For options, pass `AgxaiProvenanceRecorder(runtime, run_provider=...,
  record_links=False, reranker_id=...)` from
  `retrievalloom.integrations.agxai` as `Components(provenance_recorder=...)`.

When the application also uses the `agxai` skill, that skill covers runs,
decisions, and explanations.

## Trace requests

RetrievalLoom has no built-in OpenTelemetry instrumentation. To trace it,
wrap engine calls in application spans and add the request's `request_id`
and `status` as attributes. An agxai runtime with an OpenTelemetry
observability provider turns the recorded provenance events into spans; the
repository's `examples/observability_jaeger.py` shows the full setup,
including trace ids in logs through a structlog processor.
