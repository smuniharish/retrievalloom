# Observability with Jaeger

In production you want one trace per request that shows what the application did and what
the retrieval pipeline did inside it, plus logs you can join to that trace. The example
produces both and exports the trace to Jaeger over OTLP.

## Wiring

- **OpenTelemetry.** A `TracerProvider` with `service.name=retrievalloom-example`. When
  `RL_EXAMPLES_OTLP_URL` is set, a `BatchSpanProcessor` with the OTLP/HTTP exporter sends
  spans to `<url>/v1/traces`. An in-memory exporter always keeps a copy so the script can
  print the tree.
- **agxai runtime.** An `XAIRuntime` with
  `OpenTelemetryObservability(tracer_provider=provider)` registered as its
  `ObservabilityProvider` is passed as `EngineResources(xai_runtime=...)`. Every
  provenance event the engine records (`execution.started`, `retrieval.execution`,
  `execution.completed`) becomes a span under the span that is current at that moment.
- **Application spans.** A root span `support.request` wraps the request. The child spans
  `retrievalloom.ingest` and `retrievalloom.retrieve` wrap the engine calls and carry
  attributes such as `rl.collection`, `rl.documents`, `rl.query`, `rl.request_id`,
  `rl.status` and `rl.evidence`.
- **Log correlation.** A structlog processor adds the current `trace_id` and `span_id` to
  every log line, the library's own logs included, because RetrievalLoom logs through
  structlog. Searching the logs for a trace id from Jaeger finds every line of that
  request.

agxai events are instantaneous, so Jaeger shows them as zero-length spans and may add a
"negative duration ... sanitizing" warning to them. The warning is harmless.

## Requirements

- The `agxai` extra and the `examples` dependency group (OpenTelemetry SDK and OTLP
  exporter).
- For export, the Jaeger service from `examples/compose.yaml` (UI on port 16686, OTLP/HTTP
  on 4318).

## Run

```bash
python examples/observability_jaeger.py                                  # in memory
RL_EXAMPLES_OTLP_URL=http://localhost:4318 python examples/observability_jaeger.py
```

`RL_EXAMPLES_JAEGER_URL` (default `http://localhost:16686`) only changes the link that is
printed. Open it, or pick service `retrievalloom-example` in the Jaeger UI.

`tests/live/test_example_jaeger.py` runs the example against a real Jaeger and queries
its HTTP API (`/api/v3/services`, `/api/v3/traces/{id}`) to check that every span arrived
with the expected names and attributes.

## Source

<!-- fmt: off -->
```python title="examples/observability_jaeger.py"
--8<-- "examples/observability_jaeger.py"
```
<!-- fmt: on -->

## Expected output

JSON log lines come first, each with `trace_id` and `span_id`, for example:

```text
{"engine_id": "engine_...", "request_id": "req_...", "status": "complete", "evidence": 2, ..., "event": "retrieval_completed", "level": "info", "trace_id": "7563521a771d62ff5848f7892e75e23c", "span_id": "e26bab64ce0165b8"}
```

The span tree follows:

```text
trace_id=5f682e70793ea65dd28f6e945a73e189 request=rl_trace_325140a5
  support.request
    execution.started
    retrievalloom.ingest
    retrievalloom.retrieve
      retrieval.execution
    execution.completed
exported 6 spans to http://localhost:4318; open http://localhost:16686/trace/5f682e70793ea65dd28f6e945a73e189
```

Without `RL_EXAMPLES_OTLP_URL` the last line reads
`6 spans kept in memory (set RL_EXAMPLES_OTLP_URL to export)`. The `method_unavailable`
warning in the retrieval log appears because this engine has no embedding model, so the
default dense method is skipped.
