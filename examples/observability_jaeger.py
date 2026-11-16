"""Observability: one OpenTelemetry trace per request, exported to Jaeger.

The application owns the trace. It opens a root span per support request and child
spans around ``engine.ingest`` and ``engine.retrieve``. The engine runs with an agxai
runtime (``EngineResources(xai_runtime=...)``) whose observability provider is
``agxai.observability.otel.OpenTelemetryObservability``. Every provenance event the
runtime records during the request (run start/finish, retrieval, evidence) becomes a
span parented to the span that is current at that moment, so one trace shows the
whole pipeline. A structlog processor stamps the current ``trace_id``/``span_id`` on
every log line, the library's own logs included, so logs and traces correlate.

With ``RL_EXAMPLES_OTLP_URL`` (for example ``http://localhost:4318``) the spans are
exported over OTLP/HTTP to that collector, such as the Jaeger service in
``examples/compose.yaml``. They appear in the Jaeger UI under service
``retrievalloom-example``. Without it, the spans are kept in memory and printed::

    python examples/observability_jaeger.py
    $env:RL_EXAMPLES_OTLP_URL="http://localhost:4318"; python examples/observability_jaeger.py
"""

import asyncio
import logging
import sys
from typing import Any

import structlog
from _shared import env, run_prefix
from agxai import ObservabilityProvider, XAIRuntime
from agxai.observability.otel import OpenTelemetryObservability
from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from structlog.typing import EventDict

from retrievalloom import (
    DocumentInput,
    EngineResources,
    LifecycleConfig,
    RerankingConfig,
    RetrievalConfig,
    Scope,
    TextRepresentation,
    create_retrieval_engine,
)

SERVICE = "retrievalloom-example"
SCOPE = Scope(tenant_id="acme", collection="support")
ARTICLES = {
    "reset": "To reset a password, open Settings, choose Security and select Reset password.",
    "lockout": "Accounts lock for fifteen minutes after five failed password attempts.",
    "billing": "Invoices are emailed on the first business day of each month.",
}


def add_trace_ids(_: Any, __: str, event: EventDict) -> EventDict:
    """structlog processor: attach the current OpenTelemetry trace and span ids."""
    context = trace.get_current_span().get_span_context()
    if context.is_valid:
        event["trace_id"] = format(context.trace_id, "032x")
        event["span_id"] = format(context.span_id, "016x")
    return event


def configure_logs() -> None:
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            add_trace_ids,
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
        logger_factory=structlog.PrintLoggerFactory(sys.stdout),
    )


async def main() -> None:
    configure_logs()
    otlp = env("OTLP_URL")
    provider = TracerProvider(resource=Resource.create({"service.name": SERVICE}))
    memory = InMemorySpanExporter()
    if otlp:
        from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter

        exporter = OTLPSpanExporter(endpoint=f"{otlp.rstrip('/')}/v1/traces", timeout=10)
        provider.add_span_processor(BatchSpanProcessor(exporter))
    provider.add_span_processor(SimpleSpanProcessor(memory))
    tracer = provider.get_tracer("support-app")

    runtime = XAIRuntime(application_id="support-bot", tenant_id=SCOPE.tenant_id)
    runtime.register(
        ObservabilityProvider,
        OpenTelemetryObservability(tracer_provider=provider, owns_provider=True),
    )
    engine = create_retrieval_engine(
        retrieval=RetrievalConfig(reranking=RerankingConfig(enabled=False)),
        resources=EngineResources(xai_runtime=runtime),
        lifecycle=LifecycleConfig(background=False),
    )
    request_tag = run_prefix("trace")
    log = structlog.get_logger("support-app")
    try:
        async with engine:
            with tracer.start_as_current_span(
                "support.request", attributes={"app.request": request_tag, "rl.tenant": "acme"}
            ) as root:
                trace_id = format(root.get_span_context().trace_id, "032x")
                run = await runtime.start_run()
                with runtime.activate_run(run):
                    with tracer.start_as_current_span("retrievalloom.ingest") as span:
                        documents = [
                            DocumentInput(_id=key, representations=(TextRepresentation(text=text),))
                            for key, text in ARTICLES.items()
                        ]
                        report = await engine.ingest(documents, scope=SCOPE)
                        span.set_attribute("rl.collection", SCOPE.collection)
                        span.set_attribute("rl.documents", len(report.documents))
                    with tracer.start_as_current_span("retrievalloom.retrieve") as span:
                        query = "how do I reset my password"
                        result = await engine.retrieve(query, scope=SCOPE, top_k=2)
                        span.set_attribute("rl.query", query)
                        span.set_attribute("rl.request_id", result.request_id)
                        span.set_attribute("rl.status", result.status.value)
                        span.set_attribute("rl.evidence", len(result.evidence))
                        log.info("answered", top=result.evidence[0].sources[0].document_id)
                await runtime.finish_run(run)
    finally:
        await runtime.close()

    print(f"trace_id={trace_id} request={request_tag}")
    spans = sorted(memory.get_finished_spans(), key=lambda s: s.start_time or 0)
    parents = {span.context.span_id: span.parent for span in spans if span.context}

    def depth(parent: Any) -> int:
        return 0 if parent is None else 1 + depth(parents.get(parent.span_id))

    for span in spans:
        print(f"  {'  ' * depth(span.parent)}{span.name}")
    if otlp:
        jaeger = env("JAEGER_URL") or "http://localhost:16686"
        print(f"exported {len(spans)} spans to {otlp}; open {jaeger}/trace/{trace_id}")
    else:
        print(f"{len(spans)} spans kept in memory (set RL_EXAMPLES_OTLP_URL to export)")


if __name__ == "__main__":
    asyncio.run(main())
