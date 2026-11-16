# Examples

Runnable scripts live in the `examples/` directory of the repository. Each one models a
real deployment: a support help center, a multi-tenant SaaS, an HTTP service, a
fact-checker, and so on. Each is a self-contained `async def main()` program that prints
what it retrieves and why.

Run them from the repository root:

```bash
python examples/quickstart.py
```

## Scenarios

| Example | Use case | What it shows | Needs |
| --- | --- | --- | --- |
| [Quickstart](quickstart.md) | First contact | In-memory engine; ingest, retrieve, sources, provenance, a metadata filter | base install |
| [Customer support](customer-support.md) | Help-center search | Keyword + fuzzy (typos) + BM25 + bge-m3 dense on Qdrant, RRF, cross-encoder rerank, filters, per-request config, cache | models, Qdrant |
| [Multi-tenant SaaS](multi-tenant-saas.md) | One engine, many customers | Tenants and collections, a role/tenant `AccessPolicyABC` that denies, quotas, JSONL audit log | base install |
| [Document lifecycle](document-lifecycle.md) | Operating a corpus | Updates, deletes, `purge`, adding a dense method later, `backfill`, `rebuild` after a model change, status, health | models, pgvector |
| [Knowledge graph](knowledge-graph.md) | Platform/ops Q&A | Entity extraction and alias normalisation, graph channel on Neo4j (or LadybugDB), graph + index fusion | `graph` extra; Neo4j optional |
| [Policy manual](policy-manual.md) | Long structured documents | Structure channel over a sectioned policy, custom deduplicator and consolidator | `structure` extra |
| [Fact checking](fact-checking.md) | Claim verification | `retrieve(..., claims=[...])`, cross-encoder relevance, NLI verdicts, contradictions | models |
| [HTTP service](http-service.md) | Retrieval as a service | FastAPI app with engine lifespan, `/ingest`, `/retrieve`, `/status`, `/health`, error mapping, Postman collection | `examples` group |
| [Observability with Jaeger](observability-jaeger.md) | Tracing in production | agxai `OpenTelemetryObservability`, OTLP export to Jaeger, trace ids in structlog | Jaeger optional |
| [Durable restart](durable-restart.md) | Restarts and deploys | `data_dir` SQLite metadata + Qdrant vectors and learned-sparse index reused across processes | models, Qdrant |
| [Pre-chunked and multi-representation](prechunked-and-multi-representation.md) | Bring your own chunks | Application chunks with spans and pages; a second (structure) representation | `structure` extra |
| [agxai provenance](agxai-provenance.md) | Explainability records | Ingestion, deletion and retrieval recorded on an agxai run with provenance links | base install |

## Custom components

The scenarios use the custom components in `examples/adapters/`. Each one implements a
library ABC. Together they cover almost every extension point: embeddings, a
learned-sparse index, reranking, NLI, graph and vector stores, tokenizer, keyword
dictionary, cache, access policy, provenance, fusion, score normalisation, deduplication,
consolidation, and graph extraction and normalisation. Each one passes its contract kit
from `retrievalloom.testing`. See [Custom components](adapters.md).

## Services and settings

The live examples read `RL_EXAMPLES_<NAME>`, falling back to the test suite's
`RL_TEST_<NAME>`. When a setting is missing, the example prints what to set and exits
with status 0.

| Setting | Example value | Used by |
| --- | --- | --- |
| `MODELS_DIR` | `models` (the default when the folder exists) | customer support, lifecycle, fact checking, durable restart |
| `QDRANT_URL` | `http://localhost:6333` | customer support, durable restart |
| `PGVECTOR_DSN` | `postgresql://rl:rl@localhost:55433/rl` | document lifecycle |
| `NEO4J_URL` | `bolt://localhost:7687` (user `neo4j`, password `retrievalloom`; override with `NEO4J_USER`/`NEO4J_PASSWORD`) | knowledge graph |
| `OTLP_URL` | `http://localhost:4318` | observability |
| `JAEGER_URL` | `http://localhost:16686` | observability (UI link), Jaeger live test |

The models directory holds one folder per model, as downloaded from Hugging Face:
`bge-m3`, `bge-reranker-v2-m3` and `UltraBERT-150M-nli`. Nothing is downloaded at run
time. The model adapters use `FlagEmbedding` and `transformers`, which are in the
`examples` dependency group (`uv sync --group examples`), together with FastAPI, uvicorn,
httpx, asyncpg, the `neo4j` driver and the OpenTelemetry SDK and OTLP exporter.

All services start with the compose file shipped with the examples. Every image is pinned:

```bash
podman compose -f examples/compose.yaml up -d   # or: docker compose -f ... up -d
```

```yaml title="examples/compose.yaml"
--8<-- "examples/compose.yaml"
```

## Tests

`tests/examples/test_examples.py` runs every script as a subprocess and asserts on what it
prints, such as rankings, denials, verdicts and span trees, not just the exit code. It
also drives the FastAPI app in-process through `httpx.ASGITransport`.
`tests/examples/test_adapters.py` runs the contract kits for the offline adapters. Both
carry the `examples` marker.

The live tests carry `live` (plus `models`, `qdrant` or `graph`). They are skipped
unless their settings are present:

- `tests/live/test_example_models.py`: bge-m3 embeddings, the learned-sparse index on
  Qdrant, the cross-encoder and NLI, each against its kit;
- `tests/live/test_example_stores.py`: the Neo4j and pgvector stores against their kits;
- `tests/live/test_example_jaeger.py`: checks through the Jaeger query API that the
  spans arrived.

```bash
python -m pytest tests/examples -m "not live"              # offline
RL_TEST_MODELS_DIR=models RL_TEST_QDRANT_URL=http://localhost:6333 \
RL_TEST_PGVECTOR_DSN=postgresql://rl:rl@localhost:55433/rl \
RL_TEST_NEO4J_URL=bolt://localhost:7687 RL_TEST_JAEGER_URL=http://localhost:16686 \
python -m pytest tests/examples tests/live/test_example_models.py \
  tests/live/test_example_stores.py tests/live/test_example_jaeger.py
```
