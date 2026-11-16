<p align="center">
  <img src="https://raw.githubusercontent.com/smuniharish/retrievalloom/main/docs/assets/logo.svg" alt="RetrievalLoom logo" width="96">
</p>

<h1 align="center">RetrievalLoom</h1>

<p align="center">
  <a href="https://github.com/smuniharish/retrievalloom/actions/workflows/ci.yml"><img src="https://github.com/smuniharish/retrievalloom/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="https://retrievalloom.readthedocs.io/"><img src="https://readthedocs.org/projects/retrievalloom/badge/?version=latest" alt="Documentation"></a>
  <a href="https://pypi.org/project/retrievalloom/"><img src="https://img.shields.io/pypi/v/retrievalloom.svg" alt="PyPI"></a>
  <a href="https://pypi.org/project/retrievalloom/"><img src="https://img.shields.io/badge/python-3.12%20%7C%203.13%20%7C%203.14-blue.svg" alt="Python 3.12 | 3.13 | 3.14"></a>
  <img src="https://img.shields.io/badge/coverage-100%25-brightgreen.svg" alt="Coverage 100%">
  <img src="https://img.shields.io/badge/typing-typed-blue.svg" alt="Typed">
  <a href="https://github.com/smuniharish/retrievalloom/blob/main/LICENSE"><img src="https://img.shields.io/badge/license-Apache%202.0-blue.svg" alt="License: Apache 2.0"></a>
</p>

**Multi-channel retrieval and evidence with provenance, as an async Python library.**

RetrievalLoom indexes your documents through several retrieval methods, keeps
those indexes in step as documents change, fuses what the methods find, and
returns ranked evidence that names its document, version, span and the
pipeline stages that produced it. It is an embeddable library with an
async-only API. Bring your own embedding model, reranker, NLI model, vector
store or metadata store through `abc.ABC` contracts, or use the defaults.

## Why RetrievalLoom

Retrieval in a real application is more than one call to a vector database:

- **One method is not enough.** Exact identifiers, misspellings, terms and
  meaning need keyword, fuzzy, BM25 and dense search, and their results must be
  merged.
- **Indexes drift.** Documents change and models are replaced. Without
  tracking, a search can mix old and new versions or read an index that is
  half built.
- **Answers need sources.** Downstream code and auditors need to know which
  document version and span each result came from.
- **Tenants share infrastructure.** One tenant's data must never appear in
  another's results, and one busy tenant must not starve the rest.
- **Models block.** Embedding, reranking and NLI are CPU-bound and stall an
  async service if they run on the event loop.

How RetrievalLoom addresses each one:

| Problem | What RetrievalLoom does |
| --- | --- |
| One method is not enough | Keyword, fuzzy, sparse (BM25) and dense methods, plus optional graph and structure channels, fused with RRF or score-based fusion |
| Indexes drift | Indexes are built as generations and published atomically; coverage is tracked per document version, and retrieval reads a consistent snapshot |
| Answers need sources | Every evidence item carries provenance: sources, spans, pages, method hits and pipeline stages |
| Tenants share infrastructure | Every call is scoped; results are validated against the scope; per-tenant concurrency limits and an access policy hook |
| Models block | Blocking work runs in threads through an offloader with resourcera admission control and backpressure |

RetrievalLoom is not a SaaS, an agent framework, a RAG framework, an embedding
provider, a document parser or a chunker. It does not need LangChain,
LlamaIndex or any other framework.

## Highlights

- **Works out of the box.** `RetrievalEngine()` runs in memory with keyword,
  fuzzy and BM25 search and no model downloads.
- **Pre-chunked and multi-representation input.** Send plain text, your own
  chunks with spans and pages, document structure, or a graph.
- **Lifecycle management.** Incremental indexing, backfill when you add a
  method, rebuild when you change a model, and safe retirement of old
  generations while readers still use them.
- **Optional evidence stages.** Rerank with the built-in FlashRank reranker
  (used by default when its model is present) or your own model, check
  support and contradiction with an NLI model, and consolidate overlapping
  spans.
- **Durable storage.** SQLite metadata with Alembic migrations, and Qdrant in
  memory, on disk, on a server or in Qdrant Cloud. Several engines can share
  one store.
- **Integrations.** refresh-engine for background indexing, resourcera for
  admission control, and agxai for provenance recording.
- **Contract kits.** `retrievalloom.testing` checks your components against
  the same pytest suites the built-in ones pass.
- **Small and typed.** Async only, type hints throughout, structured logging
  with structlog, and 100% branch coverage on Python 3.12 to 3.14.

## Installation

```bash
pip install retrievalloom
```

Optional extras:

| Extra | Adds |
| --- | --- |
| `graph` | The graph channel with LadybugDB (`LadybugGraphStore`) |
| `structure` | The structure channel with TreeSearch (`TreeSearchStructureEngine`) |
| `testing` | The pytest contract kits in `retrievalloom.testing` |
| `all` | `graph` and `structure` |

The library is silent by default. To see its log events, configure structlog
or call `logging.basicConfig(level=logging.INFO)`; see
[Logging](https://retrievalloom.readthedocs.io/en/latest/getting-started/installation/#logging).

## Quickstart

Ingest three documents into an in-memory engine and retrieve evidence:

```python
import asyncio

from retrievalloom import (
    DocumentInput,
    RetrievalEngine,
    Scope,
    SourceDescriptor,
    TextRepresentation,
)

TEXTS = {
    "asyncio": "asyncio runs coroutines on a single-threaded event loop.",
    "bm25": "BM25 ranks documents by term frequency and inverse document frequency.",
    "eiffel": "The Eiffel Tower in Paris was completed in 1889.",
}


async def main() -> None:
    scope = Scope(tenant_id="acme")
    documents = [
        DocumentInput(
            id=key,
            source=SourceDescriptor(uri=f"kb://{key}"),
            representations=[TextRepresentation(text=text)],
        )
        for key, text in TEXTS.items()
    ]
    async with RetrievalEngine() as engine:
        report = await engine.ingest(documents, scope=scope)
        print("ingested:", [item.outcome.value for item in report.documents])

        result = await engine.retrieve("event loop coroutines", scope=scope, top_k=3)
        print("status:", result.status.value)
        for item in result.evidence:
            source = item.sources[0]
            methods = sorted({hit.method.key for hit in item.provenance.hits})
            print(f"{source.document_id} v{source.document_version} {item.score:.4f} {methods}")
            print("  ", item.display_text)


asyncio.run(main())
```

Output:

```text
ingested: ['created', 'created', 'created']
status: complete
asyncio v1 0.0164 ['index.fuzzy', 'index.sparse']
   asyncio runs coroutines on a single-threaded event loop.
```

`ingest` waits for one indexing round, so documents are searchable when it
returns. `result.warnings` explains anything the engine could not do, such as
running without an embedding model or a reranker. The
[quickstart](https://retrievalloom.readthedocs.io/en/latest/getting-started/quickstart/)
walks through it.

### Your own embedding model

Subclass `EmbeddingModelABC`. Run the blocking model call through the
offloader the engine passes to `initialize`, so the event loop never blocks:

```python
from collections.abc import Callable, Sequence

from retrievalloom import Components, RetrievalEngine
from retrievalloom.contracts.component import ComponentContext, ComponentInfo, make_info
from retrievalloom.contracts.models import EmbeddingModelABC
from retrievalloom.runtime.offloader import WorkloadKind


class MyEmbedding(EmbeddingModelABC):
    def __init__(self, encode: Callable[[list[str]], list[list[float]]], dimensions: int) -> None:
        self._encode = encode  # a blocking call, e.g. your model's encode()
        self._dimensions = dimensions
        self._info = make_info(
            component="embedding",
            implementation="my-embedding",
            version="1",
            model_identity="my-org/my-model",
            dimensions=dimensions,
        )

    @property
    def info(self) -> ComponentInfo:
        return self._info

    @property
    def dimensions(self) -> int:
        return self._dimensions

    async def initialize(self, context: ComponentContext) -> None:
        self._offloader = context.offloader

    async def embed_documents(self, texts: Sequence[str]) -> Sequence[Sequence[float]]:
        return await self._offloader.run(WorkloadKind.EMBEDDING, self._encode, list(texts))

    async def embed_query(self, text: str) -> Sequence[float]:
        (vector,) = await self._offloader.run(WorkloadKind.EMBEDDING, self._encode, [text])
        return vector


engine = RetrievalEngine(components=Components(embedding=MyEmbedding(model.encode, 1024)))
```

With an embedding model, the dense method joins the index channel. Changing
`model_identity` later triggers a rebuild of the dense index. See
[Use your own embedding model](https://retrievalloom.readthedocs.io/en/latest/guide/how-to/embedding-model/).

### Your own vector store / metadata store

The default stores live in memory. Pass durable ones to keep data across
restarts:

```python
from retrievalloom import Components, RetrievalEngine
from retrievalloom.index.vector import QdrantVectorStore
from retrievalloom.storage.sqlite import SQLiteMetadataStore

components = Components(
    embedding=MyEmbedding(model.encode, 1024),
    metadata_store=SQLiteMetadataStore("var/metadata.db"),
    vector_store=QdrantVectorStore(url="http://localhost:6333"),
)
engine = RetrievalEngine(components=components)
```

The engine initialises components you supply but does not close them. For
another database, subclass `DocumentMetadataStoreABC` or `VectorStoreABC` and
run `MetadataStoreContract` or `VectorStoreContract` from
`retrievalloom.testing` against it. See
[Durable storage](https://retrievalloom.readthedocs.io/en/latest/guide/how-to/durable-storage/)
and [Write your own backend](https://retrievalloom.readthedocs.io/en/latest/guide/how-to/custom-backends/).

## Benchmarks

`scripts/benchmark.py` measures ingest throughput and retrieval latency in process.
See [Benchmarks](https://retrievalloom.readthedocs.io/en/latest/benchmarks/)
for the method, the results and how to reproduce them.

## Examples

Complete programs in [`examples/`](https://github.com/smuniharish/retrievalloom/tree/main/examples),
each run by the test suite. Custom components for almost every extension point are in [`examples/adapters/`](https://retrievalloom.readthedocs.io/en/latest/examples/adapters/):

| Example | Shows | Needs |
| --- | --- | --- |
| [`quickstart.py`](https://retrievalloom.readthedocs.io/en/latest/examples/quickstart/) | Ingest and retrieve in memory | Base install |
| [`customer_support.py`](https://retrievalloom.readthedocs.io/en/latest/examples/customer-support/) | Help-center search: keyword, fuzzy, BM25 and bge-m3 dense fused, cross-encoder rerank, filters | Local models, Qdrant |
| [`multi_tenant_saas.py`](https://retrievalloom.readthedocs.io/en/latest/examples/multi-tenant-saas/) | Tenant isolation, a role-based access policy, quotas and an audit log | Base install |
| [`document_lifecycle.py`](https://retrievalloom.readthedocs.io/en/latest/examples/document-lifecycle/) | Updates, deletes, purge, backfill and rebuild on pgvector | Local models, pgvector |
| [`knowledge_graph.py`](https://retrievalloom.readthedocs.io/en/latest/examples/knowledge-graph/) | Entity extraction, graph channel on Neo4j or LadybugDB, graph + index fusion | `graph` extra; Neo4j optional |
| [`policy_manual.py`](https://retrievalloom.readthedocs.io/en/latest/examples/policy-manual/) | Long sectioned documents with the structure channel | `structure` extra |
| [`fact_checking.py`](https://retrievalloom.readthedocs.io/en/latest/examples/fact-checking/) | Claim verification with NLI and contradictions | Local models |
| [`http_service.py`](https://retrievalloom.readthedocs.io/en/latest/examples/http-service/) | A FastAPI service with a Postman collection | `examples` group |
| [`observability_jaeger.py`](https://retrievalloom.readthedocs.io/en/latest/examples/observability-jaeger/) | OpenTelemetry traces in Jaeger and trace ids in logs | `agxai` extra; Jaeger optional |
| [`durable_restart.py`](https://retrievalloom.readthedocs.io/en/latest/examples/durable-restart/) | Restart on SQLite metadata and Qdrant without re-ingesting | Local models, Qdrant |
| [`prechunked_and_multi_representation.py`](https://retrievalloom.readthedocs.io/en/latest/examples/prechunked-and-multi-representation/) | Your own chunks, spans and pages | Base install |
| [`agxai_provenance.py`](https://retrievalloom.readthedocs.io/en/latest/examples/agxai-provenance/) | Recording retrievals with agxai | Base install |

## Documentation

Full documentation: **[retrievalloom.readthedocs.io](https://retrievalloom.readthedocs.io/)**

- [Getting started](https://retrievalloom.readthedocs.io/en/latest/getting-started/)
- [User guide](https://retrievalloom.readthedocs.io/en/latest/guide/)
- [Integrations](https://retrievalloom.readthedocs.io/en/latest/integrations/)
- [Architecture](https://retrievalloom.readthedocs.io/en/latest/architecture/)
- [API reference](https://retrievalloom.readthedocs.io/en/latest/api/)

Changes are listed in the
[changelog](https://github.com/smuniharish/retrievalloom/blob/main/CHANGELOG.md).

## Contributing

Contributions are welcome. See
[CONTRIBUTING.md](https://github.com/smuniharish/retrievalloom/blob/main/CONTRIBUTING.md)
for the development setup and checks, and
[SECURITY.md](https://github.com/smuniharish/retrievalloom/blob/main/SECURITY.md)
to report a vulnerability.

## License

Apache License 2.0. See
[LICENSE](https://github.com/smuniharish/retrievalloom/blob/main/LICENSE).
