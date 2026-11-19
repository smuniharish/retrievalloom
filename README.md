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

**Find relevant passages across search methods, and show where each one came from.**

RetrievalLoom is an async-first hybrid retrieval library for Python
applications. It combines keyword, typo-tolerant, BM25 and optional semantic,
graph and structural search into ranked evidence. Each result retains its
source document, version and passage. RetrievalLoom runs inside your
application; it does not require a separate hosted service.

<p align="center">
  <img src="https://raw.githubusercontent.com/smuniharish/retrievalloom/main/docs/assets/diagrams/architecture-overview.png" alt="RetrievalLoom architecture: your application ingests documents and sends queries to the engine, which keeps the metadata store and the index, graph and structure channels in step and returns ranked, cited evidence" width="720">
</p>

## Why RetrievalLoom

Picture a support assistant for a software product. In one hour it has to
answer:

- "What does error `E1042` mean?" — an exact code that semantic search blurs.
- "How do I fix a conflcted copy?" — a typo that exact search misses.
- "How do I get my money back?" — a paraphrase of an article titled "Refunding
  a card payment" that shares no words with it.

No single search method handles all three. Real deployments add more demands:
articles change every day, each customer must see only their own content, and
every answer must cite its source.

| What you need | What RetrievalLoom gives you |
| --- | --- |
| Match codes, typos and paraphrases | Keyword, fuzzy, BM25 and vector search run together, and their rankings are fused into one list. Optional graph and section-tree search join the same list. |
| Search results that stay correct as content changes | Edits and deletions reach every index automatically. A search never mixes an old and a new version of a document, and never reads a half-built index. |
| Answers you can cite and audit | Each result names its document and version, lists the search methods that found it, and carries the character span, pages and section path you supplied. |
| Many customers on one deployment | Every call is scoped to a tenant and collection. A result from another tenant is an error, never a silent leak. Per-tenant limits keep one busy customer from slowing the rest. |
| A responsive async service | Model inference and index work run in worker threads with admission control, so your event loop keeps serving requests. |

## Highlights

- **Useful with zero setup.** `RetrievalEngine()` runs in memory with keyword,
  fuzzy and BM25 search. No model download, server or network access.
- **Add what you need.** Plug in an embedding model for semantic search, a
  reranker for precision at the top, or an NLI model that flags passages which
  contradict a claim.
- **Your content, your chunks.** Send plain text, your own chunks with spans and
  pages, a section outline, or a knowledge graph.
- **Indexes that look after themselves.** New and edited documents are indexed
  incrementally. Adding a search method backfills existing documents, and
  changing an embedding model rebuilds its index without interrupting searches.
- **Durable when you need it.** Keep metadata in SQLite or PostgreSQL and
  vectors in Qdrant (in memory, on disk, on a server or in Qdrant Cloud).
  Several engines can share one store.
- **Your own components.** Implement the public abstract base class for an
  embedding model, reranker, vector store, metadata store or other extension
  point. The contract kits in `retrievalloom.testing` verify custom
  implementations against the same requirements as built-in components.
- **Small and typed.** An async API with type hints throughout, structured
  logging with structlog, and 100% branch coverage on Python 3.12 to 3.14.

## Installation

```bash
pip install retrievalloom                # core: in-memory engine, Qdrant client, FlashRank
pip install "retrievalloom[all]"         # plus the graph and structure channels and PostgreSQL
```

RetrievalLoom supports Python 3.12, 3.13 and 3.14. Optional extras:

| Extra | Adds |
| --- | --- |
| `graph` | The graph channel with LadybugDB (`LadybugGraphStore`) |
| `structure` | The structure channel with TreeSearch (`TreeSearchStructureEngine`) |
| `postgres` | The PostgreSQL metadata store (`PostgresMetadataStore`), shared by engines on several hosts |
| `testing` | The pytest contract kits in `retrievalloom.testing` |
| `all` | `graph`, `structure` and `postgres` |

The library prints nothing by default. To see its log events, call
`logging.basicConfig(level=logging.INFO)` or configure structlog; see
[Logging](https://retrievalloom.readthedocs.io/en/latest/getting-started/installation/#logging).

## Quickstart

Index three documents in memory and search them. No model or server is needed:

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

Each result names the document and version it came from, and the search
methods that found it. Documents are searchable as soon as `ingest` returns.
`result.warnings` tells you about anything the engine skipped, such as semantic
search when no embedding model is configured. The
[Quickstart](https://retrievalloom.readthedocs.io/en/latest/getting-started/quickstart/)
walks through the program step by step.

### Your own embedding model

Add semantic search with any embedding model. Subclass `EmbeddingModelABC` and
hand the blocking model call to the worker pool the engine gives you
(`context.offloader`), so your event loop keeps serving requests:

```python
from collections.abc import Callable, Sequence

from retrievalloom import Components, RetrievalEngine
from retrievalloom.contracts.component import ComponentContext, ComponentInfo, make_info
from retrievalloom.contracts.models import EmbeddingModelABC
from retrievalloom.runtime.offloader import WorkloadKind


class MyEmbedding(EmbeddingModelABC):
    def __init__(self, encode: Callable[[list[str]], list[list[float]]], dimensions: int) -> None:
        self._encode = encode  # a blocking call, such as your model's encode()
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

With an embedding model configured, every search also runs a vector (dense)
search. When you later change `model_identity`, the engine builds a new vector
index and switches to it once it is complete. Searches keep using the old index
until then. See
[Use your own embedding model](https://retrievalloom.readthedocs.io/en/latest/guide/how-to/embedding-model/).

### Your own stores

By default everything lives in memory. Pass durable stores to keep your data
across restarts:

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

To run engines on several hosts, use `PostgresMetadataStore` (`postgres` extra)
instead of a SQLite file. To use another database, subclass
`DocumentMetadataStoreABC` or `VectorStoreABC` and check your class with
`MetadataStoreContract` or `VectorStoreContract` from `retrievalloom.testing`.
The engine opens the components you pass in but leaves closing them to you. See
[Keep data on disk](https://retrievalloom.readthedocs.io/en/latest/guide/how-to/durable-storage/)
and [Write your own backend](https://retrievalloom.readthedocs.io/en/latest/guide/how-to/custom-backends/).

## Downloads

<p>
  <a href="https://clickpy.clickhouse.com/dashboard/retrievalloom"><img src="https://img.shields.io/badge/dynamic/json?url=https%3A%2F%2Fsql-clickhouse.clickhouse.com%2F%3Fuser%3Ddemo%26query%3DSELECT%2520sum%2528count%2529%2520v%2520FROM%2520pypi.pypi_downloads_per_day%2520WHERE%2520project%253D%2527retrievalloom%2527%2520FORMAT%2520JSON&amp;query=%24.data%5B0%5D.v&amp;label=downloads&amp;color=blue&amp;cacheSeconds=3600" alt="Total downloads"></a>
  <a href="https://clickpy.clickhouse.com/dashboard/retrievalloom"><img src="https://img.shields.io/badge/dynamic/json?url=https%3A%2F%2Fsql-clickhouse.clickhouse.com%2F%3Fuser%3Ddemo%26query%3DSELECT%2520sum%2528count%2529%2520v%2520FROM%2520pypi.pypi_downloads_per_day%2520WHERE%2520project%253D%2527retrievalloom%2527%2520AND%2520date%2520%253E%253D%2520toStartOfWeek%2528today%2528%2529%252C%25201%2529%2520FORMAT%2520JSON&amp;query=%24.data%5B0%5D.v&amp;label=this%20week&amp;color=blue&amp;cacheSeconds=3600" alt="Downloads this week"></a>
  <a href="https://clickpy.clickhouse.com/dashboard/retrievalloom"><img src="https://img.shields.io/badge/dynamic/json?url=https%3A%2F%2Fsql-clickhouse.clickhouse.com%2F%3Fuser%3Ddemo%26query%3DSELECT%2520sum%2528count%2529%2520v%2520FROM%2520pypi.pypi_downloads_per_day%2520WHERE%2520project%253D%2527retrievalloom%2527%2520AND%2520date%2520%253E%253D%2520toStartOfMonth%2528today%2528%2529%2529%2520FORMAT%2520JSON&amp;query=%24.data%5B0%5D.v&amp;label=this%20month&amp;color=blue&amp;cacheSeconds=3600" alt="Downloads this month"></a>
  <a href="https://clickpy.clickhouse.com/dashboard/retrievalloom"><img src="https://img.shields.io/badge/dynamic/json?url=https%3A%2F%2Fsql-clickhouse.clickhouse.com%2F%3Fuser%3Ddemo%26query%3DSELECT%2520sum%2528count%2529%2520v%2520FROM%2520pypi.pypi_downloads_per_day%2520WHERE%2520project%253D%2527retrievalloom%2527%2520AND%2520date%2520%253E%253D%2520toStartOfYear%2528today%2528%2529%2529%2520FORMAT%2520JSON&amp;query=%24.data%5B0%5D.v&amp;label=this%20year&amp;color=blue&amp;cacheSeconds=3600" alt="Downloads this year"></a>
</p>
<p>
  <a href="https://clickpy.clickhouse.com/dashboard/retrievalloom"><img src="https://img.shields.io/badge/dynamic/json?url=https%3A%2F%2Fsql-clickhouse.clickhouse.com%2F%3Fuser%3Ddemo%26query%3DSELECT%2520arrayStringConcat%2528groupArray%2528concat%2528toString%2528y%2529%252C%2520%2527%253A%2520%2527%252C%2520toString%2528c%2529%2529%2529%252C%2520%2527%2520%257C%2520%2527%2529%2520v%2520FROM%2520%2528SELECT%2520toYear%2528date%2529%2520y%252C%2520sum%2528count%2529%2520c%2520FROM%2520pypi.pypi_downloads_per_day%2520WHERE%2520project%253D%2527retrievalloom%2527%2520GROUP%2520BY%2520y%2520ORDER%2520BY%2520y%2529%2520FORMAT%2520JSON&amp;query=%24.data%5B0%5D.v&amp;label=by%20year&amp;color=blue&amp;cacheSeconds=3600" alt="Downloads by year"></a>
  <a href="https://clickpy.clickhouse.com/dashboard/retrievalloom"><img src="https://img.shields.io/badge/dynamic/json?url=https%3A%2F%2Fsql-clickhouse.clickhouse.com%2F%3Fuser%3Ddemo%26query%3DSELECT%2520if%2528count%2528%2529%2520%253D%25200%252C%2520%2527no%2520downloads%2527%252C%2520arrayStringConcat%2528groupArray%2528concat%2528country_code%252C%2520%2527%2520%2527%252C%2520toString%2528c%2529%2529%2529%252C%2520%2527%2520%257C%2520%2527%2529%2529%2520v%2520FROM%2520%2528SELECT%2520country_code%252C%2520sum%2528count%2529%2520c%2520FROM%2520pypi.pypi_downloads_per_day_by_version_by_country%2520WHERE%2520project%253D%2527retrievalloom%2527%2520AND%2520country_code%2520NOT%2520IN%2520%2528%2527%2527%252C%2520%2527None%2527%2529%2520AND%2520country_code%2520%253C%2520%2527B%2527%2520GROUP%2520BY%2520country_code%2520ORDER%2520BY%2520c%2520DESC%252C%2520country_code%2529%2520FORMAT%2520JSON&amp;query=%24.data%5B0%5D.v&amp;label=countries%20A&amp;color=blue&amp;cacheSeconds=3600" alt="Downloads by country: A"></a>
  <a href="https://clickpy.clickhouse.com/dashboard/retrievalloom"><img src="https://img.shields.io/badge/dynamic/json?url=https%3A%2F%2Fsql-clickhouse.clickhouse.com%2F%3Fuser%3Ddemo%26query%3DSELECT%2520if%2528count%2528%2529%2520%253D%25200%252C%2520%2527no%2520downloads%2527%252C%2520arrayStringConcat%2528groupArray%2528concat%2528country_code%252C%2520%2527%2520%2527%252C%2520toString%2528c%2529%2529%2529%252C%2520%2527%2520%257C%2520%2527%2529%2529%2520v%2520FROM%2520%2528SELECT%2520country_code%252C%2520sum%2528count%2529%2520c%2520FROM%2520pypi.pypi_downloads_per_day_by_version_by_country%2520WHERE%2520project%253D%2527retrievalloom%2527%2520AND%2520country_code%2520NOT%2520IN%2520%2528%2527%2527%252C%2520%2527None%2527%2529%2520AND%2520country_code%2520%253E%253D%2520%2527B%2527%2520AND%2520country_code%2520%253C%2520%2527C%2527%2520GROUP%2520BY%2520country_code%2520ORDER%2520BY%2520c%2520DESC%252C%2520country_code%2529%2520FORMAT%2520JSON&amp;query=%24.data%5B0%5D.v&amp;label=countries%20B&amp;color=blue&amp;cacheSeconds=3600" alt="Downloads by country: B"></a>
  <a href="https://clickpy.clickhouse.com/dashboard/retrievalloom"><img src="https://img.shields.io/badge/dynamic/json?url=https%3A%2F%2Fsql-clickhouse.clickhouse.com%2F%3Fuser%3Ddemo%26query%3DSELECT%2520if%2528count%2528%2529%2520%253D%25200%252C%2520%2527no%2520downloads%2527%252C%2520arrayStringConcat%2528groupArray%2528concat%2528country_code%252C%2520%2527%2520%2527%252C%2520toString%2528c%2529%2529%2529%252C%2520%2527%2520%257C%2520%2527%2529%2529%2520v%2520FROM%2520%2528SELECT%2520country_code%252C%2520sum%2528count%2529%2520c%2520FROM%2520pypi.pypi_downloads_per_day_by_version_by_country%2520WHERE%2520project%253D%2527retrievalloom%2527%2520AND%2520country_code%2520NOT%2520IN%2520%2528%2527%2527%252C%2520%2527None%2527%2529%2520AND%2520country_code%2520%253E%253D%2520%2527C%2527%2520AND%2520country_code%2520%253C%2520%2527D%2527%2520GROUP%2520BY%2520country_code%2520ORDER%2520BY%2520c%2520DESC%252C%2520country_code%2529%2520FORMAT%2520JSON&amp;query=%24.data%5B0%5D.v&amp;label=countries%20C&amp;color=blue&amp;cacheSeconds=3600" alt="Downloads by country: C"></a>
  <a href="https://clickpy.clickhouse.com/dashboard/retrievalloom"><img src="https://img.shields.io/badge/dynamic/json?url=https%3A%2F%2Fsql-clickhouse.clickhouse.com%2F%3Fuser%3Ddemo%26query%3DSELECT%2520if%2528count%2528%2529%2520%253D%25200%252C%2520%2527no%2520downloads%2527%252C%2520arrayStringConcat%2528groupArray%2528concat%2528country_code%252C%2520%2527%2520%2527%252C%2520toString%2528c%2529%2529%2529%252C%2520%2527%2520%257C%2520%2527%2529%2529%2520v%2520FROM%2520%2528SELECT%2520country_code%252C%2520sum%2528count%2529%2520c%2520FROM%2520pypi.pypi_downloads_per_day_by_version_by_country%2520WHERE%2520project%253D%2527retrievalloom%2527%2520AND%2520country_code%2520NOT%2520IN%2520%2528%2527%2527%252C%2520%2527None%2527%2529%2520AND%2520country_code%2520%253E%253D%2520%2527D%2527%2520AND%2520country_code%2520%253C%2520%2527E%2527%2520GROUP%2520BY%2520country_code%2520ORDER%2520BY%2520c%2520DESC%252C%2520country_code%2529%2520FORMAT%2520JSON&amp;query=%24.data%5B0%5D.v&amp;label=countries%20D&amp;color=blue&amp;cacheSeconds=3600" alt="Downloads by country: D"></a>
  <a href="https://clickpy.clickhouse.com/dashboard/retrievalloom"><img src="https://img.shields.io/badge/dynamic/json?url=https%3A%2F%2Fsql-clickhouse.clickhouse.com%2F%3Fuser%3Ddemo%26query%3DSELECT%2520if%2528count%2528%2529%2520%253D%25200%252C%2520%2527no%2520downloads%2527%252C%2520arrayStringConcat%2528groupArray%2528concat%2528country_code%252C%2520%2527%2520%2527%252C%2520toString%2528c%2529%2529%2529%252C%2520%2527%2520%257C%2520%2527%2529%2529%2520v%2520FROM%2520%2528SELECT%2520country_code%252C%2520sum%2528count%2529%2520c%2520FROM%2520pypi.pypi_downloads_per_day_by_version_by_country%2520WHERE%2520project%253D%2527retrievalloom%2527%2520AND%2520country_code%2520NOT%2520IN%2520%2528%2527%2527%252C%2520%2527None%2527%2529%2520AND%2520country_code%2520%253E%253D%2520%2527E%2527%2520AND%2520country_code%2520%253C%2520%2527F%2527%2520GROUP%2520BY%2520country_code%2520ORDER%2520BY%2520c%2520DESC%252C%2520country_code%2529%2520FORMAT%2520JSON&amp;query=%24.data%5B0%5D.v&amp;label=countries%20E&amp;color=blue&amp;cacheSeconds=3600" alt="Downloads by country: E"></a>
  <a href="https://clickpy.clickhouse.com/dashboard/retrievalloom"><img src="https://img.shields.io/badge/dynamic/json?url=https%3A%2F%2Fsql-clickhouse.clickhouse.com%2F%3Fuser%3Ddemo%26query%3DSELECT%2520if%2528count%2528%2529%2520%253D%25200%252C%2520%2527no%2520downloads%2527%252C%2520arrayStringConcat%2528groupArray%2528concat%2528country_code%252C%2520%2527%2520%2527%252C%2520toString%2528c%2529%2529%2529%252C%2520%2527%2520%257C%2520%2527%2529%2529%2520v%2520FROM%2520%2528SELECT%2520country_code%252C%2520sum%2528count%2529%2520c%2520FROM%2520pypi.pypi_downloads_per_day_by_version_by_country%2520WHERE%2520project%253D%2527retrievalloom%2527%2520AND%2520country_code%2520NOT%2520IN%2520%2528%2527%2527%252C%2520%2527None%2527%2529%2520AND%2520country_code%2520%253E%253D%2520%2527F%2527%2520AND%2520country_code%2520%253C%2520%2527G%2527%2520GROUP%2520BY%2520country_code%2520ORDER%2520BY%2520c%2520DESC%252C%2520country_code%2529%2520FORMAT%2520JSON&amp;query=%24.data%5B0%5D.v&amp;label=countries%20F&amp;color=blue&amp;cacheSeconds=3600" alt="Downloads by country: F"></a>
  <a href="https://clickpy.clickhouse.com/dashboard/retrievalloom"><img src="https://img.shields.io/badge/dynamic/json?url=https%3A%2F%2Fsql-clickhouse.clickhouse.com%2F%3Fuser%3Ddemo%26query%3DSELECT%2520if%2528count%2528%2529%2520%253D%25200%252C%2520%2527no%2520downloads%2527%252C%2520arrayStringConcat%2528groupArray%2528concat%2528country_code%252C%2520%2527%2520%2527%252C%2520toString%2528c%2529%2529%2529%252C%2520%2527%2520%257C%2520%2527%2529%2529%2520v%2520FROM%2520%2528SELECT%2520country_code%252C%2520sum%2528count%2529%2520c%2520FROM%2520pypi.pypi_downloads_per_day_by_version_by_country%2520WHERE%2520project%253D%2527retrievalloom%2527%2520AND%2520country_code%2520NOT%2520IN%2520%2528%2527%2527%252C%2520%2527None%2527%2529%2520AND%2520country_code%2520%253E%253D%2520%2527G%2527%2520AND%2520country_code%2520%253C%2520%2527H%2527%2520GROUP%2520BY%2520country_code%2520ORDER%2520BY%2520c%2520DESC%252C%2520country_code%2529%2520FORMAT%2520JSON&amp;query=%24.data%5B0%5D.v&amp;label=countries%20G&amp;color=blue&amp;cacheSeconds=3600" alt="Downloads by country: G"></a>
  <a href="https://clickpy.clickhouse.com/dashboard/retrievalloom"><img src="https://img.shields.io/badge/dynamic/json?url=https%3A%2F%2Fsql-clickhouse.clickhouse.com%2F%3Fuser%3Ddemo%26query%3DSELECT%2520if%2528count%2528%2529%2520%253D%25200%252C%2520%2527no%2520downloads%2527%252C%2520arrayStringConcat%2528groupArray%2528concat%2528country_code%252C%2520%2527%2520%2527%252C%2520toString%2528c%2529%2529%2529%252C%2520%2527%2520%257C%2520%2527%2529%2529%2520v%2520FROM%2520%2528SELECT%2520country_code%252C%2520sum%2528count%2529%2520c%2520FROM%2520pypi.pypi_downloads_per_day_by_version_by_country%2520WHERE%2520project%253D%2527retrievalloom%2527%2520AND%2520country_code%2520NOT%2520IN%2520%2528%2527%2527%252C%2520%2527None%2527%2529%2520AND%2520country_code%2520%253E%253D%2520%2527H%2527%2520AND%2520country_code%2520%253C%2520%2527I%2527%2520GROUP%2520BY%2520country_code%2520ORDER%2520BY%2520c%2520DESC%252C%2520country_code%2529%2520FORMAT%2520JSON&amp;query=%24.data%5B0%5D.v&amp;label=countries%20H&amp;color=blue&amp;cacheSeconds=3600" alt="Downloads by country: H"></a>
  <a href="https://clickpy.clickhouse.com/dashboard/retrievalloom"><img src="https://img.shields.io/badge/dynamic/json?url=https%3A%2F%2Fsql-clickhouse.clickhouse.com%2F%3Fuser%3Ddemo%26query%3DSELECT%2520if%2528count%2528%2529%2520%253D%25200%252C%2520%2527no%2520downloads%2527%252C%2520arrayStringConcat%2528groupArray%2528concat%2528country_code%252C%2520%2527%2520%2527%252C%2520toString%2528c%2529%2529%2529%252C%2520%2527%2520%257C%2520%2527%2529%2529%2520v%2520FROM%2520%2528SELECT%2520country_code%252C%2520sum%2528count%2529%2520c%2520FROM%2520pypi.pypi_downloads_per_day_by_version_by_country%2520WHERE%2520project%253D%2527retrievalloom%2527%2520AND%2520country_code%2520NOT%2520IN%2520%2528%2527%2527%252C%2520%2527None%2527%2529%2520AND%2520country_code%2520%253E%253D%2520%2527I%2527%2520AND%2520country_code%2520%253C%2520%2527J%2527%2520GROUP%2520BY%2520country_code%2520ORDER%2520BY%2520c%2520DESC%252C%2520country_code%2529%2520FORMAT%2520JSON&amp;query=%24.data%5B0%5D.v&amp;label=countries%20I&amp;color=blue&amp;cacheSeconds=3600" alt="Downloads by country: I"></a>
  <a href="https://clickpy.clickhouse.com/dashboard/retrievalloom"><img src="https://img.shields.io/badge/dynamic/json?url=https%3A%2F%2Fsql-clickhouse.clickhouse.com%2F%3Fuser%3Ddemo%26query%3DSELECT%2520if%2528count%2528%2529%2520%253D%25200%252C%2520%2527no%2520downloads%2527%252C%2520arrayStringConcat%2528groupArray%2528concat%2528country_code%252C%2520%2527%2520%2527%252C%2520toString%2528c%2529%2529%2529%252C%2520%2527%2520%257C%2520%2527%2529%2529%2520v%2520FROM%2520%2528SELECT%2520country_code%252C%2520sum%2528count%2529%2520c%2520FROM%2520pypi.pypi_downloads_per_day_by_version_by_country%2520WHERE%2520project%253D%2527retrievalloom%2527%2520AND%2520country_code%2520NOT%2520IN%2520%2528%2527%2527%252C%2520%2527None%2527%2529%2520AND%2520country_code%2520%253E%253D%2520%2527J%2527%2520AND%2520country_code%2520%253C%2520%2527K%2527%2520GROUP%2520BY%2520country_code%2520ORDER%2520BY%2520c%2520DESC%252C%2520country_code%2529%2520FORMAT%2520JSON&amp;query=%24.data%5B0%5D.v&amp;label=countries%20J&amp;color=blue&amp;cacheSeconds=3600" alt="Downloads by country: J"></a>
  <a href="https://clickpy.clickhouse.com/dashboard/retrievalloom"><img src="https://img.shields.io/badge/dynamic/json?url=https%3A%2F%2Fsql-clickhouse.clickhouse.com%2F%3Fuser%3Ddemo%26query%3DSELECT%2520if%2528count%2528%2529%2520%253D%25200%252C%2520%2527no%2520downloads%2527%252C%2520arrayStringConcat%2528groupArray%2528concat%2528country_code%252C%2520%2527%2520%2527%252C%2520toString%2528c%2529%2529%2529%252C%2520%2527%2520%257C%2520%2527%2529%2529%2520v%2520FROM%2520%2528SELECT%2520country_code%252C%2520sum%2528count%2529%2520c%2520FROM%2520pypi.pypi_downloads_per_day_by_version_by_country%2520WHERE%2520project%253D%2527retrievalloom%2527%2520AND%2520country_code%2520NOT%2520IN%2520%2528%2527%2527%252C%2520%2527None%2527%2529%2520AND%2520country_code%2520%253E%253D%2520%2527K%2527%2520AND%2520country_code%2520%253C%2520%2527L%2527%2520GROUP%2520BY%2520country_code%2520ORDER%2520BY%2520c%2520DESC%252C%2520country_code%2529%2520FORMAT%2520JSON&amp;query=%24.data%5B0%5D.v&amp;label=countries%20K&amp;color=blue&amp;cacheSeconds=3600" alt="Downloads by country: K"></a>
  <a href="https://clickpy.clickhouse.com/dashboard/retrievalloom"><img src="https://img.shields.io/badge/dynamic/json?url=https%3A%2F%2Fsql-clickhouse.clickhouse.com%2F%3Fuser%3Ddemo%26query%3DSELECT%2520if%2528count%2528%2529%2520%253D%25200%252C%2520%2527no%2520downloads%2527%252C%2520arrayStringConcat%2528groupArray%2528concat%2528country_code%252C%2520%2527%2520%2527%252C%2520toString%2528c%2529%2529%2529%252C%2520%2527%2520%257C%2520%2527%2529%2529%2520v%2520FROM%2520%2528SELECT%2520country_code%252C%2520sum%2528count%2529%2520c%2520FROM%2520pypi.pypi_downloads_per_day_by_version_by_country%2520WHERE%2520project%253D%2527retrievalloom%2527%2520AND%2520country_code%2520NOT%2520IN%2520%2528%2527%2527%252C%2520%2527None%2527%2529%2520AND%2520country_code%2520%253E%253D%2520%2527L%2527%2520AND%2520country_code%2520%253C%2520%2527N%2527%2520GROUP%2520BY%2520country_code%2520ORDER%2520BY%2520c%2520DESC%252C%2520country_code%2529%2520FORMAT%2520JSON&amp;query=%24.data%5B0%5D.v&amp;label=countries%20L-M&amp;color=blue&amp;cacheSeconds=3600" alt="Downloads by country: L-M"></a>
  <a href="https://clickpy.clickhouse.com/dashboard/retrievalloom"><img src="https://img.shields.io/badge/dynamic/json?url=https%3A%2F%2Fsql-clickhouse.clickhouse.com%2F%3Fuser%3Ddemo%26query%3DSELECT%2520if%2528count%2528%2529%2520%253D%25200%252C%2520%2527no%2520downloads%2527%252C%2520arrayStringConcat%2528groupArray%2528concat%2528country_code%252C%2520%2527%2520%2527%252C%2520toString%2528c%2529%2529%2529%252C%2520%2527%2520%257C%2520%2527%2529%2529%2520v%2520FROM%2520%2528SELECT%2520country_code%252C%2520sum%2528count%2529%2520c%2520FROM%2520pypi.pypi_downloads_per_day_by_version_by_country%2520WHERE%2520project%253D%2527retrievalloom%2527%2520AND%2520country_code%2520NOT%2520IN%2520%2528%2527%2527%252C%2520%2527None%2527%2529%2520AND%2520country_code%2520%253E%253D%2520%2527N%2527%2520AND%2520country_code%2520%253C%2520%2527Q%2527%2520GROUP%2520BY%2520country_code%2520ORDER%2520BY%2520c%2520DESC%252C%2520country_code%2529%2520FORMAT%2520JSON&amp;query=%24.data%5B0%5D.v&amp;label=countries%20N-P&amp;color=blue&amp;cacheSeconds=3600" alt="Downloads by country: N-P"></a>
  <a href="https://clickpy.clickhouse.com/dashboard/retrievalloom"><img src="https://img.shields.io/badge/dynamic/json?url=https%3A%2F%2Fsql-clickhouse.clickhouse.com%2F%3Fuser%3Ddemo%26query%3DSELECT%2520if%2528count%2528%2529%2520%253D%25200%252C%2520%2527no%2520downloads%2527%252C%2520arrayStringConcat%2528groupArray%2528concat%2528country_code%252C%2520%2527%2520%2527%252C%2520toString%2528c%2529%2529%2529%252C%2520%2527%2520%257C%2520%2527%2529%2529%2520v%2520FROM%2520%2528SELECT%2520country_code%252C%2520sum%2528count%2529%2520c%2520FROM%2520pypi.pypi_downloads_per_day_by_version_by_country%2520WHERE%2520project%253D%2527retrievalloom%2527%2520AND%2520country_code%2520NOT%2520IN%2520%2528%2527%2527%252C%2520%2527None%2527%2529%2520AND%2520country_code%2520%253E%253D%2520%2527Q%2527%2520AND%2520country_code%2520%253C%2520%2527R%2527%2520GROUP%2520BY%2520country_code%2520ORDER%2520BY%2520c%2520DESC%252C%2520country_code%2529%2520FORMAT%2520JSON&amp;query=%24.data%5B0%5D.v&amp;label=countries%20Q&amp;color=blue&amp;cacheSeconds=3600" alt="Downloads by country: Q"></a>
  <a href="https://clickpy.clickhouse.com/dashboard/retrievalloom"><img src="https://img.shields.io/badge/dynamic/json?url=https%3A%2F%2Fsql-clickhouse.clickhouse.com%2F%3Fuser%3Ddemo%26query%3DSELECT%2520if%2528count%2528%2529%2520%253D%25200%252C%2520%2527no%2520downloads%2527%252C%2520arrayStringConcat%2528groupArray%2528concat%2528country_code%252C%2520%2527%2520%2527%252C%2520toString%2528c%2529%2529%2529%252C%2520%2527%2520%257C%2520%2527%2529%2529%2520v%2520FROM%2520%2528SELECT%2520country_code%252C%2520sum%2528count%2529%2520c%2520FROM%2520pypi.pypi_downloads_per_day_by_version_by_country%2520WHERE%2520project%253D%2527retrievalloom%2527%2520AND%2520country_code%2520NOT%2520IN%2520%2528%2527%2527%252C%2520%2527None%2527%2529%2520AND%2520country_code%2520%253E%253D%2520%2527R%2527%2520AND%2520country_code%2520%253C%2520%2527S%2527%2520GROUP%2520BY%2520country_code%2520ORDER%2520BY%2520c%2520DESC%252C%2520country_code%2529%2520FORMAT%2520JSON&amp;query=%24.data%5B0%5D.v&amp;label=countries%20R&amp;color=blue&amp;cacheSeconds=3600" alt="Downloads by country: R"></a>
  <a href="https://clickpy.clickhouse.com/dashboard/retrievalloom"><img src="https://img.shields.io/badge/dynamic/json?url=https%3A%2F%2Fsql-clickhouse.clickhouse.com%2F%3Fuser%3Ddemo%26query%3DSELECT%2520if%2528count%2528%2529%2520%253D%25200%252C%2520%2527no%2520downloads%2527%252C%2520arrayStringConcat%2528groupArray%2528concat%2528country_code%252C%2520%2527%2520%2527%252C%2520toString%2528c%2529%2529%2529%252C%2520%2527%2520%257C%2520%2527%2529%2529%2520v%2520FROM%2520%2528SELECT%2520country_code%252C%2520sum%2528count%2529%2520c%2520FROM%2520pypi.pypi_downloads_per_day_by_version_by_country%2520WHERE%2520project%253D%2527retrievalloom%2527%2520AND%2520country_code%2520NOT%2520IN%2520%2528%2527%2527%252C%2520%2527None%2527%2529%2520AND%2520country_code%2520%253E%253D%2520%2527S%2527%2520AND%2520country_code%2520%253C%2520%2527T%2527%2520GROUP%2520BY%2520country_code%2520ORDER%2520BY%2520c%2520DESC%252C%2520country_code%2529%2520FORMAT%2520JSON&amp;query=%24.data%5B0%5D.v&amp;label=countries%20S&amp;color=blue&amp;cacheSeconds=3600" alt="Downloads by country: S"></a>
  <a href="https://clickpy.clickhouse.com/dashboard/retrievalloom"><img src="https://img.shields.io/badge/dynamic/json?url=https%3A%2F%2Fsql-clickhouse.clickhouse.com%2F%3Fuser%3Ddemo%26query%3DSELECT%2520if%2528count%2528%2529%2520%253D%25200%252C%2520%2527no%2520downloads%2527%252C%2520arrayStringConcat%2528groupArray%2528concat%2528country_code%252C%2520%2527%2520%2527%252C%2520toString%2528c%2529%2529%2529%252C%2520%2527%2520%257C%2520%2527%2529%2529%2520v%2520FROM%2520%2528SELECT%2520country_code%252C%2520sum%2528count%2529%2520c%2520FROM%2520pypi.pypi_downloads_per_day_by_version_by_country%2520WHERE%2520project%253D%2527retrievalloom%2527%2520AND%2520country_code%2520NOT%2520IN%2520%2528%2527%2527%252C%2520%2527None%2527%2529%2520AND%2520country_code%2520%253E%253D%2520%2527T%2527%2520AND%2520country_code%2520%253C%2520%2527U%2527%2520GROUP%2520BY%2520country_code%2520ORDER%2520BY%2520c%2520DESC%252C%2520country_code%2529%2520FORMAT%2520JSON&amp;query=%24.data%5B0%5D.v&amp;label=countries%20T&amp;color=blue&amp;cacheSeconds=3600" alt="Downloads by country: T"></a>
  <a href="https://clickpy.clickhouse.com/dashboard/retrievalloom"><img src="https://img.shields.io/badge/dynamic/json?url=https%3A%2F%2Fsql-clickhouse.clickhouse.com%2F%3Fuser%3Ddemo%26query%3DSELECT%2520if%2528count%2528%2529%2520%253D%25200%252C%2520%2527no%2520downloads%2527%252C%2520arrayStringConcat%2528groupArray%2528concat%2528country_code%252C%2520%2527%2520%2527%252C%2520toString%2528c%2529%2529%2529%252C%2520%2527%2520%257C%2520%2527%2529%2529%2520v%2520FROM%2520%2528SELECT%2520country_code%252C%2520sum%2528count%2529%2520c%2520FROM%2520pypi.pypi_downloads_per_day_by_version_by_country%2520WHERE%2520project%253D%2527retrievalloom%2527%2520AND%2520country_code%2520NOT%2520IN%2520%2528%2527%2527%252C%2520%2527None%2527%2529%2520AND%2520country_code%2520%253E%253D%2520%2527U%2527%2520AND%2520country_code%2520%253C%2520%2527V%2527%2520GROUP%2520BY%2520country_code%2520ORDER%2520BY%2520c%2520DESC%252C%2520country_code%2529%2520FORMAT%2520JSON&amp;query=%24.data%5B0%5D.v&amp;label=countries%20U&amp;color=blue&amp;cacheSeconds=3600" alt="Downloads by country: U"></a>
  <a href="https://clickpy.clickhouse.com/dashboard/retrievalloom"><img src="https://img.shields.io/badge/dynamic/json?url=https%3A%2F%2Fsql-clickhouse.clickhouse.com%2F%3Fuser%3Ddemo%26query%3DSELECT%2520if%2528count%2528%2529%2520%253D%25200%252C%2520%2527no%2520downloads%2527%252C%2520arrayStringConcat%2528groupArray%2528concat%2528country_code%252C%2520%2527%2520%2527%252C%2520toString%2528c%2529%2529%2529%252C%2520%2527%2520%257C%2520%2527%2529%2529%2520v%2520FROM%2520%2528SELECT%2520country_code%252C%2520sum%2528count%2529%2520c%2520FROM%2520pypi.pypi_downloads_per_day_by_version_by_country%2520WHERE%2520project%253D%2527retrievalloom%2527%2520AND%2520country_code%2520NOT%2520IN%2520%2528%2527%2527%252C%2520%2527None%2527%2529%2520AND%2520country_code%2520%253E%253D%2520%2527V%2527%2520GROUP%2520BY%2520country_code%2520ORDER%2520BY%2520c%2520DESC%252C%2520country_code%2529%2520FORMAT%2520JSON&amp;query=%24.data%5B0%5D.v&amp;label=countries%20V-Z&amp;color=blue&amp;cacheSeconds=3600" alt="Downloads by country: V-Z"></a>
</p>

The badges use the [ClickHouse public PyPI dataset](https://clickpy.clickhouse.com/dashboard/retrievalloom).
The dataset refreshes daily, so counts may be about a day behind. Shields
caches badge images for up to an hour, and GitHub may cache them longer. Weeks
start on Monday in UTC. Country badges exclude missing country codes. Explore
interactive charts on the
[ClickPy dashboard](https://clickpy.clickhouse.com/dashboard/retrievalloom).

## Benchmarks

`scripts/benchmark.py` measures ingest throughput, search latency and
event-loop lag with the default in-memory components. See
[Benchmarks](https://retrievalloom.readthedocs.io/en/latest/benchmarks/)
for the method, the results and how to reproduce them.

## Examples

Each program in [`examples/`](https://github.com/smuniharish/retrievalloom/tree/main/examples)
models a real deployment, and the test suite runs it and checks its output.
[`examples/adapters/`](https://retrievalloom.readthedocs.io/en/latest/examples/adapters/)
holds production-style custom components you can copy, from bge-m3 embeddings
to a Neo4j graph store:

| Example | Shows | Needs |
| --- | --- | --- |
| [`quickstart.py`](https://retrievalloom.readthedocs.io/en/latest/examples/quickstart/) | Index and search three documents in memory | Base install |
| [`customer_support.py`](https://retrievalloom.readthedocs.io/en/latest/examples/customer-support/) | Help-center search that handles typos, jargon, German and paraphrases, with reranking and filters | Local models, Qdrant |
| [`multi_tenant_saas.py`](https://retrievalloom.readthedocs.io/en/latest/examples/multi-tenant-saas/) | One engine for many customers: isolation, role-based access, quotas and an audit log | Base install |
| [`document_lifecycle.py`](https://retrievalloom.readthedocs.io/en/latest/examples/document-lifecycle/) | Edit, delete and erase documents, add semantic search later, and change models | Local models, pgvector |
| [`knowledge_graph.py`](https://retrievalloom.readthedocs.io/en/latest/examples/knowledge-graph/) | Answer relationship questions from a knowledge graph built at ingest, on Neo4j or LadybugDB | `graph` extra; Neo4j optional |
| [`policy_manual.py`](https://retrievalloom.readthedocs.io/en/latest/examples/policy-manual/) | Search a long policy by section and cite the section path | `structure` extra |
| [`fact_checking.py`](https://retrievalloom.readthedocs.io/en/latest/examples/fact-checking/) | Check a claim against your documents and flag contradicting passages | Local models |
| [`http_service.py`](https://retrievalloom.readthedocs.io/en/latest/examples/http-service/) | Serve search over HTTP with FastAPI, with a Postman collection | `examples` group |
| [`observability_jaeger.py`](https://retrievalloom.readthedocs.io/en/latest/examples/observability-jaeger/) | Trace each request in Jaeger and find its log lines by trace id | `examples` group; Jaeger optional |
| [`durable_restart.py`](https://retrievalloom.readthedocs.io/en/latest/examples/durable-restart/) | Restart a service without re-ingesting or re-embedding | Local models, Qdrant |
| [`prechunked_and_multi_representation.py`](https://retrievalloom.readthedocs.io/en/latest/examples/prechunked-and-multi-representation/) | Index your own chunks with spans and pages, plus a section outline | `structure` extra |
| [`agxai_provenance.py`](https://retrievalloom.readthedocs.io/en/latest/examples/agxai-provenance/) | Record every ingestion, deletion and search in an agxai audit trail | Base install |

## Documentation

Full documentation: **[retrievalloom.readthedocs.io](https://retrievalloom.readthedocs.io/)**

- [Getting started](https://retrievalloom.readthedocs.io/en/latest/getting-started/)
- [User guide](https://retrievalloom.readthedocs.io/en/latest/guide/)
- [Integrations](https://retrievalloom.readthedocs.io/en/latest/integrations/)
- [Examples](https://retrievalloom.readthedocs.io/en/latest/examples/)
- [Architecture](https://retrievalloom.readthedocs.io/en/latest/architecture/)
- [API reference](https://retrievalloom.readthedocs.io/en/latest/api/)

Changes are listed in the
[changelog](https://github.com/smuniharish/retrievalloom/blob/main/CHANGELOG.md).

## Agent skills

Let your AI coding assistant build with RetrievalLoom correctly. Two
[Agent Skills](https://retrievalloom.readthedocs.io/en/latest/agent-skills/) teach
Claude Code, Codex, Cursor, GitHub Copilot and other compatible agents how to
use the library: `retrievalloom` adds it to your application, and
`retrievalloom-extend` helps you connect your own models and stores. Install
them with the [skills CLI](https://github.com/vercel-labs/skills):

```bash
npx skills add smuniharish/retrievalloom                                # choose interactively
npx skills add smuniharish/retrievalloom --skill retrievalloom          # only the core skill
npx skills add smuniharish/retrievalloom --skill retrievalloom-extend   # only the extension skill
```

See [Install the skills](https://retrievalloom.readthedocs.io/en/latest/agent-skills/install/)
for manual and per-agent installation, and for skills maintained by the
AGXAI, Qdrant and FastAPI projects.

## Scope

RetrievalLoom is a retrieval library. It does not parse files, split text into
chunks, host models or orchestrate agents; use it alongside the tools you
already have for those jobs. It does not depend on LangChain, LlamaIndex or any
other framework.

## Contributing

Contributions are welcome. See
[CONTRIBUTING.md](https://github.com/smuniharish/retrievalloom/blob/main/CONTRIBUTING.md)
for the development setup and checks, and
[SECURITY.md](https://github.com/smuniharish/retrievalloom/blob/main/SECURITY.md)
to report a vulnerability.

## License

Apache License 2.0. See
[LICENSE](https://github.com/smuniharish/retrievalloom/blob/main/LICENSE).
