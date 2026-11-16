# Quickstart

This example ingests three short documents into an in-memory engine and
retrieves evidence for a query. It needs only the base install.

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

Output (the library itself prints nothing; see
[Logging](installation.md#logging)):

```text
ingested: ['created', 'created', 'created']
status: complete
asyncio v1 0.0164 ['index.fuzzy', 'index.sparse']
   asyncio runs coroutines on a single-threaded event loop.
```

## What happened

1. `RetrievalEngine()` created the default components: an in-memory SQLite
   metadata store and the keyword, fuzzy and sparse (BM25) methods. The
   `async with` block initialises them and closes them on exit.
2. `ingest` validated the documents, stored version 1 of each, and derived the
   objects to index. By default it then waits for one indexing round, so the
   documents are searchable as soon as it returns.
3. `retrieve` queried every enabled method, fused the ranked lists, built
   candidates, consolidated them into evidence, and attached provenance.

The keyword method found nothing here. It matches the keywords stored under the
`keywords` metadata field (see [Channels and methods](../guide/concepts/channels.md#keyword)).

## Warnings and status

`result.warnings` explains anything the engine could not do. In this run it
holds two informational warnings: `method_unavailable` (no embedding model, so
no dense method) and `reranker_unavailable` (no reranker). Informational
warnings keep `status` at `complete`. A warning of severity `warning` or higher
makes the status `partial`. No evidence makes it `empty`.

## Next steps

- Add dense retrieval with your own [embedding model](../guide/how-to/embedding-model.md).
- Add a [reranker and NLI](../guide/how-to/rerank-and-nli.md).
- Keep data across restarts with [durable storage](../guide/how-to/durable-storage.md).
- Browse the runnable [examples](../examples/index.md).
