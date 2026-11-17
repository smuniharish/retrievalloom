# Quickstart

Index three short documents and search them in under a minute. The program
keeps everything in memory and needs only the base install.

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
[See log output](installation.md#see-log-output)):

```text
ingested: ['created', 'created', 'created']
status: complete
asyncio v1 0.0164 ['index.fuzzy', 'index.sparse']
   asyncio runs coroutines on a single-threaded event loop.
```

## What happened

1. `RetrievalEngine()` created an engine with the defaults: an in-memory
   metadata store and keyword, typo-tolerant (fuzzy) and BM25 (sparse) search.
   The `async with` block starts the engine and closes it on exit.
2. `ingest` validated the documents, stored version 1 of each, and indexed
   them. By default it waits for indexing to finish, so the documents are
   searchable as soon as it returns.
3. `retrieve` ran every enabled search method, merged their rankings, and
   returned evidence: each result with its text, score, source document and
   the methods that found it.

The keyword method found nothing here because it matches only the terms you
store under the `keywords` metadata field. See
[Channels and methods](../guide/concepts/channels.md#keyword).

## Read the status and warnings

`result.warnings` tells you anything the engine could not do, so a missing
model never fails silently. In this run it holds one informational warning,
`method_unavailable`, because no embedding model is configured for semantic
search.

`result.status` summarizes the outcome. Informational warnings keep it
`complete`. A warning of severity `warning` or higher makes it `partial`, and
a search with no results makes it `empty`.

## Next steps

- Add semantic search with your own [embedding model](../guide/how-to/embedding-model.md).
- Improve ranking with a [reranker, and check claims with NLI](../guide/how-to/rerank-and-nli.md).
- Tune timeouts and limits in [Configuration](configuration.md).
- Keep data across restarts with [durable storage](../guide/how-to/durable-storage.md).
- Browse the runnable [examples](../examples/index.md).
