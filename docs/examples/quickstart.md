# Quickstart

This example creates an in-memory engine, ingests four short documents and retrieves
evidence for a query. For each evidence item it prints the document version, the source
URI and title, the methods that found it, the matched terms and the pipeline stages it
passed through. It then repeats a retrieval with a metadata filter (`lang = "de"`).

The engine uses its default components: an in-memory SQLite metadata store and the
keyword, fuzzy and sparse (BM25) methods of the index channel. The keyword method reads
canonical keywords from each document's `keywords` metadata field.

Because no embedding model or reranker is configured, every result carries two
informational warnings: `method_unavailable` (the dense method is skipped) and
`reranker_unavailable`. Scores are reciprocal-rank-fusion scores, not probabilities.

## Requirements

The base install only (`pip install retrievalloom`).

## Run

```bash
python examples/quickstart.py
```

## Source

<!-- fmt: off -->
```python title="examples/quickstart.py"
--8<-- "examples/quickstart.py"
```
<!-- fmt: on -->

## Expected output

```text
ingested: {'created': 4}
status: complete
warnings: ['method_unavailable', 'reranker_unavailable']
#1 asyncio v1 score=0.0164
   text:    Python's asyncio library runs coroutines on a single-threaded event loop.
   source:  kb://articles/asyncio (Asyncio)
   methods: index.fuzzy, index.keyword, index.sparse
   matched: ['asyncio', 'coroutines', 'event', 'loop']
   stages:  ['method:index.keyword', 'method:index.fuzzy', 'method:index.sparse', 'method_fusion', 'dedup', 'channel_fusion', 'consolidation']
#2 eventloop v1 score=0.0161
   text:    Eine Ereignisschleife (event loop) verteilt Aufgaben an Koroutinen.
   source:  kb://articles/eventloop (Eventloop)
   methods: index.fuzzy, index.sparse
   matched: ['event', 'loop']
   stages:  ['method:index.keyword', 'method:index.fuzzy', 'method:index.sparse', 'method_fusion', 'dedup', 'channel_fusion', 'consolidation']
lang=de filter: ['eventloop']
```
