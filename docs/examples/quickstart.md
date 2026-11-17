# Quickstart

See everything a search result tells you, with no model or server. The example creates
an in-memory engine, ingests four short documents and searches them. For each result it
prints the document version, the source URI and title, the methods that found it, the
matched terms and the pipeline stages it passed through. It then repeats the search with
a metadata filter (`lang = "de"`).

The engine uses its default components: an in-memory SQLite metadata store and keyword,
typo-tolerant (fuzzy) and BM25 (sparse) search. The keyword method reads
canonical keywords from each document's `keywords` metadata field.

Because no embedding model is configured, every result carries the informational
warning `method_unavailable` (the dense method is skipped). Without a reranker the
rerank stage is skipped silently. Scores are reciprocal-rank-fusion scores, not probabilities.

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
warnings: ['method_unavailable']
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
