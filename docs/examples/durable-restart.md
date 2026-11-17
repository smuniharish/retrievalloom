# Durable restart

A service restarts after a deploy, a crash or a scale-out, and it must answer at once
without re-ingesting anything. The example runs two engine processes one after the other
against the same durable state and shows which indexes are reused and which are rebuilt.

## How it works

- **Metadata.** `data_dir` holds the SQLite metadata store: documents, versions, coverage,
  index generations and lifecycle tasks. No Postgres is needed.
- **Dense vectors.** `BgeM3Embedding` stores them in the built-in `QdrantVectorStore` on
  a Qdrant server.
- **Learned sparse.** `BgeM3SparseIndex` (a custom `SparseIndexABC`) replaces the built-in
  BM25 index with bge-m3 lexical weights stored as Qdrant sparse vectors. Both adapters
  share one loaded bge-m3 model. See [Custom components](adapters.md#learned-sparse-and-sparseindexabc)
  for how it fits the contract.
- **Restart behavior.** The keyword and fuzzy indexes are in-memory. At startup the engine
  sees that their generations are gone and rebuilds them from the metadata store. The
  durable Qdrant generations are reused untouched, with no re-embedding.

Each run uses its own Qdrant collection prefix and a temporary `data_dir`, and both are
removed at the end. In a deployment `data_dir` is a persistent volume.

## Requirements

- Local models in `models/` (`bge-m3`) and the `examples` dependency group.
- A Qdrant server: `RL_EXAMPLES_QDRANT_URL=http://localhost:6333`.

## Run

```bash
python examples/durable_restart.py
```

## Source

<!-- fmt: off -->
```python title="examples/durable_restart.py"
--8<-- "examples/durable_restart.py"
```
<!-- fmt: on -->

## Expected output

```text
first process: ingested 10 documents
   answer: ['sync-selective', 'sync-conflicts'] via [index.dense, index.fuzzy, index.sparse]
first process closed
second process: documents_active=10 (no re-ingest)
   index.dense: reused
   index.fuzzy: rebuilt
   index.keyword: rebuilt
   index.sparse: reused
   answer: ['sync-selective', 'sync-conflicts'] via [index.dense, index.fuzzy, index.sparse]
   learned-sparse terms behind the top hit: ['folder', 'laptop', 'syn']
```

The last line comes from the sparse index's `matched` explanation: the bge-m3 vocabulary
terms with the highest shared weight between the query and the document. `syn` is a
sub-word piece of "sync".
