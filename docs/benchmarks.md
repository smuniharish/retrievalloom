# Benchmarks

RetrievalLoom runs inside your application's process, so its cost is the
latency it adds to each retrieval, the throughput of ingest, and how long it
holds your event loop. This page shows all three, measured on one machine, and
how to measure them on yours.

## Results

An in-memory engine with the default components: an in-memory SQLite metadata
store and the keyword, fuzzy and sparse (BM25) index methods. 2,000 synthetic
documents of 80 words each are ingested in batches of 500, then 200 four-word
queries run after 20 untimed warmup queries. No network, model or disk time is
included.

| Scenario | p50 ms | p95 ms | p99 ms | mean ms |
| --- | ---: | ---: | ---: | ---: |
| retrieve, uncached | 56.77 | 80.00 | 102.09 | 59.94 |
| retrieve, cached | 8.90 | 13.42 | 19.53 | 9.64 |

| Throughput | Result |
| --- | ---: |
| ingest, indexed (`index_wait="all"`) | 680 documents/s |
| retrieve, 16 concurrent requests | 25 retrievals/s |
| max event-loop lag while 16 requests run | 80.2 ms |

Every query returned evidence.

### Where an uncached retrieval spends its time

Medians of `RetrievalResult.timings` over the 200 uncached queries. Methods run
concurrently, so `methods` is close to the slowest method, not their sum.

| Stage | p50 ms |
| --- | ---: |
| `snapshot` | 9.84 |
| `method:index.keyword` | 0.03 |
| `method:index.sparse` | 1.83 |
| `method:index.fuzzy` | 28.73 |
| `methods` | 28.89 |
| `method_fusion` | 0.65 |
| `candidates` | 12.80 |
| `fusion` | 1.37 |
| `rerank` | 0.21 |
| `nli` | 0.00 |
| `consolidation` | 1.31 |
| `total` | 56.59 |

- **Fuzzy matching is the largest stage.** RapidFuzz compares every query
  n-gram with the corpus vocabulary, so its cost grows with the vocabulary
  (about 17 ms at 100 documents and 29 ms at 1,000 on the same machine). It
  runs in a worker thread and releases the GIL. If you do not need typo
  tolerance, disable it with `channels.index.fuzzy.enabled=False`.
- **`snapshot` and `candidates` are metadata-store round trips.** The snapshot
  reads coverage and generations in one read transaction of about nine
  statements, and `candidates` loads the hit documents. With SQLAlchemy's
  async engine on aiosqlite, each statement costs about 0.5–1 ms in thread
  hand-offs, so these stages depend on the statement count, not the corpus
  size.
- **A cache hit still takes a snapshot.** The cache key includes the snapshot
  sequence and the active generations, so a cached result is never served
  after the index changes. That is most of the 9 ms.
- **The in-memory keyword, fuzzy and sparse indexes are updated in place.**
  An ingest batch adds, replaces and removes only its own objects, so index
  maintenance costs the same for every batch at any corpus size. A change to
  the keyword dictionary is the exception: it triggers a full keyword and fuzzy
  build. See [Index update cost](#index-update-cost).
- **Ingest writes metadata in batches.** Each batch of documents is written in
  a few multi-row statements per write transaction, and the indexer reads
  objects and opens visibility windows for a whole batch at once. A batch of
  500 documents costs about 290 metadata statements. See
  [Ingest metadata cost](#ingest-metadata-cost).
- **The loop lag comes from garbage collection pauses, not the GIL.**
  The 80 ms is the longest time a 5 ms ticker on the loop waited to wake up.
  RapidFuzz's `cdist` and the numpy scoring release the GIL. The long waits are
  full (generation 2) collections on the loop thread. At 2,000 documents the
  process tracks about 224,000 objects, and each full collection takes
  60–80 ms. With `--gc-freeze`, which calls `gc.collect()` and `gc.freeze()`
  before the concurrent scenario, the max lag drops to 21–29 ms. Fewer
  tracked objects mean shorter pauses. The index structures account for about
  23,000 of those objects at 2,000 documents and about 77,000 at 20,000.
  Storing postings in flat numpy arrays instead of dicts and sets would
  reduce that further. See
  [Keep the event loop responsive](guide/how-to/event-loop-latency.md).

## Index update cost

Before this change, each ingest batch rebuilt the keyword, fuzzy and sparse
indexes over the whole corpus. Now each batch updates them in place. The runs
below compare the previous index modules ("before") with the current ones
("after"). Everything else is the same code on the same machine, ingesting in
batches of 500. The machine was running other workloads, so absolute numbers
are higher than in [Results](#results). Compare each before and after pair,
not across tables.

Index maintenance only, without the engine (keyword, fuzzy and sparse indexes
updated per batch of 500):

| Corpus | Ingest, before | Ingest, after | Last batch, before | Last batch, after |
| --- | ---: | ---: | ---: | ---: |
| 2,000 documents | 5,112 documents/s | 11,376 documents/s | 146 ms | 39 ms |
| 20,000 documents | 632 documents/s | 15,275 documents/s | 1,469 ms | 42 ms |

Before, the cost of each batch grew with the corpus. After, it stays at about
40 ms per batch at both corpus sizes. At 20,000 documents, query time is also
lower: sparse p50 drops from 9.58 ms to 5.03 ms, and fuzzy p50 from 68.14 ms
to 65.96 ms.

The whole engine (`scripts/benchmark.py`):

| Corpus | Run | Ingest | Uncached p50 | Cached p50 | Max loop lag |
| --- | --- | ---: | ---: | ---: | ---: |
| 2,000 | before | 25 documents/s | 103.41 ms | 21.01 ms | 326.1 ms |
| 2,000 | after | 25 documents/s | 103.06 ms | 21.72 ms | 308.6 ms |
| 20,000 | before | 19 documents/s | 153.66 ms | 46.59 ms | 323.1 ms |
| 20,000 | after | 19 documents/s | 155.19 ms | 45.67 ms | 308.4 ms |

End-to-end ingest throughput was unchanged by this. Metadata-store work
dominated it: about 11 s for the first batch of 500 and about 30 s per batch at
20,000 documents. The index rebuild it replaces took at most 1.5–2.6 s per
batch. Retrieval latency and loop lag are within run-to-run noise. Batched
metadata writes later removed that cost; see
[Ingest metadata cost](#ingest-metadata-cost).

## Ingest metadata cost

Each metadata statement costs about 1 ms through aiosqlite's thread hand-off.
Ingest used to write each document in its own write transaction with about 20
single-row statements, and the indexer listed objects and opened visibility
windows one document at a time. Now:

- Ingest validates every document first, then writes up to 200 documents per
  write transaction with multi-row statements. If a chunk fails (for example a
  version conflict), its documents are retried one per transaction, so one
  failing document still does not fail the others.
- The indexer reads objects and opens visibility windows for a whole batch in
  one statement each.
- Store queries that scanned a whole scope now use the matching index
  (coverage work rows, documents by uid, objects of many documents), and the
  metadata keyword dictionary reads only the metadata column.

Statements for one batch of 500 documents ingested on top of 1,000:

| | Statements | Time |
| --- | ---: | ---: |
| before | 15,244 | 15.2 s |
| after | 277 | 1.5 s |

The whole engine (`scripts/benchmark.py --queries 200`), same machine:

| Corpus | Run | Ingest | Uncached p50 | Cached p50 | Max loop lag |
| --- | --- | ---: | ---: | ---: | ---: |
| 2,000 | before | 36 documents/s | 64.44 ms | 11.74 ms | 108.3 ms |
| 2,000 | after | 680 documents/s | 56.77 ms | 8.90 ms | 80.2 ms |
| 20,000 | before | 26 documents/s | 153.00 ms | 44.43 ms | 225.2 ms |
| 20,000 | after | 445 documents/s | 122.06 ms | 41.10 ms | 233.2 ms |

Ingest is about 17–19 times faster. It still slowed somewhat as the corpus
grew, because two checks read the whole scope on every batch: the check for
documents without coverage and the metadata keyword dictionary. A batch of 500
took about 1.5 s at 1,000 documents and about 1.9 s at 15,000.

## Ingest at any corpus size

Those two checks no longer read the whole scope:

- **Coverage.** Each index unit keeps a watermark: the highest
  `change_seq` it has created coverage rows for. Each pass reads only the
  documents changed after it, through the `(scope_key, change_seq)` index. A
  unit without a watermark (a new unit, or a new engine instance with its own
  in-memory indexes) reads the scope once.
- **Keyword dictionary.** The metadata store keeps the dictionary's terms with
  the number of active documents using each. Ingest and delete adjust the
  counts in their own write transaction, and the dictionary's revision changes
  only when a term appears or disappears. The indexer compares that revision,
  one row, instead of digesting every entry. The terms are read only when a
  keyword or fuzzy build needs them.

The whole engine (`scripts/benchmark.py`), alternating the previous and the
current code on the same machine. The machine was running other workloads, so
absolute numbers are lower than in [Results](#results) and vary between runs:

| Corpus | Ingest, before | Ingest, after |
| --- | ---: | ---: |
| 2,000 | 273–399 documents/s | 261–335 documents/s |
| 20,000 | 264–282 documents/s | 328–340 documents/s |
| 100,000 | not run | 340 documents/s |

Before, throughput dropped as the corpus grew. After, it stays the same from
2,000 to 100,000 documents, and at 20,000 it is about 20% higher. At 2,000 the
difference is within the noise. At 20,000 documents the dictionary check takes
48 ms per unit and batch instead of 159 ms, including waits for the store.

The ingest figure in [Results](#results) was measured on a quieter machine
before this change and has not been re-measured there.

## Memory and restart time

The default keyword, fuzzy and sparse indexes live in memory in every engine
process. A process rebuilds them from the metadata store when it opens. The
table shows one restart on a SQLite store that already holds the corpus, run
with `LifecycleConfig(background=False)` so the engine opens only after the
rebuild. "Index memory" is the growth in resident memory while the engine
opens. "Retrieve p50" is the median of 100 sequential requests after opening;
none of them reported `index_missing`.

| Documents | Open | Index memory | Peak memory | Retrieve p50 | Database |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 2.2 s | 36 MB | 149 MB | 12 ms | < 1 MB |
| 2,000 | 4.1 s | 84 MB | 206 MB | 51 ms | 15 MB |
| 20,000 | 26.8 s | 443 MB | 643 MB | 59 ms | 152 MB |
| 100,000 | 157 s | 2,004 MB | 2,533 MB | 137 ms | 758 MB |

Beyond a fixed overhead of about 36 MB and 2 seconds, each document costs
about 20 KB of memory and 1.5 ms of restart time. Both grow linearly with the
corpus. The peak during the rebuild is about a quarter above the steady state.

Two changes made restart and retrieval scale this way:

- **Rebuild reads.** A full rebuild pages through the scope's objects in id
  order. Before migration `0006_objects_kind_order`, the index behind those
  pages did not cover the order, so each page sorted the whole scope and a
  rebuild took quadratic time: at 100,000 documents the rebuild ran past
  `unit_timeout` (600 seconds). The new index serves each page with a seek.
- **Common terms.** Keyword and fuzzy search now add up scores across a
  term's postings, evaluate filters once per object and build match details
  only for the returned hits. A fuzzy search at 100,000 documents, where
  matched terms appear in most documents, dropped from 538 ms to 88 ms.

### Sizing guidance

- The in-memory defaults suit corpora up to about 100,000 documents per
  process. Plan about 20 KB of memory per document in every process that
  opens the engine.
- With `background=False`, each unit's startup rebuild is bounded by
  `LifecycleConfig.unit_timeout` (600 seconds). A rebuild that runs over is
  logged as `lifecycle_round_errors`, and requests skip that index with
  `index_missing` warnings until the next lifecycle round succeeds. Raise
  `unit_timeout` for large corpora.
- To serve requests while a large rebuild runs, keep the background scheduler
  (the default). Set `wait_for_rebuild=True` only when the rebuild fits in
  `rebuild_timeout`.
- For larger corpora, or to avoid rebuilding at every restart, give the
  channels durable backends such as Qdrant, pgvector or Neo4j. Disk-backed
  indexes are reused on restart rather than rebuilt; see
  [Keep data on disk](guide/how-to/durable-storage.md).

### Statements per operation

Statements the SQLite store executed for one operation on a 2,000-document
scope, before and after the batched retirement and snapshot changes in this
release:

| Operation | Before | After |
| --- | ---: | ---: |
| Retrieve, per request | 16 | 8 |
| Update 200 documents | 371 | 179 |
| Delete 200 documents | 1,725 | 138 |
| Purge 200 documents | 1,869 | 81 |
| Ingest 2,000 documents | 1,220 | 1,310 |
| Rebuild | 440 | 449 |

Taking a retrieval snapshot fell from 11.6 ms to 2.3 ms at the median.

To reproduce the restart table:

```bash
uv run python scripts/scale.py --documents 100000
```

The script ingests the corpus into a SQLite file, then opens a fresh process
on it and reports the numbers above as JSON. `--skip-build --db PATH` reuses an
existing database.

## Environment

| | |
| --- | --- |
| CPU | 13th Gen Intel Core i9-13950HX (32 logical CPUs) |
| OS | Windows 11 |
| Python | 3.14.7 |
| Packages | retrievalloom 0.1.0, sqlalchemy 2.1.8, aiosqlite 0.22.1, rapidfuzz 3.14.6, bm25s 0.3.16, ahocorasick-rs 1.0.3, resourcera 0.2.0 |

Numbers depend on the machine. Compare runs on the same machine, not with
numbers from another one.

## Reproduce

```bash
uv sync --all-extras --all-groups
uv run python scripts/benchmark.py --documents 2000 --queries 200
```

The script is
[`scripts/benchmark.py`](https://github.com/smuniharish/retrievalloom/blob/main/scripts/benchmark.py).
`--batch` sets the ingest batch size (default 500), `--concurrency` the
requests in flight (default 16) and `--warmup` the untimed queries (default
20). `--max-cpu-jobs` sets `OffloaderConfig.max_cpu_jobs`, and `--gc-freeze`
freezes the heap before the concurrent scenario. The corpus and queries come
from a fixed seed, so runs are repeatable.
