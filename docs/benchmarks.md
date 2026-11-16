# Benchmarks

RetrievalLoom runs inside your application's process, so its cost is the
latency it adds to each retrieval, the throughput of ingest, and how long it
holds your event loop. This page shows all three, measured on one machine, and
how to measure them on yours.

## Results

An in-memory engine with the default components: an in-memory SQLite metadata
store and the keyword, fuzzy and sparse (BM25) index methods. 2,000 synthetic
documents of 80 words each are ingested in batches of 500, then 500 four-word
queries run after 20 untimed warmup queries. No network, model or disk time is
included.

| Scenario | p50 ms | p95 ms | p99 ms | mean ms |
| --- | ---: | ---: | ---: | ---: |
| retrieve, uncached | 63.55 | 81.41 | 116.18 | 65.44 |
| retrieve, cached | 11.23 | 17.87 | 22.19 | 12.08 |

| Throughput | Result |
| --- | ---: |
| ingest, indexed (`index_wait="all"`) | 35 documents/s |
| retrieve, 16 concurrent requests | 20 retrievals/s |
| max event-loop lag while 16 requests run | 258.5 ms |

Every query returned evidence.

### Where an uncached retrieval spends its time

Medians of `RetrievalResult.timings` over the 500 uncached queries. Methods run
concurrently, so `methods` is close to the slowest method, not their sum.

| Stage | p50 ms |
| --- | ---: |
| `snapshot` | 10.44 |
| `method:index.keyword` | 0.03 |
| `method:index.sparse` | 2.70 |
| `method:index.fuzzy` | 33.04 |
| `methods` | 33.20 |
| `method_fusion` | 0.66 |
| `candidates` | 14.48 |
| `fusion` | 1.46 |
| `rerank` | 0.22 |
| `nli` | 0.00 |
| `consolidation` | 1.44 |
| `total` | 63.32 |

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
  after the index changes. That is most of the 11 ms.
- **Ingest rebuilds the in-memory keyword, fuzzy and sparse indexes for every
  batch.** These backends cannot be updated in place, so each ingest batch
  publishes a new generation. Larger batches amortise this cost; see
  [Generations and coverage](guide/concepts/lifecycle.md).
- **Concurrency adds little throughput, and the loop lag comes from the GIL.**
  The method stages are CPU-bound and share one interpreter. The 258 ms is the
  longest time a 5 ms ticker on the loop waited to wake up. During those waits
  the loop thread is in its I/O poll waiting for the GIL, which is held by
  worker threads, not running blocking code. See
  [Keep the event loop responsive](guide/how-to/event-loop-latency.md).

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
uv run python scripts/benchmark.py --documents 2000 --queries 500
```

The script is
[`scripts/benchmark.py`](https://github.com/smuniharish/retrievalloom/blob/main/scripts/benchmark.py).
`--batch` sets the ingest batch size (default 500), `--concurrency` the
requests in flight (default 16) and `--warmup` the untimed queries (default
20). The corpus and queries come from a fixed seed, so runs are repeatable.
