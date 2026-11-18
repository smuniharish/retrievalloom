# Benchmarks

These measurements describe one recorded environment, not a service-level
objective or capacity guarantee. Compare runs on the same machine and
workload, and measure your own corpus before choosing deployment resources.

## Retrieval and ingestion

The recorded retrieval run used the default in-memory components, 2,000
synthetic documents and 200 queries after warm-up. No model or network time
was included.

| Operation | p50 | p95 | p99 |
| --- | ---: | ---: | ---: |
| Uncached retrieval | 56.77 ms | 80.00 ms | 102.09 ms |
| Cached retrieval | 8.90 ms | 13.42 ms | 19.53 ms |

| Ingest corpus | Throughput |
| ---: | ---: |
| 2,000 documents | 680 documents/s |
| 20,000 documents | 445 documents/s |
| 100,000 documents | 340 documents/s |

The ingest measurements were recorded in separate runs and should not be
treated as directly comparable across corpus sizes.

## Memory and restart time

The built-in keyword, fuzzy and sparse indexes are in memory in each process.
The following measurements record one restart with a SQLite metadata store.
The engine waited for the in-memory indexes to rebuild before opening.

| Documents | Open time | Index memory | Peak memory | Retrieval p50 |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 2.2 s | 36 MB | 149 MB | 12 ms |
| 2,000 | 4.1 s | 84 MB | 206 MB | 51 ms |
| 20,000 | 26.8 s | 443 MB | 643 MB | 59 ms |
| 100,000 | 157 s | 2,004 MB | 2,533 MB | 137 ms |

In this run, the measured index memory and open time grew by about 20 KB and
1.5 ms per document beyond the fixed overhead. Every process that opens the
engine has its own in-memory indexes. At 100,000 documents, plan for
approximately 2 GB of steady index memory and a higher peak while rebuilding.
Validate those estimates against your data and host.

Persistent vector, graph and structure backends can be reused across
restarts. The in-memory indexes still rebuild; see
[Keep data on disk](guide/how-to/durable-storage.md).

## Measurement context

The published measurements were recorded on this Windows 11 system:

| | |
| --- | --- |
| CPU | 13th Gen Intel Core i9-13950HX, 32 logical CPUs |
| Python | 3.14.7 |
| Recorded package build | RetrievalLoom 0.1.0 |
| Recorded dependencies | SQLAlchemy 2.1.8, aiosqlite 0.22.1, RapidFuzz 3.14.6, bm25s 0.3.16, ahocorasick-rs 1.0.3, resourcera 0.2.0 |

These are historical measurements from the recorded package build, not a
benchmark of every current release or deployment configuration.

## Reproduce

Run the retrieval and ingestion benchmark:

```bash
uv run python scripts/benchmark.py --documents 2000 --queries 200
```

Measure startup, memory and retrieval after a fresh process opens an existing
store:

```bash
uv run python scripts/scale.py --documents 100000
```

Both scripts report local measurements. Results vary with document length,
filters, active channels, backend choice, concurrency, model configuration
and available resources.
