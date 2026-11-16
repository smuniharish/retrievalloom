"""Measure RetrievalLoom's in-process cost: ingest throughput and retrieval latency.

Scenarios run against an in-memory engine with the default components (in-memory
SQLite metadata store; keyword, fuzzy and sparse BM25 index methods), so no network,
model or disk time is included:

- **ingest**: ``--documents`` synthetic documents ingested in batches of ``--batch``.
  Ingest waits for indexing (``index_wait=all``), so the time covers validation,
  metadata commits and building every index generation.
- **retrieve, uncached**: ``--queries`` sequential retrievals with the default
  ``NoCache``; every request runs the full pipeline.
- **retrieve, cached**: the same queries again with an ``InMemoryLRUCache`` that is
  already warm, i.e. the cost of a cache hit.
- **retrieve, concurrent**: ``--queries`` retrievals with ``--concurrency`` in flight.
  A ticker measures how late the event loop wakes it up while they run (loop lag).

``--max-cpu-jobs`` sets ``OffloaderConfig.max_cpu_jobs``. ``--gc-freeze`` calls
``gc.collect()`` and ``gc.freeze()`` right before the concurrent scenario.

The corpus and queries are generated from a fixed seed, so runs are repeatable.
Numbers are machine-dependent; compare them on the same machine.

Run with:

    uv run python scripts/benchmark.py --documents 2000 --queries 500
"""

from __future__ import annotations

import argparse
import asyncio
import gc
import logging
import platform
import random
import statistics
import sys
import time
from dataclasses import dataclass
from importlib.metadata import version
from typing import Any

import structlog

from retrievalloom import (
    Components,
    DocumentInput,
    LifecycleConfig,
    RetrievalEngine,
    Scope,
    SourceDescriptor,
    TextRepresentation,
    create_retrieval_engine,
)
from retrievalloom.infra import InMemoryLRUCache
from retrievalloom.runtime.offloader import OffloaderConfig

SEED = 20240611
VOCABULARY_SIZE = 3000
WORDS_PER_DOCUMENT = 80
WORDS_PER_QUERY = 4
SCOPE = Scope(tenant_id="bench")


@dataclass(frozen=True, slots=True)
class Latency:
    """Latency percentiles of one scenario, in milliseconds."""

    p50: float
    p95: float
    p99: float
    mean: float

    @classmethod
    def of(cls, samples: list[float]) -> Latency:
        """Summarize ``samples`` (seconds)."""
        cuts = statistics.quantiles(samples, n=100, method="inclusive")
        return cls(
            p50=cuts[49] * 1000,
            p95=cuts[94] * 1000,
            p99=cuts[98] * 1000,
            mean=statistics.fmean(samples) * 1000,
        )


@dataclass(frozen=True, slots=True)
class Results:
    """Everything one benchmark run measured."""

    documents: int
    queries: int
    concurrency: int
    ingest_seconds: float
    uncached: Latency
    cached: Latency
    concurrent_seconds: float
    max_loop_lag_ms: float
    empty_results: int
    stage_p50_ms: dict[str, float]

    @property
    def ingest_docs_per_second(self) -> float:
        """Documents ingested (and indexed) per second."""
        return self.documents / self.ingest_seconds

    @property
    def concurrent_qps(self) -> float:
        """Retrievals completed per second with ``concurrency`` in flight."""
        return self.queries / self.concurrent_seconds


class _SwitchableCache(InMemoryLRUCache):
    """An LRU cache that can be turned off, so one indexed engine serves every scenario."""

    enabled = True

    async def get(self, namespace: str, key: str) -> Any | None:
        return await super().get(namespace, key) if self.enabled else None

    async def set(self, namespace: str, key: str, value: Any) -> None:
        if self.enabled:
            await super().set(namespace, key, value)


def _vocabulary(rng: random.Random) -> list[str]:
    letters = "abcdefghijklmnopqrstuvwxyz"
    words: set[str] = set()
    while len(words) < VOCABULARY_SIZE:
        words.add("".join(rng.choices(letters, k=rng.randint(4, 10))))
    return sorted(words)


def corpus(documents: int, queries: int) -> tuple[list[DocumentInput], list[str]]:
    """A deterministic synthetic corpus and query set (Zipf-like word frequencies)."""
    rng = random.Random(SEED)  # noqa: S311 - reproducible test data, not cryptography
    vocabulary = _vocabulary(rng)
    weights = [1 / (rank + 1) for rank in range(len(vocabulary))]
    inputs = [
        DocumentInput(
            _id=f"doc-{index:06d}",
            metadata={"group": index % 10},
            source=SourceDescriptor(uri=f"bench://doc/{index}"),
            representations=(
                TextRepresentation(
                    text=" ".join(rng.choices(vocabulary, weights, k=WORDS_PER_DOCUMENT))
                ),
            ),
        )
        for index in range(documents)
    ]
    texts = [" ".join(rng.choices(vocabulary, weights, k=WORDS_PER_QUERY)) for _ in range(queries)]
    return inputs, texts


async def _ingest(engine: RetrievalEngine, documents: list[DocumentInput], batch: int) -> float:
    started = time.perf_counter()
    for offset in range(0, len(documents), batch):
        await engine.ingest(documents[offset : offset + batch], scope=SCOPE)
    return time.perf_counter() - started


async def _sequential(
    engine: RetrievalEngine, queries: list[str]
) -> tuple[list[float], int, dict[str, list[float]]]:
    samples: list[float] = []
    stages: dict[str, list[float]] = {}
    empty = 0
    for query in queries:
        started = time.perf_counter()
        result = await engine.retrieve(query, scope=SCOPE)
        samples.append(time.perf_counter() - started)
        empty += not result.evidence
        for stage, milliseconds in result.timings.items():
            stages.setdefault(stage, []).append(milliseconds)
    return samples, empty, stages


async def _concurrent(
    engine: RetrievalEngine, queries: list[str], concurrency: int
) -> tuple[float, float]:
    """Run ``queries`` with ``concurrency`` in flight; return (seconds, max loop lag ms)."""
    stop = asyncio.Event()
    lag = 0.0

    async def ticker() -> None:
        nonlocal lag
        interval = 0.005
        while not stop.is_set():
            expected = time.perf_counter() + interval
            await asyncio.sleep(interval)
            lag = max(lag, time.perf_counter() - expected)

    pending = iter(queries)

    async def worker() -> None:
        for query in pending:
            await engine.retrieve(query, scope=SCOPE)

    tick = asyncio.create_task(ticker())
    started = time.perf_counter()
    await asyncio.gather(*(worker() for _ in range(concurrency)))
    elapsed = time.perf_counter() - started
    stop.set()
    await tick
    return elapsed, lag * 1000


async def run(
    *,
    documents: int,
    queries: int,
    batch: int,
    concurrency: int,
    warmup: int = 20,
    max_cpu_jobs: int | None = None,
    gc_freeze: bool = False,
) -> Results:
    """Run every scenario and return the measurements."""
    inputs, texts = corpus(documents, queries)
    warm = texts[:warmup]
    cache = _SwitchableCache(max_entries=queries * 2)
    engine = create_retrieval_engine(
        lifecycle=LifecycleConfig(background=False),
        components=Components(cache=cache),
        offloader=OffloaderConfig(max_cpu_jobs=max_cpu_jobs),
    )
    async with engine:
        ingest_seconds = await _ingest(engine, inputs, batch)
        cache.enabled = False
        await _sequential(engine, warm)
        samples, empty, stages = await _sequential(engine, texts)
        uncached = Latency.of(samples)
        cache.enabled = True
        await _sequential(engine, texts)
        cached = Latency.of((await _sequential(engine, texts))[0])
        cache.enabled = False
        if gc_freeze:
            gc.collect()
            gc.freeze()
        concurrent_seconds, lag = await _concurrent(engine, texts, concurrency)
    return Results(
        documents=documents,
        queries=queries,
        concurrency=concurrency,
        ingest_seconds=ingest_seconds,
        uncached=uncached,
        cached=cached,
        concurrent_seconds=concurrent_seconds,
        max_loop_lag_ms=lag,
        empty_results=empty,
        stage_p50_ms={stage: statistics.median(values) for stage, values in stages.items()},
    )


def report(results: Results) -> str:
    """Render ``results`` as Markdown tables."""
    rows = [
        "| Scenario | p50 ms | p95 ms | p99 ms | mean ms |",
        "| --- | ---: | ---: | ---: | ---: |",
    ]
    for name, latency in (
        ("retrieve, uncached", results.uncached),
        ("retrieve, cached", results.cached),
    ):
        rows.append(
            f"| {name} | {latency.p50:.2f} | {latency.p95:.2f} | {latency.p99:.2f} "
            f"| {latency.mean:.2f} |"
        )
    stage_rows = [
        "| Stage (uncached) | p50 ms |",
        "| --- | ---: |",
        *(f"| `{stage}` | {ms:.2f} |" for stage, ms in results.stage_p50_ms.items()),
    ]
    return "\n".join(
        [
            f"documents={results.documents} queries={results.queries} "
            f"concurrency={results.concurrency}",
            "",
            f"ingest: {results.ingest_seconds:.2f} s "
            f"({results.ingest_docs_per_second:.0f} documents/s, indexed)",
            "",
            *rows,
            "",
            *stage_rows,
            "",
            f"concurrent: {results.concurrent_qps:.0f} retrievals/s, "
            f"max event-loop lag {results.max_loop_lag_ms:.1f} ms",
            f"queries without evidence: {results.empty_results}",
            "",
            f"Python {platform.python_version()} on {platform.platform()} "
            f"({platform.processor() or platform.machine()}); "
            f"retrievalloom {version('retrievalloom')}",
        ]
    )


def main(argv: list[str] | None = None) -> None:
    """Command-line entry point."""
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    parser.add_argument("--documents", type=int, default=2000)
    parser.add_argument("--queries", type=int, default=500)
    parser.add_argument("--batch", type=int, default=500)
    parser.add_argument("--concurrency", type=int, default=16)
    parser.add_argument("--warmup", type=int, default=20)
    parser.add_argument("--max-cpu-jobs", type=int, default=None)
    parser.add_argument("--gc-freeze", action="store_true")
    args = parser.parse_args(argv)
    structlog.configure(
        wrapper_class=structlog.make_filtering_bound_logger(logging.WARNING),
        logger_factory=structlog.PrintLoggerFactory(sys.stderr),
    )
    gc.collect()
    results = asyncio.run(
        run(
            documents=args.documents,
            queries=args.queries,
            batch=args.batch,
            concurrency=args.concurrency,
            warmup=args.warmup,
            max_cpu_jobs=args.max_cpu_jobs,
            gc_freeze=args.gc_freeze,
        )
    )
    print(report(results))


if __name__ == "__main__":
    main()
