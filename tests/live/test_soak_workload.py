"""Soak: a long mixed workload on one engine with bounded memory, tasks and loop lag.

Opt-in with ``RL_TEST_SOAK_SECONDS`` (for example ``120``). The SQLite variant
always runs then; the Postgres variant also needs ``RL_TEST_POSTGRES_DSN``.

Four concurrent workers ingest, update, delete, purge, retrieve and read the
status of their own document ids for the configured time, while the
background lifecycle indexes and applies retention. A monitor samples
``tracemalloc``, RSS, asyncio tasks, threads and event-loop lag. After the
first half (warm-up), the traced-memory slope must stay below
``RL_TEST_SOAK_MAX_SLOPE_KIB`` KiB/s (default 32; the in-memory Qdrant index keeps deleted points
until the next generation, so update churn grows it slowly), no tasks or threads may
accumulate, and the final metadata must match every acknowledged write.
"""

from __future__ import annotations

import asyncio
import os
import random
import time
from collections import Counter
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

import pytest

from retrievalloom import Components, LifecycleConfig, Scope
from retrievalloom.domain.records import DocumentState, IngestOutcome
from retrievalloom.storage.sqlite import SQLiteMetadataStore
from tests.fakes import HashEmbedding
from tests.live._reliability_helpers import (
    POSTGRES_DSN,
    WORDS,
    Monitor,
    converge,
    drop_schema,
    emit,
    new_schema,
    percentile,
    random_text,
    slope,
    text_doc,
    unsettled_coverage,
)
from tests.live.conftest import running_engine

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from pathlib import Path

    from retrievalloom import RetrievalEngine
    from retrievalloom.storage.base import DocumentMetadataStoreABC

SOAK_SECONDS = float(os.environ.get("RL_TEST_SOAK_SECONDS", "0"))
MAX_SLOPE_KIB = float(os.environ.get("RL_TEST_SOAK_MAX_SLOPE_KIB", "32"))
# Frames per allocation to report the sites that grew most (0 = off; slows the run).
TRACE_FRAMES = int(os.environ.get("RL_TEST_SOAK_TRACE", "0"))
WORKERS = 4
IDS_PER_WORKER = 30
SCOPE = Scope(tenant_id="soak")

pytestmark = pytest.mark.skipif(
    SOAK_SECONDS <= 0, reason="set RL_TEST_SOAK_SECONDS to run the soak test"
)


@dataclass
class Model:
    """Expected state per document id, from acknowledged writes only."""

    active: dict[str, int] = field(default_factory=dict)  # id -> version
    deleted: set[str] = field(default_factory=set)
    purged: set[str] = field(default_factory=set)
    ops: Counter[str] = field(default_factory=Counter)
    statuses: Counter[str] = field(default_factory=Counter)
    warnings: Counter[str] = field(default_factory=Counter)
    latencies: dict[str, list[float]] = field(default_factory=dict)

    def timed(self, op: str, seconds: float) -> None:
        self.ops[op] += 1
        self.latencies.setdefault(op, []).append(seconds)


async def worker(
    engine: RetrievalEngine, index: int, deadline: float, model: Model, rng: random.Random
) -> None:
    """Random operations on the ids this worker owns until ``deadline``."""
    own = [f"w{index}-doc-{n:02d}" for n in range(IDS_PER_WORKER)]
    while time.monotonic() < deadline:
        roll = rng.random()
        started = time.perf_counter()
        if roll < 0.35:
            ids = rng.sample(own, rng.randint(1, 5))
            report = await engine.ingest(
                [text_doc(item, random_text(rng, item)) for item in ids], scope=SCOPE
            )
            assert not report.warnings, report.warnings
            for item in report.documents:
                assert item.outcome in {
                    IngestOutcome.CREATED,
                    IngestOutcome.UPDATED,
                    IngestOutcome.REVIVED,
                    IngestOutcome.UNCHANGED,
                }, item
                assert item.version is not None
                model.active[item.document_id] = item.version
                model.deleted.discard(item.document_id)
                model.purged.discard(item.document_id)
            model.timed("ingest", time.perf_counter() - started)
        elif roll < 0.80:
            query = " ".join(rng.choices(WORDS, k=3))
            result = await engine.retrieve(query, scope=SCOPE, top_k=10)
            model.statuses[result.status.value] += 1
            model.warnings.update(warning.code for warning in result.warnings)
            for evidence in result.evidence:
                for source in evidence.sources:
                    assert source.scope == SCOPE
            model.timed("retrieve", time.perf_counter() - started)
        elif roll < 0.90:
            target = rng.choice(own)
            deleted = await engine.delete([target], scope=SCOPE)
            if target in deleted.deleted:
                model.active.pop(target, None)
                model.deleted.add(target)
            model.timed("delete", time.perf_counter() - started)
        elif roll < 0.95:
            target = rng.choice(own)
            purged = await engine.purge([target], scope=SCOPE)
            if target in purged.purged:
                model.active.pop(target, None)
                model.deleted.discard(target)
                model.purged.add(target)
            model.timed("purge", time.perf_counter() - started)
        else:
            await engine.status(scope=SCOPE)
            model.timed("status", time.perf_counter() - started)


async def verify_model(
    engine: RetrievalEngine, store: DocumentMetadataStoreABC, model: Model
) -> None:
    """The metadata matches every acknowledged write, and coverage converges."""
    async with store.transaction(SCOPE, write=False) as tx:
        for document_id, version in model.active.items():
            record = await tx.get_document(SCOPE, document_id)
            assert record is not None, document_id
            assert record.state is DocumentState.ACTIVE, document_id
            assert record.version == version, (document_id, record.version, version)
        for document_id in model.deleted:
            # Retention (purge_after=0) may have erased a deleted document already.
            record = await tx.get_document(SCOPE, document_id)
            assert record is None or record.state is DocumentState.DELETED, document_id
        for document_id in model.purged:
            assert await tx.get_document(SCOPE, document_id) is None, document_id
    status = await converge(engine, SCOPE)
    assert status.documents_active == len(model.active)
    assert not unsettled_coverage(status), status.coverage


@pytest.fixture(params=["sqlite", "postgres"])
async def soak_store(
    request: pytest.FixtureRequest, tmp_path: Path
) -> AsyncIterator[DocumentMetadataStoreABC]:
    """The metadata store variant under test (Postgres in a dropped-afterwards schema)."""
    if request.param == "sqlite":
        yield SQLiteMetadataStore(tmp_path / "metadata.db")
        return
    if POSTGRES_DSN is None:
        pytest.skip("set RL_TEST_POSTGRES_DSN to soak the Postgres store")
    from retrievalloom.storage.postgres import PostgresMetadataStore

    schema = new_schema("rl_soak")
    try:
        yield PostgresMetadataStore(POSTGRES_DSN, schema=schema)
    finally:
        await drop_schema(POSTGRES_DSN, schema)


async def test_soak_mixed_workload(
    soak_store: DocumentMetadataStoreABC, request: pytest.FixtureRequest
) -> None:
    lifecycle = LifecycleConfig(
        background=True,
        poll_interval=0.25,
        max_versions=3,
        purge_after=0.0,
        event_retention=30.0,
        shutdown_timeout=30,
    )
    model = Model()
    components = Components(metadata_store=soak_store, embedding=HashEmbedding())
    async with running_engine(components, lifecycle=lifecycle) as engine:
        # Warm every code path once before the monitor starts.
        seed = [
            text_doc(f"warm-{n}", random_text(random.Random(n), f"warm-{n}")) for n in range(20)
        ]
        await engine.ingest(seed, scope=SCOPE)
        await engine.retrieve("alpha beta", scope=SCOPE)
        await engine.purge([item.id for item in seed], scope=SCOPE)

        monitor = Monitor(
            trace_frames=max(1, TRACE_FRAMES),
            snapshot_at=0.4 * SOAK_SECONDS if TRACE_FRAMES else None,
        )
        async with monitor:
            deadline = time.monotonic() + SOAK_SECONDS
            await asyncio.gather(
                *(
                    worker(engine, index, deadline, model, random.Random(index))
                    for index in range(WORKERS)
                )
            )
            # Let the lifecycle settle, so in-flight rounds do not count as leaked tasks.
            await converge(engine, SCOPE)
            await asyncio.sleep(1.0)
            final = monitor.sample()
            for site in monitor.growth():
                emit(f"\nSOAK GROWTH {site}")
        await verify_model(engine, soak_store, model)
        health = await engine.health()
        assert health.status.value == "healthy", health

    samples = monitor.samples
    half = samples[len(samples) // 2 :]
    xs = [item.elapsed for item in half]
    traced_slope = slope(xs, [float(item.traced) for item in half]) / 1024
    rss_slope = slope(xs, [float(item.rss) for item in half]) / 1024
    baseline = half[0]
    total_ops = sum(model.ops.values())
    summary = {
        "variant": request.node.callspec.params["soak_store"],
        "seconds": round(final.elapsed, 1),
        "samples": len(samples),
        "ops": dict(model.ops),
        "ops_per_second": round(total_ops / final.elapsed, 1),
        "p50_ms": {
            op: round(percentile(values, 0.5) * 1000, 1) for op, values in model.latencies.items()
        },
        "p99_ms": {
            op: round(percentile(values, 0.99) * 1000, 1) for op, values in model.latencies.items()
        },
        "retrieve_status": dict(model.statuses),
        "retrieve_warnings": dict(model.warnings),
        "traced_mib": [round(samples[0].traced / 2**20, 1), round(final.traced / 2**20, 1)],
        "traced_slope_kib_s": round(traced_slope, 2),
        "rss_mib": [round(samples[0].rss / 2**20, 1), round(final.rss / 2**20, 1)],
        "rss_slope_kib_s": round(rss_slope, 2),
        "tasks": [baseline.tasks, max(item.tasks for item in half), final.tasks],
        "threads": [baseline.threads, max(item.threads for item in half), final.threads],
        "lag_ms": {
            "p50": round(percentile(monitor.lags, 0.5) * 1000, 1),
            "p99": round(percentile(monitor.lags, 0.99) * 1000, 1),
            "max": round(max(monitor.lags, default=0.0) * 1000, 1),
        },
    }
    emit(f"\nSOAK {summary}")

    assert len(half) >= 5, "run longer: too few samples after warm-up"
    assert model.ops["ingest"]
    assert model.ops["retrieve"]
    assert model.ops["delete"]
    assert traced_slope < MAX_SLOPE_KIB, f"traced memory grows {traced_slope:.1f} KiB/s"
    # Tasks and threads are bounded: no growth beyond the in-flight work of the workers.
    assert final.tasks <= baseline.tasks + 2, summary["tasks"]
    assert final.threads <= baseline.threads + 2, summary["threads"]
    assert max(monitor.lags) < 1.0, f"event loop stalled for {max(monitor.lags):.3f}s"
