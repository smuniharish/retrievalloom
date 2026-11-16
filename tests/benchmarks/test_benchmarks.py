"""Performance regression tests: event-loop responsiveness and a small benchmark run.

Bounds are deliberately generous (shared CI runners, coarse Windows timers); they catch
regressions such as CPU work moving back onto the event loop, not small slowdowns.
"""

from __future__ import annotations

import asyncio
import importlib.util
import random
import sys
import time
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from retrievalloom.index.fuzzy import RapidFuzzIndex

if TYPE_CHECKING:
    from collections.abc import Awaitable
    from types import ModuleType

pytestmark = pytest.mark.benchmark

ROOT = Path(__file__).resolve().parents[2]


def load_benchmark() -> ModuleType:
    spec = importlib.util.spec_from_file_location("rl_benchmark", ROOT / "scripts" / "benchmark.py")
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


async def max_loop_lag_ms(work: Awaitable[object]) -> float:
    """Run ``work`` while a 1 ms ticker records how late the event loop wakes it."""
    lag = 0.0
    done = False

    async def ticker() -> None:
        nonlocal lag
        while not done:
            expected = time.perf_counter() + 0.001
            await asyncio.sleep(0.001)
            lag = max(lag, time.perf_counter() - expected)

    tick = asyncio.create_task(ticker())
    await asyncio.sleep(0.01)
    try:
        await work
    finally:
        done = True
        await tick
    return lag * 1000


async def test_fuzzy_matching_releases_the_gil() -> None:
    rng = random.Random(7)
    letters = "abcdefghijklmnopqrstuvwxyz"
    vocabulary: list[str] = sorted(
        {"".join(rng.choices(letters, k=rng.randint(4, 10))) for _ in range(200_000)}
    )
    grams = ["kubernetes", "asyncio", "retrieval", "kubernetes asyncio"]
    index = RapidFuzzIndex()
    started = time.perf_counter()
    lag = await max_loop_lag_ms(asyncio.to_thread(index._match_sync, vocabulary, grams))
    elapsed_ms = (time.perf_counter() - started) * 1000
    # Scoring one gram against this vocabulary takes ~200 ms of C code; holding the GIL
    # for it (as process.extract does) would stall the loop for that long.
    assert elapsed_ms > 200
    assert lag < 100, f"event loop stalled {lag:.0f} ms during fuzzy matching"


async def test_small_benchmark_run() -> None:
    benchmark = load_benchmark()
    results = await benchmark.run(documents=120, queries=30, batch=60, concurrency=4, warmup=5)
    assert results.empty_results == 0
    assert results.cached.p50 < results.uncached.p50
    assert results.uncached.p95 < 2000
    assert results.max_loop_lag_ms < 500
    report = benchmark.report(results)
    assert "| retrieve, uncached |" in report
    assert "| `snapshot` |" in report
    assert "retrievalloom" in report
