"""Measure memory use and restart time of the default in-memory indexes at a corpus size.

The keyword, fuzzy and sparse indexes live in memory in every engine process and
are rebuilt from the metadata store when an engine opens. This script measures
what that costs:

1. **build**: ingest ``--documents`` synthetic documents (the corpus of
   ``scripts/benchmark.py``) into a SQLite file with the default components, and
   report ingest throughput.
2. **restart**: in a fresh interpreter, open an engine on that file with
   ``LifecycleConfig(background=False)``, so the index rebuild finishes before the
   engine opens. Report the open time, the process memory before and after (the
   difference is what the indexes and their caches hold), the peak memory, and the
   uncached retrieval latency once open.

Run with:

    uv run python scripts/scale.py --documents 100000

The SQLite file is kept in ``--db`` (default: a temporary directory removed at the
end), so ``--skip-build`` can measure more restarts of the same corpus.
"""

from __future__ import annotations

import argparse
import asyncio
import ctypes
import gc
import json
import logging
import statistics
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import structlog
from benchmark import SCOPE, corpus

from retrievalloom import (
    Components,
    LifecycleConfig,
    Severity,
    create_retrieval_engine,
)
from retrievalloom.storage import SQLiteMetadataStore

QUERIES = 100
_MB = 1024 * 1024


def memory_mb() -> tuple[float, float]:
    """Current and peak resident memory of this process, in MiB."""
    if sys.platform == "win32":

        class Counters(ctypes.Structure):
            _fields_ = [
                ("cb", ctypes.c_ulong),
                ("PageFaultCount", ctypes.c_ulong),
                ("PeakWorkingSetSize", ctypes.c_size_t),
                ("WorkingSetSize", ctypes.c_size_t),
                ("QuotaPeakPagedPoolUsage", ctypes.c_size_t),
                ("QuotaPagedPoolUsage", ctypes.c_size_t),
                ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
                ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
                ("PagefileUsage", ctypes.c_size_t),
                ("PeakPagefileUsage", ctypes.c_size_t),
            ]

        counters = Counters()
        counters.cb = ctypes.sizeof(Counters)
        kernel32, psapi = ctypes.windll.kernel32, ctypes.windll.psapi
        kernel32.GetCurrentProcess.restype = ctypes.c_void_p
        psapi.GetProcessMemoryInfo.argtypes = [
            ctypes.c_void_p,
            ctypes.POINTER(Counters),
            ctypes.c_ulong,
        ]
        if not psapi.GetProcessMemoryInfo(
            kernel32.GetCurrentProcess(), ctypes.byref(counters), counters.cb
        ):
            raise ctypes.WinError()
        return counters.WorkingSetSize / _MB, counters.PeakWorkingSetSize / _MB
    else:
        import resource

        peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        peak_mb = peak / _MB if sys.platform == "darwin" else peak / 1024
        with Path("/proc/self/statm").open() as statm:
            pages = int(statm.read().split()[1])
        return pages * resource.getpagesize() / _MB, peak_mb


async def build(db: Path, documents: int, batch: int) -> dict[str, float]:
    """Ingest the synthetic corpus into ``db``."""
    inputs, _ = corpus(documents, 0)
    engine = create_retrieval_engine(
        lifecycle=LifecycleConfig(background=False),
        components=Components(metadata_store=SQLiteMetadataStore(db)),
    )
    started = time.perf_counter()
    async with engine:
        for offset in range(0, len(inputs), batch):
            await engine.ingest(inputs[offset : offset + batch], scope=SCOPE)
    seconds = time.perf_counter() - started
    return {"ingest_seconds": seconds, "ingest_docs_per_second": documents / seconds}


async def restart(db: Path, documents: int) -> dict[str, float]:
    """Open an engine on ``db``, wait for its rebuild, then time retrievals."""
    queries = corpus(documents, QUERIES)[1]
    gc.collect()
    before, _ = memory_mb()
    engine = create_retrieval_engine(
        lifecycle=LifecycleConfig(background=False),
        components=Components(metadata_store=SQLiteMetadataStore(db)),
    )
    started = time.perf_counter()
    async with engine:
        opened = time.perf_counter() - started
        gc.collect()
        after, peak = memory_mb()
        samples: list[float] = []
        degraded = 0
        for query in queries:
            began = time.perf_counter()
            result = await engine.retrieve(query, scope=SCOPE)
            samples.append(time.perf_counter() - began)
            # index_missing and similar warnings mark results served from incomplete indexes.
            degraded += any(warning.severity is not Severity.INFO for warning in result.warnings)
    return {
        "open_seconds": opened,
        "memory_before_mb": before,
        "memory_after_mb": after,
        "index_memory_mb": after - before,
        "peak_memory_mb": peak,
        "retrieve_p50_ms": statistics.median(samples) * 1000,
        "degraded_results": degraded,
    }


def main() -> None:
    """Parse arguments, build the corpus, and measure a restart in a fresh process."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--documents", type=int, default=100_000)
    parser.add_argument("--batch", type=int, default=1000)
    parser.add_argument("--db", type=Path, default=None)
    parser.add_argument("--skip-build", action="store_true")
    parser.add_argument("--restart-only", action="store_true", help=argparse.SUPPRESS)
    args = parser.parse_args()
    structlog.configure(wrapper_class=structlog.make_filtering_bound_logger(logging.ERROR))

    if args.restart_only:
        print(json.dumps(asyncio.run(restart(args.db, args.documents))))
        return

    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as scratch:
        db = args.db or Path(scratch) / "scale.db"
        results: dict[str, float] = {"documents": args.documents}
        if not args.skip_build:
            results |= asyncio.run(build(db, args.documents, args.batch))
        measured = subprocess.run(  # noqa: S603 - this interpreter running this script
            [
                sys.executable,
                __file__,
                "--restart-only",
                "--db",
                str(db),
                "--documents",
                str(args.documents),
            ],
            check=True,
            capture_output=True,
            text=True,
        )
        results |= json.loads(measured.stdout.strip().splitlines()[-1])
        results["database_mb"] = db.stat().st_size / _MB
    print(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
