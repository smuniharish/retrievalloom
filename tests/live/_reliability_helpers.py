"""Shared helpers of the reliability suites (soak, fault injection, multi-process)."""

from __future__ import annotations

import asyncio
import gc
import importlib
import os
import shutil
import statistics
import subprocess
import threading
import time
import tracemalloc
import uuid
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any
from urllib.parse import urlsplit

import sqlalchemy as sa

from retrievalloom import DocumentInput, TextRepresentation

if TYPE_CHECKING:
    import random
    from types import TracebackType

    from retrievalloom import RetrievalEngine, Scope
    from retrievalloom.domain.reports import ScopeStatus

try:
    psutil: Any = importlib.import_module("psutil")
except ImportError:  # pragma: no cover - psutil is optional; tracemalloc is the gate
    psutil = None

POSTGRES_DSN = os.environ.get("RL_TEST_POSTGRES_DSN")

# A bounded vocabulary keeps index vocabularies (fuzzy, sparse) from growing with time,
# so steady-state memory is flat when nothing leaks.
WORDS = [
    "alpha",
    "beta",
    "gamma",
    "delta",
    "epsilon",
    "zeta",
    "theta",
    "kappa",
    "lambda",
    "sigma",
    "omega",
    "river",
    "forest",
    "mountain",
    "valley",
    "ocean",
    "desert",
    "island",
    "harbour",
    "bridge",
    "tower",
    "castle",
    "garden",
    "market",
    "engine",
    "signal",
    "packet",
    "socket",
    "thread",
    "buffer",
    "kernel",
    "driver",
    "cache",
    "index",
    "ledger",
    "invoice",
    "ticket",
    "refund",
    "policy",
    "outage",
    "latency",
    "storage",
    "backup",
    "replica",
    "shard",
]

# Coverage states a converged unit may report for documents of a scope.
SETTLED_COVERAGE = frozenset({"ready", "not_applicable", "deleted"})


def text_doc(document_id: str, text: str, **metadata: str) -> DocumentInput:
    """Single-text document."""
    return DocumentInput(
        _id=document_id,
        representations=(TextRepresentation(text=text),),
        metadata=metadata,
    )


def random_text(rng: random.Random, document_id: str, words: int = 12) -> str:
    """Text from the bounded vocabulary, prefixed with the document id."""
    return f"{document_id} " + " ".join(rng.choices(WORDS, k=words))


def new_schema(prefix: str = "rl_rel") -> str:
    """Unique Postgres schema name."""
    return f"{prefix}_{uuid.uuid4().hex[:20]}"


async def drop_schema(dsn: str, schema: str) -> None:
    """Drop ``schema`` and everything in it."""
    from retrievalloom.storage.postgres import PostgresMetadataStore

    admin = PostgresMetadataStore(dsn, schema=schema)
    await admin.initialize()
    try:
        async with admin.engine.begin() as connection:
            await connection.execute(sa.text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
    finally:
        await admin.close()


def unsettled_coverage(status: ScopeStatus) -> dict[str, dict[str, int]]:
    """Coverage entries per unit that are not settled (pending, failed, stale, ...)."""
    return {
        unit: {state: count for state, count in states.items() if state not in SETTLED_COVERAGE}
        for unit, states in status.coverage.items()
        if any(state not in SETTLED_COVERAGE and count for state, count in states.items())
    }


async def converge(engine: RetrievalEngine, scope: Scope, *, rounds: int = 20) -> ScopeStatus:
    """Run lifecycle rounds until ``scope``'s coverage is settled; returns the last status."""
    status = await engine.status(scope=scope)
    for _ in range(rounds):
        if not unsettled_coverage(status):
            return status
        await engine.run_lifecycle_once(scope)
        status = await engine.status(scope=scope)
    return status


def emit(text: str) -> None:
    """Print a result line of a suite (shown with `pytest -s`)."""
    print(text, flush=True)  # noqa: T201 - the suites report measurements


def slope(xs: list[float], ys: list[float]) -> float:
    """Least-squares slope of ``ys`` over ``xs``."""
    mean_x, mean_y = statistics.fmean(xs), statistics.fmean(ys)
    denominator = sum((x - mean_x) ** 2 for x in xs)
    if denominator == 0:
        return 0.0
    return sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, ys, strict=True)) / denominator


def percentile(values: list[float], fraction: float) -> float:
    """Nearest-rank percentile (0 for no values)."""
    if not values:
        return 0.0
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int(fraction * len(ordered)))]


# resource monitor -------------------------------------------------------------------------
@dataclass(frozen=True)
class Sample:
    """One resource sample."""

    elapsed: float
    traced: int
    rss: int
    tasks: int
    threads: int
    worst_lag: float


@dataclass
class Monitor:
    """Samples tracemalloc, RSS, asyncio tasks, threads and event-loop lag while it runs.

    The probe sleeps ``tick`` seconds in a loop; the overshoot of every sleep is a
    lag measurement. Every ``every`` seconds a :class:`Sample` is recorded.
    """

    every: float = 2.0
    tick: float = 0.05
    trace_frames: int = 1
    snapshot_at: float | None = None
    samples: list[Sample] = field(default_factory=list)
    lags: list[float] = field(default_factory=list)
    snapshot: tracemalloc.Snapshot | None = None
    _task: asyncio.Task[None] | None = None
    _started: float = 0.0

    def sample(self, worst_lag: float = 0.0) -> Sample:
        """Record a sample now."""
        traced, _ = tracemalloc.get_traced_memory()
        rss = psutil.Process().memory_info().rss if psutil is not None else 0
        sample = Sample(
            elapsed=time.perf_counter() - self._started,
            traced=traced,
            rss=rss,
            tasks=len(asyncio.all_tasks()),
            threads=threading.active_count(),
            worst_lag=worst_lag,
        )
        self.samples.append(sample)
        return sample

    async def _run(self) -> None:
        loop = asyncio.get_running_loop()
        next_sample = loop.time() + self.every
        worst = 0.0
        while True:
            start = loop.time()
            await asyncio.sleep(self.tick)
            lag = max(0.0, loop.time() - start - self.tick)
            self.lags.append(lag)
            worst = max(worst, lag)
            if loop.time() >= next_sample:
                self.sample(worst)
                worst = 0.0
                next_sample += self.every
            if (
                self.snapshot is None
                and self.snapshot_at is not None
                and time.perf_counter() - self._started >= self.snapshot_at
            ):
                self.snapshot = tracemalloc.take_snapshot()

    def growth(self, limit: int = 12) -> list[str]:
        """Allocation sites that grew most since ``snapshot`` (diagnostics)."""
        if self.snapshot is None:
            return []
        ignored = [tracemalloc.Filter(inclusive=False, filename_pattern=tracemalloc.__file__)]
        now = tracemalloc.take_snapshot().filter_traces(ignored)
        stats = now.compare_to(self.snapshot.filter_traces(ignored), "traceback")
        stats.sort(key=lambda stat: stat.size_diff, reverse=True)
        return [
            f"{stat.size_diff / 1024:+.1f} KiB {stat.count_diff:+d} blocks\n"
            + "\n".join(stat.traceback.format())
            for stat in stats[:limit]
        ]

    async def __aenter__(self) -> Monitor:
        gc.collect()
        # Long-lived heaps of earlier tests would make full collections pause the loop.
        gc.freeze()
        tracemalloc.start(self.trace_frames)
        self._started = time.perf_counter()
        self.sample()
        self._task = asyncio.create_task(self._run())
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        assert self._task is not None
        self._task.cancel()
        await asyncio.gather(self._task, return_exceptions=True)
        tracemalloc.stop()
        gc.unfreeze()


# podman -----------------------------------------------------------------------------------
def podman_available() -> bool:
    """Whether the ``podman`` CLI is on ``PATH``."""
    return shutil.which("podman") is not None


def podman_sync(*args: str, check: bool = True, timeout: float = 120) -> str:
    """Run ``podman args`` and return its stdout."""
    completed = subprocess.run(  # noqa: S603 - fixed podman argv, no shell
        ["podman", *args],  # noqa: S607 - podman is resolved from PATH on purpose
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=timeout,
        check=False,
    )
    if check and completed.returncode != 0:
        raise RuntimeError(f"podman {' '.join(args)} failed: {completed.stderr.strip()}")
    return completed.stdout.strip()


async def podman(*args: str, check: bool = True, seconds: float = 120) -> str:
    """``podman_sync`` on a worker thread, so the event loop keeps serving operations."""
    return await asyncio.to_thread(podman_sync, *args, check=check, timeout=seconds)


def image_of(container: str, default: str) -> str:
    """Image of an existing container (to reuse a pulled image), else ``default``."""
    try:
        name = podman_sync("inspect", container, "--format", "{{.ImageName}}", check=False)
    except (OSError, subprocess.SubprocessError):
        return default
    return name or default


def service_host() -> str:
    """Host on which podman publishes container ports.

    ``RL_TEST_FAULTS_HOST``, else the host of ``RL_TEST_POSTGRES_DSN`` or
    ``RL_TEST_QDRANT_URL`` (the podman machine address on Windows and macOS).
    """
    explicit = os.environ.get("RL_TEST_FAULTS_HOST")
    if explicit:
        return explicit
    for variable in ("RL_TEST_POSTGRES_DSN", "RL_TEST_QDRANT_URL"):
        value = os.environ.get(variable)
        if value and urlsplit(value).hostname:
            return str(urlsplit(value).hostname)
    return "127.0.0.1"


@dataclass(frozen=True)
class Container:
    """A dedicated container owned by one test module (created and removed by it)."""

    name: str
    image: str
    ports: tuple[tuple[int, int], ...]
    env: tuple[tuple[str, str], ...] = ()

    def create(self) -> None:
        """(Re)create and start the container."""
        podman_sync("rm", "-f", self.name, check=False)
        args = ["run", "-d", "--name", self.name]
        for host, inner in self.ports:
            args += ["-p", f"{host}:{inner}"]
        for key, value in self.env:
            args += ["-e", f"{key}={value}"]
        podman_sync(*args, self.image)

    def remove(self) -> None:
        """Remove the container (unpausing first: a paused container cannot be removed)."""
        podman_sync("unpause", self.name, check=False)
        podman_sync("rm", "-f", "-t", "1", self.name, check=False)

    def state(self) -> str:
        """``running``, ``paused``, ``exited``, ..."""
        return podman_sync("inspect", self.name, "--format", "{{.State.Status}}", check=False)

    async def pause(self) -> None:
        """Freeze every process of the container (connections hang, nothing is refused)."""
        await podman("pause", self.name)

    async def unpause(self) -> None:
        """Resume a paused container."""
        await podman("unpause", self.name)

    async def stop(self) -> None:
        """Stop the container (connections are reset and then refused)."""
        await podman("stop", "-t", "2", self.name)

    async def start(self) -> None:
        """Start a stopped container."""
        await podman("start", self.name)

    async def restore(self) -> None:
        """Bring the container back to ``running`` whatever a test left it in."""
        state = await asyncio.to_thread(self.state)
        if state == "paused":
            await self.unpause()
        elif state != "running":
            await self.start()
