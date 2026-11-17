"""Fault injection: Postgres and Qdrant outages under live operations.

Opt-in with ``RL_TEST_FAULTS=1``; needs the ``podman`` CLI. The module creates its
own containers (``rl-fault-postgres`` and ``rl-fault-qdrant``, from the images of
the shared ``rl-postgres``/``rl-qdrant`` containers when present), so the shared
services other suites use are never paused or stopped. The containers are removed
at module teardown.

Each test pauses (connections hang) or stops (connections are refused) a service
in the middle of an operation and asserts the designed behaviour:

- Metadata-store outages fail operations with retryable, structured
  :class:`~retrievalloom.errors.RetrievalLoomError` subclasses within the configured
  driver timeouts; nothing hangs and no write is reported as successful unless it
  is durable.
- A vector-store outage degrades retrieval (the dense method fails, the other
  methods still answer, the result is ``partial`` with a warning); writes still
  commit and report the failed indexing as a warning.
- After the service returns, the same engine recovers without a restart:
  writes and retrieval succeed, the lifecycle catches up and ``health()``
  is healthy again.
"""

from __future__ import annotations

import asyncio
import os
import time
from typing import TYPE_CHECKING, Any

import pytest

from retrievalloom import (
    Components,
    IngestionConfig,
    LifecycleConfig,
    RetrievalConfig,
    RetrievalLoomError,
    Scope,
)
from retrievalloom.domain.records import DocumentState, TaskState
from retrievalloom.domain.reports import HealthStatus
from retrievalloom.domain.retrieval import ResultStatus
from retrievalloom.errors import OperationTimeoutError
from retrievalloom.storage.sqlite import SQLiteMetadataStore
from tests.fakes import HashEmbedding
from tests.live._reliability_helpers import (
    Container,
    converge,
    drop_schema,
    emit,
    image_of,
    new_schema,
    podman_available,
    service_host,
    text_doc,
    unsettled_coverage,
)
from tests.live.conftest import running_engine

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Awaitable, Iterator
    from pathlib import Path

    from retrievalloom import RetrievalEngine
    from retrievalloom.storage.postgres import PostgresMetadataStore

ENABLED = os.environ.get("RL_TEST_FAULTS") == "1"
PG_PORT = int(os.environ.get("RL_TEST_FAULTS_PG_PORT", "55442"))
QDRANT_PORT = int(os.environ.get("RL_TEST_FAULTS_QDRANT_PORT", "6343"))
QDRANT_GRPC_PORT = int(os.environ.get("RL_TEST_FAULTS_QDRANT_GRPC_PORT", "6344"))
SCOPE = Scope(tenant_id="faults")
# Upper bound for any single operation while a service is down: driver timeouts are
# a few seconds, so anything near this bound is a hang.
HANG_BOUND = 60.0

pytestmark = [
    pytest.mark.skipif(not ENABLED, reason="set RL_TEST_FAULTS=1 to run fault injection"),
    pytest.mark.skipif(ENABLED and not podman_available(), reason="needs the podman CLI"),
]


class Services:
    """The dedicated fault containers and how to reach them."""

    def __init__(self) -> None:
        host = service_host()
        self.postgres = Container(
            name="rl-fault-postgres",
            image=image_of("rl-postgres", "docker.io/library/postgres:18-alpine"),
            ports=((PG_PORT, 5432),),
            env=(("POSTGRES_USER", "rl"), ("POSTGRES_PASSWORD", "rl"), ("POSTGRES_DB", "rl")),
        )
        self.qdrant = Container(
            name="rl-fault-qdrant",
            image=image_of("rl-qdrant", "docker.io/qdrant/qdrant:latest"),
            ports=((QDRANT_PORT, 6333), (QDRANT_GRPC_PORT, 6334)),
        )
        self.dsn = f"postgresql://rl:rl@{host}:{PG_PORT}/rl"
        self.qdrant_url = f"http://{host}:{QDRANT_PORT}"

    async def postgres_ready(self, seconds: float = 90) -> None:
        """Wait until Postgres accepts TCP connections and queries."""
        import asyncpg

        deadline = time.monotonic() + seconds
        while True:
            try:
                connection = await asyncpg.connect(self.dsn, timeout=2)
                try:
                    await connection.execute("SELECT 1")
                finally:
                    await connection.close()
                return
            except (OSError, asyncpg.PostgresError, TimeoutError):
                if time.monotonic() > deadline:
                    raise
                await asyncio.sleep(0.5)

    async def qdrant_ready(self, seconds: float = 90) -> None:
        """Wait until Qdrant reports ready."""
        import httpx

        deadline = time.monotonic() + seconds
        async with httpx.AsyncClient(timeout=2) as client:
            while True:
                try:
                    if (await client.get(f"{self.qdrant_url}/readyz")).status_code == 200:
                        return
                except httpx.HTTPError:
                    pass
                if time.monotonic() > deadline:
                    raise TimeoutError("qdrant did not become ready")
                await asyncio.sleep(0.5)


@pytest.fixture(scope="module")
def services() -> Iterator[Services]:
    """Create the dedicated containers for this module; remove them afterwards."""
    created = Services()
    try:
        created.postgres.create()
        created.qdrant.create()
        yield created
    finally:
        created.postgres.remove()
        created.qdrant.remove()


@pytest.fixture
async def ready(services: Services) -> AsyncIterator[Services]:
    """Running, reachable services before and after every test."""
    await services.postgres.restore()
    await services.qdrant.restore()
    await services.postgres_ready()
    await services.qdrant_ready()
    try:
        yield services
    finally:
        await services.postgres.restore()
        await services.qdrant.restore()


@pytest.fixture
async def pg_store(ready: Services) -> AsyncIterator[PostgresMetadataStore]:
    """Postgres store with short driver timeouts, in a schema dropped afterwards."""
    from retrievalloom.storage.postgres import PostgresMetadataStore

    schema = new_schema("rl_fault")
    store = PostgresMetadataStore(
        ready.dsn,
        schema=schema,
        pool_timeout=5,
        lock_timeout_ms=5_000,
        connect_args={"timeout": 3, "command_timeout": 5},
    )
    try:
        yield store
    finally:
        await ready.postgres_ready()
        await drop_schema(ready.dsn, schema)


async def bounded[T](awaitable: Awaitable[T], what: str, bound: float = HANG_BOUND) -> T:
    """Await ``awaitable``; fail the test (not hang) when it takes longer than ``bound``."""
    try:
        async with asyncio.timeout(bound):
            return await awaitable
    except TimeoutError:
        pytest.fail(f"{what} hung for more than {bound:.0f}s")


async def outcome[T](
    awaitable: Awaitable[T], what: str
) -> tuple[T | None, BaseException | None, float]:
    """``(result, error, seconds)`` of a bounded call; only structured errors are allowed."""
    started = time.perf_counter()
    try:
        result = await bounded(awaitable, what)
    except RetrievalLoomError as exc:
        return None, exc, time.perf_counter() - started
    return result, None, time.perf_counter() - started


def assert_retryable(error: BaseException | None, what: str) -> None:
    """``error`` is a structured, retryable RetrievalLoom error."""
    assert isinstance(error, RetrievalLoomError), f"{what}: {error!r}"
    assert error.retryable, f"{what}: {error.code} is not retryable"


async def until_ok[T](make: Any, what: str, seconds: float = 45) -> T:
    """Call ``make()`` until it stops raising retryable errors (the service is back)."""
    deadline = time.monotonic() + seconds
    while True:
        try:
            return await bounded(make(), what)
        except RetrievalLoomError as exc:
            if not exc.retryable or time.monotonic() > deadline:
                raise
            await asyncio.sleep(0.5)


async def found(engine: RetrievalEngine, query: str) -> tuple[set[str], Any]:
    """Document ids retrieved for ``query`` and the result."""
    result = await engine.retrieve(query, scope=SCOPE, top_k=50)
    return {source.document_id for item in result.evidence for source in item.sources}, result


def docs(prefix: str, count: int) -> list[Any]:
    """``count`` documents with a word unique to ``prefix``."""
    return [
        text_doc(f"{prefix}-{n}", f"{prefix}word note {n} about outage recovery")
        for n in range(count)
    ]


async def wait_health(engine: RetrievalEngine, healthy: bool, seconds: float = 30) -> None:
    """Wait until ``health()`` is (not) healthy."""
    deadline = time.monotonic() + seconds
    while True:
        report = await bounded(engine.health(), "health")
        if (report.status is HealthStatus.HEALTHY) is healthy:
            return
        if time.monotonic() > deadline:
            pytest.fail(f"health stayed {report.status.value}: {report}")
        await asyncio.sleep(0.5)


# metadata store outages ----------------------------------------------------------------
async def test_postgres_pause_mid_ingest(ready: Services, pg_store: PostgresMetadataStore) -> None:
    """A frozen database fails writes, reads and health within the driver timeouts."""
    async with running_engine(
        Components(metadata_store=pg_store, embedding=HashEmbedding()),
        lifecycle=LifecycleConfig(background=False),
    ) as engine:
        await engine.ingest(docs("base", 10), scope=SCOPE)
        batch = docs("frozen", 200)
        task = asyncio.create_task(engine.ingest(batch, scope=SCOPE))
        await asyncio.sleep(0.05)
        await ready.postgres.pause()
        report, error, seconds = await outcome(task, "ingest during pause")
        if error is not None:
            assert_retryable(error, "ingest during pause")
        else:  # it finished before the pause took effect: it must then be durable
            assert report is not None
        _, read_error, read_seconds = await outcome(
            engine.retrieve("baseword", scope=SCOPE), "retrieve during pause"
        )
        assert_retryable(read_error, "retrieve during pause")
        _, delete_error, _ = await outcome(engine.delete(["base-0"], scope=SCOPE), "delete")
        assert_retryable(delete_error, "delete during pause")
        await wait_health(engine, healthy=False)

        await ready.postgres.unpause()
        retried = await until_ok(lambda: engine.ingest(batch, scope=SCOPE), "ingest after unpause")
        assert {item.document_id for item in retried.documents} == {item.id for item in batch}
        assert not [item for item in retried.documents if item.error is not None]
        ids, result = await found(engine, "frozenword")
        assert len(ids) == 50  # top_k=50
        assert ids <= {item.id for item in batch}
        assert result.status is ResultStatus.COMPLETE
        assert "base-0" in (await found(engine, "baseword"))[0]  # the failed delete did nothing
        await wait_health(engine, healthy=True)
        status = await converge(engine, SCOPE)
        assert status.documents_active == 210
        assert not unsettled_coverage(status)
        async with pg_store.transaction(SCOPE, write=False) as tx:
            for item in batch:
                record = await tx.get_document(SCOPE, item.id)
                assert record is not None, item.id
                assert record.version == 1, item.id  # the retry created no second version
    warned = [warning.code.value for warning in report.warnings] if report is not None else None
    emit(
        f"\nFAULT pg-pause ingest={seconds:.1f}s error={type(error).__name__} "
        f"warnings={warned} read={read_seconds:.1f}s"
    )


async def test_postgres_restart_under_background_lifecycle(
    ready: Services, pg_store: PostgresMetadataStore
) -> None:
    """A restarted database is reconnected; the background lifecycle survives and catches up."""
    lifecycle = LifecycleConfig(background=True, poll_interval=0.2)
    async with running_engine(
        Components(metadata_store=pg_store, embedding=HashEmbedding()), lifecycle=lifecycle
    ) as engine:
        await engine.ingest(docs("before", 20), scope=SCOPE)
        task = asyncio.create_task(engine.ingest(docs("during", 100), scope=SCOPE))
        await asyncio.sleep(0.05)
        await ready.postgres.stop()
        _, error, seconds = await outcome(task, "ingest during stop")
        if error is not None:
            assert_retryable(error, "ingest during stop")
        _, read_error, _ = await outcome(engine.retrieve("beforeword", scope=SCOPE), "retrieve")
        assert_retryable(read_error, "retrieve during stop")
        _, status_error, _ = await outcome(engine.status(scope=SCOPE), "status during stop")
        assert_retryable(status_error, "status during stop")
        await wait_health(engine, healthy=False)
        await asyncio.sleep(1.0)  # background rounds fail meanwhile

        await ready.postgres.start()
        await ready.postgres_ready()
        await until_ok(lambda: engine.ingest(docs("during", 100), scope=SCOPE), "re-ingest")
        await until_ok(lambda: engine.ingest(docs("after", 20), scope=SCOPE), "ingest after")
        # Nothing calls the lifecycle by hand: the background scheduler catches up.
        deadline = time.monotonic() + 30
        status = await engine.status(scope=SCOPE)
        while unsettled_coverage(status) and time.monotonic() < deadline:
            await asyncio.sleep(0.5)
            status = await engine.status(scope=SCOPE)
        assert not unsettled_coverage(status), status.coverage
        assert status.documents_active == 140
        for prefix in ("before", "during", "after"):
            assert len((await found(engine, f"{prefix}word"))[0]) >= 20
        await wait_health(engine, healthy=True)
    emit(f"\nFAULT pg-stop ingest={seconds:.1f}s error={type(error).__name__}")


# vector store outages ------------------------------------------------------------------
def qdrant_components(services: Services, tmp_path: Path, prefix: str) -> Components:
    from retrievalloom.index.vector import QdrantVectorStore

    return Components(
        metadata_store=SQLiteMetadataStore(tmp_path / "metadata.db"),
        embedding=HashEmbedding(),
        vector_store=QdrantVectorStore(
            url=services.qdrant_url, collection_prefix=prefix, timeout=3
        ),
    )


def dense_failure(result: Any) -> bool:
    """The result reports the failed or timed-out dense method."""
    codes = {warning.code for warning in result.warnings}
    return bool(codes & {"stage_failed_degraded", "timeout_partial"})


async def recover_dense(engine: RetrievalEngine) -> None:
    """Catch the lifecycle up; coverage that failed during the outage is reset by a backfill."""
    status = await converge(engine, SCOPE)
    if unsettled_coverage(status):
        emit(f"\nFAULT backfill needed after outage: {unsettled_coverage(status)}")
        task = await engine.backfill(scope=SCOPE)
        assert (await engine.wait_for(task, timeout=60)).state is TaskState.SUCCEEDED
        status = await converge(engine, SCOPE)
    assert not unsettled_coverage(status), status.coverage


@pytest.fixture
async def collections(ready: Services, prefix: str) -> AsyncIterator[str]:
    """Collection prefix on the fault Qdrant; its collections are dropped afterwards."""
    from tests.live.conftest import drop_collections

    yield prefix
    await ready.qdrant.restore()
    await ready.qdrant_ready()
    await drop_collections(ready.qdrant_url, prefix)


async def test_qdrant_pause_degrades_retrieval(
    ready: Services, tmp_path: Path, collections: str
) -> None:
    """A frozen vector store degrades retrieval and indexing; nothing fails or hangs."""
    retrieval = RetrievalConfig(method_timeout=4, request_timeout=12, embedding_cache_size=0)
    async with running_engine(
        qdrant_components(ready, tmp_path, collections),
        retrieval=retrieval,
        ingestion=IngestionConfig(index_wait_timeout=15),
        lifecycle=LifecycleConfig(background=False),
    ) as engine:
        await engine.ingest(docs("base", 20), scope=SCOPE)
        _, result = await found(engine, "baseword")
        assert result.status is ResultStatus.COMPLETE

        await ready.qdrant.pause()
        started = time.perf_counter()
        ids, result = await bounded(found(engine, "baseword"), "retrieve during pause")
        degraded_seconds = time.perf_counter() - started
        assert result.status is ResultStatus.PARTIAL, result.warnings
        assert dense_failure(result), result.warnings
        assert len(ids) == 20  # the in-memory methods still answer
        assert degraded_seconds < retrieval.request_timeout + 5

        started = time.perf_counter()
        report = await bounded(engine.ingest(docs("paused", 5), scope=SCOPE), "ingest")
        ingest_seconds = time.perf_counter() - started
        codes = {warning.code for warning in report.warnings}
        assert codes & {"stage_failed_degraded", "index_wait_timeout"}, report.warnings
        inspector = SQLiteMetadataStore(tmp_path / "metadata.db")
        await inspector.initialize()
        try:
            async with inspector.transaction(SCOPE, write=False) as tx:
                record = await tx.get_document(SCOPE, "paused-0")
        finally:
            await inspector.close()
        assert record is not None
        assert record.state is DocumentState.ACTIVE  # the write committed
        # The in-memory methods indexed the write; only dense lags.
        assert "paused-0" in (await bounded(found(engine, "pausedword"), "retrieve"))[0]
        await wait_health(engine, healthy=False)

        await ready.qdrant.unpause()
        await recover_dense(engine)
        ids, result = await found(engine, "pausedword")
        assert result.status is ResultStatus.COMPLETE, result.warnings
        assert {f"paused-{n}" for n in range(5)} <= ids
        await wait_health(engine, healthy=True)
    emit(
        f"\nFAULT qdrant-pause retrieve={degraded_seconds:.1f}s ingest={ingest_seconds:.1f}s "
        f"warnings={sorted(codes)}"
    )


async def test_qdrant_restart_during_rebuild(
    ready: Services, tmp_path: Path, collections: str
) -> None:
    """A rebuild interrupted by a vector-store restart fails cleanly; the next one succeeds."""
    retrieval = RetrievalConfig(method_timeout=4, request_timeout=12, embedding_cache_size=0)
    async with running_engine(
        qdrant_components(ready, tmp_path, collections),
        retrieval=retrieval,
        lifecycle=LifecycleConfig(background=False, max_attempts=2),
    ) as engine:
        await engine.ingest(docs("base", 300), scope=SCOPE)
        await ready.qdrant.stop()
        task = await engine.rebuild(scope=SCOPE, channel="index", method="dense")
        waited, error, seconds = await outcome(
            engine.wait_for(task, timeout=30), "rebuild during stop"
        )
        if error is not None:
            assert isinstance(error, OperationTimeoutError), error
        else:
            assert waited is not None
            assert waited.state is TaskState.FAILED, waited
        status = await engine.status(scope=SCOPE)
        dense = [g for g in status.generations if g.method == "index.dense"]
        assert [g.state.value for g in dense].count("active") == 1, dense  # old one serves
        ids, degraded = await bounded(found(engine, "baseword"), "retrieve during stop")
        assert degraded.status is ResultStatus.PARTIAL, degraded.warnings
        assert dense_failure(degraded), degraded.warnings
        assert len(ids) == 50  # top_k: the in-memory methods still answer
        await wait_health(engine, healthy=False)

        await ready.qdrant.start()
        await ready.qdrant_ready()
        again = await until_ok(
            lambda: engine.rebuild(scope=SCOPE, channel="index", method="dense"), "rebuild"
        )
        final = await bounded(engine.wait_for(again, timeout=HANG_BOUND - 5), "rebuild")
        assert final.state is TaskState.SUCCEEDED, final
        await recover_dense(engine)
        _, result = await found(engine, "baseword")
        assert result.status is ResultStatus.COMPLETE, result.warnings
        await wait_health(engine, healthy=True)
        status = await engine.status(scope=SCOPE)
        dense = [g for g in status.generations if g.method == "index.dense"]
        assert [g.state.value for g in dense].count("active") == 1, dense
    state = "timeout" if error is not None else (waited.state.value if waited else "?")
    emit(f"\nFAULT qdrant-stop rebuild={state} after {seconds:.1f}s")
