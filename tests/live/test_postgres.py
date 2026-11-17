"""PostgreSQL metadata store against a live server (``RL_TEST_POSTGRES_DSN``).

Every test works in its own schema, dropped afterwards, so runs never share state.
"""

from __future__ import annotations

import asyncio
import os
import uuid
from datetime import timedelta
from typing import TYPE_CHECKING, Any

import pytest
import sqlalchemy as sa
from alembic.autogenerate import compare_metadata
from alembic.runtime.migration import MigrationContext
from alembic.script import ScriptDirectory

from retrievalloom import (
    Components,
    DocumentInput,
    LifecycleConfig,
    Scope,
    TextRepresentation,
)
from retrievalloom.domain.base import utc_now
from retrievalloom.domain.hashing import random_id
from retrievalloom.domain.records import (
    GenerationState,
    LifecycleTask,
    TaskKind,
    TaskState,
)
from retrievalloom.domain.retrieval import ResultStatus, RetrievalExecutionConfig
from retrievalloom.errors import (
    BackendTimeoutError,
    BackendUnavailableError,
    TenantIsolationError,
    ValidationError,
    VersionConflictError,
)
from retrievalloom.index.memory import InMemoryGenerationalIndex
from retrievalloom.storage import schema as tables
from retrievalloom.storage.base import MigrationPolicy
from retrievalloom.storage.postgres import PostgresMetadataStore
from retrievalloom.storage.sql import VERSION_TABLE, alembic_config
from retrievalloom.testing import MetadataStoreContract
from retrievalloom.testing.storage import SCOPE, document
from tests.fakes import HashEmbedding
from tests.live.conftest import make_engine
from tests.support import SharedResources

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from pathlib import Path

    from sqlalchemy.engine import Connection

    from retrievalloom import RetrievalEngine

DSN = os.environ.get("RL_TEST_POSTGRES_DSN")
pytestmark = pytest.mark.skipif(
    DSN is None, reason="set RL_TEST_POSTGRES_DSN to run PostgreSQL tests"
)

HEAD = ScriptDirectory.from_config(alembic_config()).get_current_head()


def new_schema() -> str:
    """Unique schema name for one test."""
    return f"rl_test_{uuid.uuid4().hex[:24]}"


def store(schema: str, **options: Any) -> PostgresMetadataStore:
    """Store on the test server in ``schema``."""
    assert DSN is not None
    return PostgresMetadataStore(DSN, schema=schema, **options)


async def drop_schema(name: str) -> None:
    """Drop ``name`` and everything in it."""
    admin = store(name)
    await admin.initialize()
    try:
        async with admin.engine.begin() as connection:
            await connection.execute(sa.text(f'DROP SCHEMA IF EXISTS "{name}" CASCADE'))
    finally:
        await admin.close()


@pytest.fixture
async def schemas() -> AsyncIterator[list[str]]:
    """Schemas created by a test; dropped afterwards."""
    created: list[str] = []
    yield created
    for name in created:
        await drop_schema(name)


@pytest.fixture
def schema(schemas: list[str]) -> str:
    """A fresh schema for this test."""
    name = new_schema()
    schemas.append(name)
    return name


@pytest.fixture
async def migrated(schema: str) -> AsyncIterator[PostgresMetadataStore]:
    """Initialised, migrated store in a fresh schema."""
    component = store(schema)
    await component.initialize()
    await component.migrate()
    try:
        yield component
    finally:
        await component.close()


# contract kit ---------------------------------------------------------------------------
class TestPostgresStoreContract(SharedResources, MetadataStoreContract):
    """The full metadata-store contract kit on PostgreSQL."""

    @pytest.fixture(autouse=True)
    def _schemas(self, schemas: list[str]) -> None:
        self.created = schemas

    async def make_component(self, tmp_path: Path) -> PostgresMetadataStore:
        del tmp_path
        name = new_schema()
        self.created.append(name)
        return store(name)


# migrations -----------------------------------------------------------------------------
async def test_schema_metadata_has_no_drift_from_migrations(
    migrated: PostgresMetadataStore,
) -> None:
    def diff(connection: Connection) -> list[Any]:
        context = MigrationContext.configure(
            connection, opts={"version_table": VERSION_TABLE, "compare_type": True}
        )
        return compare_metadata(context, tables.metadata)

    async with migrated.engine.connect() as connection:
        assert await connection.run_sync(diff) == []
        columns = await connection.run_sync(
            lambda sync: {
                column["name"]: type(column["type"]).__name__
                for column in sa.inspect(sync).get_columns("rl_documents")
            }
        )
    assert columns["created_at"] == "TIMESTAMP"
    assert columns["metadata"] == "JSON"


async def test_upgrade_downgrade_upgrade_round_trip(migrated: PostgresMetadataStore) -> None:
    revisions = list(ScriptDirectory.from_config(alembic_config()).walk_revisions())
    for revision in revisions:
        assert await migrated.schema_version() == revision.revision
        await migrated.downgrade(revision.down_revision or "base")  # type: ignore[arg-type]
    assert await migrated.schema_version() is None
    with pytest.raises(Exception, match="not at head"):
        await migrated.migrate(MigrationPolicy.VERIFY)
    await migrated.migrate()
    await migrated.migrate(MigrationPolicy.VERIFY)
    async with migrated.transaction(SCOPE, write=True) as tx:
        await tx.ensure_scope(SCOPE)
        await tx.put_document(document(SCOPE, "after"), expected_revision=None)


async def test_concurrent_migrations_from_many_engines(schema: str) -> None:
    stores = [store(schema) for _ in range(4)]
    for item in stores:
        await item.initialize()
    try:
        await asyncio.gather(*(item.migrate() for item in stores))
        assert {await item.schema_version() for item in stores} == {HEAD}
    finally:
        for item in stores:
            await item.close()


async def test_timestamps_round_trip_as_aware_utc(migrated: PostgresMetadataStore) -> None:
    record = document(SCOPE, "clock")
    async with migrated.transaction(SCOPE, write=True) as tx:
        await tx.ensure_scope(SCOPE)
        await tx.put_document(record, expected_revision=None)
    async with migrated.transaction(SCOPE, write=False) as tx:
        stored = await tx.get_document(SCOPE, "clock")
    assert stored is not None
    assert stored.created_at == record.created_at
    assert stored.created_at.utcoffset() == timedelta(0)


# concurrency between stores (engines) ---------------------------------------------------
async def test_commit_sequence_has_no_gaps_or_duplicates(schema: str) -> None:
    stores = [store(schema) for _ in range(3)]
    for item in stores:
        await item.initialize()
    await stores[0].migrate()
    scopes = [Scope(tenant_id=f"tenant-{index}") for index in range(3)]
    async with stores[0].transaction(write=True) as tx:
        for scope in scopes:
            await tx.ensure_scope(scope)

    async def bump(component: PostgresMetadataStore, scope: Scope) -> int:
        async with component.transaction(scope, write=True) as tx:
            return await tx.next_commit_seq()

    try:
        seqs = await asyncio.gather(
            *(bump(item, scope) for item in stores for scope in scopes for _ in range(10))
        )
        assert sorted(seqs) == list(range(1, 91))
        async with stores[1].transaction(write=False) as tx:
            assert await tx.current_commit_seq() == 90
    finally:
        for item in stores:
            await item.close()


async def test_scope_writers_are_serialised_across_engines(schema: str) -> None:
    """Check-then-insert in many concurrent transactions never write-skews."""
    stores = [store(schema) for _ in range(3)]
    for item in stores:
        await item.initialize()
    await stores[0].migrate()
    async with stores[0].transaction(SCOPE, write=True) as tx:
        await tx.ensure_scope(SCOPE)

    async def claim(component: PostgresMetadataStore, index: int) -> bool:
        async with component.transaction(SCOPE, write=True) as tx:
            if list(await tx.list_documents(SCOPE.key)):
                return False
            await asyncio.sleep(0.01)  # widen the race window
            await tx.put_document(document(SCOPE, f"winner-{index}"), expected_revision=None)
            return True

    try:
        won = await asyncio.gather(*(claim(stores[index % 3], index) for index in range(12)))
        assert won.count(True) == 1
        async with stores[2].transaction(SCOPE, write=False) as tx:
            assert len(await tx.list_documents(SCOPE.key)) == 1
    finally:
        for item in stores:
            await item.close()


async def test_task_claim_has_a_single_winner(schema: str) -> None:
    stores = [store(schema) for _ in range(3)]
    for item in stores:
        await item.initialize()
    await stores[0].migrate()
    now = utc_now()
    task = LifecycleTask(
        task_id=random_id("task"),
        scope_key=SCOPE.key,
        kind=TaskKind.REBUILD,
        state=TaskState.QUEUED,
        created_at=now,
        updated_at=now,
    )
    async with stores[0].transaction(SCOPE, write=True) as tx:
        await tx.ensure_scope(SCOPE)
        await tx.put_task(task, expected_revision=None)

    async def claim(component: PostgresMetadataStore, owner: str) -> str | None:
        async with component.transaction(SCOPE, write=False) as tx:
            current = await tx.get_task(task.task_id)
        assert current is not None
        if current.state is not TaskState.QUEUED:
            return None
        running = current.model_copy(
            update={"state": TaskState.RUNNING, "owner_id": owner, "lease_epoch": 1}
        )
        try:
            async with component.transaction(SCOPE, write=True) as tx:
                await tx.put_task(running, expected_revision=current.row_revision)
        except VersionConflictError:
            return None
        return owner

    try:
        owners = await asyncio.gather(
            *(claim(stores[index % 3], f"owner-{index}") for index in range(9))
        )
        winners = [owner for owner in owners if owner is not None]
        assert len(winners) == 1
        async with stores[1].transaction(SCOPE, write=False) as tx:
            stored = await tx.get_task(task.task_id)
        assert stored is not None
        assert stored.owner_id == winners[0]
        assert stored.state is TaskState.RUNNING
    finally:
        for item in stores:
            await item.close()


async def test_readers_see_snapshots_and_do_not_wait_for_writers(
    migrated: PostgresMetadataStore,
) -> None:
    async with migrated.transaction(SCOPE, write=True) as writer:
        await writer.ensure_scope(SCOPE)
        async with asyncio.timeout(5), migrated.transaction(write=False) as reader:
            assert SCOPE not in await reader.list_scopes()
    async with migrated.transaction(write=False) as reader:
        assert SCOPE in await reader.list_scopes()


async def test_different_scopes_write_concurrently(schema: str) -> None:
    first, second = store(schema), store(schema)
    for item in (first, second):
        await item.initialize()
    await first.migrate()
    other = Scope(tenant_id="other")
    async with first.transaction(write=True) as tx:
        await tx.ensure_scope(SCOPE)
        await tx.ensure_scope(other)
    try:
        async with first.transaction(SCOPE, write=True) as tx:
            await tx.put_document(document(SCOPE, "held"), expected_revision=None)
            async with asyncio.timeout(5), second.transaction(other, write=True) as parallel:
                await parallel.put_document(document(other, "free"), expected_revision=None)
    finally:
        for item in (first, second):
            await item.close()


async def test_lock_timeout_maps_to_backend_timeout(schema: str) -> None:
    holder, waiter = store(schema), store(schema, lock_timeout_ms=200)
    for item in (holder, waiter):
        await item.initialize()
    await holder.migrate()
    try:
        async with holder.transaction(SCOPE, write=True) as tx:
            await tx.ensure_scope(SCOPE)
            with pytest.raises(BackendTimeoutError) as info:
                async with waiter.transaction(SCOPE, write=True):
                    pass
        assert info.value.retryable
        assert info.value.details == {"sqlstate": "55P03"}
        async with waiter.transaction(SCOPE, write=True) as tx:  # usable afterwards
            await tx.put_document(document(SCOPE, "later"), expected_revision=None)
    finally:
        for item in (holder, waiter):
            await item.close()


async def test_driver_errors_are_translated(migrated: PostgresMetadataStore) -> None:
    async with migrated.transaction(SCOPE, write=True) as tx:
        await tx.ensure_scope(SCOPE)
    with pytest.raises(ValidationError) as rejected:
        async with migrated.transaction(SCOPE, write=False) as tx:
            await tx._exec(sa.text("SELECT CAST('not a number' AS integer)"))
    assert rejected.value.details == {"sqlstate": "22P02"}
    async with migrated.transaction(SCOPE, write=True) as tx:  # NUL survives inside JSON
        await tx.put_document(
            document(SCOPE, "nul", metadata={"k": "a\x00b"}), expected_revision=None
        )
    async with migrated.transaction(SCOPE, write=False) as tx:
        stored = await tx.get_document(SCOPE, "nul")
    assert stored is not None
    assert stored.metadata == {"k": "a\x00b"}
    with pytest.raises(TenantIsolationError):
        async with migrated.transaction(SCOPE, write=False) as tx:
            await tx.list_documents("sk_00000000000000000000000000000000")
    assert DSN is not None
    wrong = PostgresMetadataStore(DSN.replace("rl:rl@", "rl:wrong@", 1))
    unreachable = PostgresMetadataStore(
        "postgresql://rl:rl@127.0.0.1:1/rl", connect_args={"timeout": 2}
    )
    for item in (wrong, unreachable):
        await item.initialize()
        try:
            with pytest.raises(BackendUnavailableError) as info:
                async with item.transaction(write=False):
                    pass
            assert info.value.retryable is (item is unreachable)
            assert (await item.health()).message == "BackendUnavailableError"
        finally:
            await item.close()


# engines --------------------------------------------------------------------------------
def text_doc(document_id: str, text: str) -> DocumentInput:
    """Single-text document."""
    return DocumentInput(_id=document_id, representations=(TextRepresentation(text=text),))


_SUPPLIED: list[PostgresMetadataStore] = []


@pytest.fixture(autouse=True)
async def close_supplied_stores() -> AsyncIterator[None]:
    """Close the stores handed to engines: supplied components stay caller-owned."""
    yield
    while _SUPPLIED:
        await _SUPPLIED.pop().close()


def engine_on(schema: str, **lifecycle: Any) -> RetrievalEngine:
    """Engine with the Postgres store in ``schema`` and a foreground lifecycle."""
    metadata_store = store(schema)
    _SUPPLIED.append(metadata_store)
    return make_engine(
        components=Components(metadata_store=metadata_store, embedding=HashEmbedding()),
        lifecycle=LifecycleConfig(background=False, **lifecycle),
    )


async def found(engine: RetrievalEngine, query: str, scope: Scope = SCOPE) -> set[str]:
    """Document ids retrieved for ``query``."""
    result = await engine.retrieve(query, scope=scope, top_k=50)
    assert result.status is ResultStatus.COMPLETE
    return {source.document_id for item in result.evidence for source in item.sources}


async def test_engine_end_to_end_on_postgres(schema: str) -> None:
    lifecycle = {"purge_after": 0.0, "max_versions": 1, "event_retention": 0.0}
    engine = engine_on(schema, **lifecycle)
    async with engine:
        report = await engine.ingest(
            [
                text_doc("alpha", "Postgres stores the metadata of many engines"),
                text_doc("beta", "Advisory locks serialise writers of one scope"),
                text_doc("gamma", "Readers use repeatable read snapshots"),
            ],
            scope=SCOPE,
        )
        assert not report.warnings
        assert "alpha" in await found(engine, "metadata engines")
        await engine.ingest([text_doc("beta", "Advisory locks, revised twice")], scope=SCOPE)
        await engine.ingest([text_doc("beta", "Advisory locks, revised thrice")], scope=SCOPE)
        deleted = await engine.delete(["gamma"], scope=SCOPE)
        assert deleted.deleted == ("gamma",)
        assert "gamma" not in await found(engine, "repeatable read snapshots")
        purged = await engine.purge(["alpha"], scope=SCOPE)
        assert purged.purged == ("alpha",)
        assert "alpha" not in await found(engine, "metadata engines")
        await engine.run_lifecycle_once(SCOPE)  # retention: tombstones, versions, events
        task = await engine.rebuild(scope=SCOPE, channel="index")
        result = await engine.wait_for(task, timeout=60)
        assert result.state is TaskState.SUCCEEDED
        assert "beta" in await found(engine, "advisory locks revised")
        status = await engine.status(scope=SCOPE)
        assert (status.documents_active, status.documents_deleted) == (1, 0)
    inspector = store(schema)
    await inspector.initialize()
    try:
        async with inspector.transaction(SCOPE, write=False) as tx:
            beta = await tx.get_document(SCOPE, "beta")
            assert beta is not None
            assert [item.version for item in await tx.get_versions(beta.document_uid)] == [3]
            assert await tx.get_document(SCOPE, "gamma") is None  # purged by retention
    finally:
        await inspector.close()

    restarted = engine_on(schema)
    async with restarted:  # ephemeral indexes are rebuilt from the shared metadata
        assert "beta" in await found(restarted, "advisory locks revised")


async def test_engines_share_one_store_concurrently(schema: str) -> None:
    """Several engines ingest, update, delete and run lifecycle rounds at once."""
    engines = [engine_on(schema) for _ in range(3)]
    # Concurrent startup also races the migrations of every engine.
    await asyncio.gather(*(engine.__aenter__() for engine in engines))
    try:
        scopes = [SCOPE, Scope(tenant_id="acme", namespace="second")]

        async def work(index: int, engine: RetrievalEngine) -> list[int]:
            versions: list[int] = []
            for round_ in range(4):
                scope = scopes[round_ % 2]
                own = [
                    text_doc(f"e{index}-r{round_}-{n}", f"engine {index} note {n}")
                    for n in range(3)
                ]
                shared = text_doc("shared", f"shared text from engine {index} round {round_}")
                report = await engine.ingest([*own, shared], scope=scope)
                versions.extend(
                    item.version or 0 for item in report.documents if item.document_id == "shared"
                )
                if round_ % 2:
                    await engine.delete([f"e{index}-r{round_}-0"], scope=scope)
                await engine.run_lifecycle_once()
            return versions

        results = await asyncio.gather(
            *(work(index, engine) for index, engine in enumerate(engines))
        )
        for engine in engines:
            await engine.run_lifecycle_once()

        # No lost update: every write of "shared" in SCOPE got its own version.
        shared_versions = sorted(version for versions in results for version in versions[0::2])
        assert shared_versions == list(range(1, len(shared_versions) + 1))
        expected = {SCOPE: (3 * 2 * 3 + 1, 0), scopes[1]: (3 * 2 * 3 + 1 - 6, 6)}
        for engine in engines:
            for scope, counts in expected.items():
                status = await engine.status(scope=scope)
                assert (status.documents_active, status.documents_deleted) == counts
                active = [g for g in status.generations if g.state is GenerationState.ACTIVE]
                assert len({g.method for g in active}) == len(active)  # one active per unit
            for index in range(3):
                assert f"e{index}-r0-1" in await found(engine, f"engine {index} note 1")
                assert f"e{index}-r1-0" not in await found(
                    engine, f"engine {index} note 0", scopes[1]
                )
                assert f"e{index}-r1-1" in await found(engine, f"engine {index} note 1", scopes[1])

        inspector = store(schema)
        await inspector.initialize()
        try:
            async with inspector.engine.connect() as connection:
                owners = (
                    await connection.execute(
                        sa.select(tables.generations.c.generation_id).where(
                            tables.generations.c.state == GenerationState.BUILDING.value,
                            tables.generations.c.owner_id.is_not(None),
                        )
                    )
                ).all()
                pointers = (
                    await connection.execute(
                        sa.select(
                            tables.generation_pointers,
                            tables.generations.c.state,
                        ).join(
                            tables.generations,
                            tables.generations.c.generation_id
                            == tables.generation_pointers.c.active_generation_id,
                        )
                    )
                ).all()
                seq = (await connection.execute(sa.select(tables.commit_seq.c.seq))).scalar_one()
                windows = (
                    await connection.execute(
                        sa.select(sa.func.count())
                        .select_from(tables.visibility)
                        .where(tables.visibility.c.visible_from_seq > seq)
                    )
                ).scalar_one()
        finally:
            await inspector.close()
        assert owners == []  # no generation left mid-build with a lease owner
        assert pointers
        assert {row.state for row in pointers} == {GenerationState.ACTIVE.value}
        assert windows == 0  # no visibility window beyond the committed sequence
    finally:
        for engine in engines:
            await engine.__aexit__(None, None, None)


ONLY = {
    method: RetrievalExecutionConfig(
        keyword=method == "keyword",
        fuzzy=method == "fuzzy",
        sparse=method == "sparse",
        dense=method == "dense",
        graph=False,
        structure=False,
    )
    for method in ("keyword", "fuzzy", "sparse", "dense")
}


def keyword_doc(document_id: str, text: str, keyword: str) -> DocumentInput:
    """Single-text document carrying one keyword for the keyword method."""
    return DocumentInput.model_validate(
        {
            "_id": document_id,
            "representations": (TextRepresentation(text=text),),
            "metadata": {"keywords": [keyword]},
        }
    )


async def found_by(engine: RetrievalEngine, query: str) -> dict[str, set[str]]:
    """Document ids retrieved for ``query`` by each in-memory method on its own."""
    hits: dict[str, set[str]] = {}
    for method, config in ONLY.items():
        result = await engine.retrieve(query, scope=SCOPE, top_k=50, config=config)
        hits[method] = {source.document_id for item in result.evidence for source in item.sources}
    return hits


async def eventually(check: Any, limit: float = 30.0) -> float:
    """Seconds until ``await check()`` is true (fails after ``limit`` seconds)."""
    loop = asyncio.get_running_loop()
    start = loop.time()
    while not await check():
        assert loop.time() - start < limit, "condition not reached in time"
        await asyncio.sleep(0.05)
    return loop.time() - start


async def object_ids_of(schema: str, document_id: str) -> set[str]:
    """Ids of the objects derived from ``document_id`` in ``SCOPE``."""
    inspector = store(schema)
    await inspector.initialize()
    try:
        async with inspector.transaction(SCOPE, write=False) as tx:
            record = await tx.get_document(SCOPE, document_id)
            assert record is not None
            objects = await tx.list_document_objects(SCOPE.key, record.document_uid)
    finally:
        await inspector.close()
    return {item.object_id for item in objects}


def held(engine: RetrievalEngine, object_ids: set[str]) -> dict[str, set[str]]:
    """Per engine-local in-memory unit: which of ``object_ids`` any generation still holds."""
    runtime = engine._runtime
    assert runtime is not None
    holding: dict[str, set[str]] = {}
    for name, unit in runtime.units.items():
        backend = unit.backend
        if unit.instance and isinstance(backend, InMemoryGenerationalIndex):
            holding[name] = {
                object_id
                for (scope_key, _), generation in backend._generations.items()
                if scope_key == SCOPE.key
                for object_id in generation.metadata
                if object_id in object_ids
            }
    assert {name.split(".")[-1] for name in holding} == {"keyword", "fuzzy", "sparse"}
    return holding


async def test_purge_evicts_from_other_engines_in_memory_indexes(schema: str) -> None:
    """A purge on one engine reaches the in-memory indexes of every engine sharing the store."""
    purger = engine_on(schema)
    foreground = engine_on(schema)
    metadata_store = store(schema)
    _SUPPLIED.append(metadata_store)
    background = make_engine(
        components=Components(metadata_store=metadata_store, embedding=HashEmbedding()),
        lifecycle=LifecycleConfig(background=True, poll_interval=0.2),
    )
    query = "confidential salary spreadsheet"
    async with purger, foreground, background:
        await purger.ingest(
            [
                keyword_doc("secret", "confidential salary spreadsheet of the board", "salary"),
                keyword_doc("keep", "public holiday spreadsheet", "spreadsheet"),
            ],
            scope=SCOPE,
        )
        await foreground.run_lifecycle_once()

        async def everyone_serves_secret() -> bool:
            for engine in (purger, foreground, background):
                hits = await found_by(engine, query)
                if any("secret" not in ids for ids in hits.values()):
                    return False
            return True

        await eventually(everyone_serves_secret)

        secret_ids = await object_ids_of(schema, "secret")
        for engine in (purger, foreground, background):
            assert all(held(engine, secret_ids).values())  # every in-memory method holds it

        report = await purger.purge(["secret"], scope=SCOPE)
        assert report.purged == ("secret",)
        assert not any(held(purger, secret_ids).values())
        assert all(held(foreground, secret_ids).values())  # until its next lifecycle call

        async def background_evicted() -> bool:
            return not any(held(background, secret_ids).values())

        latency = await eventually(background_evicted, limit=10.0)
        assert latency < 5.0  # one poll interval plus the round, with ample slack
        await foreground.run_lifecycle_once()  # the foreground mode evicts on lifecycle calls
        assert not any(held(foreground, secret_ids).values())
        for engine in (purger, foreground, background):
            hits = await found_by(engine, query)
            assert all("secret" not in ids for ids in hits.values())
            assert all("keep" in ids for ids in hits.values())
