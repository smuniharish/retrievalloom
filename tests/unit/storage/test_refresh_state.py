"""refresh-engine ``StateStore`` bridge over the metadata store."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

import pytest
from refresh_engine import ResourceFingerprint, ResourceState

from retrievalloom.storage.refresh_state import (
    MetadataStateStore,
    MetadataStateTransaction,
    dump_state,
    load_state,
)

if TYPE_CHECKING:
    from retrievalloom.storage.sqlite import SQLiteMetadataStore

STAMP = datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)


def _state(resource_id: str, value: str = "v1", **kwargs: Any) -> ResourceState:
    return ResourceState(
        resource_id,
        ResourceFingerprint("sha256", value),
        refreshed_at=STAMP,
        **kwargs,
    )


def test_dump_and_load_round_trip() -> None:
    state = _state(
        "r",
        version="7",
        cheap_indicator="etag",
        snapshot_metadata={"size": 10, "name": "x"},
        dependencies=frozenset({"b", "a"}),
    )
    data = dump_state(state)
    assert data["dependencies"] == ["a", "b"]
    assert data["snapshot_metadata"] == {"size": "10", "name": "x"}
    restored = load_state("r", data)
    assert restored.fingerprint == state.fingerprint
    assert (restored.version, restored.cheap_indicator) == ("7", "etag")
    assert dict(restored.snapshot_metadata) == {"size": "10", "name": "x"}
    assert restored.dependencies == frozenset({"a", "b"})
    assert restored.refreshed_at == STAMP


def test_load_tolerates_missing_optional_fields() -> None:
    restored = load_state(
        "r",
        {"fingerprint": {"algorithm": "a", "value": "v"}, "refreshed_at": STAMP.isoformat()},
    )
    assert restored.version is None
    assert restored.cheap_indicator is None
    assert dict(restored.snapshot_metadata) == {}
    assert restored.dependencies == frozenset()


async def test_state_store_round_trip_per_namespace(store: SQLiteMetadataStore) -> None:
    states = MetadataStateStore(store, "engine-a")
    neighbour = MetadataStateStore(store, "engine-b")
    assert dict(await states.load_all()) == {}

    async with await states.transaction() as tx:
        assert isinstance(tx, MetadataStateTransaction)
        await tx.put(_state("one"))
        await tx.put(_state("two", dependencies=frozenset({"one"})))
    async with await neighbour.transaction() as tx:
        await tx.put(_state("one", "other"))

    loaded = await states.load_all()
    assert sorted(loaded) == ["one", "two"]
    assert loaded["two"].dependencies == frozenset({"one"})

    async with await states.transaction() as tx:
        await tx.delete("one")
        await tx.put(_state("two", "v2"))
    loaded = await states.load_all()
    assert list(loaded) == ["two"]
    assert loaded["two"].fingerprint.value == "v2"
    assert (await neighbour.load_all())["one"].fingerprint.value == "other"


async def test_failed_block_rolls_back_staged_changes(store: SQLiteMetadataStore) -> None:
    states = MetadataStateStore(store, "engine-a")
    with pytest.raises(RuntimeError):  # noqa: PT012
        async with await states.transaction() as tx:
            await tx.put(_state("one"))
            raise RuntimeError("abort")
    assert dict(await states.load_all()) == {}
