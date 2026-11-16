"""refresh-engine ``StateStore`` persisted in the metadata store (``rl_refresh_state``)."""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Any

from refresh_engine import ResourceFingerprint, ResourceState, StateStore, StateTransaction

from retrievalloom.storage.base import StoredRefreshState

if TYPE_CHECKING:
    from collections.abc import Mapping
    from collections.abc import Set as AbstractSet

    from retrievalloom.storage.base import DocumentMetadataStoreABC


def dump_state(state: ResourceState) -> dict[str, Any]:
    """Serialise a refresh-engine resource state to JSON-compatible data."""
    return {
        "fingerprint": {
            "algorithm": state.fingerprint.algorithm,
            "value": state.fingerprint.value,
        },
        "version": state.version,
        "cheap_indicator": state.cheap_indicator,
        "snapshot_metadata": {key: str(value) for key, value in state.snapshot_metadata.items()},
        "dependencies": sorted(state.dependencies),
        "refreshed_at": state.refreshed_at.isoformat(),
    }


def load_state(resource_id: str, data: Mapping[str, Any]) -> ResourceState:
    """Rebuild a resource state serialised by ``dump_state``."""
    fingerprint = data["fingerprint"]
    return ResourceState(
        resource_id,
        ResourceFingerprint(fingerprint["algorithm"], fingerprint["value"]),
        version=data.get("version"),
        cheap_indicator=data.get("cheap_indicator"),
        snapshot_metadata=dict(data.get("snapshot_metadata") or {}),
        dependencies=frozenset(data.get("dependencies") or ()),
        refreshed_at=datetime.fromisoformat(data["refreshed_at"]),
    )


class MetadataStateTransaction(StateTransaction):
    """Applies staged refresh-engine changes in one metadata write transaction."""

    def __init__(self, store: DocumentMetadataStoreABC, namespace: str) -> None:
        super().__init__()
        self._store = store
        self._namespace = namespace

    async def apply_changes(
        self, puts: Mapping[str, ResourceState], deletes: AbstractSet[str], /
    ) -> None:
        """Persist ``puts`` and ``deletes`` atomically."""
        stored = [
            StoredRefreshState(resource_id=resource_id, state=dump_state(state))
            for resource_id, state in sorted(puts.items())
        ]
        async with self._store.transaction(write=True) as transaction:
            await transaction.apply_refresh_states(self._namespace, stored, sorted(deletes))


class MetadataStateStore(StateStore):
    """refresh-engine state kept next to the engine metadata (one namespace per engine role)."""

    def __init__(self, store: DocumentMetadataStoreABC, namespace: str) -> None:
        self._store = store
        self._namespace = namespace

    async def load_all(self) -> Mapping[str, ResourceState]:
        """Return every committed resource state of the namespace."""
        async with self._store.transaction(write=False) as transaction:
            rows = await transaction.load_refresh_states(self._namespace)
        return {row.resource_id: load_state(row.resource_id, row.state) for row in rows}

    async def transaction(self) -> StateTransaction:
        """Open a staging transaction."""
        return MetadataStateTransaction(self._store, self._namespace)
