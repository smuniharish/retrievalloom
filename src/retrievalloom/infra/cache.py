"""Caches: ``NoCache`` (result cache default) and a bounded in-memory LRU with TTL."""

from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any

from cachetools import TTLCache

from retrievalloom.contracts.component import ComponentInfo, make_info
from retrievalloom.contracts.infra import CacheABC
from retrievalloom.errors import ConfigurationError

if TYPE_CHECKING:
    from collections.abc import Callable


class NoCache(CacheABC):
    """Cache that stores nothing."""

    _INFO = make_info(component="cache", implementation="none", version="1")

    @property
    def info(self) -> ComponentInfo:
        """Component identity."""
        return self._INFO

    async def get(self, namespace: str, key: str) -> Any | None:
        """Always a miss."""
        del namespace, key
        return None

    async def set(self, namespace: str, key: str, value: Any) -> None:
        """Discard ``value``."""
        del namespace, key, value

    async def invalidate(self, namespace: str, prefix: str | None = None) -> int:
        """Nothing to remove."""
        del namespace, prefix
        return 0


class InMemoryLRUCache(CacheABC):
    """Process-local LRU cache with per-entry TTL (``cachetools.TTLCache``).

    Entries of every namespace share the ``max_entries`` bound. The cache is used from the
    event loop only, so it needs no locking; callers must store immutable values.

    Args:
        max_entries: Most entries kept (``0`` disables caching).
        ttl: Seconds an entry lives.
        timer: Monotonic time source (injectable for tests).
    """

    def __init__(
        self,
        max_entries: int = 1024,
        ttl: float = 600.0,
        *,
        timer: Callable[[], float] = time.monotonic,
    ) -> None:
        if max_entries < 0 or ttl <= 0:
            raise ConfigurationError("max_entries must be >= 0 and ttl > 0")
        self._max_entries = max_entries
        self._entries: TTLCache[tuple[str, str], Any] = TTLCache(
            maxsize=max(max_entries, 1), ttl=ttl, timer=timer
        )
        self._info = make_info(
            component="cache",
            implementation="in_memory_lru",
            version="1",
            config={"max_entries": max_entries, "ttl": ttl},
        )

    @property
    def info(self) -> ComponentInfo:
        """Component identity."""
        return self._info

    def __len__(self) -> int:
        self._entries.expire()
        return len(self._entries)

    async def get(self, namespace: str, key: str) -> Any | None:
        """Return a live entry and mark it recently used."""
        return self._entries.get((namespace, key))

    async def set(self, namespace: str, key: str, value: Any) -> None:
        """Store ``value``, evicting the least recently used entries beyond the bound."""
        if self._max_entries == 0:
            return
        self._entries[(namespace, key)] = value

    async def invalidate(self, namespace: str, prefix: str | None = None) -> int:
        """Remove the live entries of ``namespace`` whose key starts with ``prefix``."""
        self._entries.expire()
        doomed = [
            entry
            for entry in self._entries
            if entry[0] == namespace and (prefix is None or entry[1].startswith(prefix))
        ]
        for entry in doomed:
            del self._entries[entry]
        return len(doomed)

    async def close(self) -> None:
        """Drop every entry."""
        self._entries.clear()
