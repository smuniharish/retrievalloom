"""A cache adapter to adapt to Redis, memcached or another key-value store.

Copy this file and replace each ``self._entries`` operation with the call its
comment names. The engine builds every namespace and key, and they include the
scope, so the cache never decides isolation itself: it must keep namespaces
apart and support prefix invalidation. Keys include the store's commit
sequence, so a write makes older entries unreachable; a TTL bounds their
lifetime. The engine stores only ``complete`` results, as Python objects:
serialize them so that ``get`` returns an equal object of the same type
(pickle works). A value of another type counts as a miss.

Prove it with ``test_cache_contract.py`` (``CacheContract``), then pass it as
``Components(cache=DictCache())``.
"""

from __future__ import annotations

import copy
from typing import TYPE_CHECKING, Any

from retrievalloom.contracts.component import make_info
from retrievalloom.contracts.infra import CacheABC

if TYPE_CHECKING:
    from retrievalloom.contracts.component import ComponentInfo


class DictCache(CacheABC):
    """Namespaced dictionary cache; one key space per namespace."""

    def __init__(self, *, key_prefix: str = "rl") -> None:
        self._key_prefix = key_prefix
        self._entries: dict[tuple[str, str], Any] = {}
        self._info = make_info(component="cache", implementation="dict-cache", version="1")

    @property
    def info(self) -> ComponentInfo:
        return self._info

    async def close(self) -> None:
        # Close a client you created; never one the application passed in.
        self._entries.clear()

    async def get(self, namespace: str, key: str) -> Any | None:
        # GET f"{key_prefix}:{namespace}:{key}"; deserialize. A miss returns None.
        value = self._entries.get((namespace, key))
        return copy.deepcopy(value)

    async def set(self, namespace: str, key: str, value: Any) -> None:
        # SET f"{key_prefix}:{namespace}:{key}" with your TTL; serialize the value.
        self._entries[(namespace, key)] = copy.deepcopy(value)

    async def invalidate(self, namespace: str, prefix: str | None = None) -> int:
        # SCAN f"{key_prefix}:{namespace}:{prefix or ''}*" and DEL; return how many were removed.
        doomed = [
            entry
            for entry in self._entries
            if entry[0] == namespace and (prefix is None or entry[1].startswith(prefix))
        ]
        for entry in doomed:
            del self._entries[entry]
        return len(doomed)
