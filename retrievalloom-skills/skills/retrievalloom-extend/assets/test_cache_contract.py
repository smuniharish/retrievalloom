"""Run RetrievalLoom's cache contract against your cache.

Copy this file next to your cache and change the import. ``make_component``
must return an empty cache for every test; for Redis, use a disposable
database or a unique key prefix here. Set ``stores_values = False`` only for
a cache that never stores anything.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from cache_template import DictCache

from retrievalloom.testing import CacheContract

if TYPE_CHECKING:
    from pathlib import Path


class TestDictCache(CacheContract):
    async def make_component(self, tmp_path: Path) -> DictCache:
        return DictCache()
