"""Contract kit for ``CacheABC``."""

from __future__ import annotations

from typing import Any, ClassVar

from retrievalloom.testing._base import ComponentContract


class CacheContract(ComponentContract):
    """Kit for ``CacheABC``.

    Attributes:
        stores_values: ``False`` for no-op caches, which must always miss.
    """

    stores_values: ClassVar[bool] = True

    async def test_set_get_and_namespace_isolation(self, component: Any) -> None:
        """Values round-trip per namespace and never leak across namespaces."""
        await component.set("ns-a", "key", {"value": 1})
        assert await component.get("ns-b", "key") is None
        expected = {"value": 1} if self.stores_values else None
        assert await component.get("ns-a", "key") == expected
        assert await component.get("ns-a", "missing") is None

    async def test_invalidate_by_prefix_and_namespace(self, component: Any) -> None:
        """Prefix invalidation removes only matching keys and reports the count."""
        for key in ("user:1", "user:2", "group:1"):
            await component.set("ns", key, key)
        await component.set("other", "user:1", "kept")
        removed = await component.invalidate("ns", "user:")
        assert removed == (2 if self.stores_values else 0)
        assert await component.get("ns", "user:1") is None
        if self.stores_values:
            assert await component.get("ns", "group:1") == "group:1"
            assert await component.get("other", "user:1") == "kept"
        assert await component.invalidate("ns") == (1 if self.stores_values else 0)
        assert await component.get("ns", "group:1") is None
