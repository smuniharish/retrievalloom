"""Helpers shared by test modules (the session resource manager registry)."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from resourcera import ResourceManager

SHARED: list[ResourceManager] = []


def shared_manager() -> ResourceManager:
    """The session manager (available once the session fixture ran)."""
    return SHARED[0]


class SharedResources:
    """Mixin for contract kits: reuse the session resource manager."""

    def resource_manager(self) -> ResourceManager:
        """The started session manager."""
        return shared_manager()
