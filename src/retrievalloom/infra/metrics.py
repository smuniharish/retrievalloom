"""Metrics sink used when the application does not provide one."""

from __future__ import annotations

from typing import TYPE_CHECKING

from refresh_engine import MetricsSink

if TYPE_CHECKING:
    from collections.abc import Mapping


class NullMetrics(MetricsSink):
    """Discards every metric."""

    async def increment(
        self, name: str, value: int = 1, attributes: Mapping[str, str] | None = None, /
    ) -> None:
        """Ignore a counter increment."""

    async def observe(
        self, name: str, value: float, attributes: Mapping[str, str] | None = None, /
    ) -> None:
        """Ignore an observation."""


__all__ = ["NullMetrics"]
