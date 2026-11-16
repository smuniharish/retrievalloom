"""Metadata storage: the authoritative store for identity, lifecycle and visibility.

``SQLiteMetadataStore`` (default) and ``PostgresMetadataStore`` (``postgres`` extra) are
imported on first access.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from retrievalloom.storage.postgres import PostgresMetadataStore
    from retrievalloom.storage.sqlite import SQLiteMetadataStore

__all__ = ["PostgresMetadataStore", "SQLiteMetadataStore"]


def __getattr__(name: str) -> Any:
    if name == "PostgresMetadataStore":
        from retrievalloom.storage.postgres import PostgresMetadataStore

        return PostgresMetadataStore
    if name == "SQLiteMetadataStore":
        from retrievalloom.storage.sqlite import SQLiteMetadataStore

        return SQLiteMetadataStore
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
