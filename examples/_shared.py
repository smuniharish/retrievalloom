"""Helpers shared by the example scripts (not part of the library)."""

from __future__ import annotations

import json
import logging
import os
import sys
import uuid
from pathlib import Path
from typing import TYPE_CHECKING, Any

import structlog

if TYPE_CHECKING:
    from retrievalloom import Evidence

DATA = Path(__file__).resolve().parent / "data"


def env(name: str) -> str | None:
    """``RL_EXAMPLES_<name>``, falling back to the test suite's ``RL_TEST_<name>``."""
    return os.environ.get(f"RL_EXAMPLES_{name}") or os.environ.get(f"RL_TEST_{name}")


def models_dir() -> Path:
    """Directory holding the local model weights (default ``models``)."""
    return Path(env("MODELS_DIR") or "models").resolve()


def require(*names: str) -> dict[str, str] | None:
    """Values of the required ``RL_EXAMPLES_<name>`` settings, or ``None`` (and a notice).

    ``MODELS_DIR`` is satisfied when the local model directory exists.
    """
    found: dict[str, str] = {}
    for name in names:
        value = str(models_dir()) if name == "MODELS_DIR" and models_dir().is_dir() else env(name)
        if not value:
            print(f"skipped: set RL_EXAMPLES_{name} (or RL_TEST_{name}) to run this example")
            return None
        found[name] = value
    return found


def load(name: str) -> Any:
    """Parse ``examples/data/<name>`` (JSON)."""
    return json.loads((DATA / name).read_text(encoding="utf-8"))


def run_prefix(name: str) -> str:
    """Unique, lowercase backend prefix for one run (tables, collections, labels)."""
    return f"rl_{name}_{uuid.uuid4().hex[:8]}"


async def drop_qdrant_collections(url: str, prefix: str) -> None:
    """Delete the Qdrant server collections created by one example run."""
    from qdrant_client import AsyncQdrantClient

    client = AsyncQdrantClient(url=url)
    try:
        for collection in (await client.get_collections()).collections:
            if collection.name.startswith(prefix):
                await client.delete_collection(collection.name)
    finally:
        await client.close()


def quiet_logs() -> None:
    """Send only library warnings and errors to stderr, so stdout stays readable."""
    structlog.configure(
        wrapper_class=structlog.make_filtering_bound_logger(logging.WARNING),
        logger_factory=structlog.PrintLoggerFactory(sys.stderr),
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)


def methods(item: Evidence) -> str:
    """Retrieval methods (``channel.method``) that produced ``item``."""
    return ", ".join(sorted({hit.method.key for hit in item.provenance.hits}))


def doc_ids(items: Any) -> list[str]:
    """Source document id of each evidence item, in rank order."""
    return [item.sources[0].document_id for item in items]
