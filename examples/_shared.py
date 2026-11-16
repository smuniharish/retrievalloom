"""Helpers shared by the example scripts (not part of the library)."""

from __future__ import annotations

import logging
import os
import sys
from pathlib import Path
from typing import TYPE_CHECKING

import structlog

if TYPE_CHECKING:
    from retrievalloom import Evidence


def env(name: str) -> str | None:
    """``RL_EXAMPLES_<name>``, falling back to the test suite's ``RL_TEST_<name>``."""
    return os.environ.get(f"RL_EXAMPLES_{name}") or os.environ.get(f"RL_TEST_{name}")


def models_dir() -> Path:
    """Directory holding the local model weights (default ``models``)."""
    return Path(env("MODELS_DIR") or "models").resolve()


def quiet_logs() -> None:
    """Send only library warnings and errors to stderr, so stdout stays readable."""
    structlog.configure(
        wrapper_class=structlog.make_filtering_bound_logger(logging.WARNING),
        logger_factory=structlog.PrintLoggerFactory(sys.stderr),
    )


def methods(item: Evidence) -> str:
    """Retrieval methods (``channel.method``) that produced ``item``."""
    return ", ".join(sorted({hit.method.key for hit in item.provenance.hits}))
