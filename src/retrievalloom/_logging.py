"""Library logging helpers built on structlog.

RetrievalLoom never configures structlog or the standard library logging system.
Loggers returned here are ``structlog.stdlib`` loggers, so events flow into the
``retrievalloom.*`` standard library loggers and are rendered by whatever
configuration the host application installs.
"""

from __future__ import annotations

import logging
from typing import Any

import structlog

ROOT_LOGGER_NAME = "retrievalloom"

logging.getLogger(ROOT_LOGGER_NAME).addHandler(logging.NullHandler())


def get_logger(name: str | None = None, **initial_values: Any) -> Any:
    """Return a structlog logger bound to a ``retrievalloom`` logger name.

    Args:
        name: Dotted suffix under ``retrievalloom``; ``None`` returns the root logger.
        **initial_values: Key-value context bound to the returned logger.

    Returns:
        A lazily configured structlog stdlib logger proxy.
    """
    full_name = ROOT_LOGGER_NAME if not name else f"{ROOT_LOGGER_NAME}.{name}"
    return structlog.stdlib.get_logger(full_name, **initial_values)
