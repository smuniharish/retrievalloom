"""Library logging helpers built on structlog.

RetrievalLoom never configures structlog or the standard library logging system.

- When the host application has configured structlog, events go through that
  configuration (``structlog.stdlib.get_logger``) under ``retrievalloom.*`` names.
- Otherwise structlog's default would print to stdout, so events are instead rendered
  as ``key=value`` text into the ``retrievalloom.*`` standard library loggers. A
  ``NullHandler`` on ``retrievalloom`` keeps them silent until the application adds
  handlers or levels for them.
"""

from __future__ import annotations

import logging
from typing import Any

import structlog

ROOT_LOGGER_NAME = "retrievalloom"

logging.getLogger(ROOT_LOGGER_NAME).addHandler(logging.NullHandler())

_FALLBACK_PROCESSORS: list[Any] = [
    structlog.stdlib.filter_by_level,
    structlog.processors.format_exc_info,
    structlog.processors.KeyValueRenderer(key_order=["event"], sort_keys=True),
]


class _LibraryLogger:
    """Bound logger that picks structlog's or the stdlib route at every call."""

    __slots__ = ("_context", "_name")

    def __init__(self, name: str, context: dict[str, Any]) -> None:
        self._name = name
        self._context = context

    def bind(self, **values: Any) -> _LibraryLogger:
        return _LibraryLogger(self._name, {**self._context, **values})

    def _target(self) -> Any:
        if structlog.is_configured():
            return structlog.stdlib.get_logger(self._name, **self._context)
        return structlog.wrap_logger(
            logging.getLogger(self._name),
            processors=_FALLBACK_PROCESSORS,
            wrapper_class=structlog.stdlib.BoundLogger,
            **self._context,
        )

    def __getattr__(self, method: str) -> Any:
        return getattr(self._target(), method)


def get_logger(name: str | None = None, **initial_values: Any) -> Any:
    """Return a structlog-style logger bound to a ``retrievalloom`` logger name.

    Args:
        name: Dotted suffix under ``retrievalloom``; ``None`` returns the root logger.
        **initial_values: Key-value context bound to the returned logger.

    Returns:
        A logger with ``bind`` and the usual level methods (``info``, ``warning``, ...).
    """
    full_name = ROOT_LOGGER_NAME if not name else f"{ROOT_LOGGER_NAME}.{name}"
    return _LibraryLogger(full_name, dict(initial_values))
