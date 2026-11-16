"""Process-wide model loading shared by the model adapters.

Loading bge-m3 takes seconds to minutes and ~2 GB of RAM, so the dense embedding and the
learned-sparse index share one ``BGEM3FlagModel``. Hugging Face fast tokenizers are not
safe for concurrent use from several threads, so every model call holds the model's lock.
"""

from __future__ import annotations

import os
import threading
from pathlib import Path
from typing import TYPE_CHECKING, Any

from retrievalloom._logging import get_logger

if TYPE_CHECKING:
    from collections.abc import Callable

_log = get_logger("examples.adapters")
_LOADED: dict[tuple[str, Path], LoadedModel] = {}
_GUARD = threading.Lock()


class LoadedModel:
    """A loaded model plus the lock serialising calls into it."""

    def __init__(self, model: Any) -> None:
        self.model = model
        self.lock = threading.Lock()


def offline() -> None:
    """Forbid Hugging Face downloads: every model must already be on disk."""
    os.environ.setdefault("HF_HUB_OFFLINE", "1")
    os.environ.setdefault("TRANSFORMERS_OFFLINE", "1")


def shared(kind: str, path: Path, load: Callable[[], Any]) -> LoadedModel:
    """Load ``kind`` from ``path`` once per process (blocking; call it off the event loop)."""
    key = (kind, Path(path).resolve())
    with _GUARD:
        found = _LOADED.get(key)
        if found is None:
            if not key[1].is_dir():
                raise FileNotFoundError(f"model directory not found: {key[1]}")
            offline()
            _log.info("model.loading", kind=kind, path=str(key[1]))
            found = LoadedModel(load())
            _LOADED[key] = found
        return found


def bge_m3(path: Path) -> LoadedModel:
    """The shared ``BGEM3FlagModel`` (dense and lexical heads) on CPU."""

    def load() -> Any:
        from FlagEmbedding import BGEM3FlagModel

        return BGEM3FlagModel(str(path), use_fp16=False, devices="cpu")

    return shared("bge-m3", path, load)


def bge_reranker(path: Path) -> LoadedModel:
    """The shared ``FlagReranker`` cross-encoder on CPU."""

    def load() -> Any:
        from FlagEmbedding import FlagReranker

        return FlagReranker(str(path), use_fp16=False, devices="cpu")

    return shared("bge-reranker", path, load)


def sequence_classifier(path: Path, *, trust_remote_code: bool = False) -> LoadedModel:
    """A transformers ``(tokenizer, model)`` pair for sequence classification (eval mode)."""

    def load() -> Any:
        from transformers import AutoModelForSequenceClassification, AutoTokenizer

        tokenizer = AutoTokenizer.from_pretrained(path)
        # Local weights only; UltraBERT ships its (reviewed) modeling code with the model.
        model = AutoModelForSequenceClassification.from_pretrained(
            path, trust_remote_code=trust_remote_code
        ).eval()
        return tokenizer, model

    return shared("sequence-classifier", path, load)
