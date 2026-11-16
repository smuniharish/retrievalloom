"""Custom text components: a stemming tokenizer and a per-tenant glossary dictionary."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import TYPE_CHECKING

from retrievalloom.contracts.component import ComponentInfo, make_info
from retrievalloom.contracts.models import KeywordDictionaryABC, KeywordEntry, TokenizerABC
from retrievalloom.errors import ConfigurationError, LifecycleError
from retrievalloom.index.text import fold
from retrievalloom.runtime.offloader import WorkloadKind

if TYPE_CHECKING:
    from collections.abc import Iterable, Sequence

    from retrievalloom.contracts.component import ComponentContext
    from retrievalloom.domain.inputs import Scope
    from retrievalloom.runtime.offloader import Offloader

_WORD = re.compile(r"\w+", re.UNICODE)
ENGLISH_STOPWORDS = frozenset(
    re.findall(
        r"\S+",
        "a an and are as at be by can do does for from has have how i in is it its my of on "
        "or our so that the this to was we what when where which why will with you your",
    )
)
# Longest suffix first; (suffix, replacement, minimum stem length).
_SUFFIXES: tuple[tuple[str, str, int], ...] = (
    ("ational", "ate", 3),
    ("ization", "ize", 3),
    ("fulness", "ful", 3),
    ("iveness", "ive", 3),
    ("ements", "", 4),
    ("ement", "", 4),
    ("ments", "", 4),
    ("ment", "", 4),
    ("ings", "", 3),
    ("ing", "", 3),
    ("ies", "y", 2),
    ("ied", "y", 2),
    ("edly", "", 3),
    ("ed", "", 3),
    ("s", "", 3),
)
_OFFLOAD_CHARS = 64_000


def stem(word: str) -> str:
    """Light English suffix stripping (``refunds``/``refunded``/``refunding`` -> ``refund``)."""
    if len(word) <= 3 or word.endswith("ss") or not word.isalpha():
        return word
    for suffix, replacement, minimum in _SUFFIXES:
        if word.endswith(suffix) and len(word) - len(suffix) >= minimum:
            return word[: len(word) - len(suffix)] + replacement
    return word


class StemmingTokenizer(TokenizerABC):
    """Casefolding word tokenizer with stop words and light English stemming.

    The engine shares one tokenizer between BM25 and fuzzy matching, so stemming makes
    "refunded" match "refunds" in BM25 while fuzzy still repairs typos ("refnd").
    Large batches (bulk ingestion) are tokenized off the event loop.

    Args:
        stopwords: Words removed after folding (default: a short English list).
        min_length: Shortest token kept.
    """

    def __init__(
        self, *, stopwords: Iterable[str] = ENGLISH_STOPWORDS, min_length: int = 2
    ) -> None:
        if min_length < 1:
            raise ConfigurationError("min_length must be at least 1")
        self._stopwords = frozenset(fold(word) for word in stopwords)
        self._min_length = min_length
        self._offloader: Offloader | None = None
        self._info = make_info(
            component="tokenizer",
            implementation="stemming-en",
            version="1",
            config={"stopwords": sorted(self._stopwords), "min_length": min_length},
        )

    @property
    def info(self) -> ComponentInfo:
        """Component identity (stop words are part of the fingerprint)."""
        return self._info

    async def initialize(self, context: ComponentContext) -> None:
        """Keep the offloader for large batches."""
        self._offloader = context.offloader

    def tokenize_one(self, text: str) -> list[str]:
        """Tokenize one text synchronously."""
        return [
            stem(token)
            for token in _WORD.findall(fold(text))
            if len(token) >= self._min_length and token not in self._stopwords
        ]

    def _batch(self, texts: Sequence[str]) -> list[list[str]]:
        return [self.tokenize_one(text) for text in texts]

    async def tokenize(self, texts: Sequence[str]) -> Sequence[Sequence[str]]:
        """Tokenize each text (offloaded above ~64k characters)."""
        if self._offloader is not None and sum(map(len, texts)) > _OFFLOAD_CHARS:
            return await self._offloader.run(WorkloadKind.SPARSE_INDEX, self._batch, list(texts))
        return self._batch(texts)


class GlossaryKeywordDictionary(KeywordDictionaryABC):
    """Per-tenant product glossary loaded from a JSON file.

    File format: ``{"*": [{"canonical": "...", "aliases": [...]}, ...], "<tenant>": [...]}``.
    Entries under ``"*"`` apply to every tenant; tenant entries are appended. The file is
    read off the event loop at ``initialize``; call ``reload()`` after editing it, then
    ``engine.rebuild(channel="index", method="keyword")`` so stored generations follow.

    Args:
        path: Glossary JSON file.
    """

    def __init__(self, path: str | Path) -> None:
        self._path = Path(path)
        self._entries: dict[str, tuple[KeywordEntry, ...]] | None = None
        self._offloader: Offloader | None = None
        self._info = make_info(
            component="keyword_dictionary",
            implementation="glossary-json",
            version="1",
            config={"path": self._path.name},
        )

    @property
    def info(self) -> ComponentInfo:
        """Component identity."""
        return self._info

    def _read(self) -> dict[str, tuple[KeywordEntry, ...]]:
        raw = json.loads(self._path.read_text(encoding="utf-8"))
        if not isinstance(raw, dict):
            raise ConfigurationError("glossary must be a JSON object keyed by tenant")
        return {
            str(tenant): tuple(KeywordEntry.model_validate(entry) for entry in entries)
            for tenant, entries in raw.items()
        }

    async def initialize(self, context: ComponentContext) -> None:
        """Load the glossary (idempotent)."""
        self._offloader = context.offloader
        if self._entries is None:
            await self.reload()

    async def reload(self) -> None:
        """Re-read the glossary file."""
        if self._offloader is None:
            raise LifecycleError("glossary dictionary is not initialised")
        try:
            self._entries = await self._offloader.run(WorkloadKind.IO_RETRIEVAL, self._read)
        except FileNotFoundError as exc:
            raise ConfigurationError(f"glossary not found: {self._path}") from exc

    async def entries(self, scope: Scope) -> Sequence[KeywordEntry]:
        """Shared entries followed by the tenant's own."""
        if self._entries is None:
            raise LifecycleError("glossary dictionary is not initialised")
        return (*self._entries.get("*", ()), *self._entries.get(scope.tenant_id, ()))
