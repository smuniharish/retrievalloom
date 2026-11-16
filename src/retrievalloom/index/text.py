"""Default tokenizer, keyword dictionaries and the dictionary phrase matcher."""

from __future__ import annotations

import re
import unicodedata
from typing import TYPE_CHECKING

import ahocorasick_rs

from retrievalloom.contracts.component import ComponentInfo, make_info
from retrievalloom.contracts.models import KeywordDictionaryABC, KeywordEntry, TokenizerABC
from retrievalloom.domain.records import DocumentState
from retrievalloom.errors import ConfigurationError

if TYPE_CHECKING:
    from collections.abc import Iterable, Sequence

    from retrievalloom.contracts.component import ComponentContext
    from retrievalloom.domain.inputs import Scope

_WORD = re.compile(r"\w+", re.UNICODE)


def fold(text: str) -> str:
    """NFC-normalise and casefold ``text`` (the matching normal form)."""
    return unicodedata.normalize("NFC", text).casefold()


class SimpleTokenizer(TokenizerABC):
    """Unicode word tokenizer: NFC, casefold, regex word runs, optional stopwords.

    Args:
        stopwords: Words removed after folding.
        min_length: Shortest token kept.
    """

    def __init__(self, *, stopwords: Iterable[str] = (), min_length: int = 1) -> None:
        if min_length < 1:
            raise ConfigurationError("min_length must be at least 1")
        self._stopwords = frozenset(fold(word) for word in stopwords)
        self._min_length = min_length
        self._info = make_info(
            component="tokenizer",
            implementation="simple",
            version="1",
            config={"stopwords": sorted(self._stopwords), "min_length": min_length},
        )

    @property
    def info(self) -> ComponentInfo:
        """Component identity."""
        return self._info

    def tokenize_one(self, text: str) -> list[str]:
        """Tokenize one text synchronously (pure, cheap)."""
        return [
            token
            for token in _WORD.findall(fold(text))
            if len(token) >= self._min_length and token not in self._stopwords
        ]

    async def tokenize(self, texts: Sequence[str]) -> Sequence[Sequence[str]]:
        """Tokenize each text."""
        return [self.tokenize_one(text) for text in texts]


class StaticKeywordDictionary(KeywordDictionaryABC):
    """Fixed keyword dictionary shared by every scope.

    Args:
        entries: Dictionary entries.
    """

    def __init__(self, entries: Iterable[KeywordEntry] = ()) -> None:
        self._entries = tuple(entries)
        self._info = make_info(
            component="keyword_dictionary",
            implementation="static",
            version="1",
            config=[entry.model_dump(mode="json") for entry in self._entries],
        )

    @property
    def info(self) -> ComponentInfo:
        """Component identity."""
        return self._info

    async def entries(self, scope: Scope) -> Sequence[KeywordEntry]:
        """Return the static entries."""
        del scope
        return self._entries


class MetadataKeywordDictionary(KeywordDictionaryABC):
    """Dictionary built from document metadata fields plus static entries.

    Every string (or list of strings) stored under one of ``fields`` in an active
    document's metadata becomes a canonical keyword of that scope.

    Args:
        fields: Metadata fields holding keywords.
        static_entries: Entries added to every scope.
    """

    def __init__(
        self,
        fields: Sequence[str] = ("keywords",),
        static_entries: Iterable[KeywordEntry] = (),
    ) -> None:
        self._fields = tuple(fields)
        self._static = tuple(static_entries)
        self._store = None
        self._info = make_info(
            component="keyword_dictionary",
            implementation="metadata",
            version="1",
            config={
                "fields": list(self._fields),
                "static": [entry.model_dump(mode="json") for entry in self._static],
            },
        )

    @property
    def info(self) -> ComponentInfo:
        """Component identity."""
        return self._info

    async def initialize(self, context: ComponentContext) -> None:
        """Bind the engine metadata store."""
        self._store = context.metadata_store

    async def entries(self, scope: Scope) -> Sequence[KeywordEntry]:
        """Collect keywords of active documents plus static entries."""
        terms: dict[str, KeywordEntry] = {fold(entry.canonical): entry for entry in self._static}
        if self._store is not None and self._fields:
            after: str | None = None
            while True:
                async with self._store.transaction(scope, write=False) as tx:
                    page = await tx.list_documents(
                        scope.key, state=DocumentState.ACTIVE, after_id=after, limit=500
                    )
                for document in page:
                    for value in _strings(document.metadata, self._fields):
                        terms.setdefault(fold(value), KeywordEntry(canonical=value))
                if len(page) < 500:
                    break
                after = page[-1].document_id
        return [terms[key] for key in sorted(terms)]


def _strings(metadata: dict[str, object], fields: Sequence[str]) -> list[str]:
    found: list[str] = []
    for name in fields:
        value = metadata.get(name)
        values = value if isinstance(value, list) else [value]
        found.extend(item.strip() for item in values if isinstance(item, str) and item.strip())
    return found


def _is_word_char(character: str) -> bool:
    return character.isalnum() or character == "_"


class PhraseMatcher:
    """Aho-Corasick phrase matcher over folded text with word-boundary filtering.

    Args:
        phrases: Mapping of surface form to the canonical value it denotes.
    """

    def __init__(self, phrases: dict[str, str]) -> None:
        folded: dict[str, str] = {}
        for surface, canonical in phrases.items():
            key = fold(surface).strip()
            if key:
                folded.setdefault(key, canonical)
        self._patterns = sorted(folded)
        self._canonical = [folded[pattern] for pattern in self._patterns]
        self._automaton = (
            ahocorasick_rs.AhoCorasick(
                self._patterns, matchkind=ahocorasick_rs.MatchKind.LeftmostLongest
            )
            if self._patterns
            else None
        )

    @property
    def empty(self) -> bool:
        """Whether the matcher has no phrases."""
        return self._automaton is None

    def find(self, text: str) -> list[tuple[str, str, int, int]]:
        """Return ``(canonical, surface, start, end)`` word-bounded matches in ``text``."""
        if self._automaton is None:
            return []
        folded = fold(text)
        matches: list[tuple[str, str, int, int]] = []
        for index, start, end in self._automaton.find_matches_as_indexes(folded):
            before = folded[start - 1] if start > 0 else ""
            after = folded[end] if end < len(folded) else ""
            if (before and _is_word_char(before)) or (after and _is_word_char(after)):
                continue
            matches.append((self._canonical[index], self._patterns[index], start, end))
        return matches
