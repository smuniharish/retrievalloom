"""Default tokenizer, keyword dictionaries and the dictionary phrase matcher."""

from __future__ import annotations

import re
import unicodedata
from collections import Counter
from typing import TYPE_CHECKING

import ahocorasick_rs

from retrievalloom.contracts.component import ComponentInfo, make_info
from retrievalloom.contracts.models import KeywordDictionaryABC, KeywordEntry, TokenizerABC
from retrievalloom.domain.hashing import digest_json
from retrievalloom.domain.records import DocumentState
from retrievalloom.errors import ConfigurationError
from retrievalloom.storage.base import KeywordDictionaryState

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping, Sequence

    from retrievalloom.contracts.component import ComponentContext
    from retrievalloom.domain.inputs import Scope
    from retrievalloom.domain.records import DocumentRecord
    from retrievalloom.storage.base import DocumentMetadataStoreABC, MetadataTransactionABC

_WORD = re.compile(r"\w+", re.UNICODE)
_PAGE = 5000


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
    document's metadata becomes a keyword of that scope. Surface forms that fold to
    the same keyword make one entry whose canonical form is the smallest of them;
    static entries win over metadata ones.

    The metadata store keeps the dictionary's terms with their document counts. The
    first use in a scope registers the dictionary, reading every active document once;
    from then on ingestion and deletion adjust the counts in their own transactions
    (:func:`track_keyword_changes`), so :meth:`entries` reads only the terms and
    :meth:`revision` a single row.

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
        self._store: DocumentMetadataStoreABC | None = None
        self._dictionary_id = digest_json(["metadata-keywords", 1, list(self._fields)])
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
        """Keywords of active documents plus static entries, sorted by folded form."""
        terms: dict[str, KeywordEntry] = {fold(entry.canonical): entry for entry in self._static}
        if self._store is not None and self._fields:
            await self._state(self._store, scope)
            async with self._store.transaction(scope, write=False) as tx:
                stored = await tx.list_keyword_terms(scope.key, self._dictionary_id)
            for value in sorted(stored):
                terms.setdefault(fold(value), KeywordEntry(canonical=value))
        return [terms[key] for key in sorted(terms)]

    async def revision(self, scope: Scope) -> str:
        """The store's dictionary revision (changes whenever a term appears or vanishes)."""
        if self._store is None or not self._fields:
            return await super().revision(scope)
        state = await self._state(self._store, scope)
        return f"{self._dictionary_id}:{state.revision}"

    async def _state(self, store: DocumentMetadataStoreABC, scope: Scope) -> KeywordDictionaryState:
        """The dictionary's stored state, registering it on first use in ``scope``."""
        async with store.transaction(scope, write=False) as tx:
            state = _find(await tx.list_keyword_dictionaries(scope.key), self._dictionary_id)
        if state is not None:
            return state
        # Writers of a scope are serialised, so no document changes between the scan and
        # the registration, and every later change adjusts the registered counts.
        async with store.transaction(scope, write=True) as tx:
            state = _find(await tx.list_keyword_dictionaries(scope.key), self._dictionary_id)
            if state is not None:
                return state
            counts: Counter[str] = Counter()
            after: str | None = None
            while True:
                page = await tx.list_document_metadata(
                    scope.key, state=DocumentState.ACTIVE, after_id=after, limit=_PAGE
                )
                for _, metadata in page:
                    counts.update(set(_strings(metadata, self._fields)))
                if len(page) < _PAGE:
                    break
                after = page[-1][0]
            await tx.create_keyword_dictionary(
                scope.key, self._dictionary_id, self._fields, dict(counts)
            )
        return KeywordDictionaryState(
            dictionary_id=self._dictionary_id, fields=self._fields, revision=1
        )


def _find(
    states: Sequence[KeywordDictionaryState], dictionary_id: str
) -> KeywordDictionaryState | None:
    return next((state for state in states if state.dictionary_id == dictionary_id), None)


async def track_keyword_changes(
    transaction: MetadataTransactionABC,
    scope_key: str,
    changes: Sequence[tuple[DocumentRecord | None, DocumentRecord]],
) -> None:
    """Adjust the term counts of every metadata keyword dictionary of a scope.

    Call it in the transaction that writes the documents, with ``(before, after)``
    records of each written document (``before`` is ``None`` for a new document).
    """
    if not changes:
        return
    for state in await transaction.list_keyword_dictionaries(scope_key):
        deltas: Counter[str] = Counter()
        for before, after in changes:
            old = _active_terms(before, state.fields)
            new = _active_terms(after, state.fields)
            deltas.update(new - old)
            deltas.subtract(old - new)
        if any(deltas.values()):
            await transaction.adjust_keyword_terms(scope_key, state.dictionary_id, deltas)


def _active_terms(record: DocumentRecord | None, fields: Sequence[str]) -> set[str]:
    if record is None or record.state != DocumentState.ACTIVE:
        return set()
    return set(_strings(record.metadata, fields))


def _strings(metadata: Mapping[str, object], fields: Sequence[str]) -> list[str]:
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
