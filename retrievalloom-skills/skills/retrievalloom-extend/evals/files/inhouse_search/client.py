"""Client of our in-house search service: the starting point of eval 1.

The real client talks HTTP; this stand-in keeps everything in memory so the
eval runs offline. Every call is blocking.
"""

from __future__ import annotations

import math
import threading
from collections import Counter
from typing import Any


class InhouseSearchClient:
    """Blocking client: one named collection per call site, documents with terms and tags."""

    def __init__(self, base_url: str, api_key: str) -> None:
        self.base_url = base_url
        self._api_key = api_key
        self._lock = threading.Lock()
        self._collections: dict[str, dict[str, tuple[list[str], dict[str, object]]]] = {}

    def create_collection(self, name: str) -> None:
        """Create a collection; raises KeyError if it already exists."""
        with self._lock:
            if name in self._collections:
                raise KeyError(f"collection {name} exists")
            self._collections[name] = {}

    def delete_collection(self, name: str) -> None:
        """Delete a collection; raises KeyError if it is missing."""
        with self._lock:
            del self._collections[name]

    def collections(self) -> list[str]:
        """Names of every collection."""
        with self._lock:
            return sorted(self._collections)

    def upsert(self, name: str, docs: list[dict[str, Any]]) -> None:
        """Insert or replace documents: {"id": str, "terms": list[str], "tags": dict}."""
        with self._lock:
            collection = self._collections[name]
            for doc in docs:
                terms = [str(term) for term in doc["terms"]]
                collection[str(doc["id"])] = (terms, dict(doc.get("tags", {})))

    def delete(self, name: str, ids: list[str]) -> int:
        """Delete documents by id; returns how many existed."""
        with self._lock:
            collection = self._collections[name]
            return sum(collection.pop(doc_id, None) is not None for doc_id in ids)

    def set_tags(self, name: str, doc_id: str, tags: dict[str, object]) -> bool:
        """Replace one document's tags; False if the document is missing."""
        with self._lock:
            collection = self._collections[name]
            if doc_id not in collection:
                return False
            collection[doc_id] = (collection[doc_id][0], dict(tags))
            return True

    def count(self, name: str) -> int:
        """Number of documents in a collection."""
        with self._lock:
            return len(self._collections[name])

    def search(
        self, name: str, terms: list[str], tags: dict[str, object] | None = None, size: int = 10
    ) -> list[tuple[str, float]]:
        """BM25 search; ``tags`` keeps documents whose tags equal every given value."""
        with self._lock:
            rows = list(self._collections[name].items())
        if not rows or not terms:
            return []
        average = sum(len(t) for _, (t, _) in rows) / len(rows) or 1.0
        frequency = Counter(term for _, (t, _) in rows for term in set(t))
        scored = []
        for doc_id, (doc_terms, doc_tags) in rows:
            if tags and any(doc_tags.get(k) != v for k, v in tags.items()):
                continue
            counts = Counter(doc_terms)
            score = 0.0
            for term in dict.fromkeys(terms):
                if counts[term]:
                    idf = math.log(
                        1 + (len(rows) - frequency[term] + 0.5) / (frequency[term] + 0.5)
                    )
                    norm = 1.2 * (0.25 + 0.75 * len(doc_terms) / average)
                    score += idf * counts[term] * 2.2 / (counts[term] + norm)
            if score > 0:
                scored.append((doc_id, score))
        scored.sort(key=lambda row: (-row[1], row[0]))
        return scored[:size]
