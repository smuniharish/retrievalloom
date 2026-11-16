"""Production-style custom components for RetrievalLoom, one per extension point.

Every class inherits the real ABC from ``retrievalloom.contracts`` and is verified by the
matching contract kit from ``retrievalloom.testing`` (see ``tests/live/test_example_*.py``
and ``tests/examples/test_adapters.py``).

Heavy optional imports (FlagEmbedding, neo4j, asyncpg, qdrant-client) are deferred to
attribute access, so ``from adapters import TTLCache`` works without them.
"""

from __future__ import annotations

import importlib
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .evidence import (
        DocumentDiversityConsolidator,
        NearDuplicateDeduplicator,
        PercentileNormalizer,
        WeightedCombSUMFusion,
    )
    from .graph import AliasGraphNormalizer, CapitalizedEntityExtractor
    from .infra import JsonlProvenanceRecorder, TenantRoleAccessPolicy, TTLCache
    from .models import BgeCrossEncoderReranker, BgeM3Embedding, UltraBertNLI
    from .neo4j_graph import Neo4jGraphStore
    from .pgvector import PgVectorStore
    from .sparse import BgeM3SparseIndex
    from .text import GlossaryKeywordDictionary, StemmingTokenizer

_EXPORTS = {
    "AliasGraphNormalizer": "graph",
    "BgeCrossEncoderReranker": "models",
    "BgeM3Embedding": "models",
    "BgeM3SparseIndex": "sparse",
    "CapitalizedEntityExtractor": "graph",
    "DocumentDiversityConsolidator": "evidence",
    "GlossaryKeywordDictionary": "text",
    "JsonlProvenanceRecorder": "infra",
    "NearDuplicateDeduplicator": "evidence",
    "Neo4jGraphStore": "neo4j_graph",
    "PercentileNormalizer": "evidence",
    "PgVectorStore": "pgvector",
    "StemmingTokenizer": "text",
    "TTLCache": "infra",
    "TenantRoleAccessPolicy": "infra",
    "UltraBertNLI": "models",
    "WeightedCombSUMFusion": "evidence",
}

__all__ = [
    "AliasGraphNormalizer",
    "BgeCrossEncoderReranker",
    "BgeM3Embedding",
    "BgeM3SparseIndex",
    "CapitalizedEntityExtractor",
    "DocumentDiversityConsolidator",
    "GlossaryKeywordDictionary",
    "JsonlProvenanceRecorder",
    "NearDuplicateDeduplicator",
    "Neo4jGraphStore",
    "PercentileNormalizer",
    "PgVectorStore",
    "StemmingTokenizer",
    "TTLCache",
    "TenantRoleAccessPolicy",
    "UltraBertNLI",
    "WeightedCombSUMFusion",
]


def __getattr__(name: str) -> Any:
    module = _EXPORTS.get(name)
    if module is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    return getattr(importlib.import_module(f".{module}", __name__), name)
