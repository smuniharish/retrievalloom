"""Reusable pytest contract kits for RetrievalLoom adapters.

Subclass a kit, implement ``make_component`` (and the kit's abstract hooks), and pytest
collects the inherited contract tests. Requires the ``testing`` extra.
"""

from retrievalloom.testing._base import ComponentContract
from retrievalloom.testing.evidence import (
    DeduplicatorContract,
    EvidenceConsolidatorContract,
    FusionContract,
    ScoreNormalizerContract,
)
from retrievalloom.testing.indexes import (
    FuzzyIndexContract,
    GenerationalIndexContract,
    GraphStoreContract,
    KeywordIndexContract,
    SparseIndexContract,
    StructureEngineContract,
    VectorStoreContract,
)
from retrievalloom.testing.infra import CacheContract
from retrievalloom.testing.models import (
    EmbeddingModelContract,
    GraphExtractorContract,
    NLIContract,
    RerankerContract,
)
from retrievalloom.testing.storage import MetadataStoreContract

__all__ = [
    "CacheContract",
    "ComponentContract",
    "DeduplicatorContract",
    "EmbeddingModelContract",
    "EvidenceConsolidatorContract",
    "FusionContract",
    "FuzzyIndexContract",
    "GenerationalIndexContract",
    "GraphExtractorContract",
    "GraphStoreContract",
    "KeywordIndexContract",
    "MetadataStoreContract",
    "NLIContract",
    "RerankerContract",
    "ScoreNormalizerContract",
    "SparseIndexContract",
    "StructureEngineContract",
    "VectorStoreContract",
]
