# Contracts

Every pluggable component subclasses one of these `abc.ABC` classes. See
[Write a custom backend](../guide/how-to/custom-backends.md).

## Component base

::: retrievalloom.contracts.component.ComponentABC

::: retrievalloom.contracts.component.ComponentContext

::: retrievalloom.contracts.component.ComponentInfo

::: retrievalloom.contracts.component.ComponentFingerprint

::: retrievalloom.contracts.component.make_info

## Models

::: retrievalloom.contracts.models.EmbeddingModelABC

::: retrievalloom.contracts.models.RerankerABC

::: retrievalloom.contracts.models.RerankItem

::: retrievalloom.contracts.models.RerankScore

::: retrievalloom.contracts.models.NLIABC

::: retrievalloom.contracts.models.NLIPair

::: retrievalloom.contracts.models.TokenizerABC

::: retrievalloom.contracts.models.KeywordDictionaryABC

::: retrievalloom.contracts.models.KeywordEntry

## Graph models

::: retrievalloom.contracts.models.GraphExtractorABC

::: retrievalloom.contracts.models.ExtractionSource

::: retrievalloom.contracts.models.GraphExtraction

::: retrievalloom.contracts.models.GraphNormalizerABC

::: retrievalloom.contracts.models.NormalizedGraph

::: retrievalloom.contracts.models.NormalizedEntity

::: retrievalloom.contracts.models.NormalizedRelationship

## Indexes

::: retrievalloom.contracts.indexes.GenerationalIndexABC

::: retrievalloom.contracts.indexes.IndexCapabilities

::: retrievalloom.contracts.indexes.GenerationSpec

::: retrievalloom.contracts.indexes.IndexItem

::: retrievalloom.contracts.indexes.WriteReport

::: retrievalloom.contracts.indexes.GenerationStats

::: retrievalloom.contracts.indexes.KeywordIndexABC

::: retrievalloom.contracts.indexes.KeywordQuery

::: retrievalloom.contracts.indexes.FuzzyIndexABC

::: retrievalloom.contracts.indexes.FuzzyQuery

::: retrievalloom.contracts.indexes.SparseIndexABC

::: retrievalloom.contracts.indexes.SparseQuery

::: retrievalloom.contracts.indexes.VectorStoreABC

::: retrievalloom.contracts.indexes.GraphStoreABC

::: retrievalloom.contracts.indexes.GraphNode

::: retrievalloom.contracts.indexes.GraphSubgraph

::: retrievalloom.contracts.indexes.GraphRetrieverABC

::: retrievalloom.contracts.indexes.StructureEngineABC

::: retrievalloom.contracts.indexes.NavigationDirection

## Evidence

::: retrievalloom.contracts.evidence.ScoreNormalizerABC

::: retrievalloom.contracts.evidence.FusionABC

::: retrievalloom.contracts.evidence.RankedEntry

::: retrievalloom.contracts.evidence.FusedEntry

::: retrievalloom.contracts.evidence.DeduplicatorABC

::: retrievalloom.contracts.evidence.DedupResult

::: retrievalloom.contracts.evidence.EvidenceConsolidatorABC

::: retrievalloom.contracts.evidence.ConsolidationContext

## Infrastructure

::: retrievalloom.contracts.infra.CacheABC

::: retrievalloom.contracts.infra.AccessPolicyABC

::: retrievalloom.contracts.infra.AccessDecision

::: retrievalloom.contracts.infra.Action

::: retrievalloom.contracts.infra.ProvenanceRecorderABC
