# Built-in components

Default implementations of the [contracts](contracts.md). The engine creates the
defaults listed in [Engine and components](../architecture/engine.md#default-components);
the others you construct and pass in `Components`.

## Text and keywords

::: retrievalloom.index.text.SimpleTokenizer

::: retrievalloom.index.text.MetadataKeywordDictionary

::: retrievalloom.index.text.StaticKeywordDictionary

## Index methods

::: retrievalloom.index.keyword.AhoCorasickKeywordIndex

::: retrievalloom.index.fuzzy.RapidFuzzIndex

::: retrievalloom.index.sparse.BM25sIndex

::: retrievalloom.index.vector.QdrantVectorStore

## Graph channel

::: retrievalloom.graph.ladybug_store.LadybugGraphStore

::: retrievalloom.graph.retriever.NeighborhoodGraphRetriever

::: retrievalloom.graph.normalizer.IdentityGraphNormalizer

## Structure channel

::: retrievalloom.structure.treesearch_engine.TreeSearchStructureEngine

## Fusion and normalisation

::: retrievalloom.evidence.fusion.RRFFusion

::: retrievalloom.evidence.fusion.WeightedRRFFusion

::: retrievalloom.evidence.fusion.WeightedScoreFusion

::: retrievalloom.evidence.fusion.CombSUMFusion

::: retrievalloom.evidence.fusion.CombMNZFusion

::: retrievalloom.evidence.normalizers.MinMaxNormalizer

::: retrievalloom.evidence.normalizers.ZScoreNormalizer

::: retrievalloom.evidence.normalizers.RankNormalizer

::: retrievalloom.evidence.normalizers.SigmoidNormalizer

## Evidence

::: retrievalloom.evidence.rerank.FlashRankReranker

::: retrievalloom.evidence.dedup.IdentityAndHashDeduplicator

::: retrievalloom.evidence.consolidation.HierarchicalConsolidator

## Infrastructure

::: retrievalloom.infra.cache.InMemoryLRUCache

::: retrievalloom.infra.cache.NoCache

::: retrievalloom.infra.access.AllowAllAccessPolicy

::: retrievalloom.infra.recorders.NoOpProvenanceRecorder

::: retrievalloom.infra.metrics.NullMetrics
