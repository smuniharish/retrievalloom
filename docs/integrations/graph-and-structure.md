# LadybugDB and TreeSearch

The graph and structure channels are off by default. Their default backends
come from optional extras.

```bash
pip install "retrievalloom[graph]"      # LadybugDB
pip install "retrievalloom[structure]"  # TreeSearch
pip install "retrievalloom[all]"        # both
```

Enable the channels in `RetrievalConfig.channels`:

```python
from retrievalloom import (
    ChannelsConfig,
    EngineConfig,
    GraphChannelConfig,
    RetrievalConfig,
    RetrievalEngine,
    StructureChannelConfig,
)

config = EngineConfig(
    retrieval=RetrievalConfig(
        channels=ChannelsConfig(
            graph=GraphChannelConfig(enabled=True, depth=2),
            structure=StructureChannelConfig(enabled=True),
        )
    )
)
engine = RetrievalEngine(config)
```

The extras are imported only when a channel is enabled and you did not pass
your own backend.

## Graph channel: LadybugDB

`LadybugGraphStore` (`retrievalloom.graph.ladybug_store`) stores graph
aggregates in an embedded [LadybugDB](https://pypi.org/project/ladybug/)
database.

- `LadybugGraphStore(path=None, *, buffer_pool_size=0)`. `path=None` keeps the
  graph in memory (not durable). The engine's default uses
  `data_dir/graph/graph.lbug` when `data_dir` is set.
- Entities and relationships are both nodes; a relationship links to its
  endpoints, so one entity hop is two edge hops.
- Every query is parameterised with the scope key and generation id.
- All access goes through one connection on a dedicated single thread
  (LadybugDB has a single writer), through the offloader as
  `WorkloadKind.GRAPH_QUERY`.

The default retriever, `NeighborhoodGraphRetriever(depth=1, max_ngram=4,
seed_limit=20)`, finds entities whose name or alias equals a query n-gram
(seeds, score 1.0) and expands `depth` entity hops; a node at distance `d`
scores `1 / (1 + d)`. `GraphChannelConfig.depth` (1 to 4) sets its depth. The
default normaliser, `IdentityGraphNormalizer`, merges entities of one type with
the same folded key or name.

Graph input comes from `GraphRepresentation` documents or from a
`GraphExtractorABC` you supply. No extractor is installed by default.

## Structure channel: TreeSearch

`TreeSearchStructureEngine` (`retrievalloom.structure.treesearch_engine`)
searches section trees with [TreeSearch](https://pypi.org/project/pytreesearch/)'s
SQLite FTS5 index.

- `TreeSearchStructureEngine(directory=None)`. `directory=None` keeps indexes
  in memory. The engine's default uses `data_dir/structure/` when `data_dir` is
  set.
- Each (scope, generation) gets its own FTS5 database.
- Every SQLite call runs on one dedicated thread through the offloader
  (`WorkloadKind.STRUCTURE_INDEX`).

Input comes from `StructureRepresentation` documents.

## Filters

Neither backend pushes filters down. Filters are applied after the search, and
results carry a `filter_post_applied` info warning. Expect fewer than
`candidate_k` hits with selective filters.

## Your own backends

Implement `GraphStoreABC`, `GraphRetrieverABC`, `GraphNormalizerABC`,
`GraphExtractorABC` or `StructureEngineABC` and pass them in `Components`. See
[Write a custom backend](../guide/how-to/custom-backends.md).

See the [graph channel](../examples/graph-channel.md) and
[structure channel](../examples/structure-channel.md) examples.
