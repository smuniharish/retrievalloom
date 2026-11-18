# LadybugDB and TreeSearch

Answer questions about connected entities, and find sections of long documents
by their titles. The graph channel searches a knowledge graph stored in
LadybugDB; the structure channel searches section trees with TreeSearch. Both
are off by default, and their backends come from optional extras:

```bash
pip install "retrievalloom[graph]"      # LadybugDB
pip install "retrievalloom[structure]"  # TreeSearch
pip install "retrievalloom[all]"        # both
```

## Enable the channels

Turn them on in `RetrievalConfig.channels`:

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
- Every query is parameterized with the scope key and generation id.
- All access goes through one connection on a dedicated single thread
  (LadybugDB has a single writer), through the offloader as
  `WorkloadKind.GRAPH_QUERY`.
- A database file has one owner process. Opening a file that another process
  holds raises `BackendUnavailableError` (retryable, with the file in
  `details["path"]`) immediately; it does not wait for the lock. To share a
  graph between processes, use one owner process or a `GraphStoreABC` on a
  server database such as Neo4j. See
  [Keep data on disk](../guide/how-to/durable-storage.md#durable-search-backends).

The default retriever, `NeighborhoodGraphRetriever(depth=1, max_ngram=4,
seed_limit=20)`, finds entities whose name or alias equals a query n-gram
(seeds, score 1.0) and expands `depth` entity hops; a node at distance `d`
scores `1 / (1 + d)`. `GraphChannelConfig.depth` (1 to 4) sets its depth. The
default normalizer, `IdentityGraphNormalizer`, merges entities of one type with
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

## Use your own backends

To use a graph database you already run, such as Neo4j, implement `GraphStoreABC`, `GraphRetrieverABC`, `GraphNormalizerABC`,
`GraphExtractorABC` or `StructureEngineABC` and pass them in `Components`. See
[Write your own backend](../guide/how-to/custom-backends.md).

## Next steps

- Run the [knowledge graph](../examples/knowledge-graph.md) and
  [policy manual](../examples/policy-manual.md) examples.
- Learn how the graph and structure channels rank results in
  [Channels and methods](../guide/concepts/channels.md).
