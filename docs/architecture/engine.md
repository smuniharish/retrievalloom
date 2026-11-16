# Engine and components

`RetrievalEngine` (`retrievalloom.engine`) is the only entry point. It holds
the configuration, wires components, and routes each operation to the
ingestion service, the lifecycle coordinator or the retrieval pipeline.

## States

| State | Meaning |
| --- | --- |
| `new` | Created; nothing started. |
| `open` | `initialize()` (or `async with`) succeeded. Operations are accepted. |
| `closing` | `close()` is draining in-flight operations. New operations raise `LifecycleError`. |
| `closed` | Everything the engine owns is closed. The engine cannot be reopened. |

`initialize()` is idempotent while open. `close()` is idempotent.

## Startup

`initialize()` does, in order:

1. Start the offloader (executor and resourcera manager, unless supplied).
2. Resolve the metadata store, initialise it and run the migration policy
   (`LifecycleConfig.migration`).
3. Resolve every other component: supplied ones are used as is; missing ones
   get a default or stay disabled (see below).
4. Call `initialize(context)` on each component once, in wiring order. The
   `ComponentContext` carries the engine id, offloader, clock, logger, metrics
   sink, `data_dir` and the metadata store.
5. Build one **index unit** per enabled method with a backend
   (`index.keyword`, `index.fuzzy`, `index.sparse`, `index.dense`, `graph`,
   `structure`). A unit fingerprints its backend and dependencies (tokenizer,
   keyword dictionary, embedding model).
6. Start the lifecycle coordinator: register the engine instance and, with
   `background=True`, start the scheduler. With `auto_rebuild_ephemeral=True`
   and either `background=False` or `wait_for_rebuild=True`, run one round
   before returning (bounded by `rebuild_timeout` when the scheduler runs).

If any step fails, everything started so far is closed and the error is
raised.

## Default components

| Component | Default | Condition |
| --- | --- | --- |
| `metadata_store` | `SQLiteMetadataStore` (in memory, or `data_dir/metadata.db`) | Always |
| `tokenizer` | `SimpleTokenizer` | Always |
| `keyword_dictionary` | `MetadataKeywordDictionary` | Always |
| `keyword_index` | `AhoCorasickKeywordIndex` | `keyword` method enabled |
| `fuzzy_index` | `RapidFuzzIndex` | `fuzzy` method enabled |
| `sparse_index` | `BM25sIndex` | `sparse` method enabled |
| `embedding` | None | Supply one to enable `dense` |
| `vector_store` | `QdrantVectorStore` (memory, or `data_dir/qdrant`) | Embedding model set and `dense` enabled |
| `graph_store` | `LadybugGraphStore` (memory, or `data_dir/graph/graph.lbug`) | Graph channel enabled |
| `graph_retriever` | `NeighborhoodGraphRetriever(depth=channels.graph.depth)` | Graph channel enabled |
| `graph_normalizer` | `IdentityGraphNormalizer` | Graph channel enabled |
| `graph_extractor` | None | Optional |
| `structure_engine` | `TreeSearchStructureEngine` (memory, or `data_dir/structure`) | Structure channel enabled |
| `deduplicator` | `IdentityAndHashDeduplicator` | Always |
| `consolidator` | `HierarchicalConsolidator` | Always |
| `method_fusion`, `channel_fusion` | Built from `FusionConfig` (RRF, `k=60`) | Always |
| `reranker` | `FlashRankReranker` from `RerankingConfig` (`model_dir`, `model_name`); dropped at startup when the model files are missing | `RerankingConfig.enabled` |
| `nli` | None | Supply one |
| `cache` | `NoCache` | Always |
| `access_policy` | `AllowAllAccessPolicy` | Always. Not a security control. |
| `provenance_recorder` | `AgxaiProvenanceRecorder` with `EngineResources.xai_runtime`, else `NoOpProvenanceRecorder` | Always |

A query-embedding LRU cache is added when an embedding model is set and
`embedding_cache_size > 0`.

Passing a `vector_store` without an `embedding` model raises
`ConfigurationError`.

## Ownership

- The engine closes the components and resources it created.
- Components and resources you pass in are initialised by the engine but never
  closed by it. They stay yours.
- `EngineResources` (`resource_manager`, `xai_runtime`, `executor`,
  `data_dir`, `metrics`) is never closed or deleted.

## Shutdown

`close()`:

1. Rejects new operations.
2. Waits up to `LifecycleConfig.shutdown_timeout` for in-flight operations and
   background indexing rounds, then cancels what is left.
3. Stops the coordinator and deregisters the engine instance, which deletes
   its reader leases and the metadata rows of its ephemeral units.
4. Closes engine-owned components in reverse wiring order, then the offloader.

## Identity

- `engine_id` names the logical engine (pass `engine_id=` to keep it stable
  across restarts).
- Each `RetrievalEngine` object also has a random **instance id**. Ephemeral
  units, reader leases and heartbeats are per instance. See
  [Concurrency and isolation](concurrency.md).
