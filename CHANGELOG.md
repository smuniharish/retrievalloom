# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.0]

### Added

- `EngineLimits` (`EngineConfig.limits`, `create_retrieval_engine(limits=...)`):
  every upper bound on a setting is now a default that applications can raise.
  This covers `candidate_k`, `rerank_k`, `nli_k`, fuzzy n-gram length and
  matches, graph depth, ingestion, lifecycle and offloader concurrency,
  lifecycle batch size and attempts, claims per request and the consolidation
  window. A setting above its bound fails with an error that names the limit
  to raise.
- `AutoConcurrency`, the new default for `IngestionConfig.concurrency`,
  `LifecycleConfig.max_concurrency` and `OffloaderConfig.max_workers`. Limits
  are sized from the host capacity resourcera measures, grow while work
  waits, shrink when capacity drops, and halve on timeouts or unavailable
  backends. An integer still fixes the limit.
- `health().details["concurrency"]` reports the state of every limiter.
- `FuzzyConfig.matches_per_ngram` (default 5, previously fixed).
- `RetrievalConfig.filter_overfetch` (default 4): methods without filter
  pushdown fetch `candidate_k` times this factor before post-filtering.
- `LifecycleConfig.housekeeping_interval` (default 300 s) and
  `EngineConfig.health_timeout` (default 5 s), both previously fixed.
- `IndexCapabilities.max_depth` lets a graph store declare its deepest
  supported traversal; `initialize` rejects a deeper configured depth.
- Warning codes `limit_clamped` (a tenant policy reduced a configured value)
  and `filter_underfetch` (post-filtering left fewer than `candidate_k` hits).

### Changed

- `TenantPolicy.max_concurrent_cpu_jobs` is renamed
  `max_concurrent_offloaded_jobs` and defaults to `None`, which falls back to
  `OffloaderConfig.max_concurrent_jobs_per_tenant`. Previously the tenant
  policy default (4) always overrode the offloader setting.
- `IngestionConfig.concurrency` is now shared by all `ingest` calls of an
  engine instead of applying per call.
- `LifecycleConfig.unit_timeout` now starts when a unit begins running, not
  when it is queued.
- Incremental index writes and every index removal are split to the
  backend's `IndexCapabilities.max_batch`.

### Deprecated

- `TenantPolicy.max_concurrent_cpu_jobs`. The old name is still accepted and
  emits a `DeprecationWarning`.

### Migration from 0.1

- Settings that 0.1 rejected above a fixed maximum are accepted after raising
  the matching `EngineLimits` field.
- To keep 0.1's fixed concurrency, set `IngestionConfig(concurrency=8)`,
  `LifecycleConfig(max_concurrency=4)` and `OffloaderConfig(max_workers=8)`.
- Rename `max_concurrent_cpu_jobs` to `max_concurrent_offloaded_jobs`.

## [0.1.1]

### Added

- First release of RetrievalLoom: an async-first, provider-agnostic,
  multi-channel retrieval and evidence orchestration engine.
- `create_retrieval_engine` and `RetrievalEngine` with async `initialize`,
  `ingest`, `retrieve`, `delete`, `backfill`, `rebuild`, `wait_for`, `status`,
  `health`, and `close`.
- Canonical, validated domain model for documents, representations
  (text, pre-chunked, structure trees, graph input), scopes, versions,
  derived objects, lineage, and retrieval results with provenance.
- Abstract base classes for every extension point: metadata store, index
  backends (keyword, fuzzy, sparse, vector, graph, structure), embedding
  model, tokenizer, keyword dictionary, graph extractor and normalizer, graph
  retriever, reranker, NLI, score normalizer, fusion, deduplication, evidence
  consolidation, access policy, cache, and provenance recorder.
- SQLite metadata store on SQLAlchemy async and aiosqlite, with Alembic
  migrations, commit-sequence visibility windows, and optimistic concurrency.
- Index channel: keyword (ahocorasick-rs), fuzzy (rapidfuzz), sparse (bm25s),
  and dense (Qdrant in memory, local path, server, or cloud).
- Graph channel on LadybugDB (`graph` extra) and structure channel on
  TreeSearch (`structure` extra).
- Evidence pipeline: candidate validation and deduplication, score
  normalization, two-level fusion with ranknexus (RRF by default), reranking
  with the default FlashRank reranker (loaded from `RerankingConfig.model_dir`,
  never downloaded implicitly, dropped when its model is missing) or a custom
  `RerankerABC`, NLI, and hierarchical consolidation.
- Generational index lifecycle with incremental updates, rebuilds, backfill,
  fenced retirement, durable reader leases shared across engines, crash
  recovery, and reconciliation, scheduled through refresh-engine.
- Resource admission and bounded offloading of blocking work through
  resourcera, structured logging with structlog, and agxai provenance
  recording.
- `retrievalloom.testing` contract kits for third-party adapters.
- `RetrievalEngine.purge()` for physical erasure (right to erasure), returning
  `PurgeReport`, plus background purge of tombstoned documents after
  `LifecycleConfig.purge_after` (default 7 days). Documents are erased in
  batches of up to 500 (one backend removal call per generation, one write
  transaction); erasing 1,000 documents on SQLite takes ~3 s instead of ~25 s.
- Purge log (`rl_purge_log`, migration `0003_purge_log`): engines sharing a
  metadata store evict purged documents from their in-memory keyword, fuzzy and
  sparse indexes at their next lifecycle round, and rebuild those indexes when
  they missed pruned log entries. `LifecycleConfig.purge_log_retention`
  (default 1 day) bounds the log.
- Version and event retention: `LifecycleConfig.max_versions` (default 10) and
  `LifecycleConfig.event_retention` (default 90 days). A version an ingestion
  event still references is kept until that event expires.
- `LifecycleConfig.wait_for_rebuild` and `rebuild_timeout` to finish the
  in-memory index rebuild before the engine opens.
- Metadata schema migration `0002_retention_indexes`.
- `OffloaderConfig.max_cpu_jobs` and `workload_limits` to cap concurrently
  running CPU-bound offloaded work.
- `LadybugGraphStore` raises a retryable `BackendUnavailableError` when another
  process holds the database file.
- Keyword, fuzzy and sparse in-memory indexes are updated in place per ingest
  batch instead of rebuilt over the whole corpus; BM25 statistics are computed
  over live objects at query time.
- Ingestion writes documents in chunked write transactions (up to 200 per
  transaction, with a per-document fallback when a chunk fails with a library
  error), cutting metadata statements per 500-document batch from ~15,000 to
  ~280; end-to-end ingest is ~18x faster (680 docs/s at 2k, 445 docs/s at 20k
  documents). The indexer lists objects and opens visibility windows in batches.
- Batch metadata store methods `get_documents`, `put_documents`,
  `put_versions`, `get_events`, `put_events`, `find_events_by_fingerprints`,
  `list_objects_for_documents`, `open_visibility_many`,
  `set_documents_coverage_state`, and `list_document_metadata`, covered by the
  contract kit.
- `PostgresMetadataStore` (`retrievalloom[postgres]`, asyncpg): several engines
  on different hosts can share one metadata store with concurrent writers.
  The SQL lives in the dialect-neutral
  `retrievalloom.storage.sql.SQLAlchemyMetadataStore`; `SQLiteMetadataStore`
  behaviour is unchanged.
- Scenario examples (customer support, multi-tenant SaaS, document
  lifecycle, knowledge graph, policy manual, fact checking, HTTP service with a
  Postman collection, Jaeger observability, durable restart) and reference
  custom adapters for 16 ABCs, including bge-m3 dense and sparse embeddings,
  the bge-reranker-v2-m3 cross-encoder, Neo4j and pgvector stores, each
  verified by its contract kit and by live tests.
- Indexing passes read only documents changed since each unit's coverage
  watermark, and the metadata keyword dictionary keeps its terms with document
  counts in the store, so per-batch indexing cost no longer grows with the
  corpus (migration `0004_incremental_indexing`). New units and engine
  instances scan their scope once.
- `KeywordDictionaryABC.revision(scope)` (defaults to a digest of `entries()`).
- `SparseQuery.text` carries the raw query, so learned-sparse indexes encode it
  with their own tokenizer whatever engine tokenizer is configured.
- Opt-in live reliability suites: soak workload (`RL_TEST_SOAK_SECONDS`),
  podman fault injection (`RL_TEST_FAULTS`) and multi-process PostgreSQL
  contention.
- Automatic retry with exponential backoff for index coverage that failed with
  a retryable error, such as a vector store outage
  (`LifecycleConfig.retry_failed_after`, default 30 s;
  `retry_failed_max_delay`, default 15 min).
- Channels can be enabled or disabled by reopening the engine with a new
  `ChannelsConfig`. A newly enabled channel catches up on existing documents,
  including graph extraction for documents ingested while it was off, and
  applies deletes and purges made in the meantime. Configurations with no
  retrieval method enabled are rejected.
- `examples/channel_combinations.py` and live tests for every channel
  combination and transition on SQLite and PostgreSQL.
- Agent skills (`retrievalloom`, `retrievalloom-extend`) for AI coding
  assistants, installable with `npx skills add smuniharish/retrievalloom`.
- `lifecycle_tick_failed` log events include the error code, message and
  details.
- Retrieval snapshots reuse their metadata reads while nothing in the scope has
  changed: each scope carries a snapshot token, rotated by every commit that
  touches it (migration `0005_snapshot_token`). An uncached retrieval issues 8
  metadata statements instead of 16, and the snapshot stage takes about 2 ms
  instead of 12 ms at 2,000 documents.
- Delete, update and purge retire documents in batches (batch metadata store
  methods `get_change_seqs`, `set_documents_objects_state`,
  `retract_contributions`, `get_lineage_many`, `close_visibility_many`, with
  per-item defaults on `MetadataTransactionABC`). Deleting 200 documents takes
  138 metadata statements instead of 1,725, and purging them 81 instead of
  1,869.
- `scripts/scale.py` measures index memory and restart time at a corpus size;
  results to 100,000 documents are in the benchmarks page.
- Architecture diagrams in the README and documentation: overview, retrieval
  pipeline, channels and fusion, ingestion, generation states, document
  lifecycle, channel changes, multiple engines, extension points and
  evidence. Mermaid sources live in `diagrams/`; `scripts/render-diagrams.mjs`
  renders them and CI checks that the images match their sources.

### Changed

- Mark the package as Production/Stable in its PyPI metadata.
- The fuzzy match cache and the snapshot read memo use `cachetools.LRUCache`.
- `BM25sIndex` restates bm25s's scoring formulas instead of importing private
  bm25s functions; a test pins them to bm25s's results.
- Full index rebuilds page through objects with an index seek (migration
  `0006_objects_kind_order`). Previously each page sorted the whole scope, so
  restart time grew quadratically and a 100,000-document rebuild exceeded
  `unit_timeout`; it now opens in about 160 seconds.
- Keyword and fuzzy search evaluate filters once per object and build match
  details only for returned hits. A fuzzy search at 100,000 documents takes
  88 ms instead of 538 ms.
