# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
