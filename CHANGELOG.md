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
  `LifecycleConfig.purge_after` (default 7 days).
- Version and event retention: `LifecycleConfig.max_versions` (default 10) and
  `LifecycleConfig.event_retention` (default 90 days).
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
