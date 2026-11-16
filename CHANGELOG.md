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
  normalization, two-level fusion with ranknexus, FlashRank or custom
  reranking, NLI, and hierarchical consolidation.
- Generational index lifecycle with incremental updates, rebuilds, backfill,
  fenced retirement, durable reader leases shared across engines, crash
  recovery, and reconciliation, scheduled through refresh-engine.
- Resource admission and bounded offloading of blocking work through
  resourcera, structured logging with structlog, and agxai provenance
  recording.
- `retrievalloom.testing` contract kits for third-party adapters.
