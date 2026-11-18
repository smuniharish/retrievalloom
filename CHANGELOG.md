# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.0]

### Added

- `EngineLimits` lets applications raise the configurable upper bounds for
  retrieval, indexing, lifecycle and offloading settings.
- `AutoConcurrency` sizes ingestion, lifecycle and offloader concurrency from
  available host capacity. Applications can still set a fixed integer.
- Health reports include the current concurrency state.
- `RetrievalEngine.purge()` physically erases documents. Retention settings
  cover document versions and ingestion events, preserving versions referenced
  by retained events.
- Engines sharing a metadata store learn about erasures and remove affected
  entries from their local indexes.
- Retryable indexing failures are retried automatically with configurable
  backoff.
- Search channels can be enabled or disabled between engine runs. Newly
  enabled channels can index existing documents; `backfill` and `rebuild`
  remain available for explicit lifecycle control.
- `SparseQuery.text` preserves the original query for learned-sparse models.
- Runnable channel-combination examples demonstrate enabling methods and
  channels at startup and later in the document lifecycle.
- Agent Skills help coding assistants use RetrievalLoom and implement custom
  components.

### Changed

- The package is classified as Production/Stable.
- The lockfile uses current compatible stable releases for the direct
  dependencies. The optional examples group bounds Transformers below 6,
  matching FlagEmbedding's declared compatibility, and FastAPI below 1.
- Ingestion, indexing, retrieval and document lifecycle operations use
  batched store operations where supported, reducing corpus-wide work during
  incremental updates.
- The default in-memory indexes are updated incrementally and rebuilt from
  durable metadata after restart.
- Documentation and examples clarify channel configuration, storage choices,
  lifecycle behavior, resource limits and evidence provenance.

### Deprecated

- `TenantPolicy.max_concurrent_cpu_jobs` is deprecated in favor of
  `max_concurrent_offloaded_jobs`.

### Migration from 0.1

- Settings that previously exceeded fixed limits are accepted after raising
  the corresponding field in `EngineLimits`.
- To retain fixed concurrency, set integer values for
  `IngestionConfig.concurrency`, `LifecycleConfig.max_concurrency` and
  `OffloaderConfig.max_workers`.
- Rename `TenantPolicy.max_concurrent_cpu_jobs` to
  `TenantPolicy.max_concurrent_offloaded_jobs`.

## [0.1.1]

### Added

- The async `RetrievalEngine` API for ingestion, retrieval, deletion, erasure,
  lifecycle tasks, health and shutdown.
- Validated document, scope, representation, evidence and provenance models.
- Keyword, fuzzy, BM25 and vector retrieval, with optional graph and structure
  channels.
- Evidence fusion, optional reranking and NLI, source citations and explicit
  result statuses and warnings.
- SQLite and PostgreSQL metadata stores, Qdrant vector storage, and abstract
  base classes for custom components.
- Ingestion, backfill, rebuild, deletion, retention and erasure workflows.
- Resource-aware offloading, structured logging and optional provenance
  recording.
- Adapter contract kits, runnable examples and Agent Skills.
