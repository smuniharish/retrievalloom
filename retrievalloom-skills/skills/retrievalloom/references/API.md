# API

Use this page to look up the exact names and shapes of the public API: how
to build an engine, what each call accepts, and what it returns. Everything
listed here is importable from `retrievalloom` unless a module is named.

## Build an engine

```python
from retrievalloom import create_retrieval_engine

engine = create_retrieval_engine(
    ingestion=None,  # IngestionConfig
    retrieval=None,  # RetrievalConfig
    lifecycle=None,  # LifecycleConfig
    offloader=None,  # OffloaderConfig
    components=None,  # Components: your own components; None fields use defaults
    resources=None,  # EngineResources: data_dir, resource_manager, executor, xai_runtime, metrics
    log_content=False,  # log query and document text
    engine_id=None,  # stable id when several engines share a store
)
async with engine:
    ...
```

`RetrievalEngine(config=None, components=None, resources=None, *, engine_id=None)`
builds the same engine from one `EngineConfig(ingestion=, retrieval=,
lifecycle=, offloader=, log_content=)`. It takes no `lifecycle=` or
`retrieval=` keyword.

`EngineResources(*, resource_manager=None, xai_runtime=None, executor=None,
data_dir=None, metrics=None)` holds infrastructure the application owns. The
engine never closes it.

`async with engine` opens the engine (`state` becomes `open`) and closes it on
exit, waiting up to `LifecycleConfig.shutdown_timeout` for in-flight work.
Calls on an engine that is not open raise `LifecycleError`. Properties:
`engine_id`, `instance_id`, `config`, `state` (`new`, `open`, `closing`,
`closed`).

## Describe a document

```python
from retrievalloom import ChunkInput, ChunksRepresentation, DocumentInput, SourceDescriptor
from retrievalloom import TextRepresentation

document = DocumentInput(
    id="policy-7",  # unique within the scope
    metadata={"team": "hr", "keywords": ["PTO"]},  # filterable; "keywords" feeds the keyword method
    source=SourceDescriptor(uri="https://kb/7", title="Leave policy", external_version="v3"),
    representations=[TextRepresentation(text="...")],
)
prechunked = DocumentInput(
    id="manual",
    representations=[
        ChunksRepresentation(
            chunks=[ChunkInput(local_id="c1", text="...", section_path=("Setup",))]
        )
    ],
)
```

| Field | Meaning |
| --- | --- |
| `id` | Document id, unique per scope. Reusing it creates a new version. |
| `metadata` | JSON values used by filters. `keywords` (a string or list) feeds the keyword method. |
| `dataset` | Optional grouping, filterable as `_dataset`. |
| `source` | `SourceDescriptor(uri, media_type, title, external_version)`, returned in citations. |
| `representations` | One or more of `TextRepresentation(text)`, `ChunksRepresentation(chunks)`, `StructureRepresentation(root)`, `GraphRepresentation(entities, relationships)`. |
| `appearance_id`, `delivery` | Exact redelivery detection for at-least-once queues. |

`StructureRepresentation(root=StructureNodeInput(title, text, children=...))`
feeds the structure channel. `GraphRepresentation(entities=[GraphEntityInput(key,
name, entity_type, aliases)], relationships=[GraphRelationshipInput(source_key,
target_key, relation_type)])` feeds the graph channel.

## Write and erase

| Call | Returns |
| --- | --- |
| `await engine.ingest(documents, *, scope, principal=None)` | `IngestReport` |
| `await engine.delete(document_ids, *, scope, principal=None)` | `DeleteReport` |
| `await engine.purge(document_ids, *, scope, principal=None)` | `PurgeReport` |

- `IngestReport`: `documents` (one `DocumentIngestResult` per input with
  `document_id`, `outcome`, `version`, `coverage`, `error`), `warnings`,
  `task_ids`, and the properties `outcomes` (counts per outcome) and `failed`.
- Outcomes: `created`, `updated`, `unchanged`, `revived` (re-ingested after
  delete), `replayed` (same delivery seen before), `rejected` (invalid),
  `failed`.
- `DeleteReport`: `deleted`, `already_deleted`, `not_found`, `task_ids`.
- `PurgeReport`: `purged`, `not_found`, `task_ids` (a graph rebuild, when
  needed).

## Retrieve

```python
result = await engine.retrieve(
    "how do I reset the vpn",
    scope=scope,
    filters=None,  # FilterExpression
    top_k=None,  # default RetrievalConfig.default_top_k (10)
    claims=(),  # up to 32 statements for NLI to check
    principal=None,
    config=None,  # RetrievalExecutionConfig, per-request overrides
    request_id=None,
)
```

`RetrievalResult`:

| Field | Meaning |
| --- | --- |
| `status` | `complete`, `partial` (a warning of severity `warning` or `error`), or `empty` (no evidence) |
| `evidence` | Ranked `Evidence` items, at most `top_k` plus linked contradictions |
| `warnings` | `RetrievalWarning(code, severity, channel, method, message, details)` |
| `candidates` | Pre-consolidation candidates, only with `include_candidates=True` |
| `coverage` | `counts` and `fraction_ready` per method key |
| `snapshot` | `snapshot_seq`, the generations and fingerprints read |
| `timings` | Seconds per stage |
| `request_id`, `scope` | Echoed from the request |

`Evidence`:

| Field | Meaning |
| --- | --- |
| `display_text` | Text to show or pass to a model |
| `sources` | `SourceReference(document_id, document_uid, document_version, span, pages, lines, section_path, source_uri, title, ...)` |
| `score`, `scores` | Final score; `ScoreCard(normalized, method_fusion, channel_fusion, rerank, final)` |
| `provenance` | `hits` (each with `method.key` such as `index.sparse`, `raw_score`, `rank`, `matched`) and `stage_trail` |
| `nli_summary` | `entailment`, `contradiction`, `neutral`, `has_contradiction` when NLI ran |
| `segments`, `relations`, `linked_evidence_ids` | Consolidated members and links |

`RetrievalExecutionConfig` overrides one request: `index`, `graph`,
`structure`, `keyword`, `fuzzy`, `sparse`, `dense`, `reranking`, `nli`
(`None` keeps the configuration, `True` forces on, `False` turns off),
`top_k`, `timeout`, `missing_index_policy`, `include_candidates`,
`method_fusion`, `channel_fusion`, `allow_stale_versions`.

## Filter

`Eq(field, value)`, `In(field, values)`, `Range(field, gt=, gte=, lt=, lte=)`,
`Exists(field)`, `And(clauses)`, `Or(clauses)`, `Not(clause)`. Fields are
dotted paths into metadata; `_id` and `_dataset` are reserved. See
[access-and-tenancy.md](access-and-tenancy.md).

## Run and watch the lifecycle

| Call | Returns |
| --- | --- |
| `await engine.backfill(*, scope, channels=None, methods=None, principal=None)` | `LifecycleTaskRef` |
| `await engine.rebuild(*, scope, channel, method=None, principal=None)` | `LifecycleTaskRef` |
| `await engine.wait_for(task, *, timeout=None)` | `LifecycleTaskResult(task_id, kind, state, attempts, error, details)` |
| `await engine.run_lifecycle_once(scope=None)` | One indexing and maintenance round |
| `await engine.status(*, scope, principal=None)` | `ScopeStatus(documents_active, documents_deleted, coverage, generations, tasks)` |
| `await engine.health()` | `HealthReport(component, status, message, details, components)` |

Task states: `succeeded`, `failed`, `cancelled` when terminal. Health status:
`healthy`, `degraded`, `unhealthy`.

## Storage classes

| Class | Import |
| --- | --- |
| `SQLiteMetadataStore(path=None, *, synchronous="NORMAL", busy_timeout_ms=30000, engine=None)` | `retrievalloom.storage.sqlite` |
| `PostgresMetadataStore(dsn, *, schema=None, pool_size=5, ...)` | `retrievalloom` (needs the `postgres` extra) |
| `QdrantVectorStore(*, location=None, path=None, url=None, api_key=None, client=None, collection_prefix="retrievalloom", ...)` | `retrievalloom.index.vector` |
| `FlashRankReranker(cache_dir, *, model_name=..., allow_download=False, ...)` | `retrievalloom.evidence.rerank` |
| `AgxaiProvenanceRecorder(runtime, *, run_provider=None, record_links=True, reranker_id=None)` | `retrievalloom.integrations.agxai` |

## Errors

Every error derives from `RetrievalLoomError` and carries `code`,
`retryable`, and `details`: `ConfigurationError`, `ValidationError`
(`EmbeddingValidationError`, `ComponentContractError`), `IdentityError`,
`IngestionError`, `RetrievalError` (`CoverageError`), `BackendError`
(`BackendUnavailableError`, `BackendTimeoutError`),
`LifecycleError`, `ResourceAdmissionError`, `OperationTimeoutError`,
`OperationCancelledError`, `ProvenanceError`, `MigrationError`,
`TenantIsolationError`, `AccessDeniedError`. See
[troubleshooting.md](troubleshooting.md) for causes.
