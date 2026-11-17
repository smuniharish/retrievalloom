# Contract kits

Prove a custom component with the same tests RetrievalLoom runs against its
own backends. Each kit is a pytest base class: subclass it, return your
component, and pytest runs every inherited check.

## Choose the base class and kit

Pass the component to `Components` under the field in the first column.

| `Components` field | Base class | Module | Contract kit |
| --- | --- | --- | --- |
| `metadata_store` | `DocumentMetadataStoreABC` | `retrievalloom.storage.base` | `MetadataStoreContract` |
| `embedding` | `EmbeddingModelABC` | `retrievalloom.contracts.models` | `EmbeddingModelContract` |
| `reranker` | `RerankerABC` | `retrievalloom.contracts.models` | `RerankerContract` |
| `nli` | `NLIABC` | `retrievalloom.contracts.models` | `NLIContract` |
| `tokenizer` | `TokenizerABC` | `retrievalloom.contracts.models` | none |
| `keyword_dictionary` | `KeywordDictionaryABC` | `retrievalloom.contracts.models` | none |
| `graph_extractor` | `GraphExtractorABC` | `retrievalloom.contracts.models` | `GraphExtractorContract` |
| `graph_normalizer` | `GraphNormalizerABC` | `retrievalloom.contracts.models` | none |
| `keyword_index` | `KeywordIndexABC` | `retrievalloom.contracts.indexes` | `KeywordIndexContract` |
| `fuzzy_index` | `FuzzyIndexABC` | `retrievalloom.contracts.indexes` | `FuzzyIndexContract` |
| `sparse_index` | `SparseIndexABC` | `retrievalloom.contracts.indexes` | `SparseIndexContract` |
| `vector_store` | `VectorStoreABC` | `retrievalloom.contracts.indexes` | `VectorStoreContract` |
| `graph_store` | `GraphStoreABC` | `retrievalloom.contracts.indexes` | `GraphStoreContract` |
| `graph_retriever` | `GraphRetrieverABC` | `retrievalloom.contracts.indexes` | none |
| `structure_engine` | `StructureEngineABC` | `retrievalloom.contracts.indexes` | `StructureEngineContract` |
| `method_fusion`, `channel_fusion` | `FusionABC` | `retrievalloom.contracts.evidence` | `FusionContract` |
| `deduplicator` | `DeduplicatorABC` | `retrievalloom.contracts.evidence` | `DeduplicatorContract` |
| `consolidator` | `EvidenceConsolidatorABC` | `retrievalloom.contracts.evidence` | `EvidenceConsolidatorContract` |
| `cache` | `CacheABC` | `retrievalloom.contracts.infra` | `CacheContract` |
| `access_policy` | `AccessPolicyABC` | `retrievalloom.contracts.infra` | none |
| `provenance_recorder` | `ProvenanceRecorderABC` | `retrievalloom.contracts.infra` | none |

`ScoreNormalizerABC` (`retrievalloom.contracts.evidence`, kit
`ScoreNormalizerContract`) has no `Components` field. Pass it to a
score-based fusion class, for example `WeightedScoreFusion(normalizer=...)`
from `retrievalloom.evidence.fusion`.

## Implement the shared members

Every base class derives from `ComponentABC`
(`retrievalloom.contracts.component`).

| Member | Required | What it must do |
| --- | --- | --- |
| `info` | Yes | Return the same `ComponentInfo` every time. Build it once with `make_info(component=, implementation=, version=, config=, model_identity=, dimensions=)`. |
| `initialize(context)` | No | Open clients and load models. `context` has `engine_id`, `offloader`, `clock`, `logger`, `metrics`, `data_dir`, and `metadata_store`. Idempotent. |
| `close()` | No | Release what `initialize` opened. Idempotent. |
| `health()` | No | Return a `HealthReport` whose `component` is `info.name`. The default reports healthy. |

## Write the test

Install the kits with `pip install "retrievalloom[testing]"`. The kits are
async, so run them with pytest-asyncio in auto mode:

```toml
[tool.pytest.ini_options]
asyncio_mode = "auto"
```

Subclass the kit in a class whose name starts with `Test`, and return a new,
uninitialized component from `make_component`:

```python
from pathlib import Path

from retrievalloom.testing import SparseIndexContract

from my_app.search import MySparseIndex


class TestMySparseIndex(SparseIndexContract):
    async def make_component(self, tmp_path: Path) -> MySparseIndex:
        return MySparseIndex(url="http://localhost:9200", prefix=f"test-{tmp_path.name}")
```

The kit initializes the component with a real `ComponentContext`, including
a started `Offloader`, and closes it after each test. Give each test its own
empty backend state, for example a prefix derived from `tmp_path`.

## Use the hooks

| Hook or attribute | Kit | Purpose |
| --- | --- | --- |
| `make_component(tmp_path)` | all | Abstract. Return a fresh, uninitialized component. |
| `prepare(component)` | all | Bring the initialized component into a usable state. `MetadataStoreContract` runs `migrate()` here. |
| `resource_manager()` | all | Return a started resourcera `ResourceManager` to share between tests. The default creates one per test. |
| `tolerance` | `EmbeddingModelContract` | Absolute tolerance for repeated and batched embeddings. Default `1e-4`. |
| `check_relevance` | `RerankerContract` | Also require that the relevant passage scores highest. Default `True`. |
| `check_semantics` | `NLIContract` | Also require the obvious entailment and contradiction labels. Default `True`. |
| `stores_values` | `CacheContract` | Set `False` for a cache that never stores anything; it must then always miss. |
| `reference(lists, weights)` | `FusionContract` | Abstract. Return the fused score per candidate for your formula. |

## Know what every kit checks

Every kit runs these three tests:

- `test_info_is_stable`: `info` is equal on each access, with a stable
  fingerprint digest.
- `test_health_reports_component`: `health()` returns a `HealthReport`
  naming the component.
- `test_initialize_and_close_are_idempotent`: a second `initialize` and
  repeated `close` calls are harmless.

The index kits share these tests, using three objects where the query
`python` matches two:

| Test | Obligation |
| --- | --- |
| `test_capabilities` | `capabilities` is an `IndexCapabilities` with `max_batch >= 1`. |
| `test_search_returns_matching_objects` | Only matches are returned, best first, and `limit` holds. |
| `test_create_and_write_are_idempotent` | Creating twice and rewriting an object do not duplicate it. |
| `test_remove_hides_objects` | Removed objects disappear; a second `remove` returns 0. |
| `test_filters_restrict_results` | A metadata filter restricts the hits. |
| `test_update_metadata_changes_filtering` | New metadata applies to present objects; absent ones are not counted. |
| `test_scopes_are_isolated` | Equal generation ids in two scopes stay separate. |
| `test_sealed_generation_mutability` | Mutable backends apply writes after `seal`; immutable ones raise a `RetrievalLoomError`. |
| `test_drop_generation_is_idempotent` | A dropped generation is no longer listed; dropping twice is harmless. |
| `test_cancelled_search_leaves_backend_usable` | Cancelling a search raises `CancelledError` and corrupts nothing. |

`FuzzyIndexContract` adds `test_typo_matches`. `StructureEngineContract` adds
`test_navigation`. `GraphStoreContract` checks entity lookup by name and
alias, neighborhoods, removal, and scope isolation.

## Test a metadata store

`MetadataStoreContract` covers transactions, compare-and-set on documents,
tenant isolation, visibility windows, batched writes, change and coverage
watermarks, keyword dictionaries, generation pointers, task fencing, reader
leases, purge with its purge log, and retention. Start from one of two paths:

- Subclass `SQLAlchemyMetadataStore` (`retrievalloom.storage.sql`) for
  another SQLAlchemy async dialect. The dialect supplies the engine,
  isolation and locking, the dialect `INSERT ... ON CONFLICT`, and the
  mapping of driver errors. `SQLiteMetadataStore` and
  `PostgresMetadataStore` are the two examples to read.
- Implement `DocumentMetadataStoreABC` (`retrievalloom.storage.base`)
  directly. It needs `info`, `migrate`, `schema_version`, and `transaction`,
  and the transaction class has 78 abstract methods. Prefer the dialect path.

Run the kit against a disposable database. RetrievalLoom's own runs are in
`tests/contracts/test_builtin_contracts.py`.

## Test components without a kit

Tokenizers, keyword dictionaries, graph normalizers, graph retrievers,
access policies, and provenance recorders have no kit. Test them through an
engine: ingest a few documents, retrieve, and assert on the result.
`assets/test_access_policy.py` shows the pattern.
