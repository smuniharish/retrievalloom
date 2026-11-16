# API reference

Generated from the docstrings of the public API. The classes you use day to day
are importable from the top-level `retrievalloom` package. Contracts, built-in
components, storage and integrations are imported from their modules. Each entry
shows the module where the object is defined.

| Page | Contents |
| --- | --- |
| [Engine](engine.md) | `RetrievalEngine`, `create_retrieval_engine`, `EngineState` |
| [Configuration](config.md) | `EngineConfig`, `Components`, `EngineResources`, ingestion, retrieval and lifecycle settings |
| [Runtime](runtime.md) | `Offloader`, `OffloaderConfig`, `WorkloadKind` |
| [Inputs and filters](inputs.md) | `Scope`, `DocumentInput`, the representations, and the filter expressions |
| [Results and evidence](results.md) | `RetrievalExecutionConfig`, `RetrievalResult`, `Evidence`, candidates, warnings and provenance |
| [Records and reports](records.md) | `IngestReport`, `DeleteReport`, `PurgeReport`, `ScopeStatus`, `HealthReport`, lifecycle enumerations |
| [Contracts](contracts.md) | The component abstract base classes, from `EmbeddingModelABC` to `ProvenanceRecorderABC` |
| [Built-in components](components.md) | Default tokenizer, index backends, Qdrant, LadybugDB, TreeSearch, fusion, FlashRank, caches |
| [Storage](storage.md) | `SQLiteMetadataStore` and the metadata store contracts |
| [Integrations](integrations.md) | `AgxaiProvenanceRecorder` |
| [Errors](errors.md) | `RetrievalLoomError` and its subclasses |
| [Testing](testing.md) | The pytest contract kits for custom components |

## Stability

- The public API is everything listed in this reference. Names that start with
  an underscore are private and may change at any time.
- Domain models carry a `schema_version` (currently `1.0.0`).
- The API is async only. There are no synchronous wrappers.
- Breaking changes are listed in the [changelog](../changelog.md).
