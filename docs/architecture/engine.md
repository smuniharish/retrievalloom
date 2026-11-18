# Engine and components

`RetrievalEngine` is the async entry point for ingestion, retrieval and
document lifecycle operations. It coordinates the metadata store, configured
search channels and optional model components. Applications configure these
through `EngineConfig`, `Components` and `EngineResources`.

## Lifecycle

| State | Meaning |
| --- | --- |
| `new` | Created but not initialized. |
| `open` | Initialization succeeded; operations are accepted. |
| `closing` | Shutdown is in progress; new operations are rejected. |
| `closed` | Owned resources are closed. The engine cannot be reopened. |

Use `async with` or call `initialize()` and `close()` explicitly. Initialization
is idempotent while the engine is open, as is shutdown. If initialization
fails, the engine releases resources it started before propagating the error.

## Components

Pass custom implementations through `Components`. Any component the
application does not provide is selected from the configuration and available
extras:

| Capability | Default behavior |
| --- | --- |
| Metadata | In-memory SQLite by default; `data_dir` enables a local database file. |
| Keyword, fuzzy and BM25 search | Enabled by default with built-in indexes. |
| Semantic search | Requires an embedding model and vector store. |
| Graph and structure search | Optional channels; enable the corresponding extra and configuration. |
| Reranking | Uses FlashRank when enabled and its model files are available; models are not downloaded implicitly. |
| NLI | Disabled unless an implementation is supplied. |
| Access policy | Allows access by default. Configure a policy for application authorization. |

See the [component reference](../api/components.md) and
[configuration guide](../getting-started/configuration.md) for all built-in
choices and settings.

## Ownership

The engine initializes the components passed to it, but closes only the
components and resources it creates. Application-owned components, executors,
resource managers and metadata stores remain the application's responsibility.
Close them after the engine has stopped using them.

## Shutdown

`close()` stops accepting new work, waits for in-flight operations within the
configured shutdown timeout, and then releases engine-owned components. Work
that cannot be interrupted in a worker thread may finish after its awaiting
coroutine is cancelled; the offloader retains its capacity reservation until
the thread completes.

## Identity

Use `engine_id` to identify a logical engine across restarts. Each engine
instance also has its own runtime identity for its temporary state. See
[Concurrency and isolation](concurrency.md) and
[Run several engines on one store](../guide/how-to/multiple-engines.md).
