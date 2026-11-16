# How-to guides

Task-focused recipes. Each assumes you have read the
[quickstart](../../getting-started/quickstart.md).

| Guide | Task |
| --- | --- |
| [Use your own embedding model](embedding-model.md) | Enable dense retrieval with any embedding model |
| [Rerank and verify with NLI](rerank-and-nli.md) | Add FlashRank or your own reranker and an NLI model |
| [Keep data on disk](durable-storage.md) | Persist metadata and indexes across restarts |
| [Run several engines on one store](multiple-engines.md) | Share durable indexes between processes safely |
| [Filter and authorise](filters-and-access.md) | Metadata filters, access policies and row filters |
| [Backfill and rebuild](backfill-and-rebuild.md) | Fill coverage gaps and rebuild indexes |
| [Write your own backend](custom-backends.md) | Implement a contract and test it with a contract kit |
| [Keep the event loop responsive](event-loop-latency.md) | Offloading, timeouts and garbage-collector pauses |
