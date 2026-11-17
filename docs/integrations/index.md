# Integrations

See which libraries RetrievalLoom works with and how to configure each one.
Some are installed with the base package and work without setup; others are
optional extras or components you pass in.

| Library | Role | How it is used |
| --- | --- | --- |
| [Qdrant](qdrant.md) | Dense vector store | Stores vectors for semantic search. Runs in memory, on disk, on a server or in Qdrant Cloud. |
| [resourcera](resourcera.md) | Admission and backpressure | Limits how much memory and CPU background work may use at once. |
| [refresh-engine](refresh-engine.md) | Index lifecycle scheduling | Runs background indexing with retries, timeouts and a periodic scheduler. Also defines the `MetricsSink` contract. |
| [agxai](agxai.md) | Provenance recording | Records every ingestion, deletion and search in an agxai audit trail. |
| [LadybugDB and TreeSearch](graph-and-structure.md) | Graph and structure channels | Knowledge-graph search and section-tree search (`graph` and `structure` extras). |

## Libraries that need no setup

The keyword, fuzzy and sparse methods use
[ahocorasick-rs](https://github.com/G-Research/ahocorasick_rs),
[RapidFuzz](https://github.com/rapidfuzz/RapidFuzz) and
[bm25s](https://github.com/xhluca/bm25s). Fusion uses
[ranknexus](https://pypi.org/project/ranknexus/). The default reranker class
wraps [FlashRank](https://github.com/PrithivirajDamodaran/FlashRank). None of
these need configuration beyond what the [channels](../guide/concepts/channels.md)
and [rerank](../guide/how-to/rerank-and-nli.md) pages describe.

RetrievalLoom does not depend on LangChain, LlamaIndex or any other RAG or
agent framework, so it fits next to whichever one you use.
