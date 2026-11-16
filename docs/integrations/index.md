# Integrations

RetrievalLoom builds on a few libraries. Some are required dependencies and
always active; others are optional extras or components you pass in.

| Library | Role | How it is used |
| --- | --- | --- |
| [Qdrant](qdrant.md) | Dense vector store | Default `vector_store` when an embedding model is configured. Memory, local path, server or cloud. |
| [resourcera](resourcera.md) | Admission and backpressure | Every offloaded job reserves capacity from a `ResourceManager`. |
| [refresh-engine](refresh-engine.md) | Index lifecycle scheduling | Drives index units to coverage with retries, timeouts and a periodic scheduler. Also defines the `MetricsSink` contract. |
| [agxai](agxai.md) | Provenance recording | `AgxaiProvenanceRecorder` records retrievals into an application-owned `XAIRuntime`. |
| [LadybugDB and TreeSearch](graph-and-structure.md) | Graph and structure channels | Defaults for the graph and structure channels (`graph` and `structure` extras). |

The keyword, fuzzy and sparse methods use
[ahocorasick-rs](https://github.com/G-Research/ahocorasick_rs),
[RapidFuzz](https://github.com/rapidfuzz/RapidFuzz) and
[bm25s](https://github.com/xhluca/bm25s). Fusion uses
[ranknexus](https://pypi.org/project/ranknexus/). The default reranker class
wraps [FlashRank](https://github.com/PrithivirajDamodaran/FlashRank). None of
these need configuration beyond what the [channels](../guide/concepts/channels.md)
and [rerank](../guide/how-to/rerank-and-nli.md) pages describe.

RetrievalLoom does not depend on LangChain, LlamaIndex or any other RAG or
agent framework.
