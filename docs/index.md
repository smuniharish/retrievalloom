---
hide:
  - navigation
  - toc
---

<div class="rl-hero" markdown>

# RetrievalLoom

<p class="rl-tagline">An async-first, provider-agnostic retrieval and evidence
orchestration engine for Python. It indexes your documents through several
retrieval channels, fuses what they find, and returns ranked evidence with full
provenance.</p>

[Get started](getting-started/index.md){ .md-button .md-button--primary }
[Quickstart](getting-started/quickstart.md){ .md-button }
[API reference](api/index.md){ .md-button }

</div>

## Why RetrievalLoom

Retrieval code in production does more than call a vector database. It keeps
several indexes in step with changing documents, mixes exact, lexical and
semantic matches, isolates tenants, and must explain where every answer came
from. RetrievalLoom does that work as an embeddable library. You bring the
documents, and optionally your own models and stores. It brings the indexing
lifecycle, the retrieval pipeline and the evidence model.

RetrievalLoom is not a SaaS, an agent framework, a RAG framework, an embedding
provider, a document parser or a chunker. It does not need LangChain,
LlamaIndex or any other framework.

<div class="grid cards" markdown>

-   :material-source-merge:{ .lg .middle } **Multi-channel retrieval**

    ---

    Keyword, fuzzy, BM25 and dense methods in the index channel, plus optional
    graph and structure channels, fused with RRF or score-based fusion.

-   :material-file-certificate-outline:{ .lg .middle } **Evidence with provenance**

    ---

    Every result names its document, version, representation, span, pages and
    the pipeline stages that produced it.

-   :material-layers-triple-outline:{ .lg .middle } **Generational indexes**

    ---

    Indexes are built as generations and published atomically. Coverage is
    tracked per document, so retrieval knows what is ready.

-   :material-account-group-outline:{ .lg .middle } **Tenant isolation**

    ---

    Every call is scoped to a tenant, namespace and collection, with per-tenant
    concurrency limits and an access policy hook.

-   :material-timer-sand:{ .lg .middle } **Never blocks the event loop**

    ---

    Blocking work runs through an offloader with resourcera admission control
    and backpressure.

-   :material-puzzle-outline:{ .lg .middle } **Bring your own components**

    ---

    Embedding models, rerankers, NLI models, vector stores and metadata stores
    plug in through `abc.ABC` contracts, with pytest contract kits to check them.

</div>

## Install

=== "pip"

    ```bash
    pip install retrievalloom
    ```

=== "uv"

    ```bash
    uv add retrievalloom
    ```

Python 3.12 or later. Optional extras add the graph channel (`graph`), the
structure channel (`structure`) and the contract kits (`testing`). See
[Installation](getting-started/installation.md).

## Where to go next

<div class="grid cards" markdown>

-   **[Quickstart](getting-started/quickstart.md)**

    Ingest documents and retrieve evidence in a few lines.

-   **[Concepts](guide/concepts/index.md)**

    Scopes, channels, generations, coverage and evidence.

-   **[How-to guides](guide/how-to/index.md)**

    Plug in your embedding model, reranker, stores and access policy.

-   **[Examples](examples/index.md)**

    Runnable scripts, from in-memory quickstart to Qdrant with real models.

</div>
