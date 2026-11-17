# Getting started

Go from `pip install` to your first search results in a few minutes. You
create a `RetrievalEngine` inside your async application, add documents, and
search them. None of these steps needs a model, a server or network access.

<div class="grid cards" markdown>

-   :material-download:{ .lg .middle } __[Installation](installation.md)__

    ---

    Install the package, plus optional extras for knowledge-graph search,
    section-tree search, PostgreSQL or the component test kits.

-   :material-rocket-launch:{ .lg .middle } __[Quickstart](quickstart.md)__

    ---

    Index three documents, search them, and see where each result came
    from.

-   :material-tune:{ .lg .middle } __[Configuration](configuration.md)__

    ---

    Change timeouts, search methods, result counts and per-customer limits,
    for the whole engine or for one request.

</div>

Without configuration, the engine keeps everything in memory and runs keyword,
typo-tolerant and BM25 search. You add semantic search, reranking, claim
checking, knowledge-graph search and section-tree search when you need them.

Every engine method is a coroutine, so you call it with `await` from async
code. There are no synchronous wrappers.

## Next steps

After the quickstart, the [User guide](../guide/index.md) explains how the
engine works and walks through common tasks.
