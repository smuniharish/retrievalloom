# Getting started

RetrievalLoom is a library you embed in an async Python application. You create
a `RetrievalEngine`, ingest documents into a scope, and retrieve evidence from
that scope.

1. [Installation](installation.md): install the package and the extras you need.
2. [Quickstart](quickstart.md): ingest three documents and retrieve evidence.
3. [Configuration](configuration.md): the settings you are most likely to change.

With no configuration the engine runs entirely in process: an in-memory SQLite
metadata store and the keyword, fuzzy and BM25 methods of the index channel. No
model, server or network access is needed. Dense retrieval, reranking, NLI and
the graph and structure channels are added by configuration.

The public API is async only. Every engine method is a coroutine, and there are
no synchronous wrappers.
