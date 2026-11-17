# Channels and methods

Pick the search methods that match how your users ask questions. Each
*method* finds passages in a different way: exact terms, typo-tolerant terms,
BM25 term statistics, meaning, graph connections or section titles. Methods are
grouped into three *channels*, and one search runs every enabled method and
merges their rankings.

| Your users type | Method that finds it |
| --- | --- |
| Product names, error codes, part numbers | `keyword` |
| Misspelled or partial terms | `fuzzy` |
| Natural-language questions with the right words | `sparse` (BM25) |
| Paraphrases that share no words with the answer | `dense` |
| Questions about related entities | `graph` |
| Section names in long, structured documents | `structure` |

The table below lists each method's default backend and what it needs. Each
method keeps its own index, built and published independently.

| Channel | Method | Default backend | Enabled by default | Needs |
| --- | --- | --- | --- | --- |
| `index` | `keyword` | `AhoCorasickKeywordIndex` | Yes | Keyword dictionary entries |
| `index` | `fuzzy` | `RapidFuzzIndex` | Yes | Nothing |
| `index` | `sparse` | `BM25sIndex` | Yes | Nothing |
| `index` | `dense` | `QdrantVectorStore` | Yes | An embedding model |
| `graph` | `graph` | `LadybugGraphStore` + `NeighborhoodGraphRetriever` | No | The `graph` extra |
| `structure` | `structure` | `TreeSearchStructureEngine` | No | The `structure` extra |

Method keys in results have the form `<channel>.<method>`, for example
`index.sparse`. Graph and structure keys are `graph.graph` and
`structure.structure`.

## Index channel

The index channel searches the chunks of each document's `text` or `chunks`
representation. It holds the four methods most applications use.

### Keyword

Keyword search finds exact mentions of the terms you care about, such as
product names and error codes. `AhoCorasickKeywordIndex` matches dictionary
terms, not arbitrary words. The
dictionary comes from a `KeywordDictionaryABC`. The default,
`MetadataKeywordDictionary`, collects every string (or list of strings) stored
under the `keywords` metadata field of the scope's active documents. Forms that
differ only in case make one term, written in the smallest of them
(for example `PYTHON` before `Python`). The metadata store keeps the terms with
the number of documents using each, and ingest and delete update them, so the
dictionary costs the same at any corpus size. Each term
is compiled into an Aho-Corasick automaton (ahocorasick-rs). Chunks are scored
by the sum of `log(1 + N/df)` over matched terms. A generation keeps the
dictionary it was built with; when the dictionary changes (for example a newly
ingested document adds a keyword no other document has, or the last document
with a keyword is deleted), the next lifecycle round builds a new keyword and
fuzzy generation.

Without dictionary entries, the keyword method returns nothing. To use other
fields or fixed terms:

```python
from retrievalloom import Components
from retrievalloom.contracts.models import KeywordEntry
from retrievalloom.index.text import MetadataKeywordDictionary

components = Components(
    keyword_dictionary=MetadataKeywordDictionary(
        fields=("keywords", "product"),
        static_entries=[KeywordEntry(canonical="WPA3", aliases=("WPA 3",))],
    )
)
```

### Fuzzy

Fuzzy search finds terms even when the user misspells them.
`RapidFuzzIndex` matches query n-grams (1 to `max_ngram` tokens) against a
vocabulary with RapidFuzz `WRatio`. The vocabulary holds the dictionary terms
and, with `corpus_vocabulary=True`, every corpus token of at least
`min_token_length` characters. It never compares against whole texts. Matches
below `score_cutoff` (0 to 100) are ignored. Tune it in `FuzzyConfig`.

### Sparse

Sparse search ranks chunks by how often they use the query's words, weighted by
how rare those words are. `BM25sIndex` scores BM25 with the bm25s formulas (`lucene` variant, `k1=1.5`,
`b=0.75`) over the tokens produced by the tokenizer. The default
`SimpleTokenizer` folds case, applies NFC and splits on Unicode word runs. The
corpus statistics (chunk count, document frequencies, average length) are
those of the generation's current chunks and are applied at query time, so
scores equal those of a fresh bm25s index over the same chunks, however the
generation was updated.

### Dense

Dense search finds passages by meaning, so it matches paraphrases. It runs
only when you pass an embedding model in `Components(embedding=...)`. The default vector store is `QdrantVectorStore`:
in memory, or on disk under `data_dir/qdrant` when `EngineResources.data_dir`
is set. See [Use your own embedding model](../how-to/embedding-model.md) and
[Qdrant](../../integrations/qdrant.md).

Passing a `vector_store` without an `embedding` raises `ConfigurationError`.

## Graph channel

The graph channel answers questions about connected entities, such as which
products a component belongs to. It searches entity and relationship
aggregates. They come from
`GraphRepresentation` inputs or from a `GraphExtractorABC` you supply. The
`IdentityGraphNormalizer` merges entities with the same type and folded key.
Aggregates are shared across documents and each keeps its sources.

`NeighborhoodGraphRetriever` finds seed entities whose name or alias equals a
query n-gram, then expands `depth` hops. Seeds score `1.0`, nodes at distance
`d` score `1 / (1 + d)`. Each hit records its path.

Enable it with `GraphChannelConfig(enabled=True)` and install the `graph` extra.
See [Graph and structure](../../integrations/graph-and-structure.md).

## Structure channel

The structure channel finds sections of long documents by their titles and
text. It searches the nodes of `StructureRepresentation` inputs. `TreeSearchStructureEngine` keeps one SQLite FTS5 database per scope and
generation. Enable it with `StructureChannelConfig(enabled=True)` and install
the `structure` extra.

## Change channels on an existing store

Start with the index channel and add graph or structure search later, without
re-ingesting anything. Channels are fixed while an engine is open, so you change
them by closing the engine and opening it again with a new `ChannelsConfig` on
the same durable store: the same `data_dir`, or the same metadata store and
backends.

```python
from retrievalloom import (
    ChannelsConfig,
    EngineResources,
    GraphChannelConfig,
    LifecycleConfig,
    RetrievalConfig,
    create_retrieval_engine,
)

engine = create_retrieval_engine(
    retrieval=RetrievalConfig(channels=ChannelsConfig(graph=GraphChannelConfig(enabled=True))),
    lifecycle=LifecycleConfig(wait_for_rebuild=True),
    resources=EngineResources(data_dir=data_dir),
)
```

When you enable a channel, it catches up on the documents already in the
store:

- The graph extractor runs for documents that were ingested while the graph
  channel was off.
- Documents without a `StructureRepresentation` are marked `not_applicable`
  for the structure channel; they count as covered.
- Documents deleted or purged while the channel was off are removed from it
  when it comes back, and documents updated meanwhile are indexed at their
  current version.

Until the new channel has caught up, searches return `partial` results, or
`empty` when no enabled method can answer yet, with an `index_missing` warning
for each method that has no index. To avoid that window, block until the
channel is ready:

- With the default startup rebuild, the first lifecycle round builds the new
  channel. In foreground mode (`LifecycleConfig(background=False)`) it finishes
  before the engine opens; in background mode, set `wait_for_rebuild=True`.
- Otherwise, call `backfill` and `wait_for` after opening (see
  [Backfill and rebuild](../how-to/backfill-and-rebuild.md)).

When you disable a channel, it is no longer searched or maintained. Its
coverage stays in the metadata store and still appears in `engine.status()`.
`RetrievalExecutionConfig` can narrow a single request to some of the enabled
channels, but it cannot turn on a channel that the engine did not open with.

Keyword, fuzzy and sparse indexes that you pass in `Components` stay wired and
indexed even when their method is disabled, so a request can still force them
on. A vector store, graph store or structure engine you pass is used only
while its method or channel is enabled.

Two configurations are rejected:

- A `ChannelsConfig` with no channel enabled, or with only the index channel
  and all of its methods off, fails validation when you build it.
- An engine whose enabled methods all lack a backend, for example `dense`
  alone without an embedding model, raises `ConfigurationError` when it opens.
  A method without a backend next to others that work only adds a
  `method_unavailable` info warning.

The [Channel combinations](../../examples/channel-combinations.md) example runs
this cycle end to end: index only, then graph and structure added, then graph
switched off and on again.

## Choose methods per request

Every method and channel can be switched off globally in `RetrievalConfig`, or
per request with `RetrievalExecutionConfig`:

```python
from retrievalloom import RetrievalExecutionConfig

result = await engine.retrieve(
    "wpa3 setup",
    scope=scope,
    config=RetrievalExecutionConfig(fuzzy=False, dense=True),
)
```

`None` keeps the configured value. `True` forces a method on: if it has no
backend you get a `method_unavailable` warning with `warning` severity, and
the result is `partial`. Forcing a channel that the tenant policy does not
allow raises `ConfigurationError`.

## Weight methods and channels

`MethodConfig.weight` weights a method inside index-channel method fusion.
`IndexChannelConfig.weight`, `GraphChannelConfig.weight` and
`StructureChannelConfig.weight` weight channels in channel fusion. Weights only
matter for the weighted fusion methods. See
[Retrieval pipeline](retrieval.md#fusion).

## Next steps

- Follow a query through each stage in [Retrieval pipeline](retrieval.md).
- Turn on dense search with [your own embedding model](../how-to/embedding-model.md).
- Add and remove channels on a live store in the
  [Channel combinations](../../examples/channel-combinations.md) example.
