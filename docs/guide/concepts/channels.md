# Channels and methods

A retrieval runs one or more *methods*. Methods are grouped into three
*channels*. Each method is backed by one index unit with its own generations
and coverage.

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

The index channel reads chunk objects (from `text` or `chunks`
representations).

### Keyword

`AhoCorasickKeywordIndex` matches dictionary terms, not arbitrary words. The
dictionary comes from a `KeywordDictionaryABC`. The default,
`MetadataKeywordDictionary`, collects every string (or list of strings) stored
under the `keywords` metadata field of the scope's active documents. Each term
is compiled into an Aho-Corasick automaton (ahocorasick-rs). Chunks are scored
by the sum of `log(1 + N/df)` over matched terms. A generation keeps the
dictionary it was built with; when the dictionary changes (for example a newly
ingested document adds keywords), the next lifecycle round builds a new
keyword and fuzzy generation.

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

`RapidFuzzIndex` matches query n-grams (1 to `max_ngram` tokens) against a
vocabulary with RapidFuzz `WRatio`. The vocabulary holds the dictionary terms
and, with `corpus_vocabulary=True`, every corpus token of at least
`min_token_length` characters. It never compares against whole texts. Matches
below `score_cutoff` (0 to 100) are ignored. Tune it in `FuzzyConfig`.

### Sparse

`BM25sIndex` scores BM25 with the bm25s formulas (`lucene` variant, `k1=1.5`,
`b=0.75`) over the tokens produced by the tokenizer. The default
`SimpleTokenizer` folds case, applies NFC and splits on Unicode word runs. The
corpus statistics (chunk count, document frequencies, average length) are
those of the generation's current chunks and are applied at query time, so
scores equal those of a fresh bm25s index over the same chunks, however the
generation was updated.

### Dense

The dense method is only wired when you pass an embedding model in
`Components(embedding=...)`. The default vector store is `QdrantVectorStore`:
in memory, or on disk under `data_dir/qdrant` when `EngineResources.data_dir`
is set. See [Use your own embedding model](../how-to/embedding-model.md) and
[Qdrant](../../integrations/qdrant.md).

Passing a `vector_store` without an `embedding` raises `ConfigurationError`.

## Graph channel

The graph channel reads entity and relationship aggregates. They come from
`GraphRepresentation` inputs or from a `GraphExtractorABC` you supply. The
`IdentityGraphNormalizer` merges entities with the same type and folded key.
Aggregates are shared across documents and each keeps its sources.

`NeighborhoodGraphRetriever` finds seed entities whose name or alias equals a
query n-gram, then expands `depth` hops. Seeds score `1.0`, nodes at distance
`d` score `1 / (1 + d)`. Each hit records its path.

Enable it with `GraphChannelConfig(enabled=True)` and install the `graph` extra.
See [Graph and structure](../../integrations/graph-and-structure.md).

## Structure channel

The structure channel reads section-tree nodes from `StructureRepresentation`
inputs. `TreeSearchStructureEngine` keeps one SQLite FTS5 database per scope and
generation. Enable it with `StructureChannelConfig(enabled=True)` and install
the `structure` extra.

## Choosing methods per request

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

## Weights

`MethodConfig.weight` weights a method inside index-channel method fusion.
`IndexChannelConfig.weight`, `GraphChannelConfig.weight` and
`StructureChannelConfig.weight` weight channels in channel fusion. Weights only
matter for the weighted fusion methods. See
[Retrieval pipeline](retrieval.md#fusion).
