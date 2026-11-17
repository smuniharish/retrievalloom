# Index backends

Store and search one retrieval method in your own search service or
database. The engine handles consistency: it builds each index as a
*generation*, writes the items, seals it, and then switches searches to it.
Your backend only has to keep generations apart and answer searches.

## Implement the generation methods

Keyword, fuzzy, sparse, vector, graph, and structure backends all derive from
`GenerationalIndexABC` (`retrievalloom.contracts.indexes`).

| Method | Obligation |
| --- | --- |
| `capabilities` | Return `IndexCapabilities(mutable_in_place, durable, filter_pushdown=True, max_batch=256)`. |
| `create_generation(scope, spec)` | Create an empty generation. Idempotent. |
| `write(scope, generation_id, items)` | Insert or replace items by `object_id`. Return `WriteReport(written=...)`. |
| `remove(scope, generation_id, object_ids)` | Delete items and return how many were present. Always accepted. |
| `update_metadata(scope, generation_id, updates)` | Replace the filter metadata of present items. Return how many changed. |
| `seal(scope, generation_id)` | Finish the build so searches see every write. Return `GenerationStats(object_count=...)`. |
| `drop_generation(scope, generation_id)` | Delete the generation. Idempotent. |
| `list_generations(scope)` | Return the generation ids of this scope only. |

Store each generation under both `scope.key` and the generation id, for
example one table, collection, or index per pair. Two scopes can use the same
generation id.

## Declare capabilities

| Field | Meaning |
| --- | --- |
| `mutable_in_place` | `True` lets the engine write changes into the active generation. With `False`, every change builds a new generation, and `write` after `seal` must raise a `RetrievalLoomError` such as `GenerationError`. |
| `durable` | `True` declares that data outlives the process and can be shared by several engines. The engine then protects generations with durable reader leases. |
| `filter_pushdown` | `True` means `search` applies `filters` itself. With `False`, the engine filters afterwards and adds an `info` `filter_post_applied` warning. |
| `max_batch` | The most items the engine passes to one `write` call. |

Keyword and fuzzy indexes get a new generation whenever the keyword
dictionary changes, because `GenerationSpec.keyword_entries` is fixed per
generation.

## Read the inputs

`GenerationSpec` has `generation_id`, `method`, `dependency_fingerprint`,
`dimensions` (dense only), `keyword_entries`, and `options`.

Each `IndexItem` has `object_id`, `document_uid`, `version`, `kind`, `text`,
`metadata`, `tokens`, `vector`, and `payload`. The engine fills `tokens` for
sparse and fuzzy units from its tokenizer, and `vector` for dense units from
the embedding model. Index `metadata` for filtering; it is the same metadata
the application filters on.

The search query depends on the base class:

| Base class | `search` query argument |
| --- | --- |
| `KeywordIndexABC` | `KeywordQuery(text=...)`: the raw query. Match `keyword_entries` surface forms. |
| `FuzzyIndexABC` | `FuzzyQuery(tokens=...)`: the tokenized query. Tolerate typos. |
| `SparseIndexABC` | `SparseQuery(tokens=..., text=...)`: BM25-style indexes score `tokens`; learned-sparse models that run their own tokenizer encode `text`. |
| `VectorStoreABC` | `vector`: the query embedding. |
| `StructureEngineABC` | `query`: the raw text. It also implements `navigate(scope, generation_id, object_id, direction)`. |
| `GraphStoreABC` | No `search`. Implement `find_entities(scope, generation_id, terms, limit)` and `neighborhood(scope, generation_id, seeds, depth, limit)`. |

All `search` methods take `(scope, generation_id, query, limit, filters)` and
return `Sequence[RetrievalHit]`.

## Return hits

```python
from retrievalloom.domain.records import Channel
from retrievalloom.domain.retrieval import (
    MatchDetail,
    MethodRef,
    ObjectRef,
    RetrievalHit,
    ScoreRange,
    ScoreSemantics,
)

hit = RetrievalHit(
    object_ref=ObjectRef(scope_key=scope.key, object_id=object_id, generation_id=generation_id),
    method=MethodRef(channel=Channel.INDEX, method="sparse"),
    backend_id="my-search",
    raw_score=score,
    score_semantics=ScoreSemantics(higher_is_better=True, range=ScoreRange.UNBOUNDED),
    rank=rank,  # 1, 2, 3, ... in result order
    matched=(MatchDetail(kind="term", term="vpn"),),
)
```

This is a fragment: `scope`, `generation_id`, `object_id`, `score`, and
`rank` come from your search loop.

- Return only real matches, best first, at most `limit`, with no duplicate
  object ids.
- Never return a hit of another scope. The engine raises
  `TenantIsolationError` for it, and `ComponentContractError` for a hit of
  another generation or channel.
- Use `Channel.INDEX` for keyword, fuzzy, sparse, and dense,
  `Channel.STRUCTURE` for structure, and `Channel.GRAPH` for graph.
- Declare honest `score_semantics`. `ScoreRange` values are `unbounded`,
  `unit_interval`, `distance`, `similarity`, and `rank_only`. Fusion and
  normalization read them.
- `matched` explains the match. Use it for matched terms or entities.

## Apply filters

Translate the `FilterExpression` into the backend's query language when you
can. Otherwise evaluate it per item with
`retrievalloom.domain.filters.evaluate(filters, metadata)`, as the sparse
template does. Apply filters before `limit`, or the engine receives fewer
hits than it asked for.

## Stay responsive

- Run blocking client calls through the offloader from `initialize`:
  `await self._offloader.run(WorkloadKind.SPARSE_QUERY, fn, *args, tenant=scope.tenant_id)`.
  Pick the matching `WorkloadKind`, for example `VECTOR`, `KEYWORD_QUERY`,
  `FUZZY_MATCH`, `SPARSE_INDEX`, or `STRUCTURE_QUERY`.
- Pass the worker a snapshot of what it reads. It must not mutate shared
  state that the event loop also changes.
- Let `asyncio.CancelledError` propagate. A cancelled search must leave the
  backend usable.

## Start from a template

`assets/sparse_index_template.py` implements every method above for a BM25
sparse index. To adapt it to another method, change the base class, the
`search` query argument, the `MethodRef`, and the `WorkloadKind`, then switch
the test to the matching kit. RetrievalLoom's `QdrantVectorStore`
(`retrievalloom.index.vector`) is a complete durable vector store to read.
