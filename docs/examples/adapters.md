# Custom components

`examples/adapters/` is a small package with one production-style implementation per
extension point. Every class inherits the real ABC from `retrievalloom.contracts`. All of
them are async. Blocking or CPU-heavy work (model inference, regexes over large texts)
goes through `context.offloader` with the matching `WorkloadKind`, so the event loop stays
responsive. Heavy imports (FlagEmbedding, `neo4j`, `asyncpg`, `qdrant-client`) are
deferred until a class is first used, so `from adapters import TTLCache` stays cheap.

The scenario scripts import the package as `adapters`, because `examples/` is on their
path. Tests import it as `examples.adapters`.

| Component | ABC (`Components` field) | What it does | Used by | Verified by |
| --- | --- | --- | --- | --- |
| `BgeM3Embedding` | `EmbeddingModelABC` (`embedding`) | Dense bge-m3 vectors via FlagEmbedding `BGEM3FlagModel`; one loaded model per path and device is shared by all adapters | customer support, lifecycle, durable restart | `EmbeddingModelContract` (live) |
| `BgeM3SparseIndex` | `SparseIndexABC` (`sparse_index`) | Learned-sparse retrieval: bge-m3 lexical weights stored as Qdrant sparse vectors; every point carries its scope and generation and every request filters on both; replaces BM25 | durable restart | `SparseIndexContract` (live) |
| `BgeCrossEncoderReranker` | `RerankerABC` (`reranker`) | FlagEmbedding `FlagReranker` over `bge-reranker-v2-m3`, optional sigmoid normalisation | customer support, fact checking | `RerankerContract` (live) |
| `UltraBertNLI` | `NLIABC` (`nli`) | Premise/claim entailment, neutral or contradiction from `UltraBERT-150M-nli` | fact checking | `NLIContract` (live) |
| `Neo4jGraphStore` | `GraphStoreABC` (`graph_store`) | Official async `neo4j` driver. Entities and relationships keyed by scope and generation, with the same isolation semantics as `LadybugGraphStore`. Works with the default `NeighborhoodGraphRetriever` | knowledge graph | `GraphStoreContract` (live) |
| `PgVectorStore` | `VectorStoreABC` (`vector_store`) | `asyncpg` + pgvector; one table per dimension with scope and generation columns, cosine search, metadata filters translated to SQL over `jsonb` | document lifecycle | `VectorStoreContract` (live) |
| `StemmingTokenizer` | `TokenizerABC` (`tokenizer`) | Unicode word tokens, case folding, light English suffix stemming (`holidays` matches `holiday`) | multi-tenant, policy manual, HTTP service | `ComponentContract` + behaviour tests |
| `GlossaryKeywordDictionary` | `KeywordDictionaryABC` (`keyword_dictionary`) | Maps product jargon and synonyms (`MFA`, `2FA`) to canonical keywords from `data/glossary.json`. Entries under `"*"` apply to every tenant, other entries only to their tenant | customer support | `ComponentContract` + behaviour tests |
| `TTLCache` | `CacheABC` (`cache`) | In-process LRU cache with per-entry TTL and hit/miss counters | customer support | `CacheContract` |
| `TenantRoleAccessPolicy` | `AccessPolicyABC` (`access_policy`) | Principals `"<tenant>:<role>"`; denies cross-tenant access, anonymous access and actions the role does not allow; adds a `visibility` row filter per role | multi-tenant, HTTP service | `ComponentContract` + decision tests |
| `JsonlProvenanceRecorder` | `ProvenanceRecorderABC` (`provenance_recorder`) | Append-only JSONL audit log: one line per ingestion, deletion and retrieval with principal, scope, query and the exact document versions returned | multi-tenant | `ComponentContract` + behaviour tests |
| `WeightedCombSUMFusion` | `FusionABC` (`method_fusion`/`channel_fusion`) | Weighted CombSUM over normalised scores; a drop-in alternative to the default reciprocal rank fusion | kit and unit tests only | `FusionContract` |
| `PercentileNormalizer` | `ScoreNormalizerABC` | Rank-percentile normalisation. Score normalisers are not a `Components` field, so it plugs into `WeightedCombSUMFusion` | kit and unit tests only | `ScoreNormalizerContract` |
| `NearDuplicateDeduplicator` | `DeduplicatorABC` (`deduplicator`) | Exact (identity and content hash) dedup, then merges candidates whose word shingles overlap by Jaccard ≥ threshold, keeping every source | policy manual | `DeduplicatorContract` |
| `DocumentDiversityConsolidator` | `EvidenceConsolidatorABC` (`consolidator`) | Caps candidates per source document so one long manual cannot fill the result, never dropping a contradiction, then consolidates hierarchically | policy manual | `EvidenceConsolidatorContract` |
| `CapitalizedEntityExtractor` | `GraphExtractorABC` (`graph_extractor`) | Capitalised-phrase entities with configured types, plus `A VERB B` relationships from patterns | knowledge graph | `GraphExtractorContract` |
| `AliasGraphNormalizer` | `GraphNormalizerABC` (`graph_normalizer`) | Case folding and an alias map (`PG` → `Postgres`), merging duplicate entities and edges | knowledge graph | `ComponentContract` + behaviour tests |

Not provided:

- **Structure engine.** The built-in TreeSearch engine already handles application-supplied
  section trees well (see [Policy manual](policy-manual.md)). A useful alternative would
  have to be a full tree index and is beyond an example.
- **Graph retriever.** `Neo4jGraphStore` implements the store contract, so the default
  `NeighborhoodGraphRetriever` runs on it unchanged.
- **Metadata store.** The library ships SQLite and PostgreSQL stores; the durable restart
  example uses SQLite in `data_dir`.

## Learned sparse and `SparseIndexABC`

`SparseIndexABC` is the engine's lexical slot (`index.sparse`), so a learned-sparse model
replaces BM25 without any engine change. Indexing receives each chunk's text, which the
index encodes with bge-m3 into the model's own lexical weights. Every Qdrant point
carries its scope and generation, so building, publishing and retiring work as they do
for any other index.

The contract has one rough edge. A `SparseQuery` carries the engine tokenizer's tokens,
not the raw query, so the index encodes `" ".join(query.tokens)` and indexes casefolded
text to match. With the default `SimpleTokenizer` this loses only case and punctuation.
A stemming or stop-word tokenizer would make queries and documents disagree, so pair
the index with `SimpleTokenizer`, as the durable restart example does. A raw query text
field on `SparseQuery` would remove this constraint.

## Using a component

```python
from adapters import BgeM3Embedding, PgVectorStore, TenantRoleAccessPolicy
from retrievalloom import Components, create_retrieval_engine

engine = create_retrieval_engine(
    components=Components(
        embedding=BgeM3Embedding("models/bge-m3"),
        vector_store=PgVectorStore("postgresql://rl:rl@localhost:55433/rl"),
        access_policy=TenantRoleAccessPolicy(),
    ),
)
```

The engine initialises supplied components but does not close them. You own them and
close them, or drop their tables, after the engine has closed. The examples do this in
`finally` blocks.

## Testing your own component

Each kit is a pytest mixin class. Subclass it, implement `make_component`, and the kit
runs the full contract: identity, lifecycle, generations, tenant isolation, error types
and so on. For example, `tests/live/test_example_stores.py` does this:

```python
from retrievalloom.testing import VectorStoreContract


class TestPgVectorStore(VectorStoreContract):
    async def make_component(self, tmp_path):
        return PgVectorStore(DSN, table_prefix=unique_prefix())
```

## Source

#### `examples/adapters/models.py`

<!-- fmt: off -->
```python title="examples/adapters/models.py"
--8<-- "examples/adapters/models.py"
```
<!-- fmt: on -->

#### `examples/adapters/sparse.py`

<!-- fmt: off -->
```python title="examples/adapters/sparse.py"
--8<-- "examples/adapters/sparse.py"
```
<!-- fmt: on -->

#### `examples/adapters/neo4j_graph.py`

<!-- fmt: off -->
```python title="examples/adapters/neo4j_graph.py"
--8<-- "examples/adapters/neo4j_graph.py"
```
<!-- fmt: on -->

#### `examples/adapters/pgvector.py`

<!-- fmt: off -->
```python title="examples/adapters/pgvector.py"
--8<-- "examples/adapters/pgvector.py"
```
<!-- fmt: on -->

#### `examples/adapters/infra.py`

<!-- fmt: off -->
```python title="examples/adapters/infra.py"
--8<-- "examples/adapters/infra.py"
```
<!-- fmt: on -->

#### `examples/adapters/text.py`

<!-- fmt: off -->
```python title="examples/adapters/text.py"
--8<-- "examples/adapters/text.py"
```
<!-- fmt: on -->

#### `examples/adapters/evidence.py`

<!-- fmt: off -->
```python title="examples/adapters/evidence.py"
--8<-- "examples/adapters/evidence.py"
```
<!-- fmt: on -->

#### `examples/adapters/graph.py`

<!-- fmt: off -->
```python title="examples/adapters/graph.py"
--8<-- "examples/adapters/graph.py"
```
<!-- fmt: on -->
