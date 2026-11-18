---
name: retrievalloom-extend
description: >-
  Use this skill when writing a custom RetrievalLoom (retrievalloom package)
  component against its base classes: an embedding model (EmbeddingModelABC) for sentence-transformers, FlagEmbedding, ONNX, or an
  HTTP API; a reranker (RerankerABC) or NLI model (NLIABC); a keyword, fuzzy,
  or sparse index (SparseIndexABC, BM25 or SPLADE); a vector store
  (VectorStoreABC) for pgvector, Elasticsearch, OpenSearch, Milvus, or
  Weaviate; a graph store, graph extractor, or structure engine; a metadata
  store (DocumentMetadataStoreABC or a SQLAlchemyMetadataStore dialect); a
  cache, access policy, provenance recorder, tokenizer, keyword dictionary,
  fusion, normalizer, deduplicator, or consolidator. Also use it to test such
  a component with the contract kits in retrievalloom.testing (for example a
  failing SparseIndexContract or EmbeddingModelContract test) or to debug a
  custom component the engine rejects. Not for installing, configuring, or
  calling RetrievalLoom in an app or reading result warnings; use the
  retrievalloom skill.
license: Apache-2.0
compatibility: >-
  Python 3.12 to 3.14 with retrievalloom 0.2.x and its testing extra
  (pip install "retrievalloom[testing]"). The templates and their tests run
  offline and download nothing.
metadata:
  version: "0.2.0"
  documentation: "https://retrievalloom.readthedocs.io/en/latest/guide/how-to/custom-backends/"
---

# Extending RetrievalLoom

Connect RetrievalLoom to the model, database, or search service the project
already runs. Every replaceable part of the engine is a small base class in
`retrievalloom.contracts`. You subclass it, prove it with a ready-made pytest
suite (a *contract kit*), and pass an instance in `Components`.

## Workflow

Copy this checklist and keep it current:

```text
- [ ] 1. Base class and contract kit chosen
- [ ] 2. Template copied and adapted
- [ ] 3. Contract kit passes against the real backend
- [ ] 4. Component wired through Components; one ingest and retrieve work
- [ ] 5. Project tests pass
```

1. Pick the base class from the table in
   [references/contract-kits.md](references/contract-kits.md). Install the
   kits with `pip install "retrievalloom[testing]"`.
2. Copy the closest template from `assets/` into the project:

   | You are writing | Copy | And its test |
   | --- | --- | --- |
   | Keyword, fuzzy, or sparse index; vector store; any generational backend | `sparse_index_template.py` | `test_sparse_index_contract.py` |
   | Embedding model | `embedding_template.py` | `test_embedding_contract.py` |
   | Reranker (NLI follows the same shape) | `reranker_template.py` | `test_reranker_contract.py` |
   | Cache | `cache_template.py` | `test_cache_contract.py` |
   | Access policy | `access_policy_template.py` | `test_access_policy.py` |

   Replace each in-memory operation with the call its comment names. Keep the
   method signatures, the `info` fingerprint, and the offloader calls.
3. Point the copied test at the new class, connected to a disposable instance
   of the real backend. Run it with pytest-asyncio in auto mode (copy
   `assets/pytest.ini` or set `asyncio_mode = "auto"`). Fix the first failing
   test, rerun, and repeat until every test passes.
4. Pass the component to the engine and run one real round trip:

   ```python
   from retrievalloom import Components, Scope, create_retrieval_engine

   engine = create_retrieval_engine(components=Components(sparse_index=MySparseIndex()))
   async with engine:
       ...  # ingest one document, then retrieve it with scope=Scope(tenant_id="t1")
   ```

5. Run the project's own tests. Do not report the task done until the kit
   and the project tests pass.

## Read more only when needed

| Task | Read |
| --- | --- |
| Base classes, kits, hooks, and pytest setup | [references/contract-kits.md](references/contract-kits.md) |
| Index backends: generations, capabilities, hits, filters, scopes | [references/index-backends.md](references/index-backends.md) |
| Embedding, reranker, NLI, tokenizer, keyword dictionary, graph extraction | [references/models.md](references/models.md) |
| Metadata store, cache, access policy, provenance, fusion, deduplication | [references/infra-and-evidence.md](references/infra-and-evidence.md) |

## Rules

- **Fingerprint what changes the index.** Build `info` with `make_info(...)`
  and put every setting that changes stored content into `config`,
  `model_identity`, or `dimensions`. The engine rebuilds generations whose
  fingerprint no longer matches.
- **Never block the event loop.** Run blocking client or model calls with
  `await context.offloader.run(WorkloadKind..., fn, *args, tenant=...)`.
  Await async clients directly.
- **Keep scopes apart.** Key every stored object by `scope.key` and the
  generation id. Data of one scope never surfaces in another, even with equal
  generation ids.
- **Own what you create.** `close()` releases only what `initialize()`
  opened. Never close a client the application passed in. Make both methods
  idempotent.
- **Never download at runtime.** Load model files from a local path the
  application provides.
- **Do not weaken the kit.** Fix the component, not the test. Turn off
  `check_relevance` or `check_semantics` only for a model that cannot meet
  them, and say so.

## Gotchas

- `remove` is always accepted, also after `seal` and for an unknown
  generation, and returns how many objects were present. Removed objects
  must be unsearchable when it returns.
- With `mutable_in_place=False`, `write` after `seal` must raise a
  `RetrievalLoomError` such as `GenerationError`. With `True`, it must apply.
- Return only real matches. The index kits count every returned hit as a
  match, except `VectorStoreContract`, which ignores near-zero similarity.
- Ranks start at 1 and are consecutive. Each hit's `object_ref.scope_key`,
  `generation_id`, and `method.channel` must match the request.
- `SparseQuery.tokens` comes from the engine's tokenizer. A learned-sparse
  model that runs its own tokenizer encodes `SparseQuery.text` instead.
- With `filter_pushdown=False`, the engine filters after your search and adds
  an `info` `filter_post_applied` warning. Pushdown is better: return fewer,
  correct hits.
- `max_batch` caps how many items the engine passes to one `write` or
  `remove` call.
- A graph store whose traversal has a hard depth limit reports it in
  `capabilities.max_depth`; `initialize` rejects a deeper configured depth.
- `EmbeddingModelABC.max_batch_size` defaults to 64. The engine rejects
  vectors with the wrong count or dimensions, non-finite values, or a zero
  norm.
- There is no kit for tokenizers, keyword dictionaries, graph normalizers,
  graph retrievers, access policies, or provenance recorders. Test them
  through an engine, as `assets/test_access_policy.py` does.
- A recorder that raises `TenantIsolationError` always fails the call. Other
  recorder errors become a `provenance_recording_failed` warning unless
  `fail_on_recording_error=True`.

## Files

- `assets/sparse_index_template.py` and `assets/test_sparse_index_contract.py`:
  a BM25 sparse index shaped like one table per scope and generation, and the
  `SparseIndexContract` it passes.
- `assets/embedding_template.py` and `assets/test_embedding_contract.py`: a
  deterministic hashed embedding, and the `EmbeddingModelContract` it passes.
- `assets/reranker_template.py` and `assets/test_reranker_contract.py`: a
  term-overlap reranker, and the `RerankerContract` it passes.
- `assets/cache_template.py` and `assets/test_cache_contract.py`: a
  dictionary cache, and the `CacheContract` it passes.
- `assets/access_policy_template.py` and `assets/test_access_policy.py`: a
  tenant-and-team policy, tested through a real engine.
- `assets/pytest.ini`: the pytest-asyncio settings the kits need.
