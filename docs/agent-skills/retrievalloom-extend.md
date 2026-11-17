# The retrievalloom-extend skill

The `retrievalloom-extend` skill teaches a coding agent to connect
RetrievalLoom to the models, databases, and services you already run. Every
replaceable part of the engine is a small base class, and the agent proves
its implementation with the same contract kits RetrievalLoom runs against its
own components. It covers:

- embedding models, rerankers, NLI models, tokenizers, and keyword
  dictionaries;
- keyword, fuzzy, and sparse indexes, vector stores, graph stores, and
  structure engines;
- metadata stores, caches, access policies, and provenance recorders;
- fusion, score normalization, deduplication, and consolidation.

[:material-github: View on GitHub](https://github.com/smuniharish/retrievalloom/tree/main/retrievalloom-skills/skills/retrievalloom-extend){ .md-button }
[:material-download: Install](index.md#install-the-skills){ .md-button }

## When the agent uses it

The agent loads the skill to write, test, or debug such a component.
Examples include a pgvector vector store, a Cohere reranker, a
sentence-transformers embedding model, a failing `SparseIndexContract` test,
or an engine error about a hit from another scope.

To install, configure, or call RetrievalLoom in an application, it hands
over to the [retrievalloom skill](retrievalloom.md).

## Workflow

1. Pick the base class and its contract kit.
2. Copy the closest template from `assets/`. Each in-memory operation has a
   comment naming the backend call that replaces it.
3. Point the template's test at the new class and a disposable backend. Fix
   the first failing test and rerun until every test passes.
4. Pass the component to the engine through `Components` and run one real
   ingest and retrieve.
5. Run the project's own tests.

The agent follows these rules throughout:

- put every setting that changes stored content into the fingerprint;
- run blocking calls through the engine's offloader;
- keep each scope's data apart, even with equal generation ids;
- close only what the component opened;
- never weaken a kit test to make it pass.

## What's inside

```text
retrievalloom-extend/
├── SKILL.md                              # workflow, rules, gotchas
├── references/
│   ├── contract-kits.md                  # base classes, kits, hooks, pytest setup
│   ├── index-backends.md                 # generations, capabilities, hits, filters
│   ├── models.md                         # embedding, reranker, NLI, tokenizer, graph
│   └── infra-and-evidence.md             # metadata store, cache, access, fusion
├── assets/
│   ├── sparse_index_template.py          # a BM25 sparse index to adapt
│   ├── test_sparse_index_contract.py     # its SparseIndexContract test
│   ├── embedding_template.py             # a deterministic embedding model
│   ├── test_embedding_contract.py        # its EmbeddingModelContract test
│   ├── reranker_template.py              # a term-overlap reranker
│   ├── test_reranker_contract.py         # its RerankerContract test
│   ├── cache_template.py                 # a dictionary cache
│   ├── test_cache_contract.py            # its CacheContract test
│   ├── access_policy_template.py         # a tenant-and-team access policy
│   ├── test_access_policy.py             # engine-level tests for it
│   └── pytest.ini                        # pytest-asyncio settings for the kits
└── evals/                                # tasks, assertions, trigger queries
```

The templates run as they are: their tests pass against RetrievalLoom alone,
offline, so the agent starts from working code and changes one piece at a
time.

## Run the templates' tests

From the skill directory, run the tests with pytest:

```bash
pip install "retrievalloom[testing]"
pytest assets
```

`assets/pytest.ini` sets pytest-asyncio's auto mode, which the contract kits
need. In your project, set `asyncio_mode = "auto"` in your pytest
configuration instead.

## Learn more

- [Write your own backend](../guide/how-to/custom-backends.md): the base
  classes, the index obligations, and the contract kits.
- [Use your own embedding model](../guide/how-to/embedding-model.md): enable the
  dense method with a model of your own.
- [Custom components](../examples/adapters.md): complete adapters to copy.
- [Contracts API](../api/contracts.md) and [testing API](../api/testing.md):
  every method and kit.
