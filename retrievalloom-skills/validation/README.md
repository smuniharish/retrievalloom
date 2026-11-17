# Validating the RetrievalLoom skills

Check that the `retrievalloom` and `retrievalloom-extend` skills stay correct
in three ways: automated checks on every change, evaluations against real
tasks, and a review of every claim against its source.

## Run the automated checks

Run these commands from the repository root. They work offline and download
no models.

```bash
# The setup script, with warnings treated as errors; --json must print one object
python -W error retrievalloom-skills/skills/retrievalloom/scripts/verify_setup.py
python -W error retrievalloom-skills/skills/retrievalloom/scripts/verify_setup.py --json

# The asset tests: the application tests and every extension template
pytest retrievalloom-skills/skills/retrievalloom/assets --no-cov -p no:cacheprovider
pytest retrievalloom-skills/skills/retrievalloom-extend/assets --no-cov -p no:cacheprovider \
  --rootdir retrievalloom-skills/skills/retrievalloom-extend/assets

# Lint, format, and type-check the bundled Python files
ruff check retrievalloom-skills
ruff format --check retrievalloom-skills
pyrefly check retrievalloom-skills/skills/retrievalloom/scripts/verify_setup.py \
  retrievalloom-skills/skills/retrievalloom/assets/*.py \
  retrievalloom-skills/skills/retrievalloom-extend/assets/*.py

# The frontmatter, against the Agent Skills specification
uvx --from skills-ref agentskills validate retrievalloom-skills/skills/retrievalloom
uvx --from skills-ref agentskills validate retrievalloom-skills/skills/retrievalloom-extend
```

The extension assets carry their own `pytest.ini`, which sets
pytest-asyncio's auto mode for the contract kits. `--rootdir` makes pytest
use it instead of the repository's settings.

To check the optional services too, set `RETRIEVALLOOM_DSN` to a PostgreSQL
URL or `RETRIEVALLOOM_QDRANT_URL` to a Qdrant URL before running
`verify_setup.py`. A wrong address must make it exit with code 1.

Also check these rules by hand or in a test:

- `name` matches the directory, and the description starts with "Use this
  skill when" and names the other skill for the tasks it does not cover.
- `license` is `Apache-2.0`, and `metadata.version` equals the package
  version in `pyproject.toml`.
- `SKILL.md` has fewer than 500 lines and links every reference.
  `references/` is one level deep, and every relative link resolves.
- `SKILL.md` names every bundled script and asset.
- `evals/evals.json` has at least three tasks with at least three assertions
  each, and every listed input file exists.
- `evals/trigger_queries.json` has 10 queries that should trigger the skill
  and 10 that shouldn't, split 12/8 into training and validation. No query
  triggers both skills.
- The skills CLI discovers both skills through `skills.sh.json`.

## Run the evaluations

Each skill has an `evals/` folder in the format described in
[Evaluating skills](https://agentskills.io/skill-creation/evaluating-skills):

- `evals.json`: realistic tasks with input files under `evals/files/`, an
  expected result, and assertions a grader can verify.
- `trigger_queries.json`: requests that should load the skill and near misses
  that shouldn't.

Two input files are starting points with known faults:
`retrievalloom-extend/evals/files/broken_index/index.py` fails 8 of the 13
`SparseIndexContract` tests, and `retrievalloom/evals/files/support_bot/app.py`
ignores the tenant when it searches.

To evaluate a change:

1. Copy each task's input files into fresh workspaces, one with the skill and
   one without.
2. Run each task in a new agent session. The session without the skill can
   read the installed package but not the skill, the docs, or the
   repository's source.
3. Grade every assertion as passed or failed, with the evidence. Record time
   and tool calls.
4. Compare the two variants. Change the skill where the agent went wrong with
   it, or where it did no better than without it, then run the evaluation
   again.

Keep workspaces and benchmark files out of the repository.

### Latest results

No evaluation has been run with agents yet. Record the first results here, in
a table of task, variant, assertions passed, tool calls, and time.

## Review every claim against its source

Automated checks can't tell whether guidance is correct. For every change,
check each snippet and claim against its source:

| Claim area | Source of truth |
| --- | --- |
| Public imports, signatures, and the package version | [`src/retrievalloom/__init__.py`](../../src/retrievalloom/__init__.py), [`pyproject.toml`](../../pyproject.toml), and the [API reference](../../docs/api/index.md) |
| Engine construction, ingest, delete, purge, and retrieve | [`src/retrievalloom/engine.py`](../../src/retrievalloom/engine.py) and [`docs/api/engine.md`](../../docs/api/engine.md) |
| Configuration, channels, methods, and tenant limits | [`src/retrievalloom/config.py`](../../src/retrievalloom/config.py) and [configuration](../../docs/getting-started/configuration.md) |
| Result statuses, warnings, and degradation | [`src/retrievalloom/retrieval/pipeline.py`](../../src/retrievalloom/retrieval/pipeline.py), [`src/retrievalloom/errors.py`](../../src/retrievalloom/errors.py), and [troubleshooting](../../docs/guide/troubleshooting.md) |
| Lifecycle, backfill, rebuild, and restarts | [`src/retrievalloom/lifecycle/`](../../src/retrievalloom/lifecycle/), [lifecycle concepts](../../docs/guide/concepts/lifecycle.md), and [backfill and rebuild](../../docs/guide/how-to/backfill-and-rebuild.md) |
| Durable storage and several engines | [`src/retrievalloom/storage/`](../../src/retrievalloom/storage/), [durable storage](../../docs/guide/how-to/durable-storage.md), and [several engines](../../docs/guide/how-to/multiple-engines.md) |
| Base classes and contract kits | [`src/retrievalloom/contracts/`](../../src/retrievalloom/contracts/), [`src/retrievalloom/testing/`](../../src/retrievalloom/testing/), and [custom backends](../../docs/guide/how-to/custom-backends.md) |
| Reranking, NLI, and agxai provenance | [rerank and NLI](../../docs/guide/how-to/rerank-and-nli.md) and [agxai](../../docs/integrations/agxai.md) |
| Working patterns | [`examples/`](../../examples/) and [`tests/`](../../tests/) |

If a behavior has no implementation, test, or documentation, leave it out of
the skill rather than infer it.

## Check the routing

Check that the descriptions send each task to the right skill:

| Task | Skill | Route | Avoids |
| --- | --- | --- | --- |
| "Add hybrid search to our RAG chatbot." | `retrievalloom` | One engine, a `Scope` per tenant, `result.status` checked. | One engine per request, or one shared scope with a tenant filter. |
| "Results are partial after every restart." | `retrievalloom` | `LifecycleConfig(wait_for_rebuild=True)` or `run_lifecycle_once`. | Hiding the warnings. |
| "Erase these documents for GDPR." | `retrievalloom` | `engine.purge` with the tenant's scope. | Using `delete`, which keeps history. |
| "Add semantic search to existing documents." | `retrievalloom` | `Components(embedding=...)`, then `backfill` and `wait_for`. | Re-ingesting everything. |
| "Use our pgvector database as the vector store." | `retrievalloom-extend` | `VectorStoreABC` from the index template, proven with `VectorStoreContract`. | Skipping or weakening a kit test. |
| "Wrap our embedding API." | `retrievalloom-extend` | The embedding template and `EmbeddingModelContract`. | Blocking the event loop or downloading models at runtime. |
| "Only admins may ingest." | `retrievalloom-extend` | The access policy template and its engine-level tests. | Trusting a principal taken from request input. |
| "Build a LangChain retriever." | Neither | Not this package. | Inventing integrations. |
