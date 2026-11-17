# The retrievalloom skill

The `retrievalloom` skill teaches a coding agent to add RetrievalLoom to an
application and work with it once it is there. It covers:

- installing the package and the extras the application needs;
- configuring channels and methods, and enabling a method later;
- ingesting, deleting, purging, and retrieving documents per scope;
- metadata filters, access policies, and per-tenant limits;
- durable storage with SQLite, PostgreSQL, and Qdrant;
- background and foreground indexing, backfill, and rebuild;
- reranking, NLI claim checks, logging, and agxai provenance;
- reading result statuses and warnings, and troubleshooting.

[:material-github: View on GitHub](https://github.com/smuniharish/retrievalloom/tree/main/retrievalloom-skills/skills/retrievalloom){ .md-button }
[:material-download: Install](index.md#install-the-skills){ .md-button }

## When the agent uses it

The agent loads the skill when the task involves RetrievalLoom or hybrid
retrieval for RAG, search, or agents. Examples include tenant isolation,
erasure requests, results that come back `partial`, and moving from SQLite to
PostgreSQL.

To write an embedding model, reranker, index backend, store, cache, or access
policy of your own, it hands over to
[retrievalloom-extend](retrievalloom-extend.md). It does not cover other
retrieval libraries such as LangChain retrievers or LlamaIndex.

## Workflow

The skill gives the agent a checklist to keep current while it works:

```text
- [ ] 1. verify_setup.py passes
- [ ] 2. One engine per process, opened with async with, configured once
- [ ] 3. Every call passes the caller's Scope (and principal with a policy)
- [ ] 4. Callers check result.status and result.warnings
- [ ] 5. Storage and lifecycle chosen for the deployment
- [ ] 6. Project tests and verify_setup.py pass again
```

The agent checks the environment before it changes anything and again when
it finishes. It also copies a behavior test template into the project, which
proves scope isolation, filters, citations, deletion, and erasure.

## What's inside

```text
retrievalloom/
├── SKILL.md                          # workflow, rules, gotchas
├── references/
│   ├── API.md                        # imports, signatures, inputs, reports, errors
│   ├── configuration.md              # channels, methods, fusion, timeouts, limits
│   ├── lifecycle.md                  # background and foreground, wait_for, backfill
│   ├── storage.md                    # data_dir, SQLite, PostgreSQL, Qdrant, retention
│   ├── access-and-tenancy.md         # scopes, filters, access policies
│   ├── rerank-and-nli.md             # FlashRank or your model, NLI claims
│   ├── observability.md              # logs, health, status, agxai, tracing
│   ├── failure-modes.md              # what degrades and what raises
│   └── troubleshooting.md            # warning codes, errors, unexpected results
├── scripts/
│   └── verify_setup.py               # offline environment and behavior check
├── assets/
│   └── test_retrieval_behavior.py    # pytest template for the application
└── evals/                            # tasks, assertions, trigger queries
```

The agent always sees the skill's name and description. It reads `SKILL.md`
when a task matches, and opens a reference only when the task needs it, so a
simple task costs little context.

## The setup check

`scripts/verify_setup.py` runs in the project's Python environment. It works
offline and never downloads a model. It checks:

- the Python version, RetrievalLoom, and its dependency ranges;
- the optional extras: graph, structure, postgres, and testing;
- whether the FlashRank model files are present;
- a full ingest, retrieve, filter, delete, and purge cycle in memory, with
  scope isolation;
- PostgreSQL and Qdrant servers, when their environment variables are set.

| Option or variable | Effect |
| --- | --- |
| `--require EXTRA` | Fail when the `graph`, `structure`, `postgres`, or `testing` extra is missing. Repeatable. |
| `--model-dir DIR` | Look for the FlashRank model in this directory instead of `~/.cache/retrievalloom/models`. |
| `--json` | Print one JSON object for the agent to parse. |
| `RETRIEVALLOOM_DSN` | Connect to this PostgreSQL database and run `SELECT 1`. |
| `RETRIEVALLOOM_QDRANT_URL` | List the collections of this Qdrant server. `RETRIEVALLOOM_QDRANT_API_KEY` sets its key. |

It exits with `0` when every check passes and `1` otherwise. Sample output is
on the [install page](install.md#verify).

## Rules the agent follows

- **Scope every call.** Pass the caller's scope to each call, and never widen
  it to search across tenants.
- **Report degradation.** Never treat a `partial` result as complete, or hide
  its warnings.
- **Never download models implicitly.** Provision model files ahead of time.
- **Keep content out of logs** unless you ask for it.
- **Own what you create.** The engine closes only what it created.
