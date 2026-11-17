# RetrievalLoom Agent Skills

Give your coding agent the instructions, references, checks, and templates it
needs to build with RetrievalLoom correctly. This directory holds two Agent
Skills that work in any agent that reads the open Agent Skills format.

| Skill | Use it to |
| --- | --- |
| [`retrievalloom`](skills/retrievalloom/SKILL.md) | Add RetrievalLoom to an application: install and configure the engine, ingest, delete, purge and retrieve per scope, enable channels and methods, keep data on disk (SQLite, PostgreSQL, Qdrant), run the lifecycle, rerank and verify with NLI, record provenance with agxai, and troubleshoot `partial` results |
| [`retrievalloom-extend`](skills/retrievalloom-extend/SKILL.md) | Extend RetrievalLoom: implement an embedding model, reranker, NLI model, index backend, vector store, graph store, metadata store, cache or access policy against its base classes, and prove it with the contract kits in `retrievalloom.testing` |

Install them with the [skills CLI](https://github.com/vercel-labs/skills),
which finds them through [`skills.sh.json`](../skills.sh.json):

```bash
npx skills add smuniharish/retrievalloom                                # choose interactively
npx skills add smuniharish/retrievalloom --skill retrievalloom          # only the core skill
npx skills add smuniharish/retrievalloom --skill retrievalloom-extend   # only the extension skill
```

Manual installation, global installs, and per-agent folders are covered in the
[documentation](https://retrievalloom.readthedocs.io/en/latest/agent-skills/install/).

## Layout

Each skill follows the [Agent Skills specification](https://agentskills.io/specification):

```text
skills/<skill>/
├── SKILL.md      # frontmatter, workflow, rules, gotchas, links to every file
├── references/   # focused documents, one level deep, read only when a task needs them
├── scripts/      # offline checks with --help, --json, and meaningful exit codes
├── assets/       # templates the agent copies into the project, each with its tests
└── evals/        # evals.json, trigger_queries.json, and the input files they use
```

| Skill | Scripts | Assets |
| --- | --- | --- |
| `retrievalloom` | `verify_setup.py`: offline environment and behavior check; optional PostgreSQL and Qdrant probes from environment variables | `test_retrieval_behavior.py` |
| `retrievalloom-extend` | - | `embedding_template.py`, `reranker_template.py`, `sparse_index_template.py`, `cache_template.py`, `access_policy_template.py`, a `test_*.py` for each, and `pytest.ini` |

Every Agent Skills host reads the same directories, so there is no separate
copy for Claude Code, Codex, Cursor, or GitHub Copilot.

## Format

- `name` matches the directory. The `description` starts with "Use this skill
  when", names near misses, and hands over to the other skill where they
  meet.
- `license`, `compatibility`, and `metadata.version` are set. The version
  equals the `retrievalloom` release the skill describes.
- `SKILL.md` stays short and links every reference with the task that needs
  it. Scripts and assets are referenced by paths relative to the skill
  directory.

## Maintaining the skills

When the public API or documented behavior changes:

1. Update the affected skill, and `metadata.version` in both skills with each
   release.
2. Check every changed snippet and claim against the code, tests, or
   documentation. Leave out anything that is not implemented.
3. Add or update an eval when the change affects what an agent should do.
4. Run the checks in [`validation/README.md`](validation/README.md).

The skills are covered by the repository's Apache License 2.0; see
[LICENSE](../LICENSE). They are not part of the Python package and add no
runtime behavior.
