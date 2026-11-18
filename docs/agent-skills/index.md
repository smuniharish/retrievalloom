# Agent Skills

Let your coding agent add RetrievalLoom to your application, or connect it to
your own models and databases, the way the library is meant to be used.
RetrievalLoom publishes two [Agent Skills](https://agentskills.io) with the
instructions, references, checks, templates, and tests an agent needs. They
work in Claude Code, Codex, Cursor, GitHub Copilot, and any other agent that
reads the open Agent Skills format.

## Install the skills

Install the skills so your coding agent knows how to:

- add hybrid retrieval with one scope per tenant;
- keep data on disk with SQLite, PostgreSQL, or Qdrant;
- read result statuses and warnings instead of hiding them;
- write its own embedding model, reranker, index, or store, and prove it with
  the contract kits.

=== "Ask your coding agent"

    Give your coding agent the repository and let it install the skills it
    needs.

    ```text title="Agent instruction"
    Install the Agent Skills from https://github.com/smuniharish/retrievalloom
    (retrievalloom-skills/skills/retrievalloom and
    retrievalloom-skills/skills/retrievalloom-extend) into this project, run
    the retrievalloom skill's scripts/verify_setup.py, then add retrieval to
    this application with RetrievalLoom, following the skill's workflow and
    rules.
    ```

=== "skills CLI"

    The [skills CLI](https://github.com/vercel-labs/skills) finds both skills
    in the repository and installs them for the agents in your project. It
    needs Node.js.

    ```bash
    npx skills add smuniharish/retrievalloom
    ```

    To install one skill, add `--skill retrievalloom` or
    `--skill retrievalloom-extend`. Add `-g` to install it for every project.

=== "Manual installation"

    Copy the skill directories into your agent's skills directory, for
    example:

    ```bash
    git clone --depth 1 https://github.com/smuniharish/retrievalloom.git
    cp -r retrievalloom/retrievalloom-skills/skills/retrievalloom .agents/skills/retrievalloom
    cp -r retrievalloom/retrievalloom-skills/skills/retrievalloom-extend .agents/skills/retrievalloom-extend
    ```

    [Install in your agent](install.md) lists the directory each agent reads.

## Choose a skill

<div class="grid cards" markdown>

-   :material-magnify:{ .lg .middle } __[retrievalloom](retrievalloom.md)__

    ---

    Adds RetrievalLoom to an application. Covers configuration, scopes,
    ingest, delete, purge, retrieve, durable storage, the lifecycle,
    reranking and NLI, observability, and troubleshooting.

    Includes an offline setup check and a behavior test template.

-   :material-puzzle-outline:{ .lg .middle } __[retrievalloom-extend](retrievalloom-extend.md)__

    ---

    Extends RetrievalLoom. Use it to write an embedding model, reranker,
    index backend, vector store, metadata store, cache, or access policy of
    your own.

    Includes templates that pass their contract kits.

</div>

Each skill tells the agent when to use the other one, so you can install both
and the agent picks the right one. If your application uses only the built-in
components, `retrievalloom` alone is enough.

## Use them

Once installed, describe the task. The agent loads a skill when the task
matches it:

| You ask | Skill |
| --- | --- |
| "Add hybrid search to our support bot, one tenant per customer." | `retrievalloom` |
| "Results are partial after every deploy. Fix it." | `retrievalloom` |
| "Erase these two documents for a GDPR request." | `retrievalloom` |
| "Turn on semantic search for documents we already have." | `retrievalloom` |
| "Use our pgvector database as the vector store, with tests." | `retrievalloom-extend` |
| "Wrap our embedding HTTP API for RetrievalLoom." | `retrievalloom-extend` |

You can also invoke a skill by name: `/retrievalloom` or
`/retrievalloom-extend` in Claude Code, Cursor, and GitHub Copilot, and
`$retrievalloom` or `$retrievalloom-extend` in Codex.

Both skills teach the agent the rules that keep an integration correct:

- pass the caller's scope to every call, and never search across tenants;
- report partial results and their warnings instead of hiding them;
- never download models at runtime;
- never weaken a contract kit to make a test pass.

## Skills for integrations

The projects RetrievalLoom integrates with maintain their own skills. These
are optional and are not installed with RetrievalLoom:

| Integration | Upstream skill | Install |
| --- | --- | --- |
| [AGXAI](https://github.com/smuniharish/agxai), for provenance and audit trails | `agxai` and `agxai-extend` | `npx skills add smuniharish/agxai` |
| [Qdrant](https://qdrant.tech/documentation/agentic-tools/skills/), for vector search | Qdrant Advisor | `npx skills add qdrant/skills/meta/qdrant-advisor` |
| [FastAPI](https://fastapi.tiangolo.com/ai-development/), for the optional HTTP example | FastAPI | `uvx library-skills install --skill fastapi --yes` |

Install a skill only when your application uses that integration. Follow the
upstream project's documentation for its scope and update process.

## Next steps

<div class="grid cards" markdown>

-   :material-download:{ .lg .middle } __[Install in your agent](install.md)__

    ---

    Where each agent looks for skills, and how to verify and update an
    installation.

-   :material-check-decagram-outline:{ .lg .middle } __[Evaluation](evaluation.md)__

    ---

    How the skills are checked and evaluated against real tasks.

</div>
