# Install in your agent

Install the skills once, and every agent that supports the
[Agent Skills format](https://agentskills.io/specification) can use them.
Each skill is one directory with `SKILL.md` at its root: `retrievalloom` and
`retrievalloom-extend`. All agents read the same directories, so there is no
separate copy per agent.

## With the skills CLI

The [skills CLI](https://github.com/vercel-labs/skills) finds both skills in
the repository, detects the agents in your project, and installs the skills
into each agent you choose. It needs Node.js.

```bash
npx skills add smuniharish/retrievalloom                                # choose interactively
npx skills add smuniharish/retrievalloom --skill retrievalloom          # only the core skill
npx skills add smuniharish/retrievalloom --skill retrievalloom-extend   # only the extension skill
```

| Option | Effect |
| --- | --- |
| `-g` | Install for every project instead of the current one. |
| `-a claude-code` | Install for one agent; see the CLI's [agent list](https://github.com/vercel-labs/skills/blob/main/docs/agents.md). |

## Manually

Copy each complete skill directory, including `SKILL.md`, `references/`,
`scripts/`, and `assets/`, into one of the locations for your agent. The
[`retrievalloom`](https://github.com/smuniharish/retrievalloom/tree/main/retrievalloom-skills/skills/retrievalloom)
and [`retrievalloom-extend`](https://github.com/smuniharish/retrievalloom/tree/main/retrievalloom-skills/skills/retrievalloom-extend)
directories are in the RetrievalLoom repository. Their `evals/` folders only
test the skills, so you can leave them out.

=== "Claude Code"

    | Scope | Destination |
    | --- | --- |
    | Current repository | `.claude/skills/<skill>/` |
    | All local projects | `~/.claude/skills/<skill>/` |

    Restart Claude Code. It selects a skill when the task matches, or invoke
    it with `/retrievalloom` or `/retrievalloom-extend`. See
    [Claude Code Skills](https://code.claude.com/docs/en/skills).

=== "Codex"

    | Scope | Destination |
    | --- | --- |
    | Current repository | `.agents/skills/<skill>/` |
    | All local projects | `~/.agents/skills/<skill>/` |

    Codex detects new skills automatically; restart it if a skill doesn't
    appear. Invoke one with `$retrievalloom` or `$retrievalloom-extend`, or
    list skills with `/skills`.

=== "Cursor"

    | Scope | Destination |
    | --- | --- |
    | Current repository | `.agents/skills/<skill>/` or `.cursor/skills/<skill>/` |
    | All local projects | `~/.agents/skills/<skill>/` or `~/.cursor/skills/<skill>/` |

    Restart Cursor. In Agent chat, type `/` and select the skill, or let
    Cursor activate it from its description. See
    [Cursor Agent Skills](https://cursor.com/docs/skills).

=== "GitHub Copilot"

    | Scope | Destination |
    | --- | --- |
    | Current repository | `.agents/skills/<skill>/`, `.github/skills/<skill>/`, or `.claude/skills/<skill>/` |
    | All local projects | `~/.agents/skills/<skill>/` or `~/.copilot/skills/<skill>/` |

    In Copilot CLI, start a new session or run `/skills reload`, then check
    with `/skills info retrievalloom`. Invoke a skill with `/retrievalloom`
    or `/retrievalloom-extend`. See
    [Adding agent skills for GitHub Copilot CLI](https://docs.github.com/en/copilot/how-tos/copilot-cli/customize-copilot/add-skills).

=== "Other agents"

    Copy the skill folders into the skills directory your agent documents.
    The skills use only standard `SKILL.md` frontmatter, so they need no
    agent-specific adapter.

    If an agent needs a manifest or registry entry, add only a thin adapter
    that points to these directories. Don't copy the instructions.

## Verify

1. Each installed directory is named after its skill (`retrievalloom` or
   `retrievalloom-extend`) and contains `SKILL.md`.
2. Your agent lists the skills, if it has a skill list.
3. From your project's Python environment, the setup check passes:

    ```bash
    python <skills directory>/retrievalloom/scripts/verify_setup.py
    ```

    ```text
    PASS  Python 3.14.7 (>=3.12,<3.15 required)
    PASS  retrievalloom 0.1.0 (0.1.x expected)
    PASS  pydantic 2.14.3 (>=2.14.3,<3 required)
    ...
    PASS  extra 'postgres': asyncpg 0.32.0 (>=0.30 required)
    INFO  PostgreSQL probe skipped (set RETRIEVALLOOM_DSN to run it)
    INFO  Qdrant probe skipped (set RETRIEVALLOOM_QDRANT_URL to run it)
    PASS  ingest: [('created', 3)]
    PASS  retrieve: status=complete, evidence=['loop', 'schleife'], methods=['index.fuzzy', 'index.keyword', 'index.sparse'], warnings=['method_unavailable']
    PASS  scope isolation: another tenant's document is not visible
    PASS  metadata filter lang=de: ['schleife']
    PASS  delete hides the document: ['schleife']
    PASS  purge erases it: purged=['loop'], active=2
    PASS  engine health: healthy
    All checks passed.
    ```

    Missing extras are reported and don't fail the check. Add
    `--require postgres` (or `graph`, `structure`, `testing`) to fail
    instead, or `--json` to print one JSON object. The `method_unavailable`
    warning means no embedding model is configured, so the dense method is
    off.

4. If you installed `retrievalloom-extend`, its templates pass their
   contract kits:

    ```bash
    cd <skills directory>/retrievalloom-extend
    python -m pytest assets
    ```

    The last line reports `33 passed`. The kits need the testing extra:
    `pip install "retrievalloom[testing]"`.

## Update

Run `npx skills update`, or replace the installed folders with the latest
directories from the repository, then restart or reload your agent. Each
skill's `metadata.version` matches the RetrievalLoom release it describes.
