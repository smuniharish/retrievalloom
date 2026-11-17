# Evaluation

A skill helps only when an agent picks it up for the right task and then gets
that task right. RetrievalLoom checks both, following the Agent Skills
guidance on
[evaluating skills](https://agentskills.io/skill-creation/evaluating-skills).

## Checked on every change

The
[validation guide](https://github.com/smuniharish/retrievalloom/blob/main/retrievalloom-skills/validation/README.md)
lists the commands. They run offline and download no models. For both
skills, they check:

- **Format.** The `skills-ref` reference validator accepts the frontmatter.
  `name` matches the directory, the description starts with "Use this skill
  when", and `metadata.version` equals the RetrievalLoom release.
- **Progressive disclosure.** `SKILL.md` stays under 500 lines and references
  are one level deep. Every reference is linked from `SKILL.md`, and every
  relative link resolves.
- **Bundled files.** `SKILL.md` names every script and asset, so the agent
  can find it.
- **The setup check.** `verify_setup.py` passes with warnings treated as
  errors, prints valid JSON with `--json`, and fails for a server it can't
  reach.
- **Templates.** The behavior test template and every extension template
  pass their tests, including the contract kits.
- **Code quality.** Ruff and Pyrefly accept every bundled Python file.
- **Discovery.** The skills CLI finds both skills through `skills.sh.json`.

## Evaluated against real tasks

Each skill has an `evals/` folder:

| File | Contents |
| --- | --- |
| `evals.json` | Realistic tasks, each with input files, the expected result, and assertions a grader can verify. |
| `trigger_queries.json` | 20 user requests: 10 that should load the skill and 10 near misses that shouldn't, split into training and validation sets. |
| `files/` | The projects the tasks start from, such as a support bot without RetrievalLoom or a sparse index that fails its contract kit. |

The tasks cover what agents get wrong without the skill:

| Skill | Tasks |
| --- | --- |
| `retrievalloom` | Replace a support bot's search with per-tenant retrieval and citations; fix `partial` results after every restart; erase documents for a GDPR request on PostgreSQL; enable dense retrieval for existing documents |
| `retrievalloom-extend` | Adapt an in-house BM25 service as the sparse index; fix a sparse index that fails 8 contract tests; wrap an embedding HTTP API; write an access policy from token claims |

The near-miss trigger queries keep the two skills apart: several near misses
for `retrievalloom-extend` are tasks for `retrievalloom`, and the reverse. No
query is meant to load both.

To evaluate a change to a skill, run each task with and without the skill in
a fresh agent session. Grade the outputs against the assertions, and compare
pass rates, time, and tool calls. The validation guide describes the
workflow and records the results.

## Reviewed against the source

Automated checks can't tell whether guidance is correct. Each snippet and
claim in a skill is reviewed against the code, tests, and documentation it
describes, and anything without an implementation is left out. The
validation guide lists the source of truth for each area.
