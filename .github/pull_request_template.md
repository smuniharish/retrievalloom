<!--
Thanks for contributing to RetrievalLoom! Please fill out this template:
it helps reviewers understand and validate your change quickly.
-->

## Summary

<!-- What does this PR change, and why? -->

## Related issue(s)

<!-- Link any related issues, e.g. "Closes #123". -->

## Type of change

- [ ] Bug fix
- [ ] New feature (extension ABC, adapter, configuration, domain model field)
- [ ] Documentation
- [ ] Refactor / internal cleanup (no behavior change)
- [ ] Other (describe above)

## Checklist

- [ ] I read [CONTRIBUTING.md](../CONTRIBUTING.md).
- [ ] New/changed public behavior is covered by tests.
- [ ] `uv run ruff check .` and `uv run ruff format --check .` pass.
- [ ] `uv run pyrefly check` passes.
- [ ] `uv run pytest -m "not live" --cov=retrievalloom --cov-branch` passes with 100% coverage.
- [ ] `uv run zensical build --strict` and `uv run python scripts/check_site_links.py` pass, and diagrams were re-rendered if
      a `diagrams/*.mmd` source changed (`node scripts/render-diagrams.mjs --check` passes).
- [ ] Schema changes come with a new Alembic migration and its tests.
- [ ] Relevant docs under `docs/` were updated (if this changes public API,
      configuration, or behavior).
- [ ] `CHANGELOG.md` has an entry under `[Unreleased]` (if this is a
      user-visible change).

## Data, lifecycle, and isolation impact

<!--
Does this change what is stored, how generations are built, published or
retired, what a scope can see, or the provenance attached to evidence?
Describe the effect, or write "None".
-->
