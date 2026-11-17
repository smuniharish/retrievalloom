# Contributing

Set up a development environment and make your first change. Contributions are
welcome. The full guide, including how to propose changes and
what a pull request needs, is
[CONTRIBUTING.md](https://github.com/smuniharish/retrievalloom/blob/main/CONTRIBUTING.md)
in the repository.

## Set up

```bash
git clone https://github.com/smuniharish/retrievalloom.git
cd retrievalloom
uv sync --all-extras --group dev --group docs
```

## Check a change

```bash
uv run ruff check .
uv run ruff format --check .
uv run pyrefly check
uv run pytest -m "not live" --cov=retrievalloom --cov-branch --cov-report=term-missing
uv run zensical build --strict
uv run python scripts/check_site_links.py
```

All of these run in continuous integration on every pull request. See
[Testing](testing.md) for the test layout and the live suites.

## Guidelines

- Keep the public API async. Never block the event loop: run blocking work
  through the [offloader](../guide/concepts/offloading.md).
- Keep the core provider-agnostic. Optional backends go behind an extra and
  are imported lazily.
- A new built-in backend runs the matching contract kit from
  `retrievalloom.testing`.
- Never weaken tenant isolation or provenance validation.
- Update the documentation and the changelog with every user-visible change.
