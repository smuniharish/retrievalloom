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
node scripts/render-diagrams.mjs --check
```

All of these run in continuous integration on every pull request. See
[Testing](testing.md) for the test layout and the live suites.

## Edit a diagram

The diagrams in this documentation are Mermaid sources under `diagrams/`,
rendered to PNG under `docs/assets/diagrams/` with the shared theme in
`diagrams/mermaid-config.json`. After you edit a source, render it with
Mermaid CLI 12.2.0 (Node.js 22.13 or later):

```bash
npm install --global --allow-scripts=puppeteer @mermaid-js/mermaid-cli@12.2.0
node scripts/render-diagrams.mjs
```

To render with a browser you already have instead of the one Puppeteer
downloads, set `PUPPETEER_EXECUTABLE_PATH` to its executable. Commit the
source, the image and the updated `docs/assets/diagrams/manifest.json`
together; `node scripts/render-diagrams.mjs --check` fails when an image is
missing, stale or has no source.

## Guidelines

- Keep the public API async. Never block the event loop: run blocking work
  through the [offloader](../guide/concepts/offloading.md).
- Keep the core provider-agnostic. Optional backends go behind an extra and
  are imported lazily.
- A new built-in backend runs the matching contract kit from
  `retrievalloom.testing`.
- Never weaken tenant isolation or provenance validation.
- Update the documentation and the changelog with every user-visible change.
