# Contributing

Thank you for improving `retrievalloom`. Contributions should keep its focus:
embeddable retrieval and evidence orchestration infrastructure, not a hosted
service, an agent framework, a RAG application framework, an embedding
provider, or a document parser or chunker.

Keep the core provider-agnostic. Every extension point is an abstract base
class (`abc.ABC`); adapters subclass it. The public operational API is
async-only: do not add synchronous wrappers, and never run blocking work on
the event loop. Blocking calls go through the engine's offloader.

## Before opening a change

1. Read the [architecture overview](docs/architecture/index.md).
2. Open an issue first for changes to the public API, the canonical domain
   model, the metadata schema, lifecycle semantics, or an extension contract.
3. Keep each change focused, with tests and documentation for anything a user
   can observe.

## Development setup

Python 3.12 or later and [uv](https://docs.astral.sh/uv/) are required.

```bash
uv sync --all-extras --all-groups
uv run ruff check .
uv run ruff format --check .
uv run pyrefly check
uv run pytest -m "not live" --cov=retrievalloom --cov-branch
```

The default test run needs no services, network, or model weights, and branch
coverage is kept at 100%. The live suites for a Qdrant server and local models
run when their environment variables are set; see
[Testing](docs/development/testing.md).

Never commit credentials, local environments, model weights, or generated
reports.

## Schema changes

The metadata schema is versioned with Alembic. A schema change needs a new
migration under `src/retrievalloom/storage/migrations/versions/`, an update
to `src/retrievalloom/storage/schema.py`, and tests that run the migration.
Never edit a released migration.

## Documentation

```bash
uv run zensical build --strict
uv run python scripts/check_site_links.py
```

## Pull requests

- Describe the user-visible impact, including any effect on stored data,
  lifecycle behavior, tenant isolation, or provenance.
- Add or update tests for every behavior change.
- Update the affected pages under [`docs/`](docs) and the
  [changelog](CHANGELOG.md).
- Make sure formatting, lint, type checks, tests, and the strict docs build
  pass.

## Releases

Releases follow [semantic versioning](https://semver.org/). To prepare one:

1. Set the version in `pyproject.toml`, then run `uv lock`.
2. Move the changelog entries under a heading with the version and date.
3. Run `uv build` and check the wheel and sdist.
4. Merge to `main` and tag the release with a `v` prefix, such as `v0.1.0`.

## Security reports

Do not report vulnerabilities in public issues. Follow [SECURITY.md](SECURITY.md).
