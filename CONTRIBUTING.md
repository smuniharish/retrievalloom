# Contributing

Thank you for improving RetrievalLoom. This guide takes you from a fresh clone
to a pull request that is ready for review.

RetrievalLoom is a retrieval library that runs inside other applications.
Changes should keep that focus: it is not a hosted service, an agent or RAG
framework, an embedding provider, or a document parser or chunker. Three design
rules apply to every change:

- **Stay provider-agnostic.** Every extension point is an abstract base class
  (`abc.ABC`), and adapters for specific providers subclass it.
- **Stay async.** The public API is async only. Do not add synchronous
  wrappers.
- **Keep the event loop free.** Blocking calls go through the engine's
  offloader.

## Before you start

1. Read the [architecture overview](docs/architecture/index.md).
2. Open an issue first for changes to the public API, the canonical domain
   model, the metadata schema, lifecycle semantics, or an extension contract.
3. Keep each change focused, with tests and documentation for anything a user
   can observe.

## Set up and run the checks

You need Python 3.12, 3.13 or 3.14 and [uv](https://docs.astral.sh/uv/).

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

## Change the metadata schema

The metadata schema is versioned with Alembic. A schema change needs a new
migration under `src/retrievalloom/storage/migrations/versions/`, an update
to `src/retrievalloom/storage/schema.py`, and tests that run the migration.
Never edit a released migration.

## Build the documentation

Build the site with strict checks and verify its links:

```bash
uv run zensical build --strict
uv run python scripts/check_site_links.py
```

## Open a pull request

A pull request is ready for review when it meets these points:

- Describe the user-visible impact, including any effect on stored data,
  lifecycle behavior, tenant isolation, or provenance.
- Add or update tests for every behavior change.
- Update the affected pages under [`docs/`](docs) and the
  [changelog](CHANGELOG.md).
- Make sure formatting, lint, type checks, tests, and the strict docs build
  pass.

## Prepare a release

Releases follow [semantic versioning](https://semver.org/). To prepare one:

1. Set the version in `pyproject.toml`, then run `uv lock`.
2. Move the changelog entries under a heading with the version and date.
3. Run `uv build` and check the wheel and sdist.
4. Merge to `main` and tag the release with a `v` prefix, such as `v0.1.0`.

## Report a vulnerability

Do not report vulnerabilities in public issues. Follow [SECURITY.md](SECURITY.md).
