# Manual release

RetrievalLoom releases are prepared and published by a maintainer. The
repository does not publish packages from CI. Review each artifact and
explicitly run the publish command; never put PyPI credentials in the
repository or in a release script.

## Prepare the release

1. Choose the next version according to Semantic Versioning. Update
   `pyproject.toml` and add a dated entry to `CHANGELOG.md`.
2. Review the supported Python versions, dependency ranges, examples and
   documentation for accuracy.
3. Run the documented checks and confirm the intended branch has a successful
   CI run. See [Testing](testing.md).
4. Review the final source diff. Do not include local models, databases,
   caches or build outputs.

## Build and inspect distributions

From the repository root, build both the wheel and source distribution:

```bash
uv build
```

Check the generated files and metadata before publishing:

```bash
uvx twine check --strict dist/*
```

Inspect the wheel and source archive to confirm they contain the expected
package files, README, license and metadata. Do not publish if the version,
contents or metadata differ from the reviewed source.

## Publish manually

Set `UV_PUBLISH_TOKEN` through your approved secret manager or shell
environment, then run this command only after reviewing the distributions:

```bash
uv publish
```

`uv publish` uploads the contents of `dist/` to PyPI. Confirm the published
version and files on the PyPI project page before announcing the release.

## Create the GitHub release

After confirming the PyPI upload, create and push the matching annotated tag
and create a GitHub release from that tag:

```bash
git tag -a vX.Y.Z -m "RetrievalLoom X.Y.Z"
git push origin vX.Y.Z
```

Use the corresponding changelog entry as the release notes. Tag creation,
GitHub release creation and PyPI publishing remain explicit maintainer actions;
none is triggered automatically by this repository.
