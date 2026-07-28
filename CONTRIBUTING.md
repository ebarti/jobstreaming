# Contributing to JobStreaming

Thanks for helping improve JobStreaming. This is an alpha integration library, so
small, well-tested changes that preserve public contracts are especially valuable.

## Development setup

Install Python 3.10 or newer and Poetry 2.4.1, then run:

```bash
poetry install
poetry check --lock
poetry run pytest
poetry run ruff check jobstreaming tests
poetry run black --check jobstreaming tests
poetry build
```

The continuous-integration matrix runs on Python 3.10 through 3.14.

## Change guidelines

- Add a focused regression test for every behavior change.
- Keep tests offline and deterministic. Use representative fixtures or fake sessions;
  do not call live job boards from the test suite.
- Never commit API keys, authorization headers, session tokens, cookies, personal
  data, or captures that may contain them.
- Treat board responses as untrusted and tolerate malformed individual listings where
  the adapter can safely continue.
- Preserve typed errors, cancellation, acknowledgement, and checkpoint semantics.
- Declare both filter names and supported filter values accurately. If an adapter
  cannot honor a value, warn and omit it instead of silently sending a misleading
  parameter.
- Increment an adapter's `cursor_schema_version` only when its prior saved state can no
  longer be interpreted safely.
- Keep third-party GitHub Actions pinned to immutable commit SHAs.

## Pull requests

Explain the user-visible outcome, the board or runtime contract affected, and the
verification you ran. Keep unrelated refactors out of the change. Update
`CHANGELOG.md` and public documentation when behavior, configuration, or compatibility
changes.

Use [GitHub issues](https://github.com/ebarti/jobstreaming/issues) for non-sensitive
bugs and support questions. Follow [SECURITY.md](SECURITY.md) for vulnerabilities.

By contributing, you agree that your contribution is provided under the repository's
MIT license.
