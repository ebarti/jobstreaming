# Changelog

All notable changes to JobStreaming are documented here. The project follows semantic
versioning where practical during its `0.0.x` alpha series; adapter compatibility can
still change when upstream boards drift.

## [0.0.3] - Unreleased

### Added

- Value-level adapter capability warnings for unsupported job types.
- Public security, contribution, support, and adapter-configuration guidance.
- `Retry-After` metadata on retryable terminal error events.
- Isolated wheel and source-distribution consumer checks in CI and the release
  workflow.
- SHA-256 release checksums and GitHub-hosted SLSA build provenance for wheel and
  source distributions.
- Open, validated custom adapter identifiers plus an `Adapter` protocol and offline
  `AdapterTestKit`.
- Typed search-filter and resume-capability declarations, a `py.typed` marker, and a
  static type-check gate for the SDK/core runtime surface.

### Changed

- Cancellation callbacks are latched after their first `True` result.
- Public retry counts are now owned solely by the stream coordinator, with bounded
  `Retry-After` handling.
- Naukri and Glassdoor `max_pages` limits count fetched pages independently of the
  starting offset.
- Google continuation failures retain cursor context and can request a checkpoint
  reset when the board reports an expired cursor.
- TLS-client adapters now reject custom CA-file configuration instead of silently
  ignoring it.
- Indeed, Naukri, ZipRecruiter, and Glassdoor no longer use package-embedded shared
  credentials or fallback tokens.
- Runtime adapter calls no longer inspect `scrape` signatures; legacy signatures are
  isolated behind a deprecated registration bridge.
- PyPI releases use a separated, least-privilege Trusted Publishing job with
  attestations enabled.

### Fixed

- ZipRecruiter rounds partial-day age filters up and preserves an explicit zero-mile
  radius.
- Unsupported Indeed and LinkedIn job-type values are omitted rather than raised or
  sent as empty parameters.
- Legacy adapter bridges preserve registration cursor versions and non-resume
  capability declarations.
- Resume granularities retain legacy/custom values through a normalized, open value
  object.
- Adapter factories and instances fail with typed contract errors when identifiers,
  capabilities, or scrape callables are malformed.
- Public protocol annotations remain resolvable from installed distributions.

## [0.0.2] - 2026-07-17

- Hardened acknowledgement, checkpoint, cancellation, adapter error, and compatibility
  contracts.
- Added Python 3.10 through 3.14 CI coverage and pinned release automation.

[0.0.3]: https://github.com/ebarti/jobstreaming/compare/v0.0.2...HEAD
[0.0.2]: https://github.com/ebarti/jobstreaming/releases/tag/v0.0.2
