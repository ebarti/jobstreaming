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
- Bounded `wait_closed()` lifecycle diagnostics and adapter transport ownership.
- Transactional `SqliteCheckpointStore` persistence with compare-and-swap revisions
  and incremental seen-key acknowledgements.
- An `AtomicCheckpointStore` capability for all-or-nothing checkpoint replacement.
- DataFrame-independent `collect_jobs()` outcomes with sourced jobs, invocation-local
  per-site counters, chronological failures, explicit partial/failed status, and
  aggregate `SearchFailedError` strict mode; cancellation remains distinct.
- SHA-256 release checksums and GitHub-hosted SLSA build provenance for wheel and
  source distributions.
- Open, validated custom adapter identifiers plus an `Adapter` protocol and offline
  `AdapterTestKit`.
- Typed search-filter and resume-capability declarations, a `py.typed` marker, and a
  static type-check gate for the SDK/core runtime surface.
- A `batch` extra and typed `MissingOptionalDependencyError` for the optional
  DataFrame compatibility API.
- Explicit conservative description-salary inference with multilingual interval and
  currency parsing plus typed provenance and confidence metadata.

### Changed

- Cancellation callbacks are latched after their first `True` result.
- Adapter detail transports are page-scoped, and repeated stream closure cannot
  retroactively acknowledge an event.
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
- Pandas is no longer installed or imported by the default streaming package;
  `jobstreaming[batch]` preserves the existing `scrape_jobs` DataFrame behavior.
- Description salary extraction is now off by default instead of implicitly applying
  a USD numeric-threshold heuristic; opt in with
  `description_salary_policy="conservative"`.
- Structured board compensation always takes precedence over description inference,
  and batch rows expose flattened salary provenance fields.
- PyPI releases use a separated, least-privilege Trusted Publishing job with
  attestations enabled.

### Fixed

- SQLite replacement now rolls back to the previous checkpoint when reseeding fails,
  advances the revision to fence stale owners, rejects future schemas before creating
  current tables, and closes connections when setup fails. Checkpoint generations
  fence owners across an explicit clear and same-request reseed; version-1 SQLite and
  legacy serialized checkpoints without the field use a stable legacy generation.
- ZipRecruiter rounds partial-day age filters up and preserves an explicit zero-mile
  radius.
- Unsupported Indeed and LinkedIn job-type values are omitted rather than raised or
  sent as empty parameters.
- Legacy adapter bridges preserve registration cursor versions and non-resume
  capability declarations, and forward lifecycle cleanup to wrapped adapters.
- Custom adapter identifiers round-trip through SQLite checkpoints and typed outcome
  summaries without being narrowed to built-in sites.
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
