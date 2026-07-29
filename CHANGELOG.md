# Changelog

All notable changes to JobStreaming are documented here. The project follows semantic
versioning where practical during its `0.0.x` alpha series; adapter compatibility can
still change when upstream boards drift.

## [0.0.3] - Unreleased

### Added

- Value-level adapter capability warnings for unsupported job types.
- Public security, contribution, support, and adapter-configuration guidance.
- `Retry-After` metadata on retryable terminal error events.
- Isolated wheel and source-distribution consumer checks in the release workflow.
- Bounded `wait_closed()` lifecycle diagnostics and adapter transport ownership.
- Transactional `SqliteCheckpointStore` persistence with compare-and-swap revisions
  and incremental seen-key acknowledgements.
- An `AtomicCheckpointStore` capability for all-or-nothing checkpoint replacement.

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

## [0.0.2] - 2026-07-17

- Hardened acknowledgement, checkpoint, cancellation, adapter error, and compatibility
  contracts.
- Added Python 3.10 through 3.14 CI coverage and pinned release automation.

[0.0.3]: https://github.com/ebarti/jobstreaming/compare/v0.0.2...HEAD
[0.0.2]: https://github.com/ebarti/jobstreaming/releases/tag/v0.0.2
