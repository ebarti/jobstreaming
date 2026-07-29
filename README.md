# JobStreaming

**Concurrent, resumable job collection for Python.**

JobStreaming collects and normalizes listings from multiple job boards, yielding each
result as soon as its source returns it. Concurrent adapters, typed events, durable
checkpoints, and stable job identities make it suitable for scripts, data pipelines,
background workers, workflow engines, and job-market analysis.

It is a standalone Python package: no companion service or database is required. Some
board adapters require operator-provided board credentials; the package does not ship
shared credentials. Use the full event stream for durable ingestion, a job-only
iterator for simple consumers, or the familiar DataFrame API for batch analysis.

JobStreaming is an independently maintained, heavily modified fork of an MIT-licensed
upstream project. It retains the original license and attribution while using a
separate project, distribution, and import identity.

> **Alpha:** job boards change private endpoints and markup without notice. Treat every
> adapter as a best-effort integration, respect each site's terms and rate limits, and
> persist the events you need. Adapter availability can drift between releases, and
> the project provides no uptime, compatibility, or support-response SLA.

## What it provides

- Searches run concurrently across sites; a slow or blocked site does not hold back
  healthy sites.
- Jobs, warnings, progress, site failures, and completion are typed events.
- JSON checkpoints make searches restartable with at-least-once delivery; an optional
  SQLite store scales acknowledgement history without rewriting old keys.
- Stable job identities and checkpointed deduplication prevent acknowledged jobs from
  being emitted again after a restart.
- Adapter failures are isolated. The batch API returns healthy partial results unless
  strict failure mode is requested.
- Requests and result models are immutable and validated.
- A typed `collect_jobs(...) -> SearchOutcome` entry point preserves source identity,
  per-site terminal summaries, and chronological failures without building a DataFrame.
- A `scrape_jobs(...) -> pandas.DataFrame` entry point for batch and analysis
  workflows.
- Adapters are registered through an extensible registry rather than a hard-coded
  dispatcher.

Choose the interface that matches your consumer:

| Need | API |
|---|---|
| Durable ingestion with progress, errors, and explicit acknowledgement | `stream_search` |
| A simple iterator of normalized jobs | `stream_jobs` |
| A typed aggregate with source identity and per-site outcomes | `collect_jobs` |
| A Pandas DataFrame for notebooks, exports, or batch analysis | `scrape_jobs` |
| Application-owned checkpoint persistence | `CheckpointStore` |
| Replacing or extending source behavior | `AdapterRegistry` |

```mermaid
flowchart LR
    A["SearchRequest"] --> B["Concurrent coordinator"]
    B --> C1["Indeed worker"]
    B --> C2["LinkedIn worker"]
    B --> C3["Other site workers"]
    C1 --> D["Bounded event queue"]
    C2 --> D
    C3 --> D
    D --> E["Job / progress / error events"]
    E --> F["Consumer"]
    F -->|"acknowledge"| G["Atomic checkpoint"]
    G -->|"resume"| B
```

## Installation

Python 3.10 through Python 3.14 are tested.

Install the published package from PyPI:

```bash
pip install -U jobstreaming
```

Both the PyPI distribution and Python import package are `jobstreaming`. No legacy
import alias or host application is required.

## Board credentials and TLS configuration

JobStreaming does not embed reusable board credentials or fallback tokens. Configure
only the adapters you are authorized to use, preferably through a process-level secret
manager:

| Adapter | Configuration | Behavior when absent |
|---|---|---|
| Indeed | `JOBSTREAMING_INDEED_API_KEY` | Emits `authentication_configuration`. |
| Naukri | `JOBSTREAMING_NAUKRI_NKPARAM` | Emits `authentication_configuration`. |
| ZipRecruiter | `JOBSTREAMING_ZIPRECRUITER_AUTHORIZATION` | Emits `authentication_configuration`. |
| Glassdoor | Discovers a live CSRF token; optional fallback `JOBSTREAMING_GLASSDOOR_CSRF_TOKEN` | Emits `authentication_configuration` if discovery fails and no configured token exists. |

ZipRecruiter also accepts optional
`JOBSTREAMING_ZIPRECRUITER_DEVICE_ID`,
`JOBSTREAMING_ZIPRECRUITER_PUSH_NOTIFICATION_ID`, and
`JOBSTREAMING_ZIPRECRUITER_ZVA_OVERRIDE` values when your authorized integration
requires them. Never commit these values or include them in fixtures, logs, issues, or
checkpoint files.

`ca_cert` is supported by adapters built on `requests`. Glassdoor and ZipRecruiter use
`tls-client`, whose current Python API cannot consume a custom CA bundle; those
adapters raise `AuthenticationConfigurationError` when `ca_cert` is supplied instead
of silently ignoring it. Use system trust, a correctly configured proxy, or a custom
requests-based adapter when a private CA is required.

## Stream results immediately

```python
from jobstreaming import ErrorEvent, JobEvent, SearchCompleteEvent, stream_search

with stream_search(
    site_name=["indeed", "linkedin", "zip_recruiter"],
    search_term="software engineer",
    location="Madrid",
    results_wanted=20,  # per site
    checkpoint_path=".jobstreaming/search.json",
    resume=True,
    ack_mode="explicit",
) as stream:
    for event in stream:
        if isinstance(event, JobEvent):
            print(event.site.value, event.job.title, event.job.job_url)

            # Persist the job first when durability matters, then explicitly ack it.
            save_to_database(event.job)

        elif isinstance(event, ErrorEvent):
            print(f"{event.site.value} failed: {event.message}")

        elif isinstance(event, SearchCompleteEvent):
            print("all sites completed:", event.completed)

        stream.ack(event)
```

Each site runs in its own worker. Arrival order is intentionally unspecified: faster
sites and faster pages yield first.

For a job-only iterator:

```python
from jobstreaming import stream_jobs

for job in stream_jobs(
    site_name=["indeed", "google"],
    search_term="data engineer",
    location="Barcelona",
    results_wanted=10,
):
    print(job.title)
```

Use `stream_search` when you need errors, progress, source-site metadata, or explicit
checkpoint acknowledgements.

## Restart and delivery semantics

Checkpointing is opt-in. Pass either `checkpoint_path` or a custom `CheckpointStore`.
`checkpoint_path` intentionally remains the simple JSON default.

- The default `ack_mode="implicit"` preserves the convenient behavior where requesting
  the next event acknowledges the previous event.
- Use `ack_mode="explicit"` for durable consumers. In this mode, requesting another
  event before `stream.ack(event)` raises `UnacknowledgedEventError` and does not advance
  the checkpoint.
- Call `stream.ack(event)` after a durable write when you need the checkpoint advanced
  immediately.
- Leaving the context manager early does not acknowledge the last delivered event;
  call `stream.ack(event)` first when an intentional early stop should be committed.
- If execution stops before acknowledgement, that job can be replayed on restart. This
  is at-least-once delivery: it favors avoiding data loss over pretending exactly-once
  delivery is possible.
- Acknowledged jobs are deduplicated with stable, process-independent keys.
- Page and cursor state advances only after the corresponding progress event is
  acknowledged.
- Only failures classified as `transient_network` or `rate_limited` are retried by
  default. Configure `max_retries` and `retry_backoff` on `stream_search` or
  `scrape_jobs`. `max_retries` means coordinator retries after the initial adapter
  attempt; adapters do not hide additional transport retries.
- Valid `Retry-After` delta or HTTP-date values on retryable board responses are
  honored, capped at five minutes. The selected delay is the larger of `Retry-After`
  and exponential `retry_backoff`.
- The checkpoint is written through an `fsync` plus atomic file replacement.
- Checkpoints carry an overall schema version, an opaque generation identity, a
  monotonically increasing revision within that generation, and a cursor-state schema
  version for every adapter. An incompatible library or adapter upgrade raises
  `CheckpointCompatibilityError` before any board worker starts.
- A checkpoint is bound to the complete request fingerprint. Changing the query,
  filters, sites, or result count raises `CheckpointMismatchError`; use a new path or
  `resume=False` for a new search.
- Board-owned cursors can expire. If a board rejects an old cursor, the stream emits an
  `ErrorEvent` with `code="cursor_expired"` and `reset_checkpoint=True`; restart that
  site from a fresh checkpoint.
- Custom stores can provide compare-and-swap ownership using both
  `checkpoint.generation` and `checkpoint.revision`. Raise
  `CheckpointConflictError` for a stale save; the conflict is surfaced to the caller
  immediately and the stream stops without advancing its local checkpoint.

For long-running or high-volume searches, use the stdlib-only SQLite store:

```python
from jobstreaming import SqliteCheckpointStore, stream_search

store = SqliteCheckpointStore(".jobstreaming/search.sqlite3")

with stream_search(
    site_name=["indeed", "linkedin"],
    search_term="platform engineer",
    checkpoint_store=store,
    ack_mode="explicit",
) as stream:
    for event in stream:
        persist(event)
        stream.ack(event)
```

One SQLite file owns one search checkpoint aggregate. Its header, adapter state, and
ordered seen-key history advance in one `BEGIN IMMEDIATE` transaction. Revision
compare-and-swap rejects concurrent stale owners, and each job acknowledgement appends
one key instead of serializing or rewriting all historical keys. `load()` reconstructs
the complete public `SearchCheckpoint` only when starting/resuming a stream or when
checkpoint introspection is requested. Call `store.clear()` or use `resume=False` to
replace the file with a new search. A cleared and reseeded checkpoint receives a new
generation, so an owner from before the clear cannot pass compare-and-swap even when
the new search has the same request fingerprint and restarts at revision zero.
Serialized checkpoints and supported SQLite schema-version-1 databases created before
this field existed load under a stable legacy generation; SQLite adds the missing
column only after its future-schema compatibility preflight succeeds.

Built-in stores implement `AtomicCheckpointStore`, so `resume=False` replaces an
existing search checkpoint in one all-or-nothing transition. Custom stores that only
implement `CheckpointStore` remain compatible and retain the existing `clear()` then
`save()` reset sequence; implement `AtomicCheckpointStore.replace()` when a custom
backend must preserve its previous checkpoint if reseeding fails. The method returns
the checkpoint that was actually persisted, allowing a store to advance its revision
as part of the replacement. SQLite does so when a prior checkpoint exists, fencing
stale owners even when the replacement uses the same request fingerprint.

Custom high-volume stores can independently implement `IncrementalCheckpointStore`
and accept the immutable `CheckpointWrite` command. Ordinary `CheckpointStore`
implementations continue receiving complete snapshots through `save()`.

If a process crashes while handling a job, replay is expected. Make downstream writes
idempotent using `event.job_key` or the job's stable `id`.

## Typed and DataFrame batch APIs

Use `collect_jobs` when application code needs a complete typed outcome:

```python
from jobstreaming import SearchOutcomeStatus, collect_jobs

outcome = collect_jobs(
    site_name=["indeed", "linkedin", "google"],
    search_term="software engineer",
    location="Madrid",
    results_wanted=20,
)

for sourced in outcome.jobs:
    print(sourced.site.value, sourced.job.title)

for site in outcome.sites:
    print(site.site.value, site.jobs_emitted, site.failure_count, site.completed)

if outcome.status is SearchOutcomeStatus.PARTIAL:
    handle_partial_result(outcome)
```

`SUCCEEDED` means every requested site reached terminal completion. `PARTIAL` means a
failure occurred after at least one job was emitted or another site completed;
`FAILED` means no site completed and no job was retained. Aggregate failures are
ordered by stream sequence, not request-site order. Set `raise_on_error=True` to raise
`SearchFailedError` only after collection finishes; the exception remains compatible
with `RuntimeError` and carries the full outcome, including partial jobs and every
failure.

Outcome totals and `jobs_emitted` counters describe only the current invocation. On a
resumed search, an already-completed site is reported as completed with zero newly
emitted jobs. Cancellation is not a failed outcome: `collect_jobs` propagates
`StreamCancelledError`, while its managed stream still performs the same transport
shutdown and bounded cleanup described below.

`collect_jobs` does not construct a DataFrame. Pandas remains an install-time
dependency on this branch; the core-only installation without Pandas becomes
available when the separate optional-batch stack is incorporated.

Use `scrape_jobs` when the desired result is a Pandas DataFrame:

```python
from jobstreaming import scrape_jobs

jobs = scrape_jobs(
    site_name=["indeed", "linkedin", "zip_recruiter", "google"],
    search_term="software engineer",
    google_search_term="software engineer jobs near Madrid since yesterday",
    location="Madrid",
    results_wanted=20,
    hours_old=72,
    country_indeed="Spain",
)

jobs.to_csv("jobs.csv", index=False)
```

`scrape_jobs` delegates collection to the typed outcome path, logs site failures, and
converts retained jobs to a DataFrame. By default, healthy partial results are
returned. Its `raise_on_error=True` mode raises the same `SearchFailedError` after all
sites have had a chance to finish.

Checkpoints store identities and cursor state, not full job payloads. A resumed batch
call therefore contains only jobs emitted during that invocation. For a durable full
result set across restarts, use `stream_search` and upsert each `JobEvent` into your own
store before acknowledging it.

## Events

`stream_search` can yield:

| Event | Meaning |
|---|---|
| `JobEvent` | One normalized job is ready. |
| `ProgressEvent` | A restart boundary such as a page or cursor was completed. |
| `WarningEvent` | A listing was skipped or a requested filter is unsupported. |
| `ErrorEvent` | A site failed; other sites continue. |
| `SiteCompleteEvent` | One site exhausted its work or reached its result limit. |
| `SearchCompleteEvent` | Every worker stopped. `completed=False` means at least one site failed. |

`ErrorEvent.code` is a stable `ErrorCode` value. `retryable` tells an operator whether
the same board operation can be retried, while `reset_checkpoint` tells them whether
the board cursor should be discarded first. `retry_after` preserves a valid, bounded
board-requested delay on a retryable terminal failure.

| Error code | Retry | Reset board checkpoint |
|---|---:|---:|
| `transient_network` | yes | no |
| `rate_limited` | yes | no |
| `invalid_request` | no | no |
| `cursor_expired` | no | yes |
| `authentication_configuration` | no | no |
| `cancelled` | no | no |
| `adapter_failure` | no | no |

## Cancellation

Supply a `threading.Event`, a callback, or both. Queue waits, retry backoff, and blocked
adapter/network operations are observed through the same cancellation boundary.
`close()` also wakes a consumer blocked in `next()`. Cancellation is monotonic: after
an event is set or a callback returns `True` once, that stream remains cancelled and
cannot later report a healthy completion.

`close()` intentionally returns promptly: it signals every worker, closes registered
adapter transports, and schedules cleanup without waiting for an uncooperative
third-party call. When an application must prove that no managed resource remains
active, follow it with a bounded wait and inspect the immutable diagnostics:

```python
stream.close()
diagnostics = stream.wait_closed(timeout=2)
if not diagnostics.quiescent:
    print("still stopping:", diagnostics.active_operations)
if diagnostics.cleanup_errors:
    print("transport cleanup failed:", diagnostics.cleanup_errors)
```

`wait_closed()` never cancels work itself and always requires a finite,
non-negative timeout. `stream.diagnostics` provides the same snapshot without
waiting. Worker, blocking-operation, and adapter-cleanup threads are daemon threads;
their names and counts are exposed for shutdown monitoring rather than hidden.
Closing is idempotent: once `close(acknowledge=False)` has stopped a stream, a later
close call cannot retroactively acknowledge its last event.

```python
from threading import Event

from jobstreaming import StreamCancelledError, stream_search

cancel = Event()

try:
    with stream_search(
        site_name=["indeed", "linkedin"],
        search_term="platform engineer",
        cancel_event=cancel,
        # cancel_callback=lambda: shutdown_requested(),  # optional alternative
    ) as stream:
        for event in stream:
            process(event)
except StreamCancelledError:
    pass
```

## Supported sites and important limits

| Site | Restart boundary | Notes |
|---|---|---|
| Indeed | cursor/page | Requires `JOBSTREAMING_INDEED_API_KEY`. `hours_old`, `easy_apply`, and `job_type`/`is_remote` are mutually exclusive in the upstream API. |
| LinkedIn | result offset/page | Full descriptions require `linkedin_fetch_description=True` and add one request per job. Aggressive rate limiting is common. |
| ZipRecruiter | continuation token | Requires `JOBSTREAMING_ZIPRECRUITER_AUTHORIZATION`. US and Canada are the primary supported markets. |
| Glassdoor | page/cursor | Uses live CSRF discovery or explicitly configured fallback. A location is required unless `is_remote=True`. Availability depends on `country_indeed`. |
| Google Jobs | cursor | `google_search_term` can override the generated query. The upstream response format is opaque and fragile. |
| Bayt | page | Currently supports keyword search and international results. |
| Naukri | page | Requires `JOBSTREAMING_NAUKRI_NKPARAM`. India-focused. A non-empty `search_term` is required. |
| BDJobs | page | Bangladesh-focused. Detail pages are fetched concurrently within each result page. |

Adapters declare their supported filter names and, for enum filters such as
`job_type`, supported values. If a selected adapter cannot honor a requested filter or
value, the stream emits a `WarningEvent` and omits the parameter instead of silently
implying that it was applied.

## Validated request API

For reusable searches, construct an immutable request explicitly:

```python
from jobstreaming import Country, SearchRequest, Site, stream_search

request = SearchRequest(
    site_type=(Site.INDEED, Site.LINKEDIN),
    search_term="platform engineer",
    location="Madrid",
    country=Country.SPAIN,
    results_wanted=25,
    request_timeout=20,
    max_pages=10,
)

with stream_search(request, checkpoint_path="search.json") as stream:
    for event in stream:
        ...
```

Negative offsets/result counts, invalid timeouts, malformed compensation ranges, empty
job titles/URLs, and unsupported enum values are rejected at the boundary.

## Custom adapters

```python
from jobstreaming import (
    AdapterCapabilities,
    AdapterRegistry,
    JobResponse,
    Scraper,
    Site,
    stream_search,
)

class InternalJobs(Scraper):
    capabilities = AdapterCapabilities(
        supports_resume=True,
        resume_granularity="cursor",
        cursor_schema_version=1,
    )

    def __init__(self, **kwargs):
        super().__init__(Site.INDEED)  # this example replaces the Indeed adapter
        self.session = self.track_transport(make_internal_session())

    def scrape(self, request, context=None):
        for job in fetch_internal_jobs(request):
            context.emit_job(job, {"cursor": job.id})
        return JobResponse()

registry = AdapterRegistry()
registry.register(Site.INDEED, InternalJobs)

with stream_search(
    site_name="indeed",
    registry=registry,
    search_term="engineer",
) as stream:
    for event in stream:
        ...
```

Increment `cursor_schema_version` whenever a deployed adapter can no longer interpret
cursor state written by its previous implementation. Legacy adapters that only return
`JobResponse` are still accepted, but their results cannot be streamed until that
adapter returns.

Register every closeable client/session with `track_transport()`, including sessions
created lazily or inside detail-worker threads. The base `Scraper.close()` closes each
registered transport once; adapters with additional resources can override `close()`
and call `super().close()`. Use `transport_scope()` around a bounded page/detail batch
so its thread-local sessions are released before the next page. Scopes are reentrant
and serialize overlapping batches on the same adapter, preventing one batch from
closing transports owned by another. A transport registered after adapter shutdown is
closed and rejected with `RuntimeError`; it is never returned to adapter code as a
usable client. Transport close failures remain visible in
`stream.diagnostics.cleanup_errors`, including failures from late registration races.

The registry can replace any built-in adapter. Adding an entirely new site also
requires adding that board to the `Site` enum so it participates in validation,
fingerprinting, events, and checkpoints.

## Development

```bash
poetry install
poetry run pytest
poetry run ruff check jobstreaming tests
poetry run black --check jobstreaming tests
poetry build
```

The deterministic checkpoint benchmark uses fixed 10,000 and 100,000
acknowledgement workloads, verifies final revisions and key counts, and reports
elapsed time and database size without a timing assertion:

```bash
poetry run python -m tools.benchmark_checkpoints
```

The test suite is offline: it validates domain invariants, concurrency, failure
isolation, acknowledgement/replay behavior, checkpoint persistence, compatibility, and
representative adapter parsing without calling live job boards.

## Support and security

Use [GitHub issues](https://github.com/ebarti/jobstreaming/issues) for reproducible
bugs, adapter drift reports, and non-sensitive support questions. Include the
JobStreaming version, selected adapter, sanitized error event, and an offline
reproduction when possible.

Do not place credentials or vulnerability details in an issue. See
[SECURITY.md](https://github.com/ebarti/jobstreaming/blob/main/SECURITY.md) for private
reporting and supported-version policy, and
[CONTRIBUTING.md](https://github.com/ebarti/jobstreaming/blob/main/CONTRIBUTING.md)
before submitting a change. Release history is in
[CHANGELOG.md](https://github.com/ebarti/jobstreaming/blob/main/CHANGELOG.md).

## License and attribution

MIT. The retained license identifies Cullen Watson as the original copyright holder.
This rebuild is independently maintained and is not affiliated with the original
creator or any supported job board.
