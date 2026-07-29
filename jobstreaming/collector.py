from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from jobstreaming.events import (
    ErrorEvent,
    JobEvent,
    SearchCompleteEvent,
    SearchEvent,
    SiteCompleteEvent,
)
from jobstreaming.model import SearchRequest, Site
from jobstreaming.outcome import (
    SearchFailedError,
    SearchFailure,
    SearchOutcome,
    SiteSearchSummary,
    SourcedJob,
)


@dataclass(slots=True)
class _SiteAccumulator:
    jobs_emitted: int = 0
    failures: list[SearchFailure] = field(default_factory=list)
    completed: bool = False


class _OutcomeAccumulator:
    def __init__(
        self,
        sites: tuple[Site, ...],
        *,
        initially_completed: frozenset[Site],
    ) -> None:
        self._sites = sites
        self._states = {
            site: _SiteAccumulator(completed=site in initially_completed)
            for site in sites
        }
        self._jobs: list[SourcedJob] = []
        self._terminal: SearchCompleteEvent | None = None

    def accept(self, event: SearchEvent) -> None:
        if isinstance(event, SearchCompleteEvent):
            if self._terminal is not None:
                raise RuntimeError("search stream emitted more than one terminal event")
            self._terminal = event
            return

        site = getattr(event, "site", None)
        if site not in self._states:
            raise RuntimeError(f"search stream emitted an unknown site: {site!r}")
        state = self._states[site]
        if isinstance(event, JobEvent):
            state.jobs_emitted += 1
            self._jobs.append(SourcedJob(site=event.site, job=event.job))
        elif isinstance(event, ErrorEvent):
            state.completed = False
            state.failures.append(SearchFailure.from_event(event))
        elif isinstance(event, SiteCompleteEvent):
            state.completed = True

    def finish(self) -> SearchOutcome:
        terminal = self._terminal
        if terminal is None:
            raise RuntimeError("search stream ended without a terminal event")

        summaries = tuple(
            SiteSearchSummary(
                site=site,
                jobs_emitted=self._states[site].jobs_emitted,
                failures=tuple(self._states[site].failures),
                completed=self._states[site].completed,
            )
            for site in self._sites
        )
        observed_jobs = len(self._jobs)
        observed_failures = sum(summary.failure_count for summary in summaries)
        if terminal.total_jobs != observed_jobs:
            raise RuntimeError(
                "search terminal job count does not match delivered jobs: "
                f"{terminal.total_jobs} != {observed_jobs}"
            )
        if terminal.total_errors != observed_failures:
            raise RuntimeError(
                "search terminal error count does not match delivered failures: "
                f"{terminal.total_errors} != {observed_failures}"
            )
        if terminal.completed is not all(summary.completed for summary in summaries):
            raise RuntimeError(
                "search terminal completion does not match site completion"
            )
        return SearchOutcome(
            jobs=tuple(self._jobs),
            sites=summaries,
            total_jobs=terminal.total_jobs,
            total_failures=terminal.total_errors,
            completed=terminal.completed,
        )


def collect_jobs(
    request: SearchRequest | None = None,
    *,
    raise_on_error: bool = False,
    **stream_options: Any,
) -> SearchOutcome:
    """Consume a search into a typed, pandas-independent outcome aggregate."""

    from jobstreaming.api import stream_search

    with stream_search(request, **stream_options) as stream:
        checkpoint = stream.checkpoint
        accumulator = _OutcomeAccumulator(
            stream.request.sites,
            initially_completed=frozenset(
                site
                for site in stream.request.sites
                if checkpoint.adapters[site.value].completed
            ),
        )
        for event in stream:
            accumulator.accept(event)
            stream.ack(event)
        outcome = accumulator.finish()

    if raise_on_error and outcome.failures:
        raise SearchFailedError(outcome)
    return outcome
