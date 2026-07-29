from __future__ import annotations

import math
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any

from jobstreaming.events import ErrorEvent, EventType, freeze_state
from jobstreaming.exception import ErrorCode
from jobstreaming.model import AdapterIdentifier, JobPost


class SearchOutcomeStatus(str, Enum):
    """Terminal classification for a collected multi-site search."""

    SUCCEEDED = "succeeded"
    PARTIAL = "partial"
    FAILED = "failed"


@dataclass(frozen=True, slots=True)
class SourcedJob:
    """A normalized job together with the adapter site that emitted it."""

    site: AdapterIdentifier
    job: JobPost


@dataclass(frozen=True, slots=True)
class SearchFailure:
    """Stable batch representation of one terminal ``ErrorEvent``."""

    sequence: int
    emitted_at: datetime
    site: AdapterIdentifier
    message: str
    error_type: str
    recoverable: bool
    resume_state: Mapping[str, Any]
    code: ErrorCode
    retryable: bool
    reset_checkpoint: bool
    retry_after: float | None = None
    type: EventType = field(default=EventType.ERROR, init=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "resume_state", freeze_state(self.resume_state))
        if self.sequence < 1:
            raise ValueError("failure sequence must be positive")
        if not self.message:
            raise ValueError("failure message cannot be empty")
        if not self.error_type:
            raise ValueError("failure error_type cannot be empty")
        if self.retry_after is not None and (
            not math.isfinite(self.retry_after) or self.retry_after < 0
        ):
            raise ValueError("failure retry_after must be finite and non-negative")

    @classmethod
    def from_event(cls, event: ErrorEvent) -> SearchFailure:
        return cls(
            sequence=event.sequence,
            emitted_at=event.emitted_at,
            site=event.site,
            message=event.message,
            error_type=event.error_type,
            recoverable=event.recoverable,
            resume_state=event.resume_state,
            code=event.code,
            retryable=event.retryable,
            retry_after=event.retry_after,
            reset_checkpoint=event.reset_checkpoint,
        )


@dataclass(frozen=True, slots=True)
class SiteSearchSummary:
    """Terminal counters and failures assigned to one requested site."""

    site: AdapterIdentifier
    jobs_emitted: int
    failures: tuple[SearchFailure, ...]
    completed: bool

    def __post_init__(self) -> None:
        if self.jobs_emitted < 0:
            raise ValueError("jobs_emitted cannot be negative")
        if any(failure.site != self.site for failure in self.failures):
            raise ValueError("site summary cannot contain another site's failure")
        if self.completed and self.failures:
            raise ValueError("a completed site cannot also have terminal failures")
        if not self.completed and not self.failures:
            raise ValueError("an incomplete site requires a terminal failure")

    @property
    def failure_count(self) -> int:
        return len(self.failures)


@dataclass(frozen=True, slots=True)
class SearchOutcome:
    """Complete typed result of consuming one search stream invocation."""

    jobs: tuple[SourcedJob, ...]
    sites: tuple[SiteSearchSummary, ...]
    total_jobs: int
    total_failures: int
    completed: bool

    def __post_init__(self) -> None:
        site_names = tuple(summary.site for summary in self.sites)
        if len(site_names) != len(set(site_names)):
            raise ValueError("search outcome sites must be unique")
        known_sites = set(site_names)
        if any(sourced.site not in known_sites for sourced in self.jobs):
            raise ValueError("every sourced job requires a site summary")

        jobs_by_site = {
            site: sum(sourced.site == site for sourced in self.jobs)
            for site in known_sites
        }
        if any(
            summary.jobs_emitted != jobs_by_site[summary.site] for summary in self.sites
        ):
            raise ValueError("site job counters must match sourced jobs")

        failure_count = sum(summary.failure_count for summary in self.sites)
        if self.total_jobs != len(self.jobs):
            raise ValueError("total_jobs must match sourced jobs")
        if self.total_jobs != sum(summary.jobs_emitted for summary in self.sites):
            raise ValueError("total_jobs must match site counters")
        if self.total_failures != failure_count:
            raise ValueError("total_failures must match site failures")
        if self.completed is not all(summary.completed for summary in self.sites):
            raise ValueError("aggregate completion must match site completion")

    @property
    def failures(self) -> tuple[SearchFailure, ...]:
        return tuple(
            sorted(
                (failure for summary in self.sites for failure in summary.failures),
                key=lambda failure: failure.sequence,
            )
        )

    @property
    def status(self) -> SearchOutcomeStatus:
        if self.completed:
            return SearchOutcomeStatus.SUCCEEDED
        if self.total_jobs or any(summary.completed for summary in self.sites):
            return SearchOutcomeStatus.PARTIAL
        return SearchOutcomeStatus.FAILED

    @property
    def succeeded_sites(self) -> tuple[AdapterIdentifier, ...]:
        return tuple(summary.site for summary in self.sites if summary.completed)

    @property
    def failed_sites(self) -> tuple[AdapterIdentifier, ...]:
        return tuple(summary.site for summary in self.sites if summary.failures)

    def summary_for(self, site: AdapterIdentifier) -> SiteSearchSummary:
        for summary in self.sites:
            if summary.site == site:
                return summary
        raise KeyError(site.value)


class SearchFailedError(RuntimeError):
    """Strict-mode aggregate error retaining every result and site failure."""

    def __init__(self, outcome: SearchOutcome) -> None:
        if not outcome.failures:
            raise ValueError("SearchFailedError requires at least one failure")
        self.outcome = outcome
        details = "; ".join(
            f"{failure.site.value} failed [{failure.code.value}]: "
            f"{failure.error_type}: {failure.message}"
            for failure in outcome.failures
        )
        super().__init__(
            f"{details}; search retained {outcome.total_jobs} job(s) "
            f"across {len(outcome.sites)} site(s)"
        )

    @property
    def failures(self) -> tuple[SearchFailure, ...]:
        return self.outcome.failures

    @property
    def jobs(self) -> tuple[SourcedJob, ...]:
        return self.outcome.jobs
