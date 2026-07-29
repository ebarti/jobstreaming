from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from threading import Event

import pytest

from jobstreaming import (
    AdapterRegistry,
    ErrorCode,
    JobPost,
    JobResponse,
    MemoryCheckpointStore,
    Scraper,
    SearchFailedError,
    SearchFailure,
    SearchOutcome,
    SearchOutcomeStatus,
    SearchRequest,
    Site,
    SiteSearchSummary,
    SourcedJob,
    StreamCancelledError,
    collect_jobs,
)


def _job(identifier: str) -> JobPost:
    return JobPost(
        id=identifier,
        title=f"Job {identifier}",
        job_url=f"https://example.test/jobs/{identifier}",
    )


class _WorkingAdapter(Scraper):
    def __init__(self, **_: object) -> None:
        super().__init__(Site.INDEED)

    def scrape(self, request, context=None) -> JobResponse:
        job = _job("working")
        if context is not None:
            context.emit_job(job, {"page": 1})
        return JobResponse(jobs=(job,))


class _EmptyAdapter(Scraper):
    def __init__(self, **_: object) -> None:
        super().__init__(Site.INDEED)

    def scrape(self, request, context=None) -> JobResponse:
        return JobResponse()


class _LinkedInFailure(Scraper):
    def __init__(self, **_: object) -> None:
        super().__init__(Site.LINKEDIN)

    def scrape(self, request, context=None) -> JobResponse:
        raise RuntimeError("linkedin blocked")


class _GoogleFailure(Scraper):
    def __init__(self, **_: object) -> None:
        super().__init__(Site.GOOGLE)

    def scrape(self, request, context=None) -> JobResponse:
        raise RuntimeError("google blocked")


class _JobThenFailure(Scraper):
    def __init__(self, **_: object) -> None:
        super().__init__(Site.INDEED)

    def scrape(self, request, context=None) -> JobResponse:
        assert context is not None
        context.emit_job(_job("before-failure"), {"page": 1})
        raise RuntimeError("failed after emission")


class _NeverStartedAdapter(Scraper):
    init_calls = 0
    scrape_calls = 0

    def __init__(self, **_: object) -> None:
        type(self).init_calls += 1
        super().__init__(Site.INDEED)

    def scrape(self, request, context=None) -> JobResponse:
        type(self).scrape_calls += 1
        return JobResponse()


def _registry(*adapters: tuple[Site, type[Scraper]]) -> AdapterRegistry:
    registry = AdapterRegistry()
    for site, adapter in adapters:
        registry.register(site, adapter)
    return registry


def test_collect_jobs_returns_sourced_jobs_and_per_site_terminal_summaries() -> None:
    outcome = collect_jobs(
        site_name=(Site.INDEED, Site.LINKEDIN),
        results_wanted=1,
        registry=_registry(
            (Site.INDEED, _WorkingAdapter),
            (Site.LINKEDIN, _LinkedInFailure),
        ),
        max_retries=0,
    )

    assert outcome.status is SearchOutcomeStatus.PARTIAL
    assert outcome.completed is False
    assert outcome.total_jobs == 1
    assert outcome.total_failures == 1
    assert outcome.jobs[0].site is Site.INDEED
    assert outcome.jobs[0].job.id == "working"

    indeed = outcome.summary_for(Site.INDEED)
    assert indeed.jobs_emitted == 1
    assert indeed.failure_count == 0
    assert indeed.completed is True

    linkedin = outcome.summary_for(Site.LINKEDIN)
    assert linkedin.jobs_emitted == 0
    assert linkedin.failure_count == 1
    assert linkedin.completed is False
    failure = linkedin.failures[0]
    assert failure.site is Site.LINKEDIN
    assert failure.code is ErrorCode.ADAPTER_FAILURE
    assert failure.error_type == "RuntimeError"
    assert failure.message == "linkedin blocked"
    assert failure.retryable is False
    assert failure.retry_after is None
    assert failure.reset_checkpoint is False


def test_collect_jobs_distinguishes_success_from_total_failure() -> None:
    succeeded = collect_jobs(
        site_name=Site.INDEED,
        registry=_registry((Site.INDEED, _EmptyAdapter)),
    )
    failed = collect_jobs(
        site_name=Site.LINKEDIN,
        registry=_registry((Site.LINKEDIN, _LinkedInFailure)),
        max_retries=0,
    )

    assert succeeded.status is SearchOutcomeStatus.SUCCEEDED
    assert succeeded.completed is True
    assert succeeded.total_jobs == 0
    assert succeeded.total_failures == 0
    assert succeeded.summary_for(Site.INDEED).completed is True

    assert failed.status is SearchOutcomeStatus.FAILED
    assert failed.completed is False
    assert failed.total_jobs == 0
    assert failed.total_failures == 1
    assert failed.failed_sites == (Site.LINKEDIN,)


def test_jobs_followed_by_a_failure_are_a_partial_outcome() -> None:
    outcome = collect_jobs(
        site_name=Site.INDEED,
        results_wanted=2,
        registry=_registry((Site.INDEED, _JobThenFailure)),
        max_retries=0,
    )

    assert outcome.status is SearchOutcomeStatus.PARTIAL
    assert outcome.completed is False
    assert outcome.total_jobs == 1
    assert outcome.total_failures == 1
    assert outcome.succeeded_sites == ()
    assert outcome.failed_sites == (Site.INDEED,)


def test_outcome_matches_equal_nonidentical_open_identifiers() -> None:
    @dataclass(frozen=True)
    class OpenIdentifier:
        value: str

    job_site = OpenIdentifier("partner")
    failure_site = OpenIdentifier("partner")
    summary_site = OpenIdentifier("partner")
    assert job_site == failure_site == summary_site
    assert id(job_site) != id(failure_site)

    failure = SearchFailure(
        sequence=1,
        emitted_at=datetime.now(timezone.utc),
        site=failure_site,  # type: ignore[arg-type]
        message="partner failed",
        error_type="RuntimeError",
        recoverable=False,
        resume_state={},
        code=ErrorCode.ADAPTER_FAILURE,
        retryable=False,
        reset_checkpoint=False,
    )
    summary = SiteSearchSummary(
        site=summary_site,  # type: ignore[arg-type]
        jobs_emitted=1,
        failures=(failure,),
        completed=False,
    )
    outcome = SearchOutcome(
        jobs=(
            SourcedJob(
                site=job_site,  # type: ignore[arg-type]
                job=_job("open-adapter"),
            ),
        ),
        sites=(summary,),
        total_jobs=1,
        total_failures=1,
        completed=False,
    )

    assert outcome.summary_for(job_site).site == job_site  # type: ignore[arg-type]
    assert outcome.status is SearchOutcomeStatus.PARTIAL


def test_aggregate_failures_preserve_event_chronology_not_request_order() -> None:
    google_failure = SearchFailure(
        sequence=3,
        emitted_at=datetime.now(timezone.utc),
        site=Site.GOOGLE,
        message="google failed first",
        error_type="RuntimeError",
        recoverable=False,
        resume_state={},
        code=ErrorCode.ADAPTER_FAILURE,
        retryable=False,
        reset_checkpoint=False,
    )
    linkedin_failure = SearchFailure(
        sequence=7,
        emitted_at=datetime.now(timezone.utc),
        site=Site.LINKEDIN,
        message="linkedin failed second",
        error_type="RuntimeError",
        recoverable=False,
        resume_state={},
        code=ErrorCode.ADAPTER_FAILURE,
        retryable=False,
        reset_checkpoint=False,
    )
    outcome = SearchOutcome(
        jobs=(),
        sites=(
            SiteSearchSummary(
                site=Site.LINKEDIN,
                jobs_emitted=0,
                failures=(linkedin_failure,),
                completed=False,
            ),
            SiteSearchSummary(
                site=Site.GOOGLE,
                jobs_emitted=0,
                failures=(google_failure,),
                completed=False,
            ),
        ),
        total_jobs=0,
        total_failures=2,
        completed=False,
    )

    assert [failure.site for failure in outcome.failures] == [
        Site.GOOGLE,
        Site.LINKEDIN,
    ]
    error = SearchFailedError(outcome)
    assert str(error).index("google failed") < str(error).index("linkedin failed")


def test_strict_collection_raises_runtime_compatible_aggregate() -> None:
    with pytest.raises(SearchFailedError) as raised:
        collect_jobs(
            site_name=(Site.INDEED, Site.LINKEDIN, Site.GOOGLE),
            results_wanted=1,
            registry=_registry(
                (Site.INDEED, _WorkingAdapter),
                (Site.LINKEDIN, _LinkedInFailure),
                (Site.GOOGLE, _GoogleFailure),
            ),
            raise_on_error=True,
            max_retries=0,
        )

    error = raised.value
    assert isinstance(error, RuntimeError)
    assert error.outcome.status is SearchOutcomeStatus.PARTIAL
    assert error.outcome.total_jobs == 1
    assert error.outcome.total_failures == 2
    assert {failure.site for failure in error.failures} == {
        Site.LINKEDIN,
        Site.GOOGLE,
    }
    assert [failure.sequence for failure in error.failures] == sorted(
        failure.sequence for failure in error.failures
    )
    assert error.jobs[0].site is Site.INDEED
    assert "linkedin failed" in str(error)
    assert "google failed" in str(error)


def test_collector_marks_sites_completed_by_an_existing_checkpoint() -> None:
    request = SearchRequest(site_type=(Site.INDEED,), results_wanted=1)
    store = MemoryCheckpointStore()
    registry = _registry((Site.INDEED, _WorkingAdapter))

    first = collect_jobs(
        request,
        registry=registry,
        checkpoint_store=store,
    )
    resumed = collect_jobs(
        request,
        registry=registry,
        checkpoint_store=store,
    )

    assert first.total_jobs == 1
    assert resumed.total_jobs == 0
    assert resumed.completed is True
    assert resumed.status is SearchOutcomeStatus.SUCCEEDED
    assert resumed.summary_for(Site.INDEED).completed is True


@pytest.mark.parametrize("cancellation_source", ["event", "callback"])
def test_collect_jobs_propagates_prestart_cancellation_without_adapter_work(
    cancellation_source: str,
) -> None:
    _NeverStartedAdapter.init_calls = 0
    _NeverStartedAdapter.scrape_calls = 0
    options: dict[str, object]
    if cancellation_source == "event":
        cancelled = Event()
        cancelled.set()
        options = {"cancel_event": cancelled}
    else:
        options = {"cancel_callback": lambda: True}

    with pytest.raises(StreamCancelledError):
        collect_jobs(
            site_name=Site.INDEED,
            registry=_registry((Site.INDEED, _NeverStartedAdapter)),
            **options,
        )

    assert _NeverStartedAdapter.init_calls == 0
    assert _NeverStartedAdapter.scrape_calls == 0
