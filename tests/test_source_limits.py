from __future__ import annotations

from threading import Event

import pytest

from jobstreaming import (
    AdapterRegistry,
    ErrorEvent,
    JobEvent,
    JobPost,
    JobResponse,
    MemoryCheckpointStore,
    ProgressEvent,
    Scraper,
    SearchCompleteEvent,
    SearchRequest,
    Site,
    SiteCompleteEvent,
    SiteTerminationReason,
    StreamCancelledError,
    TransientNetworkError,
    collect_jobs,
    stream_search,
)
from jobstreaming.google import Google
from jobstreaming.indeed import Indeed
from jobstreaming.naukri import Naukri
from jobstreaming.ziprecruiter import ZipRecruiter


def _job(number: int) -> JobPost:
    return JobPost(
        id=f"synthetic-{number}",
        title=f"Job {number}",
        job_url=f"https://example.test/jobs/{number}",
    )


def _pages_registry(requested_pages: list[int]) -> AdapterRegistry:
    class Pages(Indeed):
        def __init__(self, **kwargs):
            super().__init__(api_key="synthetic", **kwargs)

        def _scrape_page(self, cursor):
            page = int(cursor) if cursor is not None else 1
            requested_pages.append(page)
            return [_job(page)], str(page + 1) if page < 3 else None, 1

    registry = AdapterRegistry()
    registry.register(Site.INDEED, Pages)
    return registry


def _run(request: SearchRequest, registry: AdapterRegistry):
    with stream_search(request, registry=registry, max_retries=0) as stream:
        return list(stream)


def _reason(events) -> SiteTerminationReason:
    return next(
        event.reason for event in events if isinstance(event, SiteCompleteEvent)
    )


def test_uncapped_traverses_until_explicit_last_cursor() -> None:
    pages: list[int] = []
    events = _run(
        SearchRequest(site_type=(Site.INDEED,), results_wanted=None),
        _pages_registry(pages),
    )
    assert pages == [1, 2, 3]
    assert [event.job.id for event in events if isinstance(event, JobEvent)] == [
        "synthetic-1",
        "synthetic-2",
        "synthetic-3",
    ]
    assert _reason(events) is SiteTerminationReason.SOURCE_EXHAUSTED
    assert next(
        event for event in events if isinstance(event, SearchCompleteEvent)
    ).completed


@pytest.mark.parametrize("limit", [0, 1, 2])
def test_finite_and_zero_limits_remain_distinct_from_exhaustion(limit: int) -> None:
    pages: list[int] = []
    events = _run(
        SearchRequest(site_type=(Site.INDEED,), results_wanted=limit),
        _pages_registry(pages),
    )
    assert len([event for event in events if isinstance(event, JobEvent)]) == limit
    assert _reason(events) is SiteTerminationReason.RESULT_LIMIT
    assert pages == list(range(1, limit + 1))


def test_acknowledged_terminal_progress_survives_restart_without_another_request() -> (
    None
):
    pages: list[int] = []
    store = MemoryCheckpointStore()
    request = SearchRequest(site_type=(Site.INDEED,), results_wanted=None, max_pages=2)
    registry = _pages_registry(pages)
    with stream_search(
        request,
        registry=registry,
        checkpoint_store=store,
        ack_mode="explicit",
        max_retries=0,
    ) as stream:
        for event in stream:
            stream.ack(event)
            if isinstance(event, ProgressEvent) and event.progress.completed_units == 2:
                break
    assert pages == [1, 2]
    with stream_search(
        request,
        registry=registry,
        checkpoint_store=store,
        ack_mode="explicit",
        max_retries=0,
    ) as stream:
        events = []
        for event in stream:
            events.append(event)
            stream.ack(event)
    assert pages == [1, 2]
    assert not any(isinstance(event, JobEvent) for event in events)
    assert _reason(events) is SiteTerminationReason.PAGE_LIMIT


def test_legacy_finite_request_keeps_its_predecessor_fingerprint() -> None:
    request = SearchRequest(
        site_type=(Site.INDEED,),
        search_term="engineer",
        location="Madrid",
        results_wanted=5,
        max_pages=3,
    )
    assert (
        request.fingerprint()
        == "5e3d21603cc421e7e839635fd2ba2a391804a1e2683d024671571d160d555a58"
    )
    assert SearchRequest(site_type=(Site.INDEED,)).results_wanted == 15


def test_unknown_adapter_does_not_claim_source_exhaustion() -> None:
    class Unknown(Scraper):
        def __init__(self, **_):
            super().__init__(Site.INDEED)

        def scrape(self, request, context=None):
            return JobResponse()

    registry = AdapterRegistry()
    registry.register(Site.INDEED, Unknown)
    assert (
        _reason(
            _run(SearchRequest(site_type=(Site.INDEED,), results_wanted=None), registry)
        )
        is SiteTerminationReason.UNKNOWN
    )


def test_collected_outcome_preserves_the_site_stop_reason() -> None:
    outcome = collect_jobs(
        SearchRequest(site_type=(Site.INDEED,), results_wanted=None),
        registry=_pages_registry([]),
        max_retries=0,
    )
    assert (
        outcome.summary_for(Site.INDEED).reason
        is SiteTerminationReason.SOURCE_EXHAUSTED
    )


@pytest.mark.parametrize(
    ("payload", "reason"),
    [
        ({"jobDetails": []}, SiteTerminationReason.SOURCE_EXHAUSTED),
        ({}, SiteTerminationReason.UNKNOWN),
    ],
)
def test_naukri_empty_response_requires_an_explicit_result_list(
    payload, reason
) -> None:
    class Response:
        status_code = 200

        def json(self):
            return payload

    class Session:
        headers: dict[str, str] = {}

        def get(self, *args, **kwargs):
            return Response()

    class EmptyNaukri(Naukri):
        def __init__(self, **kwargs):
            super().__init__(nkparam="synthetic", **kwargs)
            self.session = Session()

    registry = AdapterRegistry()
    registry.register(Site.NAUKRI, EmptyNaukri)
    events = _run(
        SearchRequest(
            site_type=(Site.NAUKRI,), search_term="engineer", results_wanted=None
        ),
        registry,
    )
    assert _reason(events) is reason


@pytest.mark.parametrize(
    ("payload", "reason"),
    [
        ({"jobs": [], "continue": None}, SiteTerminationReason.SOURCE_EXHAUSTED),
        ({"jobs": []}, SiteTerminationReason.UNKNOWN),
    ],
)
def test_ziprecruiter_requires_an_explicit_terminal_cursor(payload, reason) -> None:
    class Response:
        status_code = 200

        def json(self):
            return payload

    class Session:
        def get(self, *args, **kwargs):
            return Response()

    class EmptyZip(ZipRecruiter):
        def __init__(self, **kwargs):
            super().__init__(authorization="synthetic", **kwargs)
            self.session = Session()
            self.delay = 0

        def _get_cookies(self):
            return None

    registry = AdapterRegistry()
    registry.register(Site.ZIP_RECRUITER, EmptyZip)
    assert (
        _reason(
            _run(
                SearchRequest(site_type=(Site.ZIP_RECRUITER,), results_wanted=None),
                registry,
            )
        )
        is reason
    )


def test_google_reports_its_existing_adapter_ceiling() -> None:
    class GoogleCeiling(Google):
        def _get_initial_cursor_and_jobs(self):
            return "more", [_job(number) for number in range(900)]

    registry = AdapterRegistry()
    registry.register(Site.GOOGLE, GoogleCeiling)
    events = _run(
        SearchRequest(site_type=(Site.GOOGLE,), results_wanted=None), registry
    )
    assert len([event for event in events if isinstance(event, JobEvent)]) == 900
    assert _reason(events) is SiteTerminationReason.PROVIDER_LIMIT


def test_error_after_recorded_stop_is_not_suppressed() -> None:
    class Broken(Scraper):
        def __init__(self, **_):
            super().__init__(Site.INDEED)

        def scrape(self, request, context=None):
            assert context is not None
            context.mark_termination(SiteTerminationReason.SOURCE_EXHAUSTED)
            raise RuntimeError("synthetic failure")

    registry = AdapterRegistry()
    registry.register(Site.INDEED, Broken)
    events = _run(
        SearchRequest(site_type=(Site.INDEED,), results_wanted=None), registry
    )
    assert any(isinstance(event, ErrorEvent) for event in events)
    assert not any(isinstance(event, SiteCompleteEvent) for event in events)
    assert not next(
        event for event in events if isinstance(event, SearchCompleteEvent)
    ).completed


@pytest.mark.parametrize("failure", [RuntimeError, TransientNetworkError])
def test_error_after_the_final_permitted_job_is_not_a_success(failure) -> None:
    class BrokenAfterJob(Scraper):
        def __init__(self, **_):
            super().__init__(Site.INDEED)

        def scrape(self, request, context=None):
            assert context is not None
            context.emit_job(_job(1), {"page": 1})
            raise failure("synthetic failure after result")

    registry = AdapterRegistry()
    registry.register(Site.INDEED, BrokenAfterJob)
    with stream_search(
        SearchRequest(site_type=(Site.INDEED,), results_wanted=1),
        registry=registry,
        max_retries=1,
    ) as stream:
        events = list(stream)
    assert len([event for event in events if isinstance(event, JobEvent)]) == 1
    assert any(isinstance(event, ErrorEvent) for event in events)
    assert not any(isinstance(event, SiteCompleteEvent) for event in events)


def test_external_cancellation_is_not_exhaustion() -> None:
    cancel = Event()

    class Blocked(Scraper):
        def __init__(self, **_):
            super().__init__(Site.INDEED)

        def scrape(self, request, context=None):
            assert context is not None
            cancel.set()
            context.wait(1)
            return JobResponse()

    registry = AdapterRegistry()
    registry.register(Site.INDEED, Blocked)
    with (
        pytest.raises(StreamCancelledError),
        stream_search(
            SearchRequest(site_type=(Site.INDEED,), results_wanted=None),
            registry=registry,
            cancel_event=cancel,
            max_retries=0,
        ) as stream,
    ):
        list(stream)
