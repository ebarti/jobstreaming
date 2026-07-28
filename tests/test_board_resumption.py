from __future__ import annotations

from collections.abc import Callable
from typing import Any

import pytest

from jobstreaming import (
    AdapterRegistry,
    ErrorCode,
    ErrorEvent,
    JobEvent,
    JobPost,
    MemoryCheckpointStore,
    SearchCheckpoint,
    SearchCompleteEvent,
    SearchRequest,
    Site,
    stream_search,
)
from jobstreaming.glassdoor import Glassdoor
from jobstreaming.indeed import Indeed
from jobstreaming.linkedin import LinkedIn
from jobstreaming.ziprecruiter import ZipRecruiter

FactoryBuilder = Callable[[list[int]], Callable[..., Any]]


def _job(site: Site, number: int) -> JobPost:
    return JobPost(
        id=f"{site.value}-{number}",
        title=f"{site.value} job {number}",
        job_url=f"https://example.test/{site.value}/{number}",
    )


def _indeed_factory(instances: list[int]):
    class ReplayIndeed(Indeed):
        def __init__(self, **kwargs) -> None:
            super().__init__(api_key="configured-for-test", **kwargs)
            instances.append(1)

        def _scrape_page(self, cursor):
            number = 1 if cursor is None else 2
            next_cursor = "indeed-page-2" if number == 1 else None
            return [_job(Site.INDEED, number)], next_cursor

    return ReplayIndeed


class _LinkedInResponse:
    def __init__(self, number: int) -> None:
        self.status_code = 200
        self.text = f"""
        <div class="base-search-card">
          <a class="base-card__full-link"
             href="https://example.test/linkedin-job-{number}"></a>
        </div>
        """


class _LinkedInSession:
    def get(self, url, *, params, timeout):
        del url, timeout
        return _LinkedInResponse(int(params["start"]) + 1)


def _linkedin_factory(instances: list[int]):
    class ReplayLinkedIn(LinkedIn):
        delay = 0
        band_delay = 0

        def __init__(self, **kwargs) -> None:
            super().__init__(**kwargs)
            instances.append(1)
            self.session = _LinkedInSession()

        def _process_job(self, job_card, job_id, full_descr):
            del job_card, full_descr
            return _job(Site.LINKEDIN, int(job_id))

    return ReplayLinkedIn


def _ziprecruiter_factory(instances: list[int]):
    class ReplayZipRecruiter(ZipRecruiter):
        delay = 0

        def __init__(self, **kwargs) -> None:
            super().__init__(authorization="configured-for-test", **kwargs)
            instances.append(1)
            self.delay = 0

        def _get_cookies(self):
            return None

        def _find_jobs_in_page(
            self,
            request,
            context,
            continue_token,
            skipped,
            page_state,
        ):
            del request, continue_token
            number = int(page_state["page"])
            job = _job(Site.ZIP_RECRUITER, number)
            emitted = [job] if context.emit_job(job, page_state) else []
            next_token = "zip-page-2" if number == 1 else None
            return emitted, next_token, skipped

    return ReplayZipRecruiter


def _glassdoor_factory(instances: list[int]):
    class ReplayGlassdoor(Glassdoor):
        def __init__(self, **kwargs) -> None:
            super().__init__(**kwargs)
            instances.append(1)

        def _get_csrf_token(self):
            return "test-token"

        def _get_location(self, location, is_remote):
            del location, is_remote
            return 1, "C"

        def _fetch_jobs_page(
            self,
            request,
            location_id,
            location_type,
            page_num,
            cursor,
            context,
            page_state,
        ):
            del request, location_id, location_type, cursor
            job = _job(Site.GLASSDOOR, page_num)
            emitted = [job] if context.emit_job(job, page_state) else []
            next_cursor = "glassdoor-page-2" if page_num == 1 else None
            return emitted, next_cursor, 1

    return ReplayGlassdoor


BOARDS = [
    pytest.param(Site.INDEED, _indeed_factory, id="indeed"),
    pytest.param(Site.LINKEDIN, _linkedin_factory, id="linkedin"),
    pytest.param(Site.ZIP_RECRUITER, _ziprecruiter_factory, id="ziprecruiter"),
    pytest.param(Site.GLASSDOOR, _glassdoor_factory, id="glassdoor"),
]


def _request(site: Site) -> SearchRequest:
    return SearchRequest(
        site_type=(site,),
        location="Madrid",
        results_wanted=2,
        max_pages=2,
    )


def _registry(site: Site, factory: Callable[..., Any]) -> AdapterRegistry:
    registry = AdapterRegistry()
    registry.register(site, factory)
    return registry


def _job_ids(stream) -> list[str | None]:
    return [event.job.id for event in stream if isinstance(event, JobEvent)]


@pytest.mark.parametrize(("site", "build_factory"), BOARDS)
def test_unacknowledged_production_board_page_is_replayed(
    site: Site,
    build_factory: FactoryBuilder,
) -> None:
    instances: list[int] = []
    factory = build_factory(instances)
    store = MemoryCheckpointStore()
    request = _request(site)
    registry = _registry(site, factory)
    first_stream = stream_search(
        request,
        registry=registry,
        checkpoint_store=store,
        ack_mode="explicit",
    )
    first = next(first_stream)
    assert isinstance(first, JobEvent)
    first_stream.close()

    with stream_search(
        request,
        registry=registry,
        checkpoint_store=store,
        resume=True,
    ) as resumed:
        replayed = _job_ids(resumed)

    assert replayed == [f"{site.value}-1", f"{site.value}-2"]


@pytest.mark.parametrize(("site", "build_factory"), BOARDS)
def test_acknowledged_production_board_job_is_suppressed_on_page_replay(
    site: Site,
    build_factory: FactoryBuilder,
) -> None:
    instances: list[int] = []
    factory = build_factory(instances)
    store = MemoryCheckpointStore()
    request = _request(site)
    registry = _registry(site, factory)
    first_stream = stream_search(
        request,
        registry=registry,
        checkpoint_store=store,
        ack_mode="explicit",
    )
    first = next(first_stream)
    assert isinstance(first, JobEvent)
    first_stream.ack(first)
    first_stream.close()

    with stream_search(
        request,
        registry=registry,
        checkpoint_store=store,
        resume=True,
    ) as resumed:
        replayed = _job_ids(resumed)

    assert replayed == [f"{site.value}-2"]


@pytest.mark.parametrize(("site", "build_factory"), BOARDS)
def test_completed_production_board_is_skipped_on_resume(
    site: Site,
    build_factory: FactoryBuilder,
) -> None:
    instances: list[int] = []
    factory = build_factory(instances)
    store = MemoryCheckpointStore()
    request = _request(site)
    registry = _registry(site, factory)

    with stream_search(
        request,
        registry=registry,
        checkpoint_store=store,
    ) as first:
        list(first)
    with stream_search(
        request,
        registry=registry,
        checkpoint_store=store,
        resume=True,
    ) as resumed:
        events = list(resumed)

    assert len(instances) == 1
    assert [type(event) for event in events] == [SearchCompleteEvent]


class _ExpiredResponse:
    ok = False
    status_code = 410
    text = ""

    def json(self):
        return {}


class _ExpiredSession:
    def get(self, *args, **kwargs):
        return _ExpiredResponse()

    def post(self, *args, **kwargs):
        return _ExpiredResponse()


def _expired_factory(site: Site, requests: list[int]):
    if site is Site.INDEED:

        class ExpiredIndeed(Indeed):
            def __init__(self, **kwargs) -> None:
                super().__init__(api_key="configured-for-test", **kwargs)
                self.session = _ExpiredSession()

            def _scrape_page(self, cursor):
                requests.append(1)
                return super()._scrape_page(cursor)

        return ExpiredIndeed

    if site is Site.LINKEDIN:

        class ExpiredLinkedIn(LinkedIn):
            delay = 0
            band_delay = 0

            def __init__(self, **kwargs) -> None:
                super().__init__(**kwargs)
                self.session = _ExpiredSession()

            def scrape(self, request, context=None):
                requests.append(1)
                return super().scrape(request, context=context)

        return ExpiredLinkedIn

    if site is Site.ZIP_RECRUITER:

        class ExpiredZipRecruiter(ZipRecruiter):
            delay = 0

            def __init__(self, **kwargs) -> None:
                super().__init__(authorization="configured-for-test", **kwargs)
                self.session = _ExpiredSession()
                self.delay = 0

            def _get_cookies(self):
                return None

            def _find_jobs_in_page(self, *args, **kwargs):
                requests.append(1)
                return super()._find_jobs_in_page(*args, **kwargs)

        return ExpiredZipRecruiter

    class ExpiredGlassdoor(Glassdoor):
        def _get_csrf_token(self):
            return "test-token"

        def _get_location(self, location, is_remote):
            del location, is_remote
            return 1, "C"

        def _fetch_jobs_page(self, *args, **kwargs):
            requests.append(1)
            self.session = _ExpiredSession()
            return super()._fetch_jobs_page(*args, **kwargs)

    return ExpiredGlassdoor


EXPIRED_STATES = {
    Site.INDEED: {"cursor": "expired", "page": 2, "skipped": 0},
    Site.LINKEDIN: {"start": 25, "pages_completed": 1},
    Site.ZIP_RECRUITER: {
        "continue_token": "expired",
        "page": 2,
        "skipped": 0,
    },
    Site.GLASSDOOR: {"cursor": "expired", "page": 2},
}


@pytest.mark.parametrize(("site", "build_factory"), BOARDS)
def test_expired_production_board_cursor_requests_checkpoint_reset(
    site: Site,
    build_factory: FactoryBuilder,
) -> None:
    del build_factory
    requests: list[int] = []
    factory = _expired_factory(site, requests)
    request = _request(site)
    checkpoint = SearchCheckpoint.for_request(request)
    adapter = checkpoint.adapters[site.value].model_copy(
        update={"state": EXPIRED_STATES[site]}
    )
    checkpoint = checkpoint.model_copy(update={"adapters": {site.value: adapter}})
    store = MemoryCheckpointStore()
    store.save(checkpoint)

    with stream_search(
        request,
        registry=_registry(site, factory),
        checkpoint_store=store,
        max_retries=2,
    ) as stream:
        events = list(stream)

    error = next(event for event in events if isinstance(event, ErrorEvent))
    assert requests == [1]
    assert error.code is ErrorCode.CURSOR_EXPIRED
    assert error.retryable is False
    assert error.reset_checkpoint is True
