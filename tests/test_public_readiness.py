from __future__ import annotations

from collections.abc import Callable

import pytest
from requests.exceptions import RetryError

from jobstreaming import (
    AdapterCapabilities,
    AdapterRegistry,
    AuthenticationConfigurationError,
    CursorExpiredError,
    ErrorCode,
    ErrorEvent,
    JobPost,
    JobResponse,
    JobType,
    RateLimitError,
    ScrapeContext,
    Scraper,
    SearchCompleteEvent,
    SearchRequest,
    Site,
    SiteCompleteEvent,
    StreamCancelledError,
    WarningEvent,
    stream_search,
)
from jobstreaming.exception import classify_exception, error_for_http_status
from jobstreaming.glassdoor import Glassdoor
from jobstreaming.glassdoor import constant as glassdoor_constant
from jobstreaming.google import Google
from jobstreaming.indeed import Indeed
from jobstreaming.indeed import constant as indeed_constant
from jobstreaming.linkedin.util import job_type_code
from jobstreaming.naukri import Naukri
from jobstreaming.naukri import constant as naukri_constant
from jobstreaming.util import create_session
from jobstreaming.ziprecruiter import ZipRecruiter
from jobstreaming.ziprecruiter import constant as ziprecruiter_constant
from jobstreaming.ziprecruiter.util import add_params


def _registry(adapter: Callable[..., Scraper]) -> AdapterRegistry:
    registry = AdapterRegistry()
    registry.register(Site.INDEED, adapter)
    return registry


def _job(site: Site, identifier: str) -> JobPost:
    return JobPost(
        id=f"{site.value}-{identifier}",
        title=f"Job {identifier}",
        job_url=f"https://example.test/{site.value}/{identifier}",
    )


def test_one_shot_cancellation_callback_is_latched() -> None:
    callback_calls = 0
    adapter_runs = 0

    def cancel_once() -> bool:
        nonlocal callback_calls
        callback_calls += 1
        return callback_calls == 1

    class Adapter(Scraper):
        def __init__(self, **_: object) -> None:
            super().__init__(Site.INDEED)

        def scrape(self, request, context=None) -> JobResponse:
            nonlocal adapter_runs
            adapter_runs += 1
            return JobResponse()

    stream = stream_search(
        SearchRequest(site_type=(Site.INDEED,)),
        registry=_registry(Adapter),
        cancel_callback=cancel_once,
    )
    delivered = []
    with pytest.raises(StreamCancelledError):
        for event in stream:
            delivered.append(event)

    assert adapter_runs == 0
    assert not any(
        isinstance(event, (SiteCompleteEvent, SearchCompleteEvent))
        for event in delivered
    )


def test_retry_exhaustion_and_retry_after_are_publicly_classified() -> None:
    retry_error = classify_exception(RetryError("transport retries exhausted"))
    assert retry_error.code is ErrorCode.TRANSIENT_NETWORK
    assert retry_error.retryable is True

    rate_limit = error_for_http_status("Board", 429, retry_after="7")
    classified = classify_exception(rate_limit)
    assert classified.code is ErrorCode.RATE_LIMITED
    assert classified.retryable is True
    assert classified.retry_after == 7


def test_runtime_honors_retry_after_and_exposes_it_on_exhaustion(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    waits: list[float] = []
    attempts = 0

    def record_wait(self: ScrapeContext, seconds: float) -> bool:
        waits.append(seconds)
        return self.should_continue

    monkeypatch.setattr(ScrapeContext, "wait", record_wait)

    class RetryingAdapter(Scraper):
        def __init__(self, **_: object) -> None:
            super().__init__(Site.INDEED)

        def scrape(self, request, context=None) -> JobResponse:
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                raise RateLimitError("slow down", retry_after=3)
            return JobResponse()

    events = list(
        stream_search(
            SearchRequest(site_type=(Site.INDEED,)),
            registry=_registry(RetryingAdapter),
            max_retries=1,
            retry_backoff=0.25,
        )
    )

    assert waits == [3]
    warning = next(event for event in events if isinstance(event, WarningEvent))
    assert "in 3s" in warning.message

    class ExhaustedAdapter(Scraper):
        def __init__(self, **_: object) -> None:
            super().__init__(Site.INDEED)

        def scrape(self, request, context=None) -> JobResponse:
            raise RateLimitError("still limited", retry_after=5)

    exhausted = list(
        stream_search(
            SearchRequest(site_type=(Site.INDEED,)),
            registry=_registry(ExhaustedAdapter),
            max_retries=0,
        )
    )
    error = next(event for event in exhausted if isinstance(event, ErrorEvent))
    assert error.code is ErrorCode.RATE_LIMITED
    assert error.retryable is True
    assert error.retry_after == 5


def test_google_continuation_http_error_preserves_cursor_context() -> None:
    class Response:
        status_code = 410
        text = ""
        headers = {"Retry-After": "4"}

    class Session:
        def get(self, *args, **kwargs):
            return Response()

    scraper = Google()
    scraper.session = Session()
    scraper.scraper_input = SearchRequest(site_type=(Site.GOOGLE,))

    with pytest.raises(CursorExpiredError) as raised:
        scraper._get_jobs_next_page("expired-cursor")

    assert raised.value.reset_checkpoint is True


def test_naukri_offset_does_not_consume_the_page_budget() -> None:
    requested_pages: list[int] = []

    class Response:
        status_code = 200

        def json(self):
            return {"jobDetails": [{"jobId": "21"}]}

    class Session:
        headers: dict[str, str] = {}

        def get(self, url, *, params, timeout):
            del url, timeout
            requested_pages.append(int(params["pageNo"]))
            return Response()

    class OffsetNaukri(Naukri):
        delay = 0
        band_delay = 0

        def _process_job(self, job, job_id):
            del job
            return _job(Site.NAUKRI, job_id)

    scraper = OffsetNaukri()
    scraper.nkparam = "configured-for-test"
    scraper.session = Session()
    response = scraper.scrape(
        SearchRequest(
            site_type=(Site.NAUKRI,),
            search_term="engineer",
            offset=20,
            max_pages=1,
            results_wanted=1,
        )
    )

    assert requested_pages == [2]
    assert [job.id for job in response.jobs] == ["naukri-21"]


def test_glassdoor_offset_does_not_consume_the_page_budget() -> None:
    requested_pages: list[int] = []

    class OffsetGlassdoor(Glassdoor):
        def _get_csrf_token(self):
            return "configured-for-test"

        def _get_location(self, location, is_remote):
            del location, is_remote
            return 1, "CITY"

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
            requested_pages.append(page_num)
            job = _job(Site.GLASSDOOR, str(page_num))
            context.emit_job(job, page_state)
            return [job], None, 1

    response = OffsetGlassdoor().scrape(
        SearchRequest(
            site_type=(Site.GLASSDOOR,),
            location="Madrid",
            offset=30,
            max_pages=1,
            results_wanted=1,
        )
    )

    assert requested_pages == [2]
    assert [job.id for job in response.jobs] == ["glassdoor-2"]


def test_unsupported_job_type_value_emits_a_warning() -> None:
    class FullTimeOnlyAdapter(Scraper):
        capabilities = AdapterCapabilities(
            filters=frozenset({"job_type"}),
            supported_job_types=frozenset({JobType.FULL_TIME}),
        )

        def __init__(self, **_: object) -> None:
            super().__init__(Site.INDEED)

        def scrape(self, request, context=None) -> JobResponse:
            return JobResponse()

    events = list(
        stream_search(
            SearchRequest(
                site_type=(Site.INDEED,),
                job_type=JobType.TEMPORARY,
            ),
            registry=_registry(FullTimeOnlyAdapter),
        )
    )

    warning = next(event for event in events if isinstance(event, WarningEvent))
    assert "unsupported job_type value" in warning.message.casefold()
    assert JobType.TEMPORARY.canonical in warning.message


def test_unsupported_job_type_values_are_not_sent_to_boards() -> None:
    indeed = Indeed()
    indeed.scraper_input = SearchRequest(
        site_type=(Site.INDEED,),
        job_type=JobType.PER_DIEM,
    )

    assert indeed._build_filters() == ""
    assert job_type_code(JobType.PER_DIEM) is None


def test_ziprecruiter_parameter_edges_preserve_the_request() -> None:
    params = add_params(
        SearchRequest(
            site_type=(Site.ZIP_RECRUITER,),
            search_term="engineer",
            hours_old=47,
            distance=0,
        )
    )

    assert params["days"] == 2
    assert params["radius"] == 0


def test_tls_adapters_reject_an_unsupported_custom_ca_file() -> None:
    with pytest.raises(AuthenticationConfigurationError, match="tls-client"):
        create_session(is_tls=True, ca_cert="/tmp/custom-ca.pem")

    session = create_session(is_tls=False, ca_cert="/tmp/custom-ca.pem")
    assert session.verify == "/tmp/custom-ca.pem"
    session.close()


@pytest.mark.parametrize(
    ("environment_name", "factory", "search_request"),
    [
        (
            "JOBSTREAMING_INDEED_API_KEY",
            Indeed,
            SearchRequest(site_type=(Site.INDEED,)),
        ),
        (
            "JOBSTREAMING_NAUKRI_NKPARAM",
            Naukri,
            SearchRequest(site_type=(Site.NAUKRI,), search_term="engineer"),
        ),
        (
            "JOBSTREAMING_ZIPRECRUITER_AUTHORIZATION",
            ZipRecruiter,
            SearchRequest(site_type=(Site.ZIP_RECRUITER,)),
        ),
    ],
)
def test_shared_board_credentials_require_explicit_configuration(
    monkeypatch: pytest.MonkeyPatch,
    environment_name: str,
    factory: Callable[[], Scraper],
    search_request: SearchRequest,
) -> None:
    monkeypatch.delenv(environment_name, raising=False)

    with pytest.raises(AuthenticationConfigurationError, match=environment_name):
        factory().scrape(search_request)


def test_glassdoor_requires_a_live_or_configured_csrf_token(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("JOBSTREAMING_GLASSDOOR_CSRF_TOKEN", raising=False)

    class NoTokenGlassdoor(Glassdoor):
        def _get_csrf_token(self):
            return None

    with pytest.raises(
        AuthenticationConfigurationError,
        match="JOBSTREAMING_GLASSDOOR_CSRF_TOKEN",
    ):
        NoTokenGlassdoor().scrape(
            SearchRequest(site_type=(Site.GLASSDOOR,), location="Madrid")
        )


def test_package_constants_do_not_embed_shared_credentials() -> None:
    assert "indeed-api-key" not in indeed_constant.api_headers
    assert "Nkparam" not in naukri_constant.headers
    assert "authorization" not in ziprecruiter_constant.headers
    assert "x-pushnotificationid" not in ziprecruiter_constant.headers
    assert not hasattr(glassdoor_constant, "fallback_token")
