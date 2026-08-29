from __future__ import annotations

import json
from pathlib import Path

import pytest
from pydantic import ValidationError
from requests import exceptions as requests_exceptions

from jobstreaming import (
    AdapterCapabilities,
    AdapterCheckpoint,
    AdapterTestKit,
    CompensationInterval,
    Country,
    DescriptionFormat,
    JobPost,
    JobType,
    ScrapeContext,
    SearchRequest,
    Site,
    default_registry,
)
from jobstreaming.bdjobs import BDJobs
from jobstreaming.exception import (
    AuthenticationConfigurationError,
    CursorExpiredError,
    InvalidRequestError,
    RateLimitError,
    TransientNetworkError,
)
from jobstreaming.glassdoor import Glassdoor
from jobstreaming.google import Google
from jobstreaming.linkedin import LinkedIn
from jobstreaming.util import stable_job_key

FIXTURES = Path(__file__).with_name("fixtures")


@pytest.mark.parametrize("site", tuple(Site), ids=lambda site: site.value)
def test_built_in_adapters_conform_to_the_shared_declaration_contract(
    site: Site,
) -> None:
    registry = default_registry()
    factory = type(registry.create(site))

    adapter = AdapterTestKit.assert_conforms(site, factory)

    assert adapter.site is site
    assert registry.cursor_schema_version(site) == (
        adapter.capabilities.cursor_schema_version
    )


@pytest.mark.parametrize(
    "declaration",
    [
        {"filters": {"not_a_request_filter"}},
        {"supported_job_types": {JobType.FULL_TIME}},
        {"resume": {"kind": "resumable"}},
        {"resume": {"kind": "none", "granularity": "page"}},
    ],
)
def test_adapter_capability_declarations_reject_incoherent_states(
    declaration: dict[str, object],
) -> None:
    with pytest.raises(ValidationError):
        AdapterCapabilities.model_validate(declaration)


class _Response:
    def __init__(
        self,
        *,
        status_code: int = 200,
        text: str = "",
        payload: object | None = None,
        headers: dict[str, str] | None = None,
    ) -> None:
        self.status_code = status_code
        self.text = text
        self._payload = payload
        self.headers = headers or {}

    def json(self) -> object:
        return self._payload

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


def _linkedin_cards(*numbers: int) -> str:
    return "".join(f"""
        <div class="base-search-card">
          <a class="base-card__full-link"
             href="https://www.linkedin.com/jobs/view/fixture-job-{number}"></a>
          <span class="sr-only">Engineer {number}</span>
          <h4 class="base-search-card__subtitle">Fixture Company</h4>
          <div class="base-search-card__metadata">
            <span class="job-search-card__location">Madrid, Spain</span>
          </div>
        </div>
        """ for number in numbers)


def test_bdjobs_offline_fixture_covers_filters_parsing_and_pagination(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    search_html = (FIXTURES / "bdjobs_search.html").read_text()
    detail_html = (FIXTURES / "bdjobs_detail.html").read_text()
    requested_pages: list[int] = []

    class Session:
        headers: dict[str, str] = {}

        def get(self, url, *, params=None, timeout=None):
            del timeout
            if "jobsearch" in url:
                page = int((params or {}).get("pg", 1))
                requested_pages.append(page)
                return _Response(text=search_html if page == 1 else "<html></html>")
            return _Response(text=detail_html)

    session = Session()
    monkeypatch.setattr(BDJobs, "_new_session", lambda self: session)
    scraper = BDJobs(user_agent="jobstreaming-offline-contract")
    response = scraper.scrape(
        SearchRequest(
            site_type=(Site.BDJOBS,),
            search_term="engineer",
            offset=1,
            results_wanted=2,
            max_pages=2,
            description_format=DescriptionFormat.PLAIN,
        )
    )

    assert requested_pages == [1, 2]
    assert [job.id for job in response.jobs] == ["bd-101"]
    job = response.jobs[0]
    assert job.company_name == "Fixture Analytics"
    assert job.location is not None
    assert job.location.country is Country.BANGLADESH
    assert job.is_remote is True
    assert job.job_type == (JobType.FULL_TIME,)
    assert job.company_industry == "Software"
    assert job.emails == ("hiring@example.test",)
    assert "window.fixtureSecret" not in (job.description or "")


def test_bdjobs_resume_state_starts_at_the_recorded_page(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    requested_pages: list[int] = []

    class Session:
        headers: dict[str, str] = {}

        def get(self, url, *, params=None, timeout=None):
            del url, timeout
            requested_pages.append(int((params or {}).get("pg", 1)))
            return _Response(text="<html></html>")

    monkeypatch.setattr(BDJobs, "_new_session", lambda self: Session())
    request = SearchRequest(site_type=(Site.BDJOBS,), max_pages=3)
    context = ScrapeContext(
        site=Site.BDJOBS,
        request=request,
        checkpoint=_adapter_checkpoint(Site.BDJOBS, {"page": 3, "raw_seen": 40}),
    )

    BDJobs().scrape(request, context)

    assert requested_pages == [3]


def test_google_offline_fixture_covers_cursor_resume_and_filter_budget(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pages = json.loads((FIXTURES / "google_jobs.json").read_text())
    scraper = Google(user_agent="jobstreaming-offline-contract")
    first = [scraper._parse_job(raw) for raw in pages["first_page"]]
    second = [scraper._parse_job(raw) for raw in pages["second_page"]]
    assert all(first) and all(second)

    monkeypatch.setattr(
        scraper,
        "_get_initial_cursor_and_jobs",
        lambda: ("fixture-next", first),
    )
    seen_cursors: list[str] = []

    def next_page(cursor: str):
        seen_cursors.append(cursor)
        return second, None

    monkeypatch.setattr(scraper, "_get_jobs_next_page", next_page)
    monkeypatch.setattr(
        "jobstreaming.google.create_session",
        lambda **kwargs: object(),
    )
    response = scraper.scrape(
        SearchRequest(
            site_type=(Site.GOOGLE,),
            offset=1,
            results_wanted=2,
            max_pages=2,
        )
    )

    assert seen_cursors == ["fixture-next"]
    assert [job.id for job in response.jobs] == ["go-two", "go-three"]
    assert response.jobs[0].location is not None
    assert response.jobs[0].location.state == "CT"
    assert response.jobs[1].location is not None
    assert response.jobs[1].location.country == "Portugal"


def test_google_initial_page_builds_supported_query_from_offline_fixture(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pages = json.loads((FIXTURES / "google_jobs.json").read_text())
    requests: list[dict[str, object]] = []

    class Session:
        def get(self, url, **kwargs):
            requests.append({"url": url, **kwargs})
            return _Response(
                text='<div jsname="Yust4d" data-async-fc="fixture-next"></div>'
            )

    scraper = Google(user_agent="jobstreaming-offline-contract")
    scraper.scraper_input = SearchRequest(
        site_type=(Site.GOOGLE,),
        search_term="platform engineer",
        location="Madrid",
        is_remote=True,
        job_type=JobType.FULL_TIME,
        hours_old=48,
    )
    scraper.session = Session()
    monkeypatch.setattr(
        "jobstreaming.google.find_job_info_initial_page",
        lambda html: pages["first_page"],
    )

    cursor, jobs = scraper._get_initial_cursor_and_jobs()

    assert cursor == "fixture-next"
    assert [job.id for job in jobs] == ["go-one", "go-two"]
    assert requests[0]["url"] == scraper.url
    assert requests[0]["params"] == {
        "q": "platform engineer jobs Full time near Madrid "
        "in the last 3 days remote",
        "udm": "8",
    }
    headers = requests[0]["headers"]
    assert isinstance(headers, dict)
    assert headers["user-agent"] == "jobstreaming-offline-contract"


def test_google_continuation_failure_is_classified_as_cursor_expiry() -> None:
    class Session:
        def get(self, *args, **kwargs):
            del args, kwargs
            return _Response(status_code=410, headers={"Retry-After": "2"})

    scraper = Google()
    scraper.scraper_input = SearchRequest(site_type=(Site.GOOGLE,))
    scraper.session = Session()

    with pytest.raises(CursorExpiredError) as raised:
        scraper._get_jobs_next_page("expired")

    assert raised.value.reset_checkpoint is True


def test_linkedin_partial_page_stops_without_waiting_or_requesting_again(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    requested_starts: list[int] = []
    waits: list[float] = []
    progress_has_more: list[bool | None] = []

    cards = _linkedin_cards(*range(3))

    class Session:
        headers: dict[str, str] = {}

        def get(self, url, *, params, timeout):
            del url, timeout
            requested_starts.append(int(params["start"]))
            if len(requested_starts) > 1:
                pytest.fail("a partial LinkedIn page must be terminal")
            return _Response(text=cards)

    request = SearchRequest(
        site_type=(Site.LINKEDIN,),
        search_term="engineer",
        location="Madrid",
        results_wanted=20,
        max_pages=5,
    )

    def capture_progress(message) -> bool:
        if message.progress is not None:
            progress_has_more.append(message.progress.has_more)
        return True

    context = ScrapeContext(
        site=Site.LINKEDIN,
        request=request,
        sink=capture_progress,
    )
    monkeypatch.setattr(
        context,
        "wait",
        lambda seconds: waits.append(seconds) or True,
    )
    scraper = LinkedIn(user_agent="jobstreaming-offline-contract")
    scraper.session = Session()

    response = scraper.scrape(request, context=context)

    assert requested_starts == [0]
    assert waits == []
    assert progress_has_more == [False]
    assert [job.id for job in response.jobs] == ["li-0", "li-1", "li-2"]
    assert context.resume_state == {
        "start": 3,
        "pages_completed": 1,
        "raw_seen": 3,
    }


def test_linkedin_uses_randomized_one_to_two_second_page_pacing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    requested_starts: list[int] = []
    uniform_bounds: list[tuple[float, float]] = []
    waits: list[float] = []

    class Session:
        headers: dict[str, str] = {}

        def get(self, url, *, params, timeout):
            del url, timeout
            requested_starts.append(int(params["start"]))
            cards = _linkedin_cards(*range(10)) if len(requested_starts) == 1 else ""
            return _Response(text=cards)

    def sample_uniform(lower: float, upper: float) -> float:
        uniform_bounds.append((lower, upper))
        return 1.25

    request = SearchRequest(
        site_type=(Site.LINKEDIN,),
        search_term="engineer",
        location="Madrid",
        results_wanted=20,
        max_pages=2,
    )
    context = ScrapeContext(site=Site.LINKEDIN, request=request)
    monkeypatch.setattr(
        context,
        "wait",
        lambda seconds: waits.append(seconds) or True,
    )
    monkeypatch.setattr("jobstreaming.linkedin.random.uniform", sample_uniform)
    scraper = LinkedIn(user_agent="jobstreaming-offline-contract")
    scraper.session = Session()

    scraper.scrape(request, context=context)

    assert requested_starts == [0, 10]
    assert uniform_bounds == [(1, 2)]
    assert waits == [1.25]


def test_linkedin_skips_detail_requests_for_resumed_and_same_run_identities(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cards = _linkedin_cards(1, 2, 2, 3)

    class Session:
        headers: dict[str, str] = {}

        def get(self, url, *, params, timeout):
            del url, params, timeout
            return _Response(text=cards)

    request = SearchRequest(
        site_type=(Site.LINKEDIN,),
        search_term="engineer",
        location="Madrid",
        linkedin_fetch_description=True,
        results_wanted=3,
        max_pages=1,
    )
    context = ScrapeContext(
        site=Site.LINKEDIN,
        request=request,
        checkpoint=AdapterCheckpoint(
            site=Site.LINKEDIN,
            seen_job_keys=(stable_job_key(Site.LINKEDIN.value, "li-1"),),
            emitted_count=1,
        ),
    )
    detail_requests: list[str] = []
    scraper = LinkedIn(user_agent="jobstreaming-offline-contract")
    scraper.session = Session()
    monkeypatch.setattr(
        scraper,
        "_get_job_details",
        lambda job_id: detail_requests.append(job_id)
        or {"description": f"Description {job_id}"},
    )

    response = scraper.scrape(request, context=context)

    assert detail_requests == ["2", "3"]
    assert [job.id for job in response.jobs] == ["li-2", "li-3"]


def test_linkedin_targeted_detail_enriches_one_existing_listing() -> None:
    requested_urls: list[str] = []

    class Session:
        headers: dict[str, str] = {}

        def get(self, url, *, timeout):
            requested_urls.append(url)
            assert timeout == 9
            response = _Response(
                text="""
                    <div class="show-more-less-html__markup">
                      Build reliable systems. Contact hiring@example.test.
                    </div>
                """,
            )
            response.url = url
            return response

    scraper = LinkedIn(user_agent="jobstreaming-offline-contract")
    scraper.session = Session()
    listing = JobPost(
        id="li-42",
        title="Staff Platform Engineer",
        company_name="Acme",
        job_url="https://www.linkedin.com/jobs/view/42",
    )
    request = SearchRequest(
        site_type=(Site.LINKEDIN,),
        description_format=DescriptionFormat.PLAIN,
        request_timeout=9,
    )

    detailed = scraper.fetch_job_detail(listing, request)

    assert requested_urls == ["https://www.linkedin.com/jobs/view/42"]
    assert detailed is not None
    assert detailed.id == listing.id
    assert detailed.description == (
        "Build reliable systems. Contact hiring@example.test."
    )
    assert detailed.emails == ("hiring@example.test",)


def test_linkedin_targeted_detail_returns_none_without_usable_description() -> None:
    class Session:
        headers: dict[str, str] = {}

        def get(self, url, *, timeout):
            del timeout
            response = _Response(text="<html><body>No description</body></html>")
            response.url = url
            return response

    scraper = LinkedIn(user_agent="jobstreaming-offline-contract")
    scraper.session = Session()
    listing = JobPost(
        id="li-42",
        title="Staff Platform Engineer",
        job_url="https://www.linkedin.com/jobs/view/42",
    )

    detailed = scraper.fetch_job_detail(
        listing,
        SearchRequest(site_type=(Site.LINKEDIN,)),
    )

    assert detailed is None


def test_linkedin_targeted_detail_preserves_typed_provider_failures() -> None:
    class Session:
        headers: dict[str, str] = {}

        def get(self, url, *, timeout):
            del url, timeout
            return _Response(status_code=429, headers={"Retry-After": "2"})

    scraper = LinkedIn(user_agent="jobstreaming-offline-contract")
    scraper.session = Session()
    listing = JobPost(
        id="li-42",
        title="Staff Platform Engineer",
        job_url="https://www.linkedin.com/jobs/view/42",
    )

    with pytest.raises(RateLimitError) as raised:
        scraper.fetch_job_detail(
            listing,
            SearchRequest(site_type=(Site.LINKEDIN,)),
        )

    assert raised.value.retryable is True
    assert raised.value.retry_after == 2


def test_linkedin_targeted_detail_requires_a_canonical_job_identity() -> None:
    scraper = LinkedIn(user_agent="jobstreaming-offline-contract")
    listing = JobPost(
        id="provider-opaque",
        title="Staff Platform Engineer",
        job_url="https://www.linkedin.com/jobs/view/not-numeric",
    )

    with pytest.raises(InvalidRequestError, match="canonical numeric job id"):
        scraper.fetch_job_detail(
            listing,
            SearchRequest(site_type=(Site.LINKEDIN,)),
        )


def test_linkedin_targeted_detail_rejects_conflicting_job_id_and_url() -> None:
    scraper = LinkedIn(user_agent="jobstreaming-offline-contract")
    listing = JobPost(
        id="li-42",
        title="Staff Platform Engineer",
        job_url="https://www.linkedin.com/jobs/view/99",
    )

    with pytest.raises(InvalidRequestError, match="does not match"):
        scraper.fetch_job_detail(
            listing,
            SearchRequest(site_type=(Site.LINKEDIN,)),
        )


def test_linkedin_targeted_detail_rejects_a_non_linkedin_job_url() -> None:
    scraper = LinkedIn(user_agent="jobstreaming-offline-contract")
    listing = JobPost(
        id="li-42",
        title="Staff Platform Engineer",
        job_url="https://example.test/jobs/view/42",
    )

    with pytest.raises(InvalidRequestError, match="LinkedIn job URL"):
        scraper.fetch_job_detail(
            listing,
            SearchRequest(site_type=(Site.LINKEDIN,)),
        )


@pytest.mark.parametrize(
    "failure",
    [
        requests_exceptions.Timeout("timed out"),
        requests_exceptions.ConnectionError("connection failed"),
        requests_exceptions.RetryError("retries exhausted"),
    ],
    ids=("timeout", "connection", "retry"),
)
def test_linkedin_targeted_detail_translates_retryable_transport_failures(
    failure: Exception,
) -> None:
    class Session:
        headers: dict[str, str] = {}

        def get(self, url, *, timeout):
            del url, timeout
            raise failure

    scraper = LinkedIn(user_agent="jobstreaming-offline-contract")
    scraper.session = Session()
    listing = JobPost(
        id="li-42",
        title="Staff Platform Engineer",
        job_url="https://www.linkedin.com/jobs/view/42",
    )

    with pytest.raises(TransientNetworkError) as raised:
        scraper.fetch_job_detail(
            listing,
            SearchRequest(site_type=(Site.LINKEDIN,)),
        )

    assert raised.value.retryable is True
    assert type(failure).__name__ in str(raised.value)


@pytest.mark.parametrize(
    "failure",
    [
        requests_exceptions.SSLError("certificate failed"),
        requests_exceptions.ProxyError("proxy failed"),
        requests_exceptions.InvalidProxyURL("invalid proxy"),
    ],
    ids=("tls", "proxy", "invalid-proxy"),
)
def test_linkedin_targeted_detail_translates_transport_configuration_failures(
    failure: Exception,
) -> None:
    class Session:
        headers: dict[str, str] = {}

        def get(self, url, *, timeout):
            del url, timeout
            raise failure

    scraper = LinkedIn(user_agent="jobstreaming-offline-contract")
    scraper.session = Session()
    listing = JobPost(
        id="li-42",
        title="Staff Platform Engineer",
        job_url="https://www.linkedin.com/jobs/view/42",
    )

    with pytest.raises(AuthenticationConfigurationError) as raised:
        scraper.fetch_job_detail(
            listing,
            SearchRequest(site_type=(Site.LINKEDIN,)),
        )

    assert raised.value.retryable is False
    assert type(failure).__name__ in str(raised.value)


def test_linkedin_search_retains_the_listing_when_detail_fetch_fails() -> None:
    cards = _linkedin_cards(42)

    class Session:
        headers: dict[str, str] = {}

        def get(self, url, *, params=None, timeout=None):
            del timeout
            if params is not None:
                return _Response(text=cards)
            return _Response(status_code=429, headers={"Retry-After": "2"})

    request = SearchRequest(
        site_type=(Site.LINKEDIN,),
        linkedin_fetch_description=True,
        results_wanted=1,
        max_pages=1,
    )
    scraper = LinkedIn(user_agent="jobstreaming-offline-contract")
    scraper.session = Session()

    response = scraper.scrape(request)

    assert [job.id for job in response.jobs] == ["li-42"]
    assert response.jobs[0].description is None


def test_glassdoor_offline_fixture_preserves_direct_salary_and_filters(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    graph_payload = json.loads((FIXTURES / "glassdoor_jobs.json").read_text())
    posted_payloads: list[str] = []

    class Session:
        headers: dict[str, str] = {}

        def get(self, url, **kwargs):
            del kwargs
            assert "findPopularLocationAjax" in url
            return _Response(payload=[{"locationId": 7, "locationType": "C"}])

        def post(self, url, *, data=None, **kwargs):
            del url, kwargs
            posted_payloads.append(data)
            return _Response(payload=graph_payload)

    session = Session()
    monkeypatch.setattr(
        "jobstreaming.glassdoor.create_session",
        lambda **kwargs: session,
    )
    monkeypatch.setattr(
        Glassdoor,
        "_fetch_job_description",
        lambda self, job_id: f"Contact role-{job_id}@example.test",
    )
    scraper = Glassdoor(csrf_token="configured-for-offline-test")
    response = scraper.scrape(
        SearchRequest(
            site_type=(Site.GLASSDOOR,),
            search_term="engineer",
            location="Madrid",
            easy_apply=True,
            hours_old=25,
            job_type=JobType.FULL_TIME,
            results_wanted=1,
            max_pages=1,
        )
    )

    assert len(response.jobs) == 1
    job = response.jobs[0]
    assert job.id == "gd-9001"
    assert job.compensation is not None
    assert job.compensation.interval is CompensationInterval.YEARLY
    assert job.compensation.currency == "EUR"
    assert job.salary_source is not None
    assert job.salary_source.value == "direct_data"
    assert job.emails == ("role-9001@example.test",)
    variables = json.loads(posted_payloads[0])[0]["variables"]
    assert variables["locationType"] == "CITY"
    assert variables["fromage"] == 2
    assert variables["filterParams"] == [
        {"filterKey": "applicationType", "values": "1"},
        {"filterKey": "fromAge", "values": "2"},
        {"filterKey": "jobType", "values": JobType.FULL_TIME.canonical},
    ]


def _adapter_checkpoint(site: Site, state: dict[str, object]):
    from jobstreaming import AdapterCheckpoint

    return AdapterCheckpoint(site=site, state=state)
