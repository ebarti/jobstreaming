from __future__ import annotations

import json
from pathlib import Path

import pytest
from pydantic import ValidationError

from jobstreaming import (
    AdapterCapabilities,
    CompensationInterval,
    Country,
    DescriptionFormat,
    JobType,
    ScrapeContext,
    SearchRequest,
    Site,
    default_registry,
)
from jobstreaming.bdjobs import BDJobs
from jobstreaming.exception import CursorExpiredError
from jobstreaming.glassdoor import Glassdoor
from jobstreaming.google import Google
from tests.adapter_contract import assert_adapter_declaration

FIXTURES = Path(__file__).with_name("fixtures")


@pytest.mark.parametrize("site", tuple(Site), ids=lambda site: site.value)
def test_built_in_adapters_conform_to_the_shared_declaration_contract(
    site: Site,
) -> None:
    registry = default_registry()
    factory = registry._factories[site]

    assert_adapter_declaration(registry, site, factory)


@pytest.mark.parametrize(
    "capabilities",
    [
        AdapterCapabilities.model_construct(
            filters=frozenset({"not_a_request_filter"})
        ),
        AdapterCapabilities.model_construct(
            filters=frozenset(),
            supported_job_types=frozenset({JobType.FULL_TIME}),
        ),
        AdapterCapabilities.model_construct(
            supports_resume=True,
            resume_granularity=None,
        ),
        AdapterCapabilities.model_construct(
            supports_resume=False,
            resume_granularity="page",
        ),
    ],
)
def test_adapter_capability_declarations_reject_incoherent_states(
    capabilities: AdapterCapabilities,
) -> None:
    with pytest.raises(ValidationError):
        AdapterCapabilities.model_validate(capabilities.model_dump())


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
