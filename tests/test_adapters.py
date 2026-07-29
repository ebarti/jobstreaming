from __future__ import annotations

import pytest
from bs4 import BeautifulSoup

from jobstreaming import (
    AdapterRegistry,
    CompensationInterval,
    Country,
    DescriptionFormat,
    JobEvent,
    JobPost,
    JobType,
    MemoryCheckpointStore,
    ScrapeContext,
    SearchRequest,
    Site,
    stream_search,
)
from jobstreaming.bayt import BaytScraper
from jobstreaming.bdjobs import BDJobs
from jobstreaming.bdjobs.util import parse_location as parse_bdjobs_location
from jobstreaming.glassdoor import Glassdoor
from jobstreaming.glassdoor.util import (
    parse_compensation as parse_glassdoor_compensation,
)
from jobstreaming.google import Google
from jobstreaming.indeed.util import get_compensation as parse_indeed_compensation
from jobstreaming.linkedin import LinkedIn
from jobstreaming.naukri import Naukri
from jobstreaming.ziprecruiter import ZipRecruiter


def test_glassdoor_accepts_a_one_sided_salary_range() -> None:
    compensation = parse_glassdoor_compensation(
        {
            "payPeriod": "ANNUAL",
            "payCurrency": "usd",
            "payPeriodAdjustedPay": {"p10": 100_000, "p90": None},
        }
    )

    assert compensation is not None
    assert compensation.interval is CompensationInterval.YEARLY
    assert compensation.min_amount == 100_000
    assert compensation.max_amount is None
    assert compensation.currency == "USD"


def test_glassdoor_does_not_turn_a_missing_location_into_remote() -> None:
    scraper = Glassdoor()

    with pytest.raises(ValueError, match="requires location"):
        scraper._get_location(None, False)


def test_indeed_compensation_handles_estimates_and_missing_currency() -> None:
    estimate = parse_indeed_compensation(
        {
            "estimated": {
                "currencyCode": "USD",
                "baseSalary": {
                    "unitOfWork": "YEAR",
                    "range": {"min": 90_000, "max": 120_000},
                },
            },
            "baseSalary": None,
        }
    )
    missing_currency = parse_indeed_compensation(
        {
            "baseSalary": {
                "unitOfWork": "YEAR",
                "range": {"min": 90_000, "max": 120_000},
            }
        }
    )

    assert estimate is not None
    assert estimate.min_amount == 90_000
    assert missing_currency is None


def test_google_parser_handles_relative_hours_and_non_us_locations() -> None:
    scraper = Google()
    info = [None] * 29
    info[0] = "Platform Engineer"
    info[1] = "Acme"
    info[2] = "Madrid, Spain"
    info[3] = [["https://example.test/google/42"]]
    info[12] = "2 hours ago"
    info[19] = "Remote role. Contact jobs@example.test"
    info[28] = "42"

    job = scraper._parse_job(info)

    assert job is not None
    assert job.location is not None
    assert job.location.city == "Madrid"
    assert job.location.state is None
    assert job.location.country == "Spain"
    assert job.is_remote is True
    assert job.emails == ("jobs@example.test",)


def test_bayt_uses_stable_ids_and_url_joining() -> None:
    scraper = BaytScraper()
    card = BeautifulSoup(
        """
        <li data-js-job>
          <h2><a href="/en/job/123">Engineer</a></h2>
          <div class="t-nowrap p10l"><span>Acme</span></div>
          <div class="t-mute t-small">Dubai, UAE</div>
        </li>
        """,
        "html.parser",
    ).li

    job = scraper._extract_job_info(card)

    assert job is not None
    assert job.id == "bayt-b3b7a084821a56d96ba3c9f3b90795f9"
    assert job.job_url == "https://www.bayt.com/en/job/123"
    assert job.location is not None
    assert job.location.country == "UAE"


def test_naukri_always_uses_its_own_description_and_salary_interval() -> None:
    scraper = Naukri()
    scraper.scraper_input = SearchRequest(
        site_type=(Site.NAUKRI,),
        search_term="engineer",
        description_format=DescriptionFormat.PLAIN,
    )
    job = scraper._process_job(
        {
            "jobId": "123",
            "title": "Engineer",
            "companyName": "Acme",
            "jdURL": "/job/123",
            "jobDescription": "<p>Build reliable systems.</p>",
            "placeholders": [
                {"type": "location", "label": "Bengaluru, Karnataka"},
                {"type": "salary", "label": "12-16 Lacs P.A."},
            ],
        },
        "123",
    )

    assert job.description == "Build reliable systems."
    assert job.compensation is not None
    assert job.compensation.interval is CompensationInterval.YEARLY
    assert job.compensation.min_amount == 1_200_000
    assert job.work_from_home_type is None


def test_ziprecruiter_does_not_label_every_non_us_job_as_canadian(
    monkeypatch,
) -> None:
    scraper = ZipRecruiter()
    scraper.scraper_input = SearchRequest(
        site_type=(Site.ZIP_RECRUITER,),
        description_format=DescriptionFormat.PLAIN,
    )
    monkeypatch.setattr(scraper, "_get_descr", lambda _: (None, None))

    job = scraper._process_job(
        {
            "listing_key": "abc",
            "name": "Engineer",
            "job_description": "<p>Build systems</p>",
            "hiring_company": {"name": "Acme"},
            "job_country": "GB",
            "job_city": "London",
            "employment_type": "full_time",
            "compensation_interval": "YEAR",
            "compensation_min": 80_000,
            "compensation_max": 100_000,
            "compensation_currency": "GBP",
        }
    )

    assert job is not None
    assert job.location is not None
    assert job.location.country == "GB"
    assert job.job_type == (JobType.FULL_TIME,)
    assert job.compensation is not None


def test_linkedin_card_parser_handles_country_names_and_hourly_salary() -> None:
    scraper = LinkedIn()
    scraper.scraper_input = SearchRequest(site_type=(Site.LINKEDIN,))
    card = BeautifulSoup(
        """
        <div class="base-search-card">
          <span class="sr-only">Engineer</span>
          <h4 class="base-search-card__subtitle">Acme</h4>
          <span class="job-search-card__salary-info">$20 - $30 per hour</span>
          <div class="base-search-card__metadata">
            <span class="job-search-card__location">Madrid, Spain</span>
            <time class="job-search-card__listdate" datetime="2026-07-15"></time>
          </div>
        </div>
        """,
        "html.parser",
    ).div

    job = scraper._process_job(card, "123", False)

    assert job.location is not None
    assert job.location.country == "Spain"
    assert job.compensation is not None
    assert job.compensation.interval is CompensationInterval.HOURLY
    assert job.date_posted is not None
    assert job.company_name == "Acme"


def test_ziprecruiter_follows_continuations_until_the_result_limit(
    monkeypatch,
) -> None:
    scraper = ZipRecruiter()
    scraper.authorization = "configured-for-test"
    scraper.delay = 0
    monkeypatch.setattr(scraper, "_get_cookies", lambda: None)

    def page(request, context, token, skipped, page_state):
        number = int(page_state["page"])
        job = Google()._parse_job(
            [
                f"Job {number}",
                "Acme",
                "Madrid, Spain",
                [[f"https://example.test/zip/{number}"]],
                *([None] * 15),
                "Description",
                *([None] * 8),
                str(number),
            ]
        )
        assert job is not None
        context.emit_job(job, page_state)
        next_token = str(number + 1) if number < 3 else None
        return [job], next_token, skipped

    monkeypatch.setattr(scraper, "_find_jobs_in_page", page)
    response = scraper.scrape(
        SearchRequest(
            site_type=(Site.ZIP_RECRUITER,),
            results_wanted=3,
            max_pages=3,
        )
    )

    assert len(response.jobs) == 3


def test_bdjobs_location_does_not_duplicate_country_as_state() -> None:
    location = parse_bdjobs_location("Dhaka, Bangladesh")

    assert location is not None
    assert location.city == "Dhaka"
    assert location.state is None
    assert location.country is Country.BANGLADESH


def test_bdjobs_releases_detail_sessions_after_each_page(monkeypatch) -> None:
    current_page = 0
    detail_sessions = []

    class Response:
        status_code = 200
        text = ""

    class DetailSession:
        def __init__(self) -> None:
            self.closed = False
            self.headers = {}
            detail_sessions.append(self)

        def close(self) -> None:
            self.closed = True

    class BoundedBDJobs(BDJobs):
        delay = 0
        band_delay = 0

        def __init__(self) -> None:
            self.maximum_tracked_transports = 0
            super().__init__()

        def track_transport(self, transport):
            tracked = super().track_transport(transport)
            self.maximum_tracked_transports = max(
                self.maximum_tracked_transports,
                self.tracked_transport_count,
            )
            return tracked

    scraper = BoundedBDJobs()

    def search_page(*args, **kwargs):
        nonlocal current_page
        next_page = int(kwargs.get("params", {}).get("pg", 1))
        if current_page:
            assert all(session.closed for session in detail_sessions)
            assert scraper.tracked_transport_count == 1
        current_page = next_page
        return Response()

    def process_job(card: int) -> JobPost:
        scraper._detail_session()
        return JobPost(
            id=f"{current_page}-{card}",
            title=f"Job {current_page}-{card}",
            job_url=f"https://example.test/bdjobs/{current_page}/{card}",
        )

    monkeypatch.setattr(scraper.session, "get", search_page)
    monkeypatch.setattr(
        "jobstreaming.bdjobs.find_job_listings",
        lambda _: list(range(8)),
    )
    monkeypatch.setattr(
        "jobstreaming.bdjobs.create_session",
        lambda **_: DetailSession(),
    )
    monkeypatch.setattr(scraper, "_process_job", process_job)

    response = scraper.scrape(
        SearchRequest(
            site_type=(Site.BDJOBS,),
            results_wanted=10_000,
            max_pages=40,
            request_timeout=0.1,
        )
    )

    assert len(response.jobs) == 320
    assert 1 < scraper.maximum_tracked_transports <= 9
    assert scraper.tracked_transport_count == 1
    assert len(detail_sessions) >= 40
    assert all(session.closed for session in detail_sessions)
    scraper.close()


def test_ziprecruiter_releases_detail_sessions_after_each_page(monkeypatch) -> None:
    current_page = 0
    detail_sessions = []

    class Response:
        status_code = 200

        def json(self):
            return {
                "jobs": [{"page": current_page, "index": index} for index in range(8)],
                "continue": "next",
            }

    class DetailSession:
        def __init__(self) -> None:
            self.closed = False
            self.headers = {}
            detail_sessions.append(self)

        def close(self) -> None:
            self.closed = True

    class BoundedZipRecruiter(ZipRecruiter):
        def __init__(self) -> None:
            self.maximum_tracked_transports = 0
            super().__init__()

        def track_transport(self, transport):
            tracked = super().track_transport(transport)
            self.maximum_tracked_transports = max(
                self.maximum_tracked_transports,
                self.tracked_transport_count,
            )
            return tracked

    scraper = BoundedZipRecruiter()
    request = SearchRequest(
        site_type=(Site.ZIP_RECRUITER,),
        results_wanted=10_000,
        max_pages=40,
        request_timeout=0.1,
    )
    scraper.scraper_input = request
    context = ScrapeContext(site=Site.ZIP_RECRUITER, request=request)

    monkeypatch.setattr(scraper.session, "get", lambda *_, **__: Response())
    monkeypatch.setattr(
        "jobstreaming.ziprecruiter.create_session",
        lambda **_: DetailSession(),
    )

    def process_job(job: dict) -> JobPost:
        scraper._get_detail_session()
        return JobPost(
            id=f"{job['page']}-{job['index']}",
            title=f"Job {job['page']}-{job['index']}",
            job_url=(
                "https://example.test/ziprecruiter/" f"{job['page']}/{job['index']}"
            ),
        )

    monkeypatch.setattr(scraper, "_process_job", process_job)

    jobs = []
    skipped = 0
    for current_page in range(1, 41):
        page_jobs, _, skipped = scraper._find_jobs_in_page(
            request,
            context,
            None,
            skipped,
            {"page": current_page},
        )
        jobs.extend(page_jobs)
        assert scraper.tracked_transport_count == 1
        assert all(session.closed for session in detail_sessions)

    assert len(jobs) == 320
    assert len(detail_sessions) >= 40
    assert 1 < scraper.maximum_tracked_transports <= 9
    scraper.close()


def test_glassdoor_releases_detail_sessions_after_each_page(monkeypatch) -> None:
    current_page = 0
    detail_sessions = []

    class Response:
        status_code = 200

        def json(self):
            return [
                {
                    "data": {
                        "jobListings": {
                            "jobListings": [
                                {"page": current_page, "index": index}
                                for index in range(8)
                            ],
                            "paginationCursors": [],
                        }
                    }
                }
            ]

    class PrimarySession:
        def __init__(self) -> None:
            self.closed = False

        def post(self, *args, **kwargs):
            return Response()

        def close(self) -> None:
            self.closed = True

    class DetailSession:
        def __init__(self) -> None:
            self.closed = False
            self.headers = {}
            detail_sessions.append(self)

        def close(self) -> None:
            self.closed = True

    class BoundedGlassdoor(Glassdoor):
        def __init__(self) -> None:
            self.maximum_tracked_transports = 0
            super().__init__()

        def track_transport(self, transport):
            tracked = super().track_transport(transport)
            self.maximum_tracked_transports = max(
                self.maximum_tracked_transports,
                self.tracked_transport_count,
            )
            return tracked

    scraper = BoundedGlassdoor()
    scraper.base_url = "https://example.test/"
    scraper.session = scraper.track_transport(PrimarySession())
    request = SearchRequest(
        site_type=(Site.GLASSDOOR,),
        results_wanted=10_000,
        max_pages=40,
        request_timeout=0.1,
    )
    scraper.scraper_input = request
    context = ScrapeContext(site=Site.GLASSDOOR, request=request)

    monkeypatch.setattr(scraper, "_add_payload", lambda *args, **kwargs: "{}")
    monkeypatch.setattr(
        "jobstreaming.glassdoor.get_cursor_for_page",
        lambda *args, **kwargs: "next",
    )
    monkeypatch.setattr(
        "jobstreaming.glassdoor.create_session",
        lambda **_: DetailSession(),
    )

    def process_job(job: dict) -> JobPost:
        scraper._get_detail_session()
        return JobPost(
            id=f"{job['page']}-{job['index']}",
            title=f"Job {job['page']}-{job['index']}",
            job_url=(f"https://example.test/glassdoor/{job['page']}/{job['index']}"),
        )

    monkeypatch.setattr(scraper, "_process_job", process_job)

    jobs = []
    for current_page in range(1, 41):
        page_jobs, _, raw_count = scraper._fetch_jobs_page(
            request,
            1,
            "CITY",
            current_page,
            None,
            context,
            {"page": current_page},
        )
        jobs.extend(page_jobs)
        assert raw_count == 8
        assert scraper.tracked_transport_count == 1
        assert all(session.closed for session in detail_sessions)

    assert len(jobs) == 320
    assert len(detail_sessions) >= 40
    assert 1 < scraper.maximum_tracked_transports <= 9
    scraper.close()


def test_bayt_resume_after_an_acknowledged_page_continues_to_the_next_page() -> None:
    instances = 0

    class PagedBayt(BaytScraper):
        delay = 0
        band_delay = 0

        def __init__(self, **kwargs) -> None:
            nonlocal instances
            super().__init__(**kwargs)
            instances += 1
            self.instance = instances
            self.current_page = 0

        def _fetch_jobs(self, query, page):
            self.current_page = page
            identifiers = (
                ["1"]
                if self.instance == 1 and page == 1
                else ["new", "1"] if page == 1 else ["2"] if page == 2 else []
            )
            return [
                BeautifulSoup(
                    f'<li data-identifier="{identifier}"></li>', "html.parser"
                ).li
                for identifier in identifiers
            ]

        def _extract_job_info(self, job):
            identifier = job["data-identifier"]
            return JobPost(
                id=f"bayt-{identifier}",
                title=f"Job {identifier}",
                job_url=f"https://example.test/bayt/{identifier}",
            )

    registry = AdapterRegistry()
    registry.register(Site.BAYT, PagedBayt)
    request = SearchRequest(site_type=(Site.BAYT,), results_wanted=3, max_pages=2)
    store = MemoryCheckpointStore()

    first_stream = stream_search(request, registry=registry, checkpoint_store=store)
    first = next(event for event in first_stream if isinstance(event, JobEvent))
    assert first.job.id == "bayt-1"
    first_stream.ack(first)
    first_stream.close()

    with stream_search(
        request,
        registry=registry,
        checkpoint_store=store,
        resume=True,
    ) as resumed:
        resumed_jobs = [
            event.job.id for event in resumed if isinstance(event, JobEvent)
        ]

    assert resumed_jobs == ["bayt-new", "bayt-2"]
