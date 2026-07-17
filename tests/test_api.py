from __future__ import annotations

import pytest

from jobstreaming import (
    AdapterRegistry,
    Compensation,
    CompensationInterval,
    JobPost,
    JobResponse,
    Scraper,
    Site,
    build_search_request,
    scrape_jobs,
    stream_jobs,
)
from jobstreaming.util import desired_order


class _WorkingAdapter(Scraper):
    def __init__(self, **_: object) -> None:
        super().__init__(Site.INDEED)

    def scrape(self, request, context=None) -> JobResponse:
        job = JobPost(
            id="in-1",
            title="Engineer",
            company_name="Acme",
            job_url="https://example.test/jobs/1",
            compensation=Compensation(
                interval=CompensationInterval.HOURLY,
                min_amount=20,
                max_amount=30,
                currency="USD",
            ),
        )
        if context is not None:
            context.emit_job(job, {"page": 1})
        return JobResponse(jobs=[job])


class _BrokenAdapter(Scraper):
    def __init__(self, **_: object) -> None:
        super().__init__(Site.LINKEDIN)

    def scrape(self, request, context=None) -> JobResponse:
        raise RuntimeError("blocked")


def _registry() -> AdapterRegistry:
    registry = AdapterRegistry()
    registry.register(Site.INDEED, _WorkingAdapter)
    registry.register(Site.LINKEDIN, _BrokenAdapter)
    return registry


def test_batch_api_returns_partial_results_with_a_stable_schema() -> None:
    frame = scrape_jobs(
        site_name=["indeed", "linkedin"],
        results_wanted=1,
        registry=_registry(),
        enforce_annual_salary=True,
        max_retries=0,
    )

    assert list(frame.columns) == desired_order
    assert frame["id"].tolist() == ["in-1"]
    assert frame.loc[0, "interval"] == "yearly"
    assert frame.loc[0, "min_amount"] == 41_600


def test_batch_api_can_fail_strictly_after_preserving_site_isolation() -> None:
    with pytest.raises(RuntimeError, match="linkedin failed"):
        scrape_jobs(
            site_name=["indeed", "linkedin"],
            results_wanted=1,
            registry=_registry(),
            raise_on_error=True,
            max_retries=0,
        )


def test_stream_jobs_exposes_the_simple_job_only_interface() -> None:
    jobs = list(
        stream_jobs(
            site_name=["indeed", "linkedin"],
            results_wanted=1,
            registry=_registry(),
            max_retries=0,
        )
    )

    assert [job.id for job in jobs] == ["in-1"]


def test_request_builder_copies_caller_owned_collections() -> None:
    company_ids = [1, 2]
    request = build_search_request(
        site_name="linkedin", linkedin_company_ids=company_ids
    )
    company_ids.append(3)

    assert request.linkedin_company_ids == (1, 2)
