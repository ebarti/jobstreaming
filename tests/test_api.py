from __future__ import annotations

import pytest

from jobstreaming import (
    AdapterRegistry,
    Compensation,
    CompensationInterval,
    DetailFetchUnsupportedError,
    JobPost,
    JobResponse,
    Scraper,
    SearchFailedError,
    Site,
    build_search_request,
    fetch_job_detail,
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
    assert frame["site"].tolist() == ["indeed"]
    assert frame.loc[0, "interval"] == "yearly"
    assert frame.loc[0, "min_amount"] == 41_600


def test_batch_api_can_fail_strictly_after_preserving_site_isolation() -> None:
    with pytest.raises(SearchFailedError, match="linkedin failed") as raised:
        scrape_jobs(
            site_name=["indeed", "linkedin"],
            results_wanted=1,
            registry=_registry(),
            raise_on_error=True,
            max_retries=0,
        )

    assert isinstance(raised.value, RuntimeError)
    assert raised.value.outcome.total_jobs == 1
    assert raised.value.outcome.total_failures == 1
    assert raised.value.jobs[0].site is Site.INDEED


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


def test_targeted_detail_api_uses_the_selected_adapter_and_closes_it() -> None:
    instances: list[TargetedDetailAdapter] = []

    class TargetedDetailAdapter(Scraper):
        def __init__(self, **_: object) -> None:
            super().__init__(Site.LINKEDIN)
            self.closed = False
            instances.append(self)

        def scrape(self, request, context=None) -> JobResponse:
            raise AssertionError("targeted detail must not start a search")

        def fetch_job_detail(self, job, request):
            assert request.site_type == (Site.LINKEDIN,)
            assert request.request_timeout == 7
            return job.model_copy(update={"description": "Verified detail"})

        def close(self) -> None:
            self.closed = True
            super().close()

    registry = AdapterRegistry()
    registry.register(Site.LINKEDIN, TargetedDetailAdapter)
    listing = JobPost(
        id="li-42",
        title="Engineer",
        job_url="https://www.linkedin.com/jobs/view/42",
    )

    detailed = fetch_job_detail(
        Site.LINKEDIN,
        listing,
        registry=registry,
        request_timeout=7,
    )

    assert detailed is not None
    assert detailed.description == "Verified detail"
    assert len(instances) == 1
    assert instances[0].closed is True


def test_targeted_detail_api_rejects_an_adapter_without_the_capability() -> None:
    instances: list[UnsupportedDetailAdapter] = []

    class UnsupportedDetailAdapter(Scraper):
        def __init__(self, **_: object) -> None:
            super().__init__(Site.INDEED)
            self.closed = False
            instances.append(self)

        def scrape(self, request, context=None) -> JobResponse:
            del request, context
            return JobResponse()

        def close(self) -> None:
            self.closed = True
            super().close()

    registry = AdapterRegistry()
    registry.register(Site.INDEED, UnsupportedDetailAdapter)
    listing = JobPost(
        id="in-42",
        title="Engineer",
        job_url="https://example.test/jobs/42",
    )

    with pytest.raises(DetailFetchUnsupportedError, match="indeed"):
        fetch_job_detail(Site.INDEED, listing, registry=registry)

    assert len(instances) == 1
    assert instances[0].closed is True


def test_targeted_detail_api_closes_the_adapter_when_fetching_fails() -> None:
    instances: list[FailingDetailAdapter] = []

    class FailingDetailAdapter(Scraper):
        def __init__(self, **_: object) -> None:
            super().__init__(Site.LINKEDIN)
            self.closed = False
            instances.append(self)

        def scrape(self, request, context=None) -> JobResponse:
            raise AssertionError("targeted detail must not start a search")

        def fetch_job_detail(self, job, request):
            del job, request
            raise RuntimeError("detail failed")

        def close(self) -> None:
            self.closed = True
            super().close()

    registry = AdapterRegistry()
    registry.register(Site.LINKEDIN, FailingDetailAdapter)
    listing = JobPost(
        id="li-42",
        title="Engineer",
        job_url="https://www.linkedin.com/jobs/view/42",
    )

    with pytest.raises(RuntimeError, match="detail failed"):
        fetch_job_detail(Site.LINKEDIN, listing, registry=registry)

    assert len(instances) == 1
    assert instances[0].closed is True
