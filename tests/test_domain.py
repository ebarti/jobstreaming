from __future__ import annotations

import pytest
from pydantic import ValidationError

from jobstreaming import (
    Compensation,
    CompensationInterval,
    JobPost,
    SearchRequest,
    Site,
)


def test_search_request_rejects_invalid_ranges() -> None:
    with pytest.raises(ValidationError):
        SearchRequest(site_type=(Site.INDEED,), results_wanted=-1)

    with pytest.raises(ValidationError):
        SearchRequest(site_type=(Site.INDEED,), offset=-1)

    with pytest.raises(ValidationError):
        SearchRequest(site_type=(Site.INDEED,), request_timeout=0)


def test_compensation_requires_a_valid_money_range() -> None:
    with pytest.raises(ValidationError):
        Compensation(interval=CompensationInterval.YEARLY, currency="USD")

    with pytest.raises(ValidationError):
        Compensation(
            interval=CompensationInterval.YEARLY,
            min_amount=200_000,
            max_amount=100_000,
            currency="USD",
        )


def test_domain_values_are_immutable() -> None:
    job = JobPost(
        id="in-1",
        title="Engineer",
        company_name="Acme",
        job_url="https://example.test/jobs/1",
    )

    with pytest.raises(ValidationError):
        job.title = "Changed"  # type: ignore[misc]


def test_request_fingerprint_is_stable() -> None:
    first = SearchRequest(
        site_type=(Site.INDEED, Site.LINKEDIN),
        search_term="engineer",
        location="Madrid",
    )
    second = SearchRequest.model_validate(first.model_dump())

    assert first.fingerprint() == second.fingerprint()
