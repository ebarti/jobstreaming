from __future__ import annotations

import pytest
from pydantic import ValidationError

from jobstreaming import (
    Compensation,
    CompensationInterval,
    JobPost,
    ProgressPhase,
    ProgressUnit,
    ProviderProgress,
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


def test_provider_progress_is_typed_immutable_and_explicit_about_unknowns() -> None:
    progress = ProviderProgress(
        phase=ProgressPhase.SEARCH,
        unit=ProgressUnit.PAGE,
        completed_units=2,
        total_units=None,
        raw_items_seen=None,
        jobs_emitted=3,
        has_more=None,
    )

    assert progress.model_dump(mode="json") == {
        "phase": "search",
        "unit": "page",
        "completed_units": 2,
        "total_units": None,
        "raw_items_seen": None,
        "jobs_emitted": 3,
        "has_more": None,
    }
    with pytest.raises(ValidationError):
        progress.completed_units = 3  # type: ignore[misc]


def test_provider_progress_rejects_incoherent_cumulative_counts() -> None:
    with pytest.raises(ValidationError, match="completed_units"):
        ProviderProgress(
            phase=ProgressPhase.SEARCH,
            unit=ProgressUnit.PAGE,
            completed_units=3,
            total_units=2,
            raw_items_seen=10,
            jobs_emitted=2,
        )

    with pytest.raises(ValidationError, match="jobs_emitted"):
        ProviderProgress(
            phase=ProgressPhase.SEARCH,
            unit=ProgressUnit.PAGE,
            completed_units=1,
            raw_items_seen=1,
            jobs_emitted=2,
        )
