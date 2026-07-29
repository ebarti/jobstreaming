from __future__ import annotations

from collections.abc import Iterator

import jobstreaming


def build_request() -> jobstreaming.SearchRequest:
    return jobstreaming.SearchRequest(
        site_type=(jobstreaming.Site.GOOGLE,),
        results_wanted=1,
        description_salary_policy=jobstreaming.DescriptionSalaryPolicy.CONSERVATIVE,
    )


def stream(request: jobstreaming.SearchRequest) -> Iterator[jobstreaming.JobPost]:
    return jobstreaming.stream_jobs(request)


def batch_api_remains_visible() -> object:
    return jobstreaming.scrape_jobs(
        site_name=jobstreaming.Site.GOOGLE,
        results_wanted=1,
    )


def salary_provenance() -> jobstreaming.SalaryProvenance:
    return jobstreaming.SalaryProvenance(
        source=jobstreaming.SalarySource.DESCRIPTION,
        confidence=jobstreaming.SalaryConfidence.MEDIUM,
        evidence="USD 80,000 - 100,000 per year",
    )
