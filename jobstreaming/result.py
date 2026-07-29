from __future__ import annotations

from collections.abc import Iterable

import pandas as pd

from jobstreaming.model import (
    AdapterIdentifier,
    Compensation,
    CompensationInterval,
    Country,
    JobPost,
    SalarySource,
    SearchRequest,
)
from jobstreaming.util import desired_order, extract_salary

_ANNUAL_FACTORS = {
    CompensationInterval.HOURLY: 2_080,
    CompensationInterval.DAILY: 260,
    CompensationInterval.WEEKLY: 52,
    CompensationInterval.MONTHLY: 12,
    CompensationInterval.YEARLY: 1,
}


def normalize_job(job: JobPost, request: SearchRequest) -> JobPost:
    compensation = job.compensation
    source = job.salary_source

    if compensation is not None:
        source = source or SalarySource.DIRECT_DATA
        if request.enforce_annual_salary:
            factor = _ANNUAL_FACTORS[compensation.interval]
            compensation = Compensation(
                interval=CompensationInterval.YEARLY,
                min_amount=(
                    compensation.min_amount * factor
                    if compensation.min_amount is not None
                    else None
                ),
                max_amount=(
                    compensation.max_amount * factor
                    if compensation.max_amount is not None
                    else None
                ),
                currency=compensation.currency,
            )
    elif request.country is Country.USA and job.description:
        interval, minimum, maximum, currency = extract_salary(
            job.description,
            enforce_annual_salary=request.enforce_annual_salary,
        )
        if interval and currency and (minimum is not None or maximum is not None):
            compensation = Compensation(
                interval=CompensationInterval(interval),
                min_amount=minimum,
                max_amount=maximum,
                currency=currency,
            )
            source = SalarySource.DESCRIPTION

    if compensation is job.compensation and source is job.salary_source:
        return job
    return job.model_copy(
        update={"compensation": compensation, "salary_source": source}
    )


def job_to_row(site: AdapterIdentifier, job: JobPost) -> dict[str, object]:
    data = job.model_dump(mode="python")
    compensation = data.pop("compensation", None)
    data["site"] = site.value
    data["company"] = data.pop("company_name")
    location = job.location
    data["location"] = location.display_location() if location else None
    data["job_type"] = (
        ", ".join(job_type.canonical for job_type in job.job_type)
        if job.job_type
        else None
    )
    data["emails"] = ", ".join(job.emails) if job.emails else None
    data["skills"] = ", ".join(job.skills) if job.skills else None
    data["salary_source"] = job.salary_source.value if job.salary_source else None
    data["interval"] = compensation["interval"].value if compensation else None
    data["min_amount"] = compensation["min_amount"] if compensation else None
    data["max_amount"] = compensation["max_amount"] if compensation else None
    data["currency"] = compensation["currency"] if compensation else None
    return data


def jobs_to_dataframe(
    jobs: Iterable[tuple[AdapterIdentifier, JobPost]], request: SearchRequest
) -> pd.DataFrame:
    rows = [job_to_row(site, normalize_job(job, request)) for site, job in jobs]
    frame = pd.DataFrame.from_records(rows)
    for column in desired_order:
        if column not in frame.columns:
            frame[column] = None
    frame = frame[desired_order]
    if frame.empty:
        return frame
    return frame.sort_values(
        by=["site", "date_posted"],
        ascending=[True, False],
        na_position="last",
        kind="stable",
    ).reset_index(drop=True)
