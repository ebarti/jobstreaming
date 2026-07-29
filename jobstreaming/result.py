from __future__ import annotations

from jobstreaming.model import (
    AdapterIdentifier,
    DescriptionSalaryPolicy,
    JobPost,
    SalaryConfidence,
    SalaryProvenance,
    SalarySource,
    SearchRequest,
)
from jobstreaming.salary import (
    annualize_compensation,
    currency_hint_for_country,
    infer_salary_from_text,
)


def _canonical_salary_provenance(
    source: SalarySource,
    provenance: SalaryProvenance | None,
) -> SalaryProvenance:
    return SalaryProvenance(
        source=source,
        confidence=(
            SalaryConfidence.HIGH
            if source is SalarySource.DIRECT_DATA
            else SalaryConfidence.MEDIUM
        ),
        evidence=(
            provenance.evidence
            if source is SalarySource.DESCRIPTION and provenance is not None
            else None
        ),
    )


def normalize_job(job: JobPost, request: SearchRequest) -> JobPost:
    compensation = job.compensation
    source = job.salary_source
    provenance = job.salary_provenance

    if compensation is not None:
        source = source or SalarySource.DIRECT_DATA
        provenance = _canonical_salary_provenance(source, provenance)
        if request.enforce_annual_salary:
            compensation = annualize_compensation(compensation)
    elif (
        request.description_salary_policy is DescriptionSalaryPolicy.CONSERVATIVE
        and job.description
    ):
        inference = infer_salary_from_text(
            job.description,
            currency_hint=currency_hint_for_country(request.country),
        )
        if inference is not None:
            compensation = (
                annualize_compensation(inference.compensation)
                if request.enforce_annual_salary
                else inference.compensation
            )
            source = SalarySource.DESCRIPTION
            provenance = inference.provenance

    if (
        compensation is job.compensation
        and source is job.salary_source
        and provenance is job.salary_provenance
    ):
        return job
    return job.model_copy(
        update={
            "compensation": compensation,
            "salary_source": source,
            "salary_provenance": provenance,
        }
    )


def job_to_row(site: AdapterIdentifier, job: JobPost) -> dict[str, object]:
    data = job.model_dump(mode="python")
    compensation = data.pop("compensation", None)
    provenance = data.pop("salary_provenance", None)
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
    data["salary_confidence"] = provenance["confidence"].value if provenance else None
    data["salary_evidence"] = provenance["evidence"] if provenance else None
    data["interval"] = compensation["interval"].value if compensation else None
    data["min_amount"] = compensation["min_amount"] if compensation else None
    data["max_amount"] = compensation["max_amount"] if compensation else None
    data["currency"] = compensation["currency"] if compensation else None
    return data
