from jobstream.model import Compensation, CompensationInterval, JobType
from jobstream.util import get_enum_from_job_type


def get_job_type(attributes: list) -> list[JobType]:
    """
    Parses the attributes to get list of job types
    :param attributes:
    :return: list of JobType
    """
    job_types: list[JobType] = []
    for attribute in attributes:
        label = attribute.get("label") if isinstance(attribute, dict) else None
        if not isinstance(label, str):
            continue
        job_type_str = label.replace("-", "").replace(" ", "").lower()
        job_type = get_enum_from_job_type(job_type_str)
        if job_type:
            job_types.append(job_type)
    return job_types


def get_compensation(compensation: dict) -> Compensation | None:
    """
    Parses the job to get compensation
    :param compensation:
    :return: compensation object
    """
    if not compensation or (
        not compensation.get("baseSalary") and not compensation.get("estimated")
    ):
        return None
    estimated = compensation.get("estimated") or {}
    comp = compensation.get("baseSalary") or estimated.get("baseSalary")
    if not comp:
        return None
    interval = get_compensation_interval(comp.get("unitOfWork"))
    if not interval:
        return None
    range_data = comp.get("range") or {}
    min_range = range_data.get("min")
    max_range = range_data.get("max")
    currency = estimated.get("currencyCode") or compensation.get("currencyCode")
    if (min_range is None and max_range is None) or not currency:
        return None
    return Compensation(
        interval=interval,
        min_amount=int(min_range) if min_range is not None else None,
        max_amount=int(max_range) if max_range is not None else None,
        currency=currency,
    )


def is_job_remote(job: dict, description: str) -> bool:
    """
    Searches the description, location, and attributes to check if job is remote
    """
    remote_keywords = ["remote", "work from home", "wfh"]
    is_remote_in_attributes = any(
        any(
            keyword in str(attribute.get("label", "")).lower()
            for keyword in remote_keywords
        )
        for attribute in job.get("attributes") or ()
        if isinstance(attribute, dict)
    )
    description_text = (description or "").lower()
    is_remote_in_description = any(
        keyword in description_text for keyword in remote_keywords
    )
    formatted = (job.get("location") or {}).get("formatted") or {}
    location_text = formatted.get("long", "") if isinstance(formatted, dict) else ""
    is_remote_in_location = any(
        keyword in location_text.lower() for keyword in remote_keywords
    )
    return is_remote_in_attributes or is_remote_in_description or is_remote_in_location


def get_compensation_interval(interval: str | None) -> CompensationInterval | None:
    if not interval:
        return None
    interval_mapping = {
        "DAY": "DAILY",
        "YEAR": "YEARLY",
        "HOUR": "HOURLY",
        "WEEK": "WEEKLY",
        "MONTH": "MONTHLY",
    }
    mapped_interval = interval_mapping.get(interval.upper())
    if mapped_interval and mapped_interval in CompensationInterval.__members__:
        return CompensationInterval[mapped_interval]
    return None
