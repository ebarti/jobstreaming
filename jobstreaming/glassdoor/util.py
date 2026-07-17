from jobstreaming.model import (
    Compensation,
    CompensationInterval,
    Country,
    JobType,
    Location,
)


def parse_compensation(data: dict) -> Compensation | None:
    pay_period = data.get("payPeriod")
    adjusted_pay = data.get("payPeriodAdjustedPay")
    currency = data.get("payCurrency") or "USD"
    if not pay_period or not adjusted_pay:
        return None

    interval = CompensationInterval.get_interval(pay_period)
    if interval is None:
        return None
    min_amount = adjusted_pay.get("p10")
    max_amount = adjusted_pay.get("p90")
    if min_amount is None and max_amount is None:
        return None
    return Compensation(
        interval=interval,
        min_amount=float(min_amount) if min_amount is not None else None,
        max_amount=float(max_amount) if max_amount is not None else None,
        currency=currency,
    )


def get_job_type_enum(job_type_str: str) -> list[JobType] | None:
    for job_type in JobType:
        if job_type_str in job_type.value:
            return [job_type]


def parse_location(
    location_name: str, country: Country | str | None = None
) -> Location | None:
    if not location_name or location_name == "Remote":
        return
    city, _, state = location_name.partition(", ")
    return Location(city=city, state=state or None, country=country)


def get_cursor_for_page(pagination_cursors, page_num):
    for cursor_data in pagination_cursors or ():
        if cursor_data.get("pageNumber") == page_num:
            return cursor_data.get("cursor")
    return None
