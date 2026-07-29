from __future__ import annotations

from collections.abc import Callable, Iterator
from pathlib import Path
from threading import Event
from typing import Any

import pandas as pd

from jobstreaming.checkpoint import CheckpointStore, JsonFileCheckpointStore
from jobstreaming.events import ErrorEvent, JobEvent
from jobstreaming.model import (
    AdapterIdentifier,
    AdapterIdentifierInput,
    Country,
    DescriptionFormat,
    JobPost,
    JobType,
    SearchRequest,
    Site,
    parse_adapter_identifier,
)
from jobstreaming.registry import AdapterRegistry, default_registry
from jobstreaming.result import jobs_to_dataframe
from jobstreaming.runtime import AckMode, SearchStream
from jobstreaming.util import create_logger, set_logger_level


def _parse_sites(
    site_name: (
        AdapterIdentifierInput
        | list[AdapterIdentifierInput]
        | tuple[AdapterIdentifierInput, ...]
        | None
    ),
) -> tuple[AdapterIdentifier, ...]:
    if site_name is None:
        return tuple(Site)
    raw_sites = site_name if isinstance(site_name, (list, tuple)) else (site_name,)
    return tuple(parse_adapter_identifier(site) for site in raw_sites)


def build_search_request(
    *,
    site_name: (
        AdapterIdentifierInput
        | list[AdapterIdentifierInput]
        | tuple[AdapterIdentifierInput, ...]
        | None
    ) = None,
    search_term: str | None = None,
    google_search_term: str | None = None,
    location: str | None = None,
    distance: int = 50,
    is_remote: bool = False,
    job_type: str | JobType | None = None,
    easy_apply: bool | None = None,
    results_wanted: int = 15,
    country_indeed: str | Country = Country.USA,
    description_format: str | DescriptionFormat = DescriptionFormat.MARKDOWN,
    linkedin_fetch_description: bool = False,
    linkedin_company_ids: list[int] | tuple[int, ...] | None = None,
    offset: int = 0,
    hours_old: int | None = None,
    enforce_annual_salary: bool = False,
    request_timeout: float = 30,
    max_pages: int = 50,
) -> SearchRequest:
    parsed_job_type = (
        JobType.from_string(job_type) if isinstance(job_type, str) else job_type
    )
    country = (
        Country.from_string(country_indeed)
        if isinstance(country_indeed, str)
        else country_indeed
    )
    parsed_description_format = (
        DescriptionFormat(description_format)
        if isinstance(description_format, str)
        else description_format
    )
    return SearchRequest(
        site_type=_parse_sites(site_name),
        country=country,
        search_term=search_term,
        google_search_term=google_search_term,
        location=location,
        distance=distance,
        is_remote=is_remote,
        job_type=parsed_job_type,
        easy_apply=easy_apply,
        description_format=parsed_description_format,
        linkedin_fetch_description=linkedin_fetch_description,
        results_wanted=results_wanted,
        linkedin_company_ids=(
            tuple(linkedin_company_ids) if linkedin_company_ids is not None else None
        ),
        offset=offset,
        hours_old=hours_old,
        enforce_annual_salary=enforce_annual_salary,
        request_timeout=request_timeout,
        max_pages=max_pages,
    )


def stream_search(
    request: SearchRequest | None = None,
    *,
    registry: AdapterRegistry | None = None,
    checkpoint_store: CheckpointStore | None = None,
    checkpoint_path: str | Path | None = None,
    resume: bool = True,
    proxies: list[str] | str | None = None,
    ca_cert: str | None = None,
    user_agent: str | None = None,
    queue_size: int = 128,
    max_retries: int = 1,
    retry_backoff: float = 0.5,
    ack_mode: str | AckMode = AckMode.IMPLICIT,
    cancel_event: Event | None = None,
    cancel_callback: Callable[[], bool] | None = None,
    **search_options: Any,
) -> SearchStream:
    if request is not None and search_options:
        options = ", ".join(sorted(search_options))
        raise TypeError(
            f"Cannot combine a SearchRequest with legacy search options: {options}"
        )
    if request is None:
        request = build_search_request(**search_options)
    if checkpoint_store is not None and checkpoint_path is not None:
        raise ValueError("Use checkpoint_store or checkpoint_path, not both")
    if checkpoint_path is not None:
        checkpoint_store = JsonFileCheckpointStore(checkpoint_path)
    return SearchStream(
        request,
        registry=registry or default_registry(),
        checkpoint_store=checkpoint_store,
        resume=resume,
        proxies=proxies,
        ca_cert=ca_cert,
        user_agent=user_agent,
        queue_size=queue_size,
        max_retries=max_retries,
        retry_backoff=retry_backoff,
        ack_mode=ack_mode,
        cancel_event=cancel_event,
        cancel_callback=cancel_callback,
    )


def stream_jobs(
    request: SearchRequest | None = None,
    *,
    raise_on_error: bool = False,
    **stream_options: Any,
) -> Iterator[JobPost]:
    with stream_search(request, **stream_options) as stream:
        for event in stream:
            if isinstance(event, JobEvent):
                yield event.job
            elif isinstance(event, ErrorEvent) and raise_on_error:
                raise RuntimeError(
                    f"{event.site.value} failed [{event.code.value}]: "
                    f"{event.error_type}: {event.message}"
                )
            stream.ack(event)


def scrape_jobs(
    site_name: AdapterIdentifierInput | list[AdapterIdentifierInput] | None = None,
    search_term: str | None = None,
    google_search_term: str | None = None,
    location: str | None = None,
    distance: int = 50,
    is_remote: bool = False,
    job_type: str | JobType | None = None,
    easy_apply: bool | None = None,
    results_wanted: int = 15,
    country_indeed: str | Country = Country.USA,
    proxies: list[str] | str | None = None,
    ca_cert: str | None = None,
    description_format: str | DescriptionFormat = DescriptionFormat.MARKDOWN,
    linkedin_fetch_description: bool = False,
    linkedin_company_ids: list[int] | None = None,
    offset: int = 0,
    hours_old: int | None = None,
    enforce_annual_salary: bool = False,
    verbose: int = 0,
    user_agent: str | None = None,
    *,
    request_timeout: float = 30,
    max_pages: int = 50,
    checkpoint_store: CheckpointStore | None = None,
    checkpoint_path: str | Path | None = None,
    resume: bool = True,
    registry: AdapterRegistry | None = None,
    raise_on_error: bool = False,
    max_retries: int = 1,
    retry_backoff: float = 0.5,
    ack_mode: str | AckMode = AckMode.IMPLICIT,
    cancel_event: Event | None = None,
    cancel_callback: Callable[[], bool] | None = None,
) -> pd.DataFrame:
    set_logger_level(verbose)
    request = build_search_request(
        site_name=site_name,
        search_term=search_term,
        google_search_term=google_search_term,
        location=location,
        distance=distance,
        is_remote=is_remote,
        job_type=job_type,
        easy_apply=easy_apply,
        results_wanted=results_wanted,
        country_indeed=country_indeed,
        description_format=description_format,
        linkedin_fetch_description=linkedin_fetch_description,
        linkedin_company_ids=linkedin_company_ids,
        offset=offset,
        hours_old=hours_old,
        enforce_annual_salary=enforce_annual_salary,
        request_timeout=request_timeout,
        max_pages=max_pages,
    )
    jobs: list[tuple[AdapterIdentifier, JobPost]] = []
    errors: list[ErrorEvent] = []
    with stream_search(
        request,
        registry=registry,
        checkpoint_store=checkpoint_store,
        checkpoint_path=checkpoint_path,
        resume=resume,
        proxies=proxies,
        ca_cert=ca_cert,
        user_agent=user_agent,
        max_retries=max_retries,
        retry_backoff=retry_backoff,
        ack_mode=ack_mode,
        cancel_event=cancel_event,
        cancel_callback=cancel_callback,
    ) as stream:
        for event in stream:
            if isinstance(event, JobEvent):
                jobs.append((event.site, event.job))
            elif isinstance(event, ErrorEvent):
                errors.append(event)
                create_logger(event.site.value).error(
                    "[%s] %s: %s",
                    event.code.value,
                    event.error_type,
                    event.message,
                )
            stream.ack(event)

    if errors and raise_on_error:
        first = errors[0]
        raise RuntimeError(
            f"{first.site.value} failed [{first.code.value}]: "
            f"{first.error_type}: {first.message}"
        )
    return jobs_to_dataframe(jobs, request)
