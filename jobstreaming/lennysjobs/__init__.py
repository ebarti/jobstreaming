"""Listing-only adapter for Lenny's Jobs."""

from __future__ import annotations

import os
from contextlib import suppress
from datetime import datetime
from time import time
from urllib.parse import quote

from pydantic import ValidationError

from jobstreaming.events import SiteTerminationReason
from jobstreaming.exception import (
    AdapterFailureError,
    AuthenticationConfigurationError,
    error_for_http_status,
)
from jobstreaming.lennysjobs.native import (
    BOARD,
    LOCATION_FACET,
    PAGE_SIZE,
    check_page,
    native_request,
    scope,
    select_location,
)
from jobstreaming.model import (
    AdapterCapabilities,
    Compensation,
    CompensationInterval,
    JobPost,
    JobResponse,
    JobType,
    Location,
    Resumable,
    ResumeGranularity,
    SalarySource,
    Scraper,
    ScraperInput,
    SearchFilter,
    Site,
)
from jobstreaming.runtime import ScrapeContext
from jobstreaming.util import create_session

LISTINGS_URL_ENV = "JOBSTREAMING_LENNYSJOBS_LISTINGS_URL"


def _text(value: object) -> str:
    return value.strip() if isinstance(value, str) else ""


def _listing(hit: dict, remote: bool) -> JobPost:
    key = _text(hit.get("job_id")) or _text(hit.get("objectID"))
    if not key or key in {".", ".."}:
        raise ValueError("missing listing identity")
    compensation = None
    interval = CompensationInterval.get_interval(_text(hit.get("salary_period")))
    if (
        interval
        and hit.get("salary_currency")
        and (hit.get("salary_min") is not None or hit.get("salary_max") is not None)
    ):
        with suppress(ValidationError):
            compensation = Compensation(
                interval=interval,
                currency=hit["salary_currency"],
                min_amount=hit.get("salary_min"),
                max_amount=hit.get("salary_max"),
            )
    job_type = None
    if employment := _text(hit.get("employment_type")):
        with suppress(ValueError):
            job_type = (JobType.from_string(employment.replace("_", " ")),)
    location = _text(hit.get("location"))
    return JobPost(
        id=key,
        is_remote=True if remote else None,
        job_url=f"https://www.lennysjobs.com/job/{quote(key, safe='')}",
        title=_text(hit.get("title")),
        company_name=_text(hit.get("company_name")) or None,
        location=Location(city=location) if location else None,
        company_description=_text(hit.get("business_description_short")) or None,
        job_type=job_type,
        compensation=compensation,
        salary_source=SalarySource.DIRECT_DATA if compensation else None,
    )


def _updated_at(hit: dict) -> float | None:
    try:
        date = datetime.fromisoformat(
            _text(hit.get("updated_at")).replace("Z", "+00:00")
        )
        if date.tzinfo is not None:
            return date.timestamp()
    except (ValueError, OverflowError, OSError):
        pass
    return None


class LennysJobs(Scraper):
    capabilities = AdapterCapabilities(
        filters=frozenset(
            {
                SearchFilter.SEARCH_TERM,
                SearchFilter.LOCATION,
                SearchFilter.IS_REMOTE,
                SearchFilter.HOURS_OLD,
            }
        ),
        resume=Resumable(granularity=ResumeGranularity.PAGE, cursor_schema_version=2),
    )

    def __init__(
        self,
        proxies: list[str] | str | None = None,
        ca_cert: str | None = None,
        user_agent: str | None = None,
        *,
        listings_url: str | None = None,
    ):
        super().__init__(
            Site.LENNYSJOBS, proxies=proxies, ca_cert=ca_cert, user_agent=user_agent
        )
        self.listings_url = listings_url or os.environ.get(LISTINGS_URL_ENV)
        self.session = self.track_transport(
            create_session(proxies=proxies, ca_cert=ca_cert, is_tls=False)
        )
        if user_agent:
            self.session.headers["User-Agent"] = user_agent

    def _search(self, payload: list[dict], timeout: float) -> dict:
        response = self.session.post(
            self.listings_url, json=payload, timeout=timeout, allow_redirects=False
        )
        if not 200 <= response.status_code < 300:
            raise error_for_http_status(
                BOARD,
                response.status_code,
                retry_after=response.headers.get("Retry-After"),
            )
        try:
            results = response.json()["results"]
            if isinstance(results, list) and results and isinstance(results[0], dict):
                return results[0]
        except (ValueError, KeyError, TypeError):
            pass
        raise AdapterFailureError(f"{BOARD} returned an unreadable search response")

    def scrape(
        self, scraper_input: ScraperInput, context: ScrapeContext | None = None
    ) -> JobResponse:
        if not self.listings_url:
            raise AuthenticationConfigurationError(
                f"{BOARD} requires {LISTINGS_URL_ENV}"
            )
        context = ScrapeContext.local(self.site, scraper_input, context)
        query, filters = scope((scraper_input.search_term or "").strip())
        state = context.resume_state
        page, raw_before = int(state.get("page", 0)), int(state.get("raw_before", 0))
        cutoff = state.get("updated_since")
        if cutoff is None and scraper_input.hours_old:
            cutoff = int(time()) - scraper_input.hours_old * 3600
        place = (scraper_input.location or "").strip()
        if not place and scraper_input.is_remote:
            place = "Remote"
        location = state.get("location_value")
        if place and location is None and context.should_continue:
            suggestions = self._search(
                native_request(
                    query,
                    filters,
                    page=0,
                    hitsPerPage=0,
                    facetName=LOCATION_FACET,
                    facetQuery=place.split(",")[0].strip(),
                    maxFacetHits=10,
                ),
                scraper_input.request_timeout,
            )
            location = select_location(
                place, bool(scraper_input.is_remote), suggestions
            )
            if location is None:
                context.emit_warning(
                    f"{BOARD} has no matching location for the requested search"
                )
                context.mark_termination(SiteTerminationReason.SOURCE_EXHAUSTED)
                return JobResponse(jobs=())
        if location:
            filters.append([f"{LOCATION_FACET}:{location}"])

        cursor = {
            "page": page,
            "raw_before": raw_before,
            "location_value": location,
            "updated_since": cutoff,
        }
        if context.should_continue and (location or cutoff is not None) and not state:
            context.emit_progress(
                cursor, completed_units=page, raw_items_seen=raw_before, has_more=True
            )
        jobs = []
        warned_about_date = False
        while context.should_continue and page < scraper_input.max_pages:
            data = self._search(
                native_request(query, filters, page=page, hitsPerPage=PAGE_SIZE),
                scraper_input.request_timeout,
            )
            check_page(data, query, filters, page)
            for hit in data["hits"]:
                if not context.should_continue:
                    break
                try:
                    if not isinstance(hit, dict):
                        raise ValueError
                    job = _listing(hit, bool(scraper_input.is_remote))
                except ValueError:
                    context.emit_warning(f"Skipped a malformed {BOARD} listing")
                    continue
                if cutoff is not None:
                    updated = _updated_at(hit)
                    if updated is None:
                        if not warned_about_date:
                            context.emit_warning(
                                f"{BOARD} excluded listings without a usable update date from the requested time window"
                            )
                            warned_about_date = True
                        continue
                    if updated < cutoff:
                        continue
                # Replay an interrupted page. Stable job IDs deduplicate acknowledged cards.
                if context.emit_job(job, cursor):
                    jobs.append(job)
            if not context.should_continue:
                break
            raw_before += len(data["hits"])
            page += 1
            has_more = page < data["nbPages"]
            if not has_more:
                total = data.get("nbHits")
                reason = (
                    SiteTerminationReason.PROVIDER_LIMIT
                    if type(total) is int and total > raw_before
                    else SiteTerminationReason.SOURCE_EXHAUSTED
                )
                context.mark_termination(reason)
            elif page >= scraper_input.max_pages:
                context.mark_termination(SiteTerminationReason.PAGE_LIMIT)
            cursor = {**cursor, "page": page, "raw_before": raw_before}
            context.emit_progress(
                cursor,
                completed_units=page,
                raw_items_seen=raw_before,
                has_more=has_more,
            )
        return JobResponse(jobs=tuple(jobs))
