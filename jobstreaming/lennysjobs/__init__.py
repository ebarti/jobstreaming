"""Listing-only adapter for Lenny's Jobs."""

from __future__ import annotations

import json
import os
from collections.abc import Iterator
from contextlib import suppress
from urllib.parse import parse_qs, quote

from bs4 import BeautifulSoup, SoupStrainer
from pydantic import ValidationError

from jobstreaming.events import SiteTerminationReason
from jobstreaming.exception import (
    AdapterFailureError,
    AuthenticationConfigurationError,
    error_for_http_status,
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

BOARD = "Lenny's Jobs"
LISTINGS_URL_ENV = "JOBSTREAMING_LENNYSJOBS_LISTINGS_URL"


def _page_payload(body: str, page: int) -> dict:
    try:
        if body.lstrip().startswith("<"):
            script = BeautifulSoup(
                body,
                "html.parser",
                parse_only=SoupStrainer("script", id="__NEXT_DATA__"),
            ).find("script")
            if script is None or script.string is None:
                raise ValueError
            data = json.loads(script.string)["props"]["pageProps"]["initialJobs"]
        else:
            data = json.loads(body)
    except (KeyError, TypeError, ValueError):
        raise AdapterFailureError(
            f"{BOARD} returned an unreadable listing page"
        ) from None
    if (
        not isinstance(data, dict)
        or not isinstance(data.get("hits"), list)
        or type(data.get("page")) is not int
        or data["page"] != page
        or type(data.get("nbPages")) is not int
        or data["nbPages"] < 0
        or not isinstance(data.get("query"), str)
    ):
        raise AdapterFailureError(
            f"{BOARD} returned invalid listing or pagination metadata"
        )
    return data


def _text(value: object) -> str:
    return value.strip() if isinstance(value, str) else ""


def _listing(hit: dict) -> JobPost:
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
        job_url=f"https://www.lennysjobs.com/job/{quote(key, safe='')}",
        title=_text(hit.get("title")),
        company_name=_text(hit.get("company_name")) or None,
        location=Location(city=location) if location else None,
        company_description=_text(hit.get("business_description_short")) or None,
        job_type=job_type,
        compensation=compensation,
        salary_source=SalarySource.DIRECT_DATA if compensation else None,
    )


def _matching_listings(
    data: dict, query: str, filters: dict[str, str], context: ScrapeContext
) -> Iterator[JobPost]:
    query = query.casefold()
    returned_query = data["query"].strip().casefold()
    if returned_query and returned_query != query:
        raise AdapterFailureError(f"{BOARD} returned results for a different query")
    # The public landing page is a category preview. Only trust filters that
    # the connection reports as applied or that can be checked on each card.
    applied = parse_qs(_text(data.get("params")))
    for name in ("is_remote", "hours_old"):
        if name in filters and applied.get(name) != [filters[name]]:
            raise AdapterFailureError(
                f"{BOARD} listing connection did not confirm the {name} filter"
            )
    location = filters.get("location", "")
    requested_places = {p.strip().casefold() for p in location.split(",") if p.strip()}
    check_location = bool(requested_places) and applied.get("location") != [location]
    for hit in data["hits"]:
        if not context.should_continue:
            return
        try:
            if not isinstance(hit, dict):
                raise ValueError
            job = _listing(hit)
        except ValueError:
            context.emit_warning(f"Skipped a malformed {BOARD} listing")
            continue
        if (
            not returned_query
            and query
            and not any(
                query in field.casefold()
                for field in (job.title, job.company_name or "")
            )
        ):
            continue
        if check_location and not any(
            requested_places.issubset(
                part.strip().casefold() for part in place.split(",")
            )
            for place in _text(hit.get("location")).split(";")
        ):
            continue
        yield job


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
        resume=Resumable(granularity=ResumeGranularity.PAGE),
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

    def scrape(
        self, scraper_input: ScraperInput, context: ScrapeContext | None = None
    ) -> JobResponse:
        if not self.listings_url:
            raise AuthenticationConfigurationError(
                f"{BOARD} requires {LISTINGS_URL_ENV}"
            )
        context = ScrapeContext.local(self.site, scraper_input, context)
        query = (scraper_input.search_term or "").strip()
        filters: dict[str, str] = {}
        if scraper_input.location:
            filters["location"] = scraper_input.location
        if scraper_input.is_remote:
            filters["is_remote"] = "true"
        if scraper_input.hours_old:
            filters["hours_old"] = str(scraper_input.hours_old)

        state = context.resume_state
        page, raw_before = int(state.get("page", 0)), int(state.get("raw_before", 0))
        jobs = []
        while context.should_continue and page < scraper_input.max_pages:
            response = self.session.get(
                self.listings_url,
                params={"query": query, "page": page, **filters},
                timeout=scraper_input.request_timeout,
                allow_redirects=False,
            )
            if not 200 <= response.status_code < 300:
                raise error_for_http_status(
                    BOARD,
                    response.status_code,
                    retry_after=response.headers.get("Retry-After"),
                )
            data = _page_payload(response.text, page)
            for job in _matching_listings(data, query, filters, context):
                # Replay the current page on resume; stable IDs deduplicate acknowledgements
                # even if the provider reorders its cards.
                if context.emit_job(job, {"page": page, "raw_before": raw_before}):
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
            context.emit_progress(
                {"page": page, "raw_before": raw_before},
                completed_units=page,
                raw_items_seen=raw_before,
                has_more=has_more,
            )
        return JobResponse(jobs=tuple(jobs))
