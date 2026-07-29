from __future__ import annotations

import math
import os
import random
import re
from datetime import date, datetime, timedelta
from urllib.parse import urljoin

from jobstreaming.exception import (
    AuthenticationConfigurationError,
    error_for_http_status,
)
from jobstreaming.model import (
    AdapterCapabilities,
    Compensation,
    CompensationInterval,
    Country,
    DescriptionFormat,
    JobPost,
    JobResponse,
    Location,
    Resumable,
    ResumeGranularity,
    SalarySource,
    Scraper,
    ScraperInput,
    SearchFilter,
    Site,
)
from jobstreaming.naukri.constant import headers as naukri_headers
from jobstreaming.naukri.util import (
    is_job_remote,
    parse_company_industry,
    parse_job_type,
)
from jobstreaming.runtime import ScrapeContext
from jobstreaming.util import (
    create_logger,
    create_session,
    extract_emails_from_text,
    markdown_converter,
    plain_converter,
)

log = create_logger("Naukri")


class Naukri(Scraper):
    base_url = "https://www.naukri.com/jobapi/v3/search"
    delay = 3
    band_delay = 4
    jobs_per_page = 20
    capabilities = AdapterCapabilities(
        filters=frozenset(
            {
                SearchFilter.LOCATION,
                SearchFilter.IS_REMOTE,
                SearchFilter.OFFSET,
                SearchFilter.HOURS_OLD,
                SearchFilter.DESCRIPTION_FORMAT,
            }
        ),
        resume=Resumable(granularity=ResumeGranularity.PAGE),
    )

    def __init__(
        self,
        proxies: list[str] | str | None = None,
        ca_cert: str | None = None,
        user_agent: str | None = None,
        nkparam: str | None = None,
    ) -> None:
        super().__init__(
            Site.NAUKRI,
            proxies=proxies,
            ca_cert=ca_cert,
            user_agent=user_agent,
        )
        self.session = self.track_transport(
            create_session(
                proxies=self.proxies,
                ca_cert=ca_cert,
                is_tls=False,
                clear_cookies=True,
            )
        )
        request_headers = naukri_headers.copy()
        self.nkparam = nkparam or os.getenv("JOBSTREAMING_NAUKRI_NKPARAM")
        if self.nkparam:
            request_headers["Nkparam"] = self.nkparam
        if user_agent:
            request_headers["user-agent"] = user_agent
        self.session.headers.update(request_headers)
        self.scraper_input: ScraperInput | None = None

    def scrape(
        self, scraper_input: ScraperInput, context: ScrapeContext | None = None
    ) -> JobResponse:
        self.scraper_input = scraper_input
        context = ScrapeContext.local(self.site, scraper_input, context)
        if not scraper_input.search_term:
            raise ValueError("Naukri requires a non-empty search_term")
        if not self.nkparam:
            raise AuthenticationConfigurationError(
                "Naukri requires JOBSTREAMING_NAUKRI_NKPARAM"
            )
        self.session.headers["Nkparam"] = self.nkparam

        emitted: list[JobPost] = []
        state = context.resume_state
        initial_page = (scraper_input.offset // self.jobs_per_page) + 1
        page = int(state.get("page", initial_page))
        page_skip = int(
            state.get("page_skip", scraper_input.offset % self.jobs_per_page)
        )
        raw_seen = int(
            state.get("raw_seen", (page - 1) * self.jobs_per_page + page_skip)
        )
        pages_fetched = int(state.get("pages_fetched", max(0, page - initial_page)))

        while context.should_continue and pages_fetched < scraper_input.max_pages:
            log.info(f"Fetching Naukri page {page}")
            params = self._search_params(scraper_input, page)
            response = self.session.get(
                self.base_url,
                params=params,
                timeout=scraper_input.request_timeout,
            )
            if not 200 <= response.status_code < 400:
                raise error_for_http_status(
                    "Naukri",
                    response.status_code,
                    retry_after=(getattr(response, "headers", {}) or {}).get(
                        "Retry-After"
                    ),
                )
            job_details = response.json().get("jobDetails", [])
            if not isinstance(job_details, list) or not job_details:
                break

            page_raw_start = max(0, raw_seen - page_skip)
            for index, job in enumerate(job_details):
                if not isinstance(job, dict):
                    continue
                absolute_index = page_raw_start + index
                try:
                    job_id = job.get("jobId")
                    if not job_id:
                        raise ValueError("listing has no jobId")
                    job_post = self._process_job(job, str(job_id))
                    next_state = {
                        "page": page,
                        "page_skip": index + 1,
                        "raw_seen": absolute_index + 1,
                        "pages_fetched": pages_fetched,
                    }
                    if absolute_index >= scraper_input.offset and context.emit_job(
                        job_post, next_state
                    ):
                        emitted.append(job_post)
                except Exception as exc:
                    context.emit_warning(
                        f"Skipped Naukri listing: {type(exc).__name__}: {exc}"
                    )
                if not context.should_continue:
                    break

            raw_seen = page_raw_start + len(job_details)
            context.emit_progress(
                {
                    "page": page + 1,
                    "page_skip": 0,
                    "raw_seen": raw_seen,
                    "pages_fetched": pages_fetched + 1,
                },
                f"completed Naukri page {page}",
            )
            pages_fetched += 1
            if not context.should_continue or len(job_details) < self.jobs_per_page:
                break
            page += 1
            page_skip = 0
            if not context.wait(
                random.uniform(self.delay, self.delay + self.band_delay)
            ):
                break

        return JobResponse(jobs=emitted)

    def _search_params(self, request: ScraperInput, page: int) -> dict[str, object]:
        assert request.search_term is not None
        params: dict[str, object] = {
            "noOfResults": self.jobs_per_page,
            "urlType": "search_by_keyword",
            "searchType": "adv",
            "keyword": request.search_term,
            "pageNo": page,
            "k": request.search_term,
            "seoKey": f"{request.search_term.lower().replace(' ', '-')}-jobs",
            "src": "jobsearchDesk",
            "latLong": "",
        }
        if request.location:
            params["location"] = request.location
        if request.is_remote:
            params["remote"] = "true"
        if request.hours_old is not None:
            params["days"] = math.ceil(request.hours_old / 24)
        return params

    def _process_job(self, job: dict, job_id: str) -> JobPost:
        assert self.scraper_input is not None
        title = job.get("title")
        if not isinstance(title, str) or not title.strip():
            raise ValueError("listing has no title")
        company = job.get("companyName")
        location = self._get_location(job.get("placeholders", []))
        compensation = self._get_compensation(job.get("placeholders", []))
        date_posted = self._parse_date(
            job.get("footerPlaceholderLabel"), job.get("createdDate")
        )
        job_url = urljoin(
            "https://www.naukri.com/",
            job.get("jdURL") or job.get("staticUrl") or f"job/{job_id}",
        )
        raw_description = job.get("jobDescription") or ""
        description = raw_description
        if self.scraper_input.description_format == DescriptionFormat.MARKDOWN:
            description = markdown_converter(raw_description)
        elif self.scraper_input.description_format == DescriptionFormat.PLAIN:
            description = plain_converter(raw_description)

        ambition_box = job.get("ambitionBoxData") or {}
        company_rating = self._number(ambition_box.get("AggregateRating"), float)
        company_reviews_count = self._number(ambition_box.get("ReviewsCount"), int)
        vacancy_count = self._number(job.get("vacancy"), int)
        skills = (
            tuple(
                skill.strip()
                for skill in (job.get("tagsAndSkills") or "").split(",")
                if skill.strip()
            )
            or None
        )

        return JobPost(
            id=f"nk-{job_id}",
            title=title,
            company_name=company if isinstance(company, str) else None,
            location=location,
            is_remote=is_job_remote(title, description or "", location),
            date_posted=date_posted,
            job_url=job_url,
            compensation=compensation,
            salary_source=SalarySource.DIRECT_DATA if compensation else None,
            job_type=parse_job_type(raw_description),
            company_industry=parse_company_industry(raw_description),
            description=description or None,
            emails=extract_emails_from_text(description or ""),
            company_logo=job.get("logoPathV3") or job.get("logoPath"),
            skills=skills,
            experience_range=job.get("experienceText"),
            company_rating=company_rating,
            company_reviews_count=company_reviews_count,
            vacancy_count=vacancy_count,
            work_from_home_type=self._infer_work_from_home_type(
                job.get("placeholders", []), title, description or ""
            ),
        )

    @staticmethod
    def _number(value: object, converter):
        if value in (None, ""):
            return None
        match = re.search(r"-?\d+(?:\.\d+)?", str(value).replace(",", ""))
        return converter(float(match.group())) if match else None

    def _get_location(self, placeholders: list[dict]) -> Location:
        for placeholder in placeholders or ():
            if placeholder.get("type") == "location":
                parts = [
                    part.strip()
                    for part in placeholder.get("label", "").split(",")
                    if part.strip()
                ]
                return Location(
                    city=parts[0] if parts else None,
                    state=", ".join(parts[1:]) if len(parts) > 1 else None,
                    country=Country.INDIA,
                )
        return Location(country=Country.INDIA)

    def _get_compensation(self, placeholders: list[dict]) -> Compensation | None:
        for placeholder in placeholders or ():
            if placeholder.get("type") != "salary":
                continue
            salary_text = placeholder.get("label", "").strip()
            if not salary_text or salary_text.lower() == "not disclosed":
                return None
            match = re.match(
                r"(\d+(?:\.\d+)?)\s*-\s*(\d+(?:\.\d+)?)\s*(Lacs?|Lakhs?|Cr)",
                salary_text,
                re.IGNORECASE,
            )
            if not match:
                return None
            minimum, maximum = float(match.group(1)), float(match.group(2))
            multiplier = 10_000_000 if match.group(3).lower() == "cr" else 100_000
            return Compensation(
                interval=CompensationInterval.YEARLY,
                min_amount=minimum * multiplier,
                max_amount=maximum * multiplier,
                currency="INR",
            )
        return None

    def _parse_date(self, label: str | None, created_date: int | None) -> date | None:
        now = datetime.now()
        normalized = (label or "").lower()
        if any(value in normalized for value in ("today", "just now", "hour")):
            return now.date()
        match = re.search(r"(\d+)\s*(day|week|month)", normalized)
        if match:
            amount = int(match.group(1))
            unit = match.group(2)
            days = amount * {"day": 1, "week": 7, "month": 30}[unit]
            return (now - timedelta(days=days)).date()
        if created_date:
            timestamp = float(created_date)
            if timestamp > 10_000_000_000:
                timestamp /= 1000
            return datetime.fromtimestamp(timestamp).date()
        return None

    @staticmethod
    def _infer_work_from_home_type(
        placeholders: list[dict], title: str, description: str
    ) -> str | None:
        location = next(
            (
                str(item.get("label", ""))
                for item in placeholders or ()
                if item.get("type") == "location"
            ),
            "",
        )
        text = f"{location} {title} {description}".lower()
        if "hybrid" in text:
            return "Hybrid"
        if any(marker in text for marker in ("remote", "work from home", "wfh")):
            return "Remote"
        if any(marker in text for marker in ("work from office", "on-site", "onsite")):
            return "Work from office"
        return None
