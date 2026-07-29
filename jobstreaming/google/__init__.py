from __future__ import annotations

import json
import re

from jobstreaming.exception import error_for_http_status
from jobstreaming.google.constant import async_param, headers_initial, headers_jobs
from jobstreaming.google.util import (
    find_job_info,
    find_job_info_initial_page,
    log,
    parse_relative_date,
)
from jobstreaming.model import (
    AdapterCapabilities,
    JobPost,
    JobResponse,
    JobType,
    Location,
    Resumable,
    ResumeGranularity,
    Scraper,
    ScraperInput,
    SearchFilter,
    Site,
)
from jobstreaming.runtime import ScrapeContext
from jobstreaming.util import (
    create_session,
    extract_emails_from_text,
    extract_job_type,
    stable_job_id,
)


class Google(Scraper):
    capabilities = AdapterCapabilities(
        filters=frozenset(
            {
                SearchFilter.LOCATION,
                SearchFilter.IS_REMOTE,
                SearchFilter.JOB_TYPE,
                SearchFilter.OFFSET,
                SearchFilter.HOURS_OLD,
            }
        ),
        supported_job_types=frozenset(
            {
                JobType.FULL_TIME,
                JobType.PART_TIME,
                JobType.INTERNSHIP,
                JobType.CONTRACT,
            }
        ),
        resume=Resumable(granularity=ResumeGranularity.CURSOR),
    )

    def __init__(
        self,
        proxies: list[str] | str | None = None,
        ca_cert: str | None = None,
        user_agent: str | None = None,
    ) -> None:
        super().__init__(
            Site.GOOGLE,
            proxies=proxies,
            ca_cert=ca_cert,
            user_agent=user_agent,
        )
        self.session = None
        self.scraper_input: ScraperInput | None = None
        self.jobs_per_page = 10
        self.url = "https://www.google.com/search"
        self.jobs_url = "https://www.google.com/async/callback:550"

    def scrape(
        self, scraper_input: ScraperInput, context: ScrapeContext | None = None
    ) -> JobResponse:
        self.scraper_input = scraper_input
        context = ScrapeContext.local(self.site, scraper_input, context)
        self.session = create_session(
            proxies=self.proxies,
            ca_cert=self.ca_cert,
            is_tls=False,
        )

        emitted: list[JobPost] = []
        state = context.resume_state
        phase = str(state.get("phase", "initial"))
        cursor = state.get("cursor")
        page = int(state.get("page", 1))
        page_skip = int(state.get("page_skip", 0))
        raw_seen = int(state.get("raw_seen", 0))
        result_limit = min(900, scraper_input.results_wanted)

        while (
            context.should_continue
            and context.emitted_count < result_limit
            and page <= scraper_input.max_pages
        ):
            input_cursor = cursor
            if phase == "initial":
                next_cursor, jobs = self._get_initial_cursor_and_jobs()
            else:
                if not input_cursor:
                    break
                jobs, next_cursor = self._get_jobs_next_page(str(input_cursor))

            page_raw_start = max(0, raw_seen - page_skip)
            for index, job in enumerate(jobs):
                absolute_index = page_raw_start + index
                next_state = {
                    "phase": phase,
                    "cursor": input_cursor,
                    "page": page,
                    "page_skip": index + 1,
                    "raw_seen": absolute_index + 1,
                }
                if absolute_index < scraper_input.offset:
                    continue
                if not context.should_continue or context.emitted_count >= result_limit:
                    break
                if context.emit_job(job, next_state):
                    emitted.append(job)

            raw_seen = page_raw_start + len(jobs)
            context.emit_progress(
                {
                    "phase": "next",
                    "cursor": next_cursor,
                    "page": page + 1,
                    "page_skip": 0,
                    "raw_seen": raw_seen,
                },
                f"completed Google page {page}",
            )
            if not next_cursor or not jobs:
                break
            phase = "next"
            cursor = next_cursor
            page += 1
            page_skip = 0

        return JobResponse(jobs=emitted)

    def _request_headers(self, base: dict[str, str]) -> dict[str, str]:
        request_headers = base.copy()
        if self.user_agent:
            request_headers["user-agent"] = self.user_agent
        return request_headers

    def _get_initial_cursor_and_jobs(self) -> tuple[str | None, list[JobPost]]:
        assert self.scraper_input is not None and self.session is not None
        query = f"{self.scraper_input.search_term or ''} jobs".strip()

        def get_time_range(hours_old: int) -> str:
            if hours_old <= 24:
                return "since yesterday"
            if hours_old <= 72:
                return "in the last 3 days"
            if hours_old <= 168:
                return "in the last week"
            return "in the last month"

        job_type_mapping = {
            JobType.FULL_TIME: "Full time",
            JobType.PART_TIME: "Part time",
            JobType.INTERNSHIP: "Internship",
            JobType.CONTRACT: "Contract",
        }
        if self.scraper_input.job_type in job_type_mapping:
            query += f" {job_type_mapping[self.scraper_input.job_type]}"
        if self.scraper_input.location:
            query += f" near {self.scraper_input.location}"
        if self.scraper_input.hours_old is not None:
            query += f" {get_time_range(self.scraper_input.hours_old)}"
        if self.scraper_input.is_remote:
            query += " remote"
        if self.scraper_input.google_search_term:
            query = self.scraper_input.google_search_term

        response = self.session.get(
            self.url,
            headers=self._request_headers(headers_initial),
            params={"q": query, "udm": "8"},
            timeout=self.scraper_input.request_timeout,
        )
        if response.status_code not in range(200, 400):
            raise error_for_http_status(
                "Google",
                response.status_code,
                retry_after=(getattr(response, "headers", {}) or {}).get("Retry-After"),
            )
        match = re.search(
            r'<div jsname="Yust4d"[^>]+data-async-fc="([^"]+)"', response.text
        )
        cursor = match.group(1) if match else None
        jobs = [
            parsed
            for raw in find_job_info_initial_page(response.text)
            if (parsed := self._parse_job(raw)) is not None
        ]
        if cursor is None:
            log.info("Google returned a single page of results")
        return cursor, jobs

    def _get_jobs_next_page(
        self, forward_cursor: str
    ) -> tuple[list[JobPost], str | None]:
        assert self.scraper_input is not None and self.session is not None
        params = {"fc": [forward_cursor], "fcv": ["3"], "async": [async_param]}
        response = self.session.get(
            self.jobs_url,
            headers=self._request_headers(headers_jobs),
            params=params,
            timeout=self.scraper_input.request_timeout,
        )
        if response.status_code not in range(200, 400):
            raise error_for_http_status(
                "Google",
                response.status_code,
                cursor_active=True,
                retry_after=(getattr(response, "headers", {}) or {}).get("Retry-After"),
            )
        return self._parse_jobs(response.text)

    def _parse_jobs(self, job_data: str) -> tuple[list[JobPost], str | None]:
        start_idx = job_data.find("[[[")
        end_idx = job_data.rfind("]]]")
        if start_idx < 0 or end_idx < start_idx:
            return [], None
        try:
            parsed = json.loads(job_data[start_idx : end_idx + 3])[0]
        except (json.JSONDecodeError, IndexError, TypeError):
            return [], None

        match = re.search(r'data-async-fc="([^"]+)"', job_data)
        cursor = match.group(1) if match else None
        jobs: list[JobPost] = []
        for array in parsed:
            if not isinstance(array, list) or len(array) < 2:
                continue
            encoded = array[1]
            if not isinstance(encoded, str) or not encoded.startswith("[[["):
                continue
            try:
                job_info = find_job_info(json.loads(encoded))
            except json.JSONDecodeError:
                continue
            job = self._parse_job(job_info)
            if job:
                jobs.append(job)
        return jobs, cursor

    def _parse_job(self, job_info: list | None) -> JobPost | None:
        if not isinstance(job_info, list) or len(job_info) <= 19:
            return None
        try:
            job_url = job_info[3][0][0]
        except (IndexError, TypeError):
            return None
        title = job_info[0]
        if not isinstance(title, str) or not isinstance(job_url, str):
            return None

        raw_location = job_info[2] if len(job_info) > 2 else None
        location = None
        if isinstance(raw_location, str) and raw_location.strip():
            parts = [part.strip() for part in raw_location.split(",") if part.strip()]
            second_is_region_code = (
                len(parts) == 2 and len(parts[1]) <= 3 and parts[1].upper() == parts[1]
            )
            location = Location(
                city=parts[0] if parts else None,
                state=(parts[1] if len(parts) > 2 or second_is_region_code else None),
                country=(
                    ", ".join(parts[2:])
                    if len(parts) > 2
                    else (
                        parts[1]
                        if len(parts) == 2 and not second_is_region_code
                        else None
                    )
                ),
            )

        description = job_info[19] if isinstance(job_info[19], str) else ""
        source_id = job_info[28] if len(job_info) > 28 else None
        identifier = (
            f"go-{source_id}" if source_id else stable_job_id("google", job_url)
        )
        return JobPost(
            id=str(identifier),
            title=title,
            company_name=job_info[1] if isinstance(job_info[1], str) else None,
            location=location,
            job_url=job_url,
            date_posted=parse_relative_date(
                job_info[12] if len(job_info) > 12 else None
            ),
            is_remote=bool(re.search(r"\b(remote|wfh)\b", description, re.I)),
            description=description or None,
            emails=extract_emails_from_text(description),
            job_type=extract_job_type(description),
        )
