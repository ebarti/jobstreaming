from __future__ import annotations

import math
import os
from datetime import datetime, timezone

from jobstreaming.exception import (
    AuthenticationConfigurationError,
    error_for_http_status,
    error_for_response_message,
)
from jobstreaming.indeed.constant import api_headers, job_search_query
from jobstreaming.indeed.util import get_compensation, get_job_type, is_job_remote
from jobstreaming.model import (
    AdapterCapabilities,
    DescriptionFormat,
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
from jobstreaming.util import (
    create_logger,
    create_session,
    extract_emails_from_text,
    markdown_converter,
    plain_converter,
)

log = create_logger("Indeed")


class Indeed(Scraper):
    capabilities = AdapterCapabilities(
        filters=frozenset(
            {
                SearchFilter.LOCATION,
                SearchFilter.DISTANCE,
                SearchFilter.IS_REMOTE,
                SearchFilter.JOB_TYPE,
                SearchFilter.EASY_APPLY,
                SearchFilter.OFFSET,
                SearchFilter.HOURS_OLD,
                SearchFilter.DESCRIPTION_FORMAT,
            }
        ),
        supported_job_types=frozenset(
            {
                JobType.FULL_TIME,
                JobType.PART_TIME,
                JobType.CONTRACT,
                JobType.INTERNSHIP,
            }
        ),
        resume=Resumable(granularity=ResumeGranularity.PAGE),
    )

    def __init__(
        self,
        proxies: list[str] | str | None = None,
        ca_cert: str | None = None,
        user_agent: str | None = None,
        api_key: str | None = None,
    ):
        """
        Initializes IndeedScraper with the Indeed API url
        """
        super().__init__(
            Site.INDEED,
            proxies=proxies,
            ca_cert=ca_cert,
            user_agent=user_agent,
        )

        self.session = create_session(
            proxies=self.proxies, ca_cert=ca_cert, is_tls=False
        )
        self.scraper_input = None
        self.jobs_per_page = 100
        self.num_workers = 10
        self.headers = None
        self.api_country_code = None
        self.base_url = None
        self.api_url = "https://apis.indeed.com/graphql"
        self.api_key = api_key or os.getenv("JOBSTREAMING_INDEED_API_KEY")

    def scrape(
        self, scraper_input: ScraperInput, context: ScrapeContext | None = None
    ) -> JobResponse:
        """
        Scrapes Indeed for jobs with scraper_input criteria
        :param scraper_input:
        :return: job_response
        """
        self.scraper_input = scraper_input
        context = ScrapeContext.local(self.site, scraper_input, context)
        if not self.api_key:
            raise AuthenticationConfigurationError(
                "Indeed requires JOBSTREAMING_INDEED_API_KEY"
            )
        domain, self.api_country_code = self.scraper_input.country.indeed_domain_value
        self.base_url = f"https://{domain}.indeed.com"
        self.headers = api_headers.copy()
        self.headers["indeed-api-key"] = self.api_key
        self.headers["indeed-co"] = self.api_country_code
        if self.user_agent:
            self.headers["user-agent"] = self.user_agent
        state = context.resume_state
        cursor = state.get("cursor")
        page = int(state.get("page", 1))
        skipped = int(state.get("skipped", 0))
        job_list: list[JobPost] = []

        mutually_exclusive = sum(
            bool(value)
            for value in (
                scraper_input.hours_old,
                scraper_input.easy_apply,
                scraper_input.job_type or scraper_input.is_remote,
            )
        )
        if mutually_exclusive > 1:
            context.emit_warning(
                "Indeed applies only one of hours_old, easy_apply, or "
                "job_type/is_remote; precedence follows that order"
            )

        while context.should_continue and page <= scraper_input.max_pages:
            log.info(
                f"search page: {page} / {math.ceil(scraper_input.results_wanted / self.jobs_per_page)}"
            )
            page_state = {"cursor": cursor, "page": page, "skipped": skipped}
            jobs, cursor = self._scrape_page(cursor)
            if not jobs:
                log.info(f"found no jobs on page: {page}")
                break
            for job in jobs:
                if context.already_seen(job):
                    continue
                if skipped < scraper_input.offset:
                    skipped += 1
                    continue
                if context.emit_job(job, page_state):
                    job_list.append(job)
                if not context.should_continue:
                    break
            context.emit_progress(
                {"cursor": cursor, "page": page + 1, "skipped": skipped},
                f"completed Indeed page {page}",
            )
            if not cursor:
                break
            page += 1
        return JobResponse(jobs=job_list)

    def _scrape_page(self, cursor: str | None) -> tuple[list[JobPost], str | None]:
        """
        Scrapes a page of Indeed for jobs with scraper_input criteria
        :param cursor:
        :return: jobs found on page, next page cursor
        """
        filters = self._build_filters()
        search_term = (
            self.scraper_input.search_term.replace('"', '\\"')
            if self.scraper_input.search_term
            else ""
        )
        query = job_search_query.format(
            what=(f'what: "{search_term}"' if search_term else ""),
            location=(
                f'location: {{where: "{self.scraper_input.location}", radius: {self.scraper_input.distance}, radiusUnit: MILES}}'
                if self.scraper_input.location
                else ""
            ),
            dateOnIndeed=self.scraper_input.hours_old,
            cursor=f'cursor: "{cursor}"' if cursor else "",
            filters=filters,
        )
        payload = {
            "query": query,
        }
        response = self.session.post(
            self.api_url,
            headers=self.headers,
            json=payload,
            timeout=self.scraper_input.request_timeout,
        )
        if not response.ok:
            raise error_for_http_status(
                "Indeed",
                response.status_code,
                cursor_active=cursor is not None,
                retry_after=(getattr(response, "headers", {}) or {}).get("Retry-After"),
            )
        data = response.json()
        if data.get("errors"):
            message = data["errors"][0].get("message", "Indeed GraphQL error")
            raise error_for_response_message(
                "Indeed",
                message,
                cursor_active=cursor is not None,
            )
        search_data = (data.get("data") or {}).get("jobSearch") or {}
        jobs = search_data.get("results") or []
        new_cursor = (search_data.get("pageInfo") or {}).get("nextCursor")

        job_list: list[JobPost] = []
        for job in jobs:
            try:
                processed_job = self._process_job(job["job"])
                if processed_job:
                    job_list.append(processed_job)
            except Exception as exc:
                log.warning(
                    "Skipped malformed Indeed listing: " f"{type(exc).__name__}: {exc}"
                )

        return job_list, new_cursor

    def _build_filters(self):
        """
        Builds the filters dict for job type/is_remote. If hours_old is provided, composite filter for job_type/is_remote is not possible.
        IndeedApply: filters: { keyword: { field: "indeedApplyScope", keys: ["DESKTOP"] } }
        """
        filters_str = ""
        if self.scraper_input.hours_old:
            filters_str = f"""
            filters: {{
                date: {{
                  field: "dateOnIndeed",
                  start: "{self.scraper_input.hours_old}h"
                }}
            }}
            """
        elif self.scraper_input.easy_apply:
            filters_str = """
            filters: {
                keyword: {
                  field: "indeedApplyScope",
                  keys: ["DESKTOP"]
                }
            }
            """
        elif self.scraper_input.job_type or self.scraper_input.is_remote:
            job_type_key_mapping = {
                JobType.FULL_TIME: "CF3CP",
                JobType.PART_TIME: "75GKK",
                JobType.CONTRACT: "NJXCK",
                JobType.INTERNSHIP: "VDTG7",
            }

            keys = []
            if self.scraper_input.job_type:
                key = job_type_key_mapping.get(self.scraper_input.job_type)
                if key is not None:
                    keys.append(key)

            if self.scraper_input.is_remote:
                keys.append("DSQF7")

            if keys:
                keys_str = '", "'.join(keys)
                filters_str = f"""
                filters: {{
                  composite: {{
                    filters: [{{
                      keyword: {{
                        field: "attributes",
                        keys: ["{keys_str}"]
                      }}
                    }}]
                  }}
                }}
                """
        return filters_str

    def _process_job(self, job: dict) -> JobPost | None:
        """
        Parses the job dict into JobPost model
        :param job: dict to parse
        :return: JobPost if it's a new job
        """
        key = job.get("key")
        if not key:
            raise ValueError("listing has no key")
        job_url = f"{self.base_url}/viewjob?jk={key}"
        description = (job.get("description") or {}).get("html") or ""
        if self.scraper_input.description_format == DescriptionFormat.MARKDOWN:
            description = markdown_converter(description)
        elif self.scraper_input.description_format == DescriptionFormat.PLAIN:
            description = plain_converter(description)

        job_type = get_job_type(job.get("attributes") or [])
        published = job.get("datePublished")
        date_posted = (
            datetime.fromtimestamp(float(published) / 1000, tz=timezone.utc).date()
            if published is not None
            else None
        )
        employer_data = job.get("employer") or {}
        employer = employer_data.get("dossier")
        employer_details = employer.get("employerDetails", {}) if employer else {}
        rel_url = employer_data.get("relativeCompanyPageUrl")
        location_data = job.get("location") or {}
        compensation_data = job.get("compensation") or {}
        compensation = get_compensation(compensation_data)
        salary_source = None
        if compensation:
            salary_source = (
                SalarySource.DIRECT_DATA
                if compensation_data.get("baseSalary")
                else SalarySource.ESTIMATED
            )
        return JobPost(
            id=f"in-{key}",
            title=job["title"],
            description=description,
            company_name=employer_data.get("name"),
            company_url=(f"{self.base_url}{rel_url}" if rel_url else None),
            company_url_direct=(
                (employer.get("links") or {}).get("corporateWebsite")
                if employer
                else None
            ),
            location=Location(
                city=location_data.get("city"),
                state=location_data.get("admin1Code"),
                country=location_data.get("countryCode"),
            ),
            job_type=job_type,
            compensation=compensation,
            salary_source=salary_source,
            date_posted=date_posted,
            job_url=job_url,
            job_url_direct=(
                job["recruit"].get("viewJobUrl") if job.get("recruit") else None
            ),
            emails=extract_emails_from_text(description) if description else None,
            is_remote=is_job_remote(job, description),
            company_addresses=(
                employer_details["addresses"][0]
                if employer_details.get("addresses")
                else None
            ),
            company_industry=(
                employer_details["industry"]
                .replace("Iv1", "")
                .replace("_", " ")
                .title()
                .strip()
                if employer_details.get("industry")
                else None
            ),
            company_num_employees=employer_details.get("employeesLocalizedLabel"),
            company_revenue=employer_details.get("revenueLocalizedLabel"),
            company_description=employer_details.get("briefDescription"),
            company_logo=(
                employer["images"].get("squareLogoUrl")
                if employer and employer.get("images")
                else None
            ),
            banner_photo_url=(
                employer["images"].get("headerImageUrl")
                if employer and employer.get("images")
                else None
            ),
        )
