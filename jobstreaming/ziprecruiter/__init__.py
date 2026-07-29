from __future__ import annotations

import json
import os
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from urllib.parse import parse_qs, urlparse

from bs4 import BeautifulSoup

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
    SalarySource,
    Scraper,
    ScraperInput,
    Site,
)
from jobstreaming.runtime import ScrapeContext
from jobstreaming.util import (
    create_logger,
    create_session,
    extract_emails_from_text,
    markdown_converter,
    plain_converter,
    remove_attributes,
)
from jobstreaming.ziprecruiter.constant import get_cookie_data, headers
from jobstreaming.ziprecruiter.util import add_params, get_job_type_enum

log = create_logger("ZipRecruiter")


class ZipRecruiter(Scraper):
    base_url = "https://www.ziprecruiter.com"
    api_url = "https://api.ziprecruiter.com"
    capabilities = AdapterCapabilities(
        filters=frozenset(
            {
                "location",
                "distance",
                "is_remote",
                "job_type",
                "easy_apply",
                "offset",
                "hours_old",
                "description_format",
            }
        ),
        supports_resume=True,
        resume_granularity="continuation token",
        cursor_schema_version=1,
    )

    def __init__(
        self,
        proxies: list[str] | str | None = None,
        ca_cert: str | None = None,
        user_agent: str | None = None,
        authorization: str | None = None,
        device_id: str | None = None,
        push_notification_id: str | None = None,
        zva_override: str | None = None,
    ):
        """
        Initializes ZipRecruiterScraper with the ZipRecruiter job search url
        """
        super().__init__(
            Site.ZIP_RECRUITER,
            proxies=proxies,
            ca_cert=ca_cert,
            user_agent=user_agent,
        )

        self.scraper_input = None
        self.session = self.track_transport(
            create_session(proxies=proxies, ca_cert=ca_cert)
        )
        self.authorization = authorization or os.getenv(
            "JOBSTREAMING_ZIPRECRUITER_AUTHORIZATION"
        )
        session_headers = {
            key: value for key, value in headers.items() if key.lower() != "host"
        }
        session_headers["x-deviceid"] = (
            device_id
            or os.getenv("JOBSTREAMING_ZIPRECRUITER_DEVICE_ID")
            or str(uuid.uuid4()).upper()
        )
        configured_push_id = push_notification_id or os.getenv(
            "JOBSTREAMING_ZIPRECRUITER_PUSH_NOTIFICATION_ID"
        )
        configured_override = zva_override or os.getenv(
            "JOBSTREAMING_ZIPRECRUITER_ZVA_OVERRIDE"
        )
        if self.authorization:
            session_headers["authorization"] = self.authorization
        if configured_push_id:
            session_headers["x-pushnotificationid"] = configured_push_id
        if configured_override:
            session_headers["x-zr-zva-override"] = configured_override
        if user_agent:
            session_headers["user-agent"] = user_agent
        self.session.headers.update(session_headers)
        self._session_headers = session_headers
        self._thread_local = threading.local()

        self.delay = 5
        self.jobs_per_page = 20

    def scrape(
        self, scraper_input: ScraperInput, context: ScrapeContext | None = None
    ) -> JobResponse:
        """
        Scrapes ZipRecruiter for jobs with scraper_input criteria.
        :param scraper_input: Information about job search criteria.
        :return: JobResponse containing a list of jobs.
        """
        self.scraper_input = scraper_input
        context = ScrapeContext.local(self.site, scraper_input, context)
        if not self.authorization:
            raise AuthenticationConfigurationError(
                "ZipRecruiter requires JOBSTREAMING_ZIPRECRUITER_AUTHORIZATION"
            )
        job_list: list[JobPost] = []
        state = context.resume_state
        continue_token = state.get("continue_token")
        skipped = int(state.get("skipped", 0))
        page = int(state.get("page", 1))

        try:
            self._get_cookies()
        except Exception as exc:
            context.emit_warning(f"ZipRecruiter session initialization failed: {exc}")

        max_pages = scraper_input.max_pages
        while context.should_continue and page <= max_pages:
            if page > 1 and not context.wait(self.delay):
                break
            log.info(f"search page: {page} / {max_pages}")
            page_state = {
                "continue_token": continue_token,
                "page": page,
                "skipped": skipped,
            }
            jobs_on_page, next_token, skipped = self._find_jobs_in_page(
                scraper_input,
                context,
                continue_token,
                skipped,
                page_state,
            )
            if jobs_on_page:
                job_list.extend(jobs_on_page)
            context.emit_progress(
                {
                    "continue_token": next_token,
                    "page": page + 1,
                    "skipped": skipped,
                },
                f"completed ZipRecruiter page {page}",
            )
            if not next_token:
                break
            continue_token = next_token
            page += 1
        return JobResponse(jobs=job_list)

    def _find_jobs_in_page(
        self,
        scraper_input: ScraperInput,
        context: ScrapeContext,
        continue_token: str | None,
        skipped: int,
        page_state: dict[str, object],
    ) -> tuple[list[JobPost], str | None, int]:
        """
        Scrapes a page of ZipRecruiter for jobs with scraper_input criteria
        :param scraper_input:
        :param continue_token:
        :return: jobs found on page
        """
        jobs_list: list[dict] = []
        params = add_params(scraper_input)
        if continue_token:
            params["continue_from"] = continue_token
        res = self.session.get(
            f"{self.api_url}/jobs-app/jobs",
            params=params,
            timeout_seconds=scraper_input.request_timeout,
        )
        if res.status_code not in range(200, 400):
            raise error_for_http_status(
                "ZipRecruiter",
                res.status_code,
                cursor_active=continue_token is not None,
                retry_after=(getattr(res, "headers", {}) or {}).get("Retry-After"),
            )

        res_data = res.json()
        jobs_list = res_data.get("jobs", [])
        next_continue_token = res_data.get("continue", None)
        candidates: list[dict] = []
        for job in jobs_list:
            if skipped < scraper_input.offset:
                skipped += 1
                continue
            candidates.append(job)

        job_list: list[JobPost] = []
        with (
            self.transport_scope(),
            ThreadPoolExecutor(
                max_workers=min(self.jobs_per_page, max(1, len(candidates)))
            ) as executor,
        ):
            futures = {
                executor.submit(self._process_job, job): job for job in candidates
            }
            for future in as_completed(futures):
                if not context.should_continue:
                    break
                try:
                    job_post = future.result()
                except Exception as exc:
                    context.emit_warning(
                        f"Skipped ZipRecruiter listing: {type(exc).__name__}: {exc}"
                    )
                    continue
                if job_post and context.emit_job(job_post, page_state):
                    job_list.append(job_post)
        return job_list, next_continue_token, skipped

    def _process_job(self, job: dict) -> JobPost | None:
        """
        Processes an individual job dict from the response
        """
        title = job.get("name")
        listing_key = job.get("listing_key")
        if not listing_key:
            raise ValueError("listing has no listing_key")
        job_url = f"{self.base_url}/jobs/j?lvk={listing_key}"

        description = str(job.get("job_description") or "").strip()
        listing_type = str(job.get("buyer_type") or "")
        description = (
            markdown_converter(description)
            if self.scraper_input.description_format == DescriptionFormat.MARKDOWN
            else (
                plain_converter(description)
                if self.scraper_input.description_format == DescriptionFormat.PLAIN
                else description
            )
        )
        hiring_company = job.get("hiring_company") or {}
        company = (
            hiring_company.get("name") if isinstance(hiring_company, dict) else None
        )
        country_code = (job.get("job_country") or "").upper()
        country_enum: Country | str = {
            "US": Country.USA,
            "CA": Country.CANADA,
        }.get(country_code, country_code or Country.WORLDWIDE)

        location = Location(
            city=job.get("job_city"), state=job.get("job_state"), country=country_enum
        )
        job_type = get_job_type_enum(
            job.get("employment_type", "").replace("_", "").lower()
        )
        posted_time = job.get("posted_time")
        try:
            date_posted = (
                datetime.fromisoformat(str(posted_time).replace("Z", "+00:00")).date()
                if posted_time
                else None
            )
        except ValueError:
            date_posted = None
        comp_interval = job.get("compensation_interval")
        comp_interval = CompensationInterval.get_interval(comp_interval)
        comp_min_raw = job.get("compensation_min")
        comp_max_raw = job.get("compensation_max")
        comp_min = float(comp_min_raw) if comp_min_raw not in (None, "") else None
        comp_max = float(comp_max_raw) if comp_max_raw not in (None, "") else None
        comp_currency = job.get("compensation_currency")
        description_full, job_url_direct = self._get_descr(job_url)
        final_description = description_full or description
        compensation = None
        if (
            comp_interval
            and comp_currency
            and (comp_min is not None or comp_max is not None)
        ):
            compensation = Compensation(
                interval=comp_interval,
                min_amount=comp_min,
                max_amount=comp_max,
                currency=comp_currency,
            )

        return JobPost(
            id=f"zr-{listing_key}",
            title=title,
            company_name=company,
            location=location,
            job_type=job_type,
            compensation=compensation,
            salary_source=SalarySource.DIRECT_DATA if compensation else None,
            date_posted=date_posted,
            job_url=job_url,
            description=final_description,
            emails=(
                extract_emails_from_text(final_description)
                if final_description
                else None
            ),
            job_url_direct=job_url_direct,
            listing_type=listing_type,
            is_remote=bool(job.get("remote"))
            or "remote" in (final_description or "").lower(),
        )

    def _get_descr(self, job_url):
        session = self._get_detail_session()
        try:
            res = session.get(
                job_url,
                allow_redirects=True,
                timeout_seconds=self.scraper_input.request_timeout,
            )
        except Exception:
            return None, None
        description_full = job_url_direct = None
        if res.ok:
            soup = BeautifulSoup(res.text, "html.parser")
            job_descr_div = soup.find("div", class_="job_description")
            company_descr_section = soup.find("section", class_="company_description")
            job_description_clean = (
                remove_attributes(job_descr_div).prettify(formatter="html")
                if job_descr_div
                else ""
            )
            company_description_clean = (
                remove_attributes(company_descr_section).prettify(formatter="html")
                if company_descr_section
                else ""
            )
            description_full = job_description_clean + company_description_clean

            try:
                script_tag = soup.find("script", type="application/json")
                if script_tag:
                    job_json = json.loads(script_tag.string)
                    job_url_val = job_json["model"].get("saveJobURL", "")
                    job_url_direct = parse_qs(urlparse(job_url_val).query).get(
                        "job_url", [None]
                    )[0]
            except (KeyError, TypeError, ValueError, json.JSONDecodeError):
                job_url_direct = None

            if self.scraper_input.description_format == DescriptionFormat.MARKDOWN:
                description_full = markdown_converter(description_full)
            elif self.scraper_input.description_format == DescriptionFormat.PLAIN:
                description_full = plain_converter(description_full)

        return description_full, job_url_direct

    def _get_cookies(self):
        """
        Sends a session event to the API with device properties.
        """
        url = f"{self.api_url}/jobs-app/event"
        self.session.post(
            url,
            data=get_cookie_data,
            timeout_seconds=self.scraper_input.request_timeout,
        )

    def _get_detail_session(self):
        session = getattr(self._thread_local, "session", None)
        if session is None:
            session = self.track_transport(
                create_session(proxies=self.proxies, ca_cert=self.ca_cert)
            )
            session.headers.update(self._session_headers)
            self._thread_local.session = session
        return session
