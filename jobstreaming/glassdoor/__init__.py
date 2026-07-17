from __future__ import annotations

import json
import math
import re
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta

from jobstreaming.exception import GlassdoorException
from jobstreaming.glassdoor.constant import fallback_token, headers, query_template
from jobstreaming.glassdoor.util import (
    get_cursor_for_page,
    parse_compensation,
    parse_location,
)
from jobstreaming.model import (
    AdapterCapabilities,
    DescriptionFormat,
    JobPost,
    JobResponse,
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
)

log = create_logger("Glassdoor")


class Glassdoor(Scraper):
    capabilities = AdapterCapabilities(
        filters=frozenset(
            {
                "location",
                "is_remote",
                "job_type",
                "easy_apply",
                "offset",
                "hours_old",
                "description_format",
            }
        ),
        supports_resume=True,
        resume_granularity="page",
    )

    def __init__(
        self,
        proxies: list[str] | str | None = None,
        ca_cert: str | None = None,
        user_agent: str | None = None,
    ):
        """
        Initializes GlassdoorScraper with the Glassdoor job search url
        """
        site = Site(Site.GLASSDOOR)
        super().__init__(site, proxies=proxies, ca_cert=ca_cert, user_agent=user_agent)

        self.base_url = None
        self.country = None
        self.session = None
        self.scraper_input = None
        self.jobs_per_page = 30
        self.max_pages = 30
        self._thread_local = threading.local()
        self._headers: dict[str, str] = {}

    def scrape(
        self, scraper_input: ScraperInput, context: ScrapeContext | None = None
    ) -> JobResponse:
        """
        Scrapes Glassdoor for jobs with scraper_input criteria.
        :param scraper_input: Information about job search criteria.
        :return: JobResponse containing a list of jobs.
        """
        self.scraper_input = scraper_input
        context = ScrapeContext.local(self.site, scraper_input, context)
        self.base_url = self.scraper_input.country.get_glassdoor_url()

        self.session = create_session(
            proxies=self.proxies, ca_cert=self.ca_cert, has_retry=True
        )
        token = self._get_csrf_token()
        self._headers = headers.copy()
        self._headers["gd-csrf-token"] = token if token else fallback_token
        if self.user_agent:
            self._headers["user-agent"] = self.user_agent
        domain = self.base_url.rstrip("/")
        self._headers["origin"] = domain
        self._headers["referer"] = f"{domain}/"
        self._headers.pop("authority", None)
        self.session.headers.update(self._headers)

        location_id, location_type = self._get_location(
            scraper_input.location, scraper_input.is_remote
        )
        if location_id is None or location_type is None:
            raise GlassdoorException("Glassdoor location could not be resolved")
        job_list: list[JobPost] = []
        state = context.resume_state
        page = int(state.get("page", 1 + (scraper_input.offset // self.jobs_per_page)))
        cursor = state.get("cursor")
        last_page = min(scraper_input.max_pages, self.max_pages)
        while context.should_continue and page <= last_page:
            log.info(f"search page: {page} / {last_page}")
            page_state = {"page": page, "cursor": cursor}
            jobs, next_cursor, raw_count = self._fetch_jobs_page(
                scraper_input,
                location_id,
                location_type,
                page,
                cursor,
                context,
                page_state,
            )
            job_list.extend(jobs)
            context.emit_progress(
                {"page": page + 1, "cursor": next_cursor},
                f"completed Glassdoor page {page}",
            )
            if raw_count == 0 or not next_cursor:
                break
            cursor = next_cursor
            page += 1
        return JobResponse(jobs=job_list)

    def _fetch_jobs_page(
        self,
        scraper_input: ScraperInput,
        location_id: int,
        location_type: str,
        page_num: int,
        cursor: str | None,
        context: ScrapeContext,
        page_state: dict[str, object],
    ) -> tuple[list[JobPost], str | None, int]:
        """
        Scrapes a page of Glassdoor for jobs with scraper_input criteria
        """
        jobs: list[JobPost] = []
        self.scraper_input = scraper_input
        payload = self._add_payload(location_id, location_type, page_num, cursor)
        response = self.session.post(
            f"{self.base_url}graph",
            timeout_seconds=self.scraper_input.request_timeout,
            data=payload,
        )
        if response.status_code != 200:
            raise GlassdoorException(f"Glassdoor returned HTTP {response.status_code}")
        res_json = response.json()[0]
        if "errors" in res_json:
            message = res_json["errors"][0].get("message", "GraphQL error")
            raise GlassdoorException(message)

        jobs_data = res_json["data"]["jobListings"]["jobListings"]

        candidates = [
            job
            for index, job in enumerate(jobs_data)
            if (page_num - 1) * self.jobs_per_page + index >= scraper_input.offset
        ]
        with ThreadPoolExecutor(
            max_workers=min(self.jobs_per_page, max(1, len(candidates)))
        ) as executor:
            future_to_job_data = {
                executor.submit(self._process_job, job): job for job in candidates
            }
            for future in as_completed(future_to_job_data):
                if not context.should_continue:
                    break
                try:
                    job_post = future.result()
                    if job_post and context.emit_job(job_post, page_state):
                        jobs.append(job_post)
                except Exception as exc:
                    context.emit_warning(
                        f"Skipped Glassdoor listing: {type(exc).__name__}: {exc}"
                    )

        return (
            jobs,
            get_cursor_for_page(
                res_json["data"]["jobListings"]["paginationCursors"],
                page_num + 1,
            ),
            len(jobs_data),
        )

    def _get_csrf_token(self):
        """
        Fetches csrf token needed for API by visiting a generic page
        """
        res = self.session.get(
            f"{self.base_url}Job/computer-science-jobs.htm",
            timeout_seconds=self.scraper_input.request_timeout,
        )
        pattern = r'"token":\s*"([^"]+)"'
        matches = re.findall(pattern, res.text)
        token = None
        if matches:
            token = matches[0]
        return token

    def _process_job(self, job_data):
        """
        Processes a single job and fetches its description.
        """
        job_id = job_data["jobview"]["job"]["listingId"]
        job_url = f"{self.base_url}job-listing/j?jl={job_id}"
        job = job_data["jobview"]
        title = job["job"]["jobTitleText"]
        company_name = job["header"]["employerNameFromSearch"]
        company_id = job_data["jobview"]["header"]["employer"]["id"]
        location_name = job["header"].get("locationName", "")
        location_type = job["header"].get("locationType", "")
        age_in_days = job["header"].get("ageInDays")
        is_remote, location = False, None
        date_posted = (
            (datetime.now() - timedelta(days=age_in_days)).date()
            if age_in_days is not None
            else None
        )

        if location_name.strip().lower() == "remote" or location_type == "REMOTE":
            is_remote = True
        else:
            location = parse_location(location_name, self.scraper_input.country)

        compensation = parse_compensation(job["header"])
        salary_source_name = (job["header"].get("salarySource") or "").lower()
        salary_source = None
        if compensation:
            salary_source = (
                SalarySource.ESTIMATED
                if "estimate" in salary_source_name
                else SalarySource.DIRECT_DATA
            )
        try:
            description = self._fetch_job_description(job_id)
        except Exception:
            description = None
        company_url = f"{self.base_url}Overview/W-EI_IE{company_id}.htm"
        company_logo = (
            job_data["jobview"].get("overview", {}).get("squareLogoUrl", None)
        )
        listing_type = (
            job_data["jobview"].get("header", {}).get("adOrderSponsorshipLevel", "")
            or ""
        )
        listing_type = listing_type.lower()
        return JobPost(
            id=f"gd-{job_id}",
            title=title,
            company_url=company_url if company_id else None,
            company_name=company_name,
            date_posted=date_posted,
            job_url=job_url,
            location=location,
            compensation=compensation,
            salary_source=salary_source,
            is_remote=is_remote,
            description=description,
            emails=extract_emails_from_text(description) if description else None,
            company_logo=company_logo,
            listing_type=listing_type,
        )

    def _fetch_job_description(self, job_id):
        """
        Fetches the job description for a single job ID.
        """
        url = f"{self.base_url}graph"
        body = [
            {
                "operationName": "JobDetailQuery",
                "variables": {
                    "jl": job_id,
                    "queryString": "q",
                    "pageTypeEnum": "SERP",
                },
                "query": """
                query JobDetailQuery($jl: Long!, $queryString: String, $pageTypeEnum: PageTypeEnum) {
                    jobview: jobView(
                        listingId: $jl
                        contextHolder: {queryString: $queryString, pageTypeEnum: $pageTypeEnum}
                    ) {
                        job {
                            description
                            __typename
                        }
                        __typename
                    }
                }
                """,
            }
        ]
        session = self._get_detail_session()
        res = session.post(
            url,
            json=body,
            headers=self._headers,
            timeout_seconds=self.scraper_input.request_timeout,
        )
        if res.status_code != 200:
            return None
        data = res.json()[0]
        desc = data["data"]["jobview"]["job"]["description"]
        if self.scraper_input.description_format == DescriptionFormat.MARKDOWN:
            desc = markdown_converter(desc)
        elif self.scraper_input.description_format == DescriptionFormat.PLAIN:
            desc = plain_converter(desc)
        return desc

    def _get_location(
        self, location: str | None, is_remote: bool
    ) -> tuple[int | None, str | None]:
        if is_remote:
            return 11047, "STATE"  # remote options
        if not location:
            raise ValueError("Glassdoor requires location unless is_remote=True")
        url = f"{self.base_url}findPopularLocationAjax.htm?maxLocationsToReturn=10&term={location}"
        res = self.session.get(url, timeout_seconds=self.scraper_input.request_timeout)
        if res.status_code != 200:
            if res.status_code == 429:
                err = "429 Response - Blocked by Glassdoor for too many requests"
                log.error(err)
                return None, None
            else:
                err = f"Glassdoor response status code {res.status_code}"
                err += f" - {res.text}"
                log.error(f"Glassdoor response status code {res.status_code}")
                return None, None
        items = res.json()

        if not items:
            raise ValueError(f"Location '{location}' not found on Glassdoor")
        location_type = items[0]["locationType"]
        if location_type == "C":
            location_type = "CITY"
        elif location_type == "S":
            location_type = "STATE"
        elif location_type == "N":
            location_type = "COUNTRY"
        return int(items[0]["locationId"]), location_type

    def _add_payload(
        self,
        location_id: int,
        location_type: str,
        page_num: int,
        cursor: str | None = None,
    ) -> str:
        fromage = None
        if self.scraper_input.hours_old:
            fromage = max(math.ceil(self.scraper_input.hours_old / 24), 1)
        filter_params = []
        if self.scraper_input.easy_apply:
            filter_params.append({"filterKey": "applicationType", "values": "1"})
        if fromage:
            filter_params.append({"filterKey": "fromAge", "values": str(fromage)})
        payload = {
            "operationName": "JobSearchResultsQuery",
            "variables": {
                "excludeJobListingIds": [],
                "filterParams": filter_params,
                "keyword": self.scraper_input.search_term,
                "numJobsToShow": 30,
                "locationType": location_type,
                "locationId": int(location_id),
                "parameterUrlInput": f"IL.0,12_I{location_type}{location_id}",
                "pageNumber": page_num,
                "pageCursor": cursor,
                "fromage": fromage,
                "sort": "date",
            },
            "query": query_template,
        }
        if self.scraper_input.job_type:
            payload["variables"]["filterParams"].append(
                {"filterKey": "jobType", "values": self.scraper_input.job_type.value[0]}
            )
        return json.dumps([payload])

    def _get_detail_session(self):
        session = getattr(self._thread_local, "session", None)
        if session is None:
            session = create_session(proxies=self.proxies, ca_cert=self.ca_cert)
            session.headers.update(self._headers)
            self._thread_local.session = session
        return session
