from __future__ import annotations

import math
import random
import re
from datetime import datetime
from urllib.parse import unquote, urlparse, urlunparse

from bs4 import BeautifulSoup
from bs4.element import Tag

from jobstreaming.exception import (
    AuthenticationConfigurationError,
    InvalidRequestError,
    error_for_http_status,
)
from jobstreaming.linkedin.constant import headers
from jobstreaming.linkedin.util import (
    is_job_remote,
    job_type_code,
    parse_company_industry,
    parse_job_level,
    parse_job_type,
)
from jobstreaming.model import (
    AdapterCapabilities,
    Compensation,
    CompensationInterval,
    Country,
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
    SearchRequest,
    Site,
)
from jobstreaming.runtime import ScrapeContext
from jobstreaming.util import (
    create_logger,
    create_session,
    currency_parser,
    extract_emails_from_text,
    markdown_converter,
    plain_converter,
    remove_attributes,
)

log = create_logger("LinkedIn")


class LinkedIn(Scraper):
    base_url = "https://www.linkedin.com"
    # Keep continuation requests jittered while avoiding a fixed five-second
    # average gap between otherwise sub-second result-page requests.
    delay = 1
    band_delay = 1
    jobs_per_page = 10
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
                JobType.INTERNSHIP,
                JobType.CONTRACT,
                JobType.TEMPORARY,
            }
        ),
        resume=Resumable(granularity=ResumeGranularity.PAGE),
    )

    def __init__(
        self,
        proxies: list[str] | str | None = None,
        ca_cert: str | None = None,
        user_agent: str | None = None,
    ):
        """
        Initializes LinkedInScraper with the LinkedIn job search url
        """
        super().__init__(
            Site.LINKEDIN,
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
        self.session.headers.update(headers)
        if self.user_agent:
            self.session.headers["user-agent"] = self.user_agent
        self.scraper_input = None
        self.country = "worldwide"
        self.job_url_direct_regex = re.compile(r'[?&]url=([^"&]+)')

    def scrape(
        self, scraper_input: ScraperInput, context: ScrapeContext | None = None
    ) -> JobResponse:
        """
        Scrapes LinkedIn for jobs with scraper_input criteria
        :param scraper_input:
        :return: job_response
        """
        self.scraper_input = scraper_input
        context = ScrapeContext.local(self.site, scraper_input, context)
        job_list: list[JobPost] = []
        resume_state = context.resume_state
        start = int(
            resume_state.get(
                "start",
                (
                    scraper_input.offset // self.jobs_per_page * self.jobs_per_page
                    if scraper_input.offset
                    else 0
                ),
            )
        )
        pages_completed = int(
            resume_state.get("pages_completed", resume_state.get("request_count", 0))
        )
        saved_raw_seen = resume_state.get("raw_seen")
        raw_seen = (
            int(saved_raw_seen)
            if saved_raw_seen is not None
            else (0 if pages_completed == 0 else None)
        )
        seconds_old = (
            scraper_input.hours_old * 3600 if scraper_input.hours_old else None
        )
        while (
            context.should_continue
            and start < 1000
            and pages_completed < scraper_input.max_pages
        ):
            total_pages = min(
                scraper_input.max_pages,
                math.ceil(
                    (scraper_input.results_wanted + scraper_input.offset)
                    / self.jobs_per_page
                ),
            )
            log.info(f"search page: {pages_completed + 1} / {total_pages}")
            params = {
                "keywords": scraper_input.search_term,
                "location": scraper_input.location,
                "distance": scraper_input.distance,
                "f_WT": 2 if scraper_input.is_remote else None,
                "f_JT": (
                    job_type_code(scraper_input.job_type)
                    if scraper_input.job_type
                    else None
                ),
                "pageNum": 0,
                "start": start,
                "f_AL": "true" if scraper_input.easy_apply else None,
                "f_C": (
                    ",".join(map(str, scraper_input.linkedin_company_ids))
                    if scraper_input.linkedin_company_ids
                    else None
                ),
            }
            if seconds_old is not None:
                params["f_TPR"] = f"r{seconds_old}"

            params = {k: v for k, v in params.items() if v is not None}
            response = self.session.get(
                f"{self.base_url}/jobs-guest/jobs/api/seeMoreJobPostings/search",
                params=params,
                timeout=scraper_input.request_timeout,
            )
            if response.status_code not in range(200, 400):
                raise error_for_http_status(
                    "LinkedIn",
                    response.status_code,
                    cursor_active=bool(resume_state) or pages_completed > 0,
                    retry_after=(getattr(response, "headers", {}) or {}).get(
                        "Retry-After"
                    ),
                )

            soup = BeautifulSoup(response.text, "html.parser")
            job_cards = soup.find_all("div", class_="base-search-card")
            if len(job_cards) == 0:
                break

            page_state = {
                "start": start,
                "pages_completed": pages_completed,
                "raw_seen": raw_seen,
            }
            for index, job_card in enumerate(job_cards):
                if start + index < scraper_input.offset:
                    continue
                href_tag = job_card.find("a", class_="base-card__full-link")
                if href_tag and "href" in href_tag.attrs:
                    href_value = href_tag.attrs["href"]
                    if not isinstance(href_value, str):
                        continue
                    href = href_value.split("?")[0]
                    job_id = href.split("-")[-1]
                    if not job_id:
                        continue
                    if context.already_seen_identity(f"li-{job_id}"):
                        continue

                    try:
                        fetch_desc = scraper_input.linkedin_fetch_description
                        job_post = self._process_job(job_card, job_id, fetch_desc)
                        if job_post and context.emit_job(job_post, page_state):
                            job_list.append(job_post)
                        if not context.should_continue:
                            break
                    except Exception as e:
                        context.emit_warning(
                            f"Skipped LinkedIn job {job_id}: {type(e).__name__}: {e}"
                        )
                        continue

            next_start = start + len(job_cards)
            raw_seen = raw_seen + len(job_cards) if raw_seen is not None else None
            has_more = None if len(job_cards) >= self.jobs_per_page else False
            context.emit_progress(
                {
                    "start": next_start,
                    "pages_completed": pages_completed + 1,
                    "raw_seen": raw_seen,
                },
                completed_units=pages_completed + 1,
                raw_items_seen=raw_seen,
                has_more=has_more,
                message=f"completed LinkedIn page at offset {start}",
            )
            pages_completed += 1
            start = next_start
            if not context.should_continue or has_more is False:
                break
            if pages_completed < scraper_input.max_pages and not context.wait(
                random.uniform(self.delay, self.delay + self.band_delay)
            ):
                break

        return JobResponse(jobs=job_list)

    def _process_job(
        self, job_card: Tag, job_id: str, full_descr: bool
    ) -> JobPost | None:
        salary_tag = job_card.find("span", class_="job-search-card__salary-info")

        compensation = description = None
        if salary_tag:
            salary_text = salary_tag.get_text(separator=" ").strip()
            try:
                salary_parts = re.split(r"\s*[-—–]\s*", salary_text, maxsplit=1)
                salary_values = [currency_parser(value) for value in salary_parts]
                salary_min = salary_values[0]
                salary_max = salary_values[1] if len(salary_values) > 1 else None
                symbol = next(
                    (
                        symbol
                        for symbol in ("$", "€", "£", "₹")
                        if symbol in salary_text
                    ),
                    "$",
                )
                currency = {"$": "USD", "€": "EUR", "£": "GBP", "₹": "INR"}[symbol]
                lowered_salary = salary_text.lower()
                interval = (
                    CompensationInterval.HOURLY
                    if any(token in lowered_salary for token in ("/hr", "hour"))
                    else (
                        CompensationInterval.MONTHLY
                        if any(token in lowered_salary for token in ("/mo", "month"))
                        else (
                            CompensationInterval.WEEKLY
                            if any(token in lowered_salary for token in ("/wk", "week"))
                            else (
                                CompensationInterval.DAILY
                                if any(
                                    token in lowered_salary
                                    for token in ("/day", "daily")
                                )
                                else CompensationInterval.YEARLY
                            )
                        )
                    )
                )
                compensation = Compensation(
                    interval=interval,
                    min_amount=float(salary_min),
                    max_amount=float(salary_max) if salary_max is not None else None,
                    currency=currency,
                )
            except (IndexError, TypeError, ValueError):
                compensation = None

        title_tag = job_card.find("span", class_="sr-only")
        title = title_tag.get_text(strip=True) if title_tag else ""
        if not title:
            raise ValueError("listing has no title")

        company_tag = job_card.find("h4", class_="base-search-card__subtitle")
        company_a_tag = company_tag.find("a") if company_tag else None
        company_href = company_a_tag.get("href") if company_a_tag else None
        company_url = (
            urlunparse(urlparse(company_href)._replace(query=""))
            if isinstance(company_href, str)
            else None
        )
        company = (
            company_a_tag.get_text(strip=True)
            if company_a_tag
            else company_tag.get_text(strip=True) if company_tag else None
        )

        metadata_card = job_card.find("div", class_="base-search-card__metadata")
        location = self._get_location(metadata_card)

        datetime_tag = (
            metadata_card.find("time", class_="job-search-card__listdate")
            if metadata_card
            else None
        )
        if not datetime_tag and metadata_card:
            datetime_tag = metadata_card.find(
                "time", class_="job-search-card__listdate--new"
            )
        date_posted = None
        if datetime_tag and "datetime" in datetime_tag.attrs:
            datetime_str = datetime_tag["datetime"]
            try:
                date_posted = datetime.strptime(str(datetime_str), "%Y-%m-%d").date()
            except (TypeError, ValueError):
                date_posted = None
        job_details = {}
        if full_descr:
            job_details = self._get_job_details(job_id)
            description = job_details.get("description")
        is_remote = is_job_remote(title, description, location)

        return JobPost(
            id=f"li-{job_id}",
            title=title,
            company_name=company,
            company_url=company_url,
            location=location,
            is_remote=is_remote,
            date_posted=date_posted,
            job_url=f"{self.base_url}/jobs/view/{job_id}",
            compensation=compensation,
            salary_source=SalarySource.DIRECT_DATA if compensation else None,
            job_type=job_details.get("job_type"),
            job_level=(job_details.get("job_level") or "").lower() or None,
            company_industry=job_details.get("company_industry"),
            description=job_details.get("description"),
            job_url_direct=job_details.get("job_url_direct"),
            emails=extract_emails_from_text(description or ""),
            company_logo=job_details.get("company_logo"),
            job_function=job_details.get("job_function"),
        )

    def _get_job_details(self, job_id: str) -> dict:
        """
        Retrieves job description and other job details by going to the job page url
        :param job_page_url:
        :return: dict
        """
        request = self.scraper_input
        if request is None:
            return {}
        try:
            return self._request_job_details(job_id, request)
        except Exception:
            # Search remains best-effort: a detail failure must not discard the
            # listing card. The targeted public API below preserves typed
            # failures for callers that require detail before a decision.
            return {}

    def fetch_job_detail(
        self,
        job: JobPost,
        request: SearchRequest,
    ) -> JobPost | None:
        """Enrich one LinkedIn listing without repeating its search query."""

        if request.site_type != (Site.LINKEDIN,):
            raise InvalidRequestError(
                "LinkedIn targeted detail requires a LinkedIn-only request"
            )
        job_id = self._targeted_job_id(job)
        details = self._request_job_details(job_id, request)
        description = details.get("description")
        if not isinstance(description, str) or not description.strip():
            return None

        emails = extract_emails_from_text(description)
        updates = {
            "description": description,
            "emails": tuple(emails) if emails else None,
            "is_remote": bool(job.is_remote)
            or is_job_remote(job.title, description, job.location or Location()),
        }
        optional_fields = {
            "job_type": details.get("job_type"),
            "job_level": (details.get("job_level") or "").lower() or None,
            "company_industry": details.get("company_industry"),
            "job_url_direct": details.get("job_url_direct"),
            "company_logo": details.get("company_logo"),
            "job_function": details.get("job_function"),
        }
        updates.update(
            {
                field: value
                for field, value in optional_fields.items()
                if value is not None
            }
        )
        return job.model_copy(update=updates)

    @staticmethod
    def _targeted_job_id(job: JobPost) -> str:
        if job.id:
            match = re.fullmatch(r"li-(\d+)", job.id.strip())
            if match:
                return match.group(1)
        path_id = urlparse(job.job_url).path.rstrip("/").split("-")[-1]
        if path_id.isdigit():
            return path_id
        raise InvalidRequestError(
            "LinkedIn targeted detail requires a canonical numeric job id"
        )

    def _request_job_details(
        self,
        job_id: str,
        request: SearchRequest,
    ) -> dict:
        response = self.session.get(
            f"{self.base_url}/jobs/view/{job_id}",
            timeout=request.request_timeout,
        )
        status_code = int(getattr(response, "status_code", 200))
        if status_code not in range(200, 400):
            raise error_for_http_status(
                "LinkedIn",
                status_code,
                retry_after=(getattr(response, "headers", {}) or {}).get("Retry-After"),
            )
        if "linkedin.com/signup" in str(getattr(response, "url", "")):
            raise AuthenticationConfigurationError(
                "LinkedIn redirected the detail request to sign-up"
            )

        soup = BeautifulSoup(response.text, "html.parser")
        div_content = soup.find(
            "div", class_=lambda x: x and "show-more-less-html__markup" in x
        )
        description = None
        if div_content is not None:
            div_content = remove_attributes(div_content)
            description = div_content.prettify(formatter="html")
            if request.description_format == DescriptionFormat.MARKDOWN:
                description = markdown_converter(description)
            elif request.description_format == DescriptionFormat.PLAIN:
                description = plain_converter(description)
        h3_tag = soup.find(
            "h3", text=lambda text: text and "Job function" in text.strip()
        )

        job_function = None
        if h3_tag:
            job_function_span = h3_tag.find_next(
                "span", class_="description__job-criteria-text"
            )
            if job_function_span:
                job_function = job_function_span.text.strip()

        company_logo = (
            logo_image.get("data-delayed-url")
            if (logo_image := soup.find("img", {"class": "artdeco-entity-image"}))
            else None
        )
        return {
            "description": description,
            "job_level": parse_job_level(soup),
            "company_industry": parse_company_industry(soup),
            "job_type": parse_job_type(soup),
            "job_url_direct": self._parse_job_url_direct(soup),
            "company_logo": company_logo,
            "job_function": job_function,
        }

    def _get_location(self, metadata_card: Tag | None) -> Location:
        """
        Extracts the location data from the job metadata card.
        :param metadata_card
        :return: location
        """
        location = Location(country=Country.from_string(self.country))
        if metadata_card is not None:
            location_tag = metadata_card.find(
                "span", class_="job-search-card__location"
            )
            location_string = location_tag.text.strip() if location_tag else "N/A"
            parts = location_string.split(", ")
            if len(parts) == 2:
                city, region_or_country = parts
                is_region_code = (
                    len(region_or_country) <= 3
                    and region_or_country.upper() == region_or_country
                )
                location = Location(
                    city=city,
                    state=region_or_country if is_region_code else None,
                    country=(
                        Country.from_string(self.country)
                        if is_region_code
                        else region_or_country
                    ),
                )
            elif len(parts) >= 3:
                city, state, country, *_ = parts
                try:
                    country_enum = Country.from_string(country)
                except ValueError:
                    country_enum = country
                location = Location(city=city, state=state, country=country_enum)
            elif len(parts) == 1 and parts[0] != "N/A":
                location = Location(city=parts[0], country=Country.WORLDWIDE)
        return location

    def _parse_job_url_direct(self, soup: BeautifulSoup) -> str | None:
        """
        Gets the job url direct from job page
        :param soup:
        :return: str
        """
        job_url_direct = None
        job_url_direct_content = soup.find("code", id="applyUrl")
        if job_url_direct_content:
            job_url_direct_match = self.job_url_direct_regex.search(
                job_url_direct_content.decode_contents().strip()
            )
            if job_url_direct_match:
                job_url_direct = unquote(job_url_direct_match.group(1))

        return job_url_direct
