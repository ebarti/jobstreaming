from __future__ import annotations

import random
from urllib.parse import quote, urljoin

from bs4 import BeautifulSoup

from jobstreaming.model import (
    AdapterCapabilities,
    Country,
    JobPost,
    JobResponse,
    Location,
    Scraper,
    ScraperInput,
    Site,
)
from jobstreaming.runtime import ScrapeContext
from jobstreaming.util import create_logger, create_session, stable_job_id

log = create_logger("Bayt")


class BaytScraper(Scraper):
    capabilities = AdapterCapabilities(
        filters=frozenset({"offset"}),
        supports_resume=True,
        resume_granularity="page",
        cursor_schema_version=1,
    )
    base_url = "https://www.bayt.com"
    delay = 2
    band_delay = 3

    def __init__(
        self,
        proxies: list[str] | str | None = None,
        ca_cert: str | None = None,
        user_agent: str | None = None,
    ):
        super().__init__(
            Site.BAYT,
            proxies=proxies,
            ca_cert=ca_cert,
            user_agent=user_agent,
        )
        self.scraper_input = None
        self.session = None
        self.country = "worldwide"

    def scrape(
        self, scraper_input: ScraperInput, context: ScrapeContext | None = None
    ) -> JobResponse:
        self.scraper_input = scraper_input
        context = ScrapeContext.local(self.site, scraper_input, context)
        self.session = create_session(
            proxies=self.proxies, ca_cert=self.ca_cert, is_tls=False, has_retry=True
        )
        if self.user_agent:
            self.session.headers.update({"user-agent": self.user_agent})
        job_list: list[JobPost] = []
        state = context.resume_state
        page = int(state.get("page", 1))
        page_skip = int(state.get("page_skip", 0))
        raw_seen = int(state.get("raw_seen", 0))

        while context.should_continue and page <= scraper_input.max_pages:
            log.info(f"Fetching Bayt jobs page {page}")
            job_elements = self._fetch_jobs(self.scraper_input.search_term, page)
            if not job_elements:
                break

            if job_elements:
                log.debug(
                    "First job element snippet:\n" + job_elements[0].prettify()[:500]
                )

            page_raw_start = max(0, raw_seen - page_skip)
            for index, job in enumerate(job_elements):
                absolute_index = page_raw_start + index
                try:
                    job_post = self._extract_job_info(job)
                    if job_post and absolute_index >= scraper_input.offset:
                        next_state = {
                            "page": page,
                            "page_skip": index + 1,
                            "raw_seen": absolute_index + 1,
                        }
                        if context.emit_job(job_post, next_state):
                            job_list.append(job_post)
                    if not context.should_continue:
                        break
                    if not job_post:
                        log.debug(
                            "Extraction returned None. Job snippet:\n"
                            + job.prettify()[:500]
                        )
                except Exception as e:
                    context.emit_warning(
                        f"Skipped Bayt listing: {type(e).__name__}: {e}"
                    )
                    continue

            raw_seen = page_raw_start + len(job_elements)
            context.emit_progress(
                {"page": page + 1, "page_skip": 0, "raw_seen": raw_seen},
                f"completed Bayt page {page}",
            )
            if not context.should_continue:
                break
            page += 1
            page_skip = 0
            if not context.wait(
                random.uniform(self.delay, self.delay + self.band_delay)
            ):
                break

        return JobResponse(jobs=job_list)

    def _fetch_jobs(self, query: str | None, page: int) -> list:
        """
        Grabs the job results for the given query and page number.
        """
        encoded_query = quote((query or "").strip(), safe="")
        url = f"{self.base_url}/en/international/jobs/{encoded_query}-jobs/?page={page}"
        response = self.session.get(url, timeout=self.scraper_input.request_timeout)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
        job_listings = soup.find_all("li", attrs={"data-js-job": ""})
        log.debug(f"Found {len(job_listings)} job listing elements")
        return job_listings

    def _extract_job_info(self, job: BeautifulSoup) -> JobPost | None:
        """
        Extracts the job information from a single job listing.
        """
        # Find the h2 element holding the title and link (no class filtering)
        job_general_information = job.find("h2")
        if not job_general_information:
            return

        job_title = job_general_information.get_text(strip=True)
        job_url = self._extract_job_url(job_general_information)
        if not job_url:
            return

        # Extract company name using the original approach:
        company_tag = job.find("div", class_="t-nowrap p10l")
        company_name = (
            company_tag.find("span").get_text(strip=True)
            if company_tag and company_tag.find("span")
            else None
        )

        # Extract location using the original approach:
        location_tag = job.find("div", class_="t-mute t-small")
        location = location_tag.get_text(strip=True) if location_tag else None

        job_id = stable_job_id("bayt", job_url)
        location_parts = [part.strip() for part in (location or "").split(",")]
        location_parts = [part for part in location_parts if part]
        location_obj = (
            Location(
                city=location_parts[0] if location_parts else None,
                state=(
                    ", ".join(location_parts[1:-1]) if len(location_parts) > 2 else None
                ),
                country=(
                    location_parts[-1] if len(location_parts) > 1 else Country.WORLDWIDE
                ),
            )
            if location_parts
            else None
        )
        return JobPost(
            id=job_id,
            title=job_title,
            company_name=company_name,
            location=location_obj,
            job_url=job_url,
        )

    def _extract_job_url(self, job_general_information: BeautifulSoup) -> str | None:
        """
        Pulls the job URL from the 'a' within the h2 element.
        """
        a_tag = job_general_information.find("a")
        if a_tag and a_tag.has_attr("href"):
            return urljoin(self.base_url, a_tag["href"].strip())
