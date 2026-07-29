from __future__ import annotations

import random
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import parse_qs, urljoin, urlsplit

from bs4 import BeautifulSoup
from bs4.element import Tag

from jobstreaming.bdjobs.constant import headers, search_params
from jobstreaming.bdjobs.util import (
    find_job_listings,
    is_job_remote,
    parse_date,
    parse_location,
)
from jobstreaming.exception import BDJobsException
from jobstreaming.model import (
    AdapterCapabilities,
    DescriptionFormat,
    JobPost,
    JobResponse,
    JobType,
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
    stable_job_id,
)

log = create_logger("BDJobs")


class BDJobs(Scraper):
    base_url = "https://jobs.bdjobs.com"
    search_url = "https://jobs.bdjobs.com/jobsearch.asp"
    delay = 2
    band_delay = 3
    capabilities = AdapterCapabilities(
        filters=frozenset({"offset", "description_format"}),
        supports_resume=True,
        resume_granularity="page",
        cursor_schema_version=1,
    )

    def __init__(
        self,
        proxies: list[str] | str | None = None,
        ca_cert: str | None = None,
        user_agent: str | None = None,
    ) -> None:
        super().__init__(
            Site.BDJOBS,
            proxies=proxies,
            ca_cert=ca_cert,
            user_agent=user_agent,
        )
        self.session = self._new_session()
        self.scraper_input: ScraperInput | None = None
        self._thread_local = threading.local()

    def _new_session(self):
        session = create_session(
            proxies=self.proxies,
            ca_cert=self.ca_cert,
            is_tls=False,
            clear_cookies=True,
        )
        request_headers = headers.copy()
        if self.user_agent:
            request_headers["User-Agent"] = self.user_agent
        session.headers.update(request_headers)
        return self.track_transport(session)

    def scrape(
        self, scraper_input: ScraperInput, context: ScrapeContext | None = None
    ) -> JobResponse:
        self.scraper_input = scraper_input
        context = ScrapeContext.local(self.site, scraper_input, context)
        emitted: list[JobPost] = []
        state = context.resume_state
        page = int(state.get("page", 1))
        raw_seen = int(state.get("raw_seen", 0))

        while context.should_continue and page <= scraper_input.max_pages:
            params = search_params.copy()
            params["txtsearch"] = scraper_input.search_term or ""
            if page > 1:
                params["pg"] = page
            response = self.session.get(
                self.search_url,
                params=params,
                timeout=scraper_input.request_timeout,
            )
            if response.status_code != 200:
                raise BDJobsException(f"BDJobs returned HTTP {response.status_code}")
            cards = find_job_listings(BeautifulSoup(response.text, "html.parser"))
            if not cards:
                break

            page_state = {"page": page, "raw_seen": raw_seen}
            candidates = [
                card
                for index, card in enumerate(cards)
                if raw_seen + index >= scraper_input.offset
            ]
            with (
                self.transport_scope(),
                ThreadPoolExecutor(max_workers=min(8, max(1, len(candidates)))) as pool,
            ):
                futures = {
                    pool.submit(self._process_job, card): card for card in candidates
                }
                for future in as_completed(futures):
                    if not context.should_continue:
                        break
                    try:
                        job = future.result()
                        if job and context.emit_job(job, page_state):
                            emitted.append(job)
                    except Exception as exc:
                        context.emit_warning(
                            f"Skipped BDJobs listing: {type(exc).__name__}: {exc}"
                        )

            raw_seen += len(cards)
            context.emit_progress(
                {"page": page + 1, "raw_seen": raw_seen},
                f"completed BDJobs page {page}",
            )
            if not context.should_continue:
                break
            page += 1
            if not context.wait(
                random.uniform(self.delay, self.delay + self.band_delay)
            ):
                break

        return JobResponse(jobs=emitted)

    def _process_job(self, job_card: Tag) -> JobPost | None:
        assert self.scraper_input is not None
        job_link = job_card.find(
            "a", href=lambda value: value and "jobdetail" in value.lower()
        )
        if not isinstance(job_link, Tag):
            return None
        href = job_link.get("href")
        if not isinstance(href, str):
            return None
        job_url = urljoin(self.base_url, href)
        title = job_link.get_text(strip=True)
        if not title:
            return None

        company_element = job_card.find(
            ["span", "div"],
            class_=lambda value: value
            and any(
                marker
                in " ".join(value if isinstance(value, list) else [value]).lower()
                for marker in ("comp-name-text", "company", "org", "comp-name")
            ),
        )
        company_name = company_element.get_text(strip=True) if company_element else None

        location_element = job_card.find(
            ["span", "div"],
            class_=lambda value: value
            and any(
                marker
                in " ".join(value if isinstance(value, list) else [value]).lower()
                for marker in ("locon-text-d", "location", "area", "locon")
            ),
        )
        location = parse_location(
            location_element.get_text(strip=True) if location_element else None
        )

        published_element = job_card.find(
            ["span", "div"],
            class_=lambda value: value
            and "published"
            in " ".join(value if isinstance(value, list) else [value]).lower(),
        )
        date_posted = (
            parse_date(published_element.get_text(strip=True))
            if published_element
            else None
        )
        details = self._get_job_details(job_url)
        description = details.get("description")
        is_remote = is_job_remote(title, description=description, location=location)

        query = {
            key.lower(): value
            for key, value in parse_qs(urlsplit(job_url).query).items()
        }
        source_id = query.get("jobid", [None])[0]
        job_id = f"bd-{source_id}" if source_id else stable_job_id("bdjobs", job_url)
        return JobPost(
            id=job_id,
            title=title,
            company_name=company_name or None,
            location=location,
            date_posted=date_posted,
            job_url=job_url,
            is_remote=is_remote,
            description=description,
            emails=extract_emails_from_text(description or ""),
            job_type=details.get("job_type"),
            company_industry=details.get("company_industry"),
        )

    def _get_job_details(self, job_url: str) -> dict[str, object]:
        assert self.scraper_input is not None
        session = self._detail_session()
        response = session.get(job_url, timeout=self.scraper_input.request_timeout)
        if response.status_code != 200:
            raise BDJobsException(
                f"BDJobs detail page returned HTTP {response.status_code}"
            )
        soup = BeautifulSoup(response.text, "html.parser")

        content = soup.find("div", class_="jobcontent") or soup.find(
            ["div", "section"],
            class_=lambda value: value
            and any(
                marker
                in " ".join(value if isinstance(value, list) else [value]).lower()
                for marker in ("job-description", "details", "requirements")
            ),
        )
        description = None
        if isinstance(content, Tag):
            for element in content.find_all(["script", "style"]):
                element.decompose()
            html = str(content)
            if self.scraper_input.description_format == DescriptionFormat.MARKDOWN:
                description = markdown_converter(html)
            elif self.scraper_input.description_format == DescriptionFormat.PLAIN:
                description = plain_converter(html)
            else:
                description = html

        job_type = None
        label = soup.find(
            ["span", "div"],
            string=lambda value: value
            and any(
                marker in value.lower() for marker in ("job type", "employment type")
            ),
        )
        if label:
            value_element = label.find_next(["span", "div"])
            value = value_element.get_text(strip=True) if value_element else ""
            try:
                job_type = (JobType.from_string(value),) if value else None
            except ValueError:
                job_type = None

        industry = None
        industry_label = soup.find(
            ["span", "div"],
            string=lambda value: value and "industry" in value.lower(),
        )
        if industry_label:
            industry_element = industry_label.find_next(["span", "div"])
            industry = (
                industry_element.get_text(strip=True) if industry_element else None
            )
        return {
            "description": description,
            "job_type": job_type,
            "company_industry": industry,
        }

    def _detail_session(self):
        session = getattr(self._thread_local, "session", None)
        if session is None:
            session = self._new_session()
            self._thread_local.session = session
        return session
