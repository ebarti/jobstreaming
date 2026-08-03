from __future__ import annotations

from jobstreaming import (
    Adapter,
    AdapterCapabilities,
    AdapterFactory,
    AdapterId,
    JobResponse,
    NoResume,
    ProgressEvent,
    ProgressPhase,
    ProgressUnit,
    ProviderProgress,
    ScrapeContext,
    SearchFilter,
    SearchRequest,
)


class TypedFixtureAdapter:
    capabilities = AdapterCapabilities(
        filters=frozenset({SearchFilter.SEARCH_TERM}),
        resume=NoResume(),
    )

    def __init__(
        self,
        proxies: list[str] | str | None = None,
        ca_cert: str | None = None,
        user_agent: str | None = None,
    ) -> None:
        del proxies, ca_cert, user_agent
        self.site = AdapterId("typing.fixture")

    def scrape(
        self,
        scraper_input: SearchRequest,
        context: ScrapeContext | None = None,
    ) -> JobResponse:
        del scraper_input, context
        return JobResponse()


factory: AdapterFactory = TypedFixtureAdapter
adapter: Adapter = factory()
progress: ProviderProgress = ProviderProgress(
    phase=ProgressPhase.SEARCH,
    unit=ProgressUnit.PAGE,
    completed_units=1,
    raw_items_seen=None,
    jobs_emitted=0,
    has_more=None,
)
progress_event: ProgressEvent
