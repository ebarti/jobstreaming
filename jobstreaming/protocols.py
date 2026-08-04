from __future__ import annotations

from typing import Protocol, runtime_checkable

from jobstreaming.context import ScrapeContext
from jobstreaming.model import (
    AdapterCapabilities,
    AdapterIdentifier,
    JobResponse,
    SearchRequest,
)


@runtime_checkable
class Adapter(Protocol):
    """Structural contract implemented by built-in and third-party adapters."""

    @property
    def site(self) -> AdapterIdentifier: ...

    @property
    def capabilities(self) -> AdapterCapabilities: ...

    def scrape(
        self,
        scraper_input: SearchRequest,
        context: ScrapeContext | None = None,
    ) -> JobResponse: ...


class AdapterFactory(Protocol):
    def __call__(
        self,
        proxies: list[str] | str | None = None,
        ca_cert: str | None = None,
        user_agent: str | None = None,
    ) -> Adapter: ...
