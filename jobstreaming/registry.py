from __future__ import annotations

from collections.abc import Callable
from typing import Any

from jobstreaming.model import Scraper, Site

AdapterFactory = Callable[..., Scraper]


class AdapterRegistry:
    def __init__(self) -> None:
        self._factories: dict[Site, AdapterFactory] = {}

    def register(
        self, site: Site, factory: AdapterFactory, *, replace: bool = False
    ) -> None:
        if site in self._factories and not replace:
            raise ValueError(f"An adapter is already registered for {site.value}")
        self._factories[site] = factory

    def create(self, site: Site, **kwargs: Any) -> Scraper:
        try:
            factory = self._factories[site]
        except KeyError as exc:
            raise ValueError(f"No adapter registered for {site.value}") from exc
        return factory(**kwargs)

    @property
    def sites(self) -> tuple[Site, ...]:
        return tuple(self._factories)

    def copy(self) -> AdapterRegistry:
        registry = AdapterRegistry()
        registry._factories = self._factories.copy()
        return registry


def default_registry() -> AdapterRegistry:
    from jobstreaming.bayt import BaytScraper
    from jobstreaming.bdjobs import BDJobs
    from jobstreaming.glassdoor import Glassdoor
    from jobstreaming.google import Google
    from jobstreaming.indeed import Indeed
    from jobstreaming.linkedin import LinkedIn
    from jobstreaming.naukri import Naukri
    from jobstreaming.ziprecruiter import ZipRecruiter

    registry = AdapterRegistry()
    registry.register(Site.LINKEDIN, LinkedIn)
    registry.register(Site.INDEED, Indeed)
    registry.register(Site.ZIP_RECRUITER, ZipRecruiter)
    registry.register(Site.GLASSDOOR, Glassdoor)
    registry.register(Site.GOOGLE, Google)
    registry.register(Site.BAYT, BaytScraper)
    registry.register(Site.NAUKRI, Naukri)
    registry.register(Site.BDJOBS, BDJobs)
    return registry
