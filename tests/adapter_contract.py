from __future__ import annotations

import inspect
from collections.abc import Callable

from jobstreaming import AdapterRegistry, Scraper, SearchRequest, Site


def assert_adapter_declaration(
    registry: AdapterRegistry,
    site: Site,
    factory: Callable[..., Scraper],
) -> None:
    """Assert the common construction and capability contract for an adapter."""

    adapter = registry.create(site)
    assert isinstance(adapter, Scraper)
    assert adapter.site is site
    assert type(adapter).capabilities == factory.capabilities
    assert registry.cursor_schema_version(site) == (
        factory.capabilities.cursor_schema_version
    )

    constructor = inspect.signature(factory).parameters
    assert {"proxies", "ca_cert", "user_agent"} <= set(constructor)

    scrape = inspect.signature(adapter.scrape).parameters
    assert tuple(scrape)[:2] == ("scraper_input", "context")

    capabilities = adapter.capabilities
    known_filters = set(SearchRequest.model_fields) - {"site_type"}
    assert capabilities.filters <= known_filters
    assert capabilities.supports_resume is bool(capabilities.resume_granularity)
    if capabilities.supported_job_types is not None:
        assert "job_type" in capabilities.filters
