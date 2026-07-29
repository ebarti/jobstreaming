from __future__ import annotations

import inspect
import threading
import warnings
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, cast

from jobstreaming.model import (
    AdapterCapabilities,
    AdapterId,
    AdapterIdentifier,
    AdapterIdentifierInput,
    JobResponse,
    NoResume,
    SearchRequest,
    Site,
    parse_adapter_identifier,
)
from jobstreaming.protocols import Adapter, AdapterFactory


def _legacy_capabilities(
    capabilities: AdapterCapabilities,
) -> AdapterCapabilities:
    return AdapterCapabilities(
        filters=capabilities.filters,
        supported_job_types=capabilities.supported_job_types,
        resume=NoResume(),
    )


def _read_capabilities(
    subject: Any,
    *,
    role: str,
    required: bool,
) -> AdapterCapabilities | None:
    capabilities = getattr(subject, "capabilities", None)
    if capabilities is None and not required:
        return None
    if not isinstance(capabilities, AdapterCapabilities):
        requirement = "must declare" if capabilities is None else "must expose"
        raise TypeError(f"{role} {requirement} capabilities as AdapterCapabilities")
    return capabilities


def _validate_adapter_instance(
    adapter: Any,
    *,
    expected: AdapterIdentifier | None = None,
) -> tuple[Adapter, AdapterCapabilities]:
    site = getattr(adapter, "site", None)
    if not isinstance(site, (Site, AdapterId)):
        raise TypeError("Adapter site must be a Site or AdapterId")
    capabilities = _read_capabilities(
        adapter,
        role=f"Adapter for {site.value}",
        required=True,
    )
    assert capabilities is not None
    if not callable(getattr(adapter, "scrape", None)):
        raise TypeError(f"Adapter for {site.value} must expose a callable scrape")
    if not isinstance(adapter, Adapter):
        raise TypeError(f"Adapter for {site.value} does not implement the contract")
    if expected is not None and site != expected:
        raise ValueError(f"Adapter factory for {expected.value} produced {site.value}")
    return cast(Adapter, adapter), capabilities


class _LegacyAdapter:
    def __init__(self, adapter: Adapter, capabilities: AdapterCapabilities) -> None:
        self._adapter = adapter
        self.site = adapter.site
        self.capabilities = capabilities

    def scrape(self, scraper_input: SearchRequest, context: Any = None) -> JobResponse:
        del context
        return cast(JobResponse, self._adapter.scrape(scraper_input))


class _LegacyAdapterFactory:
    def __init__(
        self,
        factory: Callable[..., Any],
        *,
        capabilities: AdapterCapabilities | None = None,
    ) -> None:
        self._factory = factory
        declared = capabilities or _read_capabilities(
            factory,
            role="Legacy adapter factory",
            required=False,
        )
        self._declared_capabilities = declared
        self.capabilities = _legacy_capabilities(declared or AdapterCapabilities())

    def __call__(
        self,
        proxies: list[str] | str | None = None,
        ca_cert: str | None = None,
        user_agent: str | None = None,
        **kwargs: Any,
    ) -> Adapter:
        raw_adapter = self._factory(
            proxies=proxies,
            ca_cert=ca_cert,
            user_agent=user_agent,
            **kwargs,
        )
        adapter, capabilities = _validate_adapter_instance(raw_adapter)
        if (
            self._declared_capabilities is not None
            and capabilities != self._declared_capabilities
        ):
            raise TypeError(
                "Legacy adapter factory capabilities do not match the "
                "produced adapter"
            )
        return _LegacyAdapter(adapter, _legacy_capabilities(capabilities))

    @property
    def has_declared_capabilities(self) -> bool:
        return self._declared_capabilities is not None


def legacy_adapter(factory: Callable[..., Any]) -> AdapterFactory:
    """
    Explicitly adapt a pre-context ``scrape(request)`` implementation.

    The compatibility wrapper cannot provide incremental emission or resume, so
    it intentionally declares ``NoResume``. Prefer implementing ``Adapter``
    directly and treat this helper as a migration boundary.
    """

    if not callable(factory):
        raise TypeError("Legacy adapter factory must be callable")
    return _LegacyAdapterFactory(factory)


@dataclass(frozen=True, slots=True)
class _Registration:
    identifier: AdapterIdentifier
    factory: AdapterFactory
    capabilities: AdapterCapabilities
    capabilities_declared: bool
    cursor_schema_version: int
    cursor_schema_overridden: bool


def _accepts_context(subject: Any) -> bool:
    scrape = getattr(subject, "scrape", None)
    if not callable(scrape):
        return True
    try:
        parameters = inspect.signature(scrape).parameters
    except (TypeError, ValueError):
        return True
    return "context" in parameters or any(
        parameter.kind is inspect.Parameter.VAR_KEYWORD
        for parameter in parameters.values()
    )


class AdapterRegistry:
    def __init__(self) -> None:
        self._registrations: dict[str, _Registration] = {}
        self._contract_lock = threading.RLock()

    def register(
        self,
        site: AdapterIdentifierInput,
        factory: AdapterFactory | Callable[..., Any],
        *,
        replace: bool = False,
        cursor_schema_version: int | None = None,
    ) -> None:
        identifier = parse_adapter_identifier(site)
        key = identifier.value
        if not callable(factory):
            raise TypeError(f"Adapter factory for {key} must be callable")

        typed_factory: AdapterFactory
        if isinstance(factory, _LegacyAdapterFactory):
            typed_factory = factory
        elif not _accepts_context(factory):
            warnings.warn(
                "Implicit legacy adapter detection is deprecated and will be "
                "removed in 1.0; implement scrape(request, context=None) or "
                "register legacy_adapter(factory) explicitly",
                DeprecationWarning,
                stacklevel=2,
            )
            typed_factory = legacy_adapter(factory)
        else:
            typed_factory = cast(AdapterFactory, factory)

        capabilities_declared = (
            typed_factory.has_declared_capabilities
            if isinstance(typed_factory, _LegacyAdapterFactory)
            else getattr(typed_factory, "capabilities", None) is not None
        )
        capabilities = _read_capabilities(
            typed_factory,
            role=f"Adapter factory for {key}",
            required=capabilities_declared,
        )
        if capabilities is None:
            capabilities = AdapterCapabilities()
        declared_version = capabilities.cursor_schema_version
        if cursor_schema_version is not None:
            if (
                not isinstance(cursor_schema_version, int)
                or isinstance(cursor_schema_version, bool)
                or cursor_schema_version < 1
            ):
                raise ValueError("cursor_schema_version must be a positive integer")
            if (
                capabilities_declared
                and not isinstance(typed_factory, _LegacyAdapterFactory)
                and cursor_schema_version != declared_version
            ):
                raise ValueError(
                    f"cursor_schema_version override {cursor_schema_version} "
                    f"contradicts the declared adapter capability version "
                    f"{declared_version}"
                )
            warnings.warn(
                "register(..., cursor_schema_version=...) is deprecated; declare "
                "the version in AdapterCapabilities.resume",
                DeprecationWarning,
                stacklevel=2,
            )
            declared_version = cursor_schema_version

        registration = _Registration(
            identifier=identifier,
            factory=typed_factory,
            capabilities=capabilities,
            capabilities_declared=capabilities_declared,
            cursor_schema_version=declared_version,
            cursor_schema_overridden=cursor_schema_version is not None,
        )
        with self._contract_lock:
            if key in self._registrations and not replace:
                raise ValueError(f"An adapter is already registered for {key}")
            self._registrations[key] = registration

    def create(self, site: AdapterIdentifierInput, **kwargs: Any) -> Adapter:
        identifier = parse_adapter_identifier(site)
        key = identifier.value
        with self._contract_lock:
            try:
                registration = self._registrations[key]
            except KeyError as exc:
                raise ValueError(f"No adapter registered for {key}") from exc
        factory = cast(Callable[..., Adapter], registration.factory)
        raw_adapter = factory(**kwargs)
        adapter, capabilities = _validate_adapter_instance(
            raw_adapter,
            expected=registration.identifier,
        )
        accepts_context = _accepts_context(adapter)
        effective_capabilities = (
            _legacy_capabilities(capabilities) if not accepts_context else capabilities
        )
        with self._contract_lock:
            registration = self._registrations[key]
            if (
                not registration.capabilities_declared
                and capabilities.cursor_schema_version
                != registration.cursor_schema_version
                and (accepts_context or not registration.cursor_schema_overridden)
            ):
                raise TypeError(
                    f"Adapter factory for {identifier.value} produced cursor schema "
                    f"version {capabilities.cursor_schema_version}, but registration "
                    f"uses {registration.cursor_schema_version}; declare matching "
                    "factory capabilities before scraping"
                )
            if (
                registration.capabilities_declared
                and effective_capabilities != registration.capabilities
            ):
                raise TypeError(
                    f"Adapter factory for {identifier.value} declared capabilities "
                    "that do not match the produced adapter"
                )
            if not accepts_context:
                warnings.warn(
                    "Implicit legacy adapter detection is deprecated and will be "
                    "removed in 1.0; implement scrape(request, context=None) or "
                    "register legacy_adapter(factory) explicitly",
                    DeprecationWarning,
                    stacklevel=2,
                )
                if not registration.capabilities_declared:
                    adapted_factory = _LegacyAdapterFactory(
                        registration.factory,
                        capabilities=capabilities,
                    )
                    self._registrations[key] = _Registration(
                        identifier=registration.identifier,
                        factory=adapted_factory,
                        capabilities=effective_capabilities,
                        capabilities_declared=True,
                        cursor_schema_version=registration.cursor_schema_version,
                        cursor_schema_overridden=registration.cursor_schema_overridden,
                    )
                adapter = _LegacyAdapter(adapter, effective_capabilities)
            elif not registration.capabilities_declared:
                self._registrations[key] = _Registration(
                    identifier=registration.identifier,
                    factory=registration.factory,
                    capabilities=capabilities,
                    capabilities_declared=True,
                    cursor_schema_version=registration.cursor_schema_version,
                    cursor_schema_overridden=registration.cursor_schema_overridden,
                )
        return adapter

    @property
    def sites(self) -> tuple[AdapterIdentifier, ...]:
        with self._contract_lock:
            return tuple(
                registration.identifier for registration in self._registrations.values()
            )

    def cursor_schema_version(self, site: AdapterIdentifierInput) -> int:
        identifier = parse_adapter_identifier(site)
        with self._contract_lock:
            try:
                return self._registrations[identifier.value].cursor_schema_version
            except KeyError as exc:
                raise ValueError(
                    f"No adapter registered for {identifier.value}"
                ) from exc

    def copy(self) -> AdapterRegistry:
        registry = AdapterRegistry()
        with self._contract_lock:
            registry._registrations = self._registrations.copy()
        return registry


def default_registry() -> AdapterRegistry:
    from jobstreaming.bayt import BaytScraper
    from jobstreaming.bdjobs import BDJobs
    from jobstreaming.glassdoor import Glassdoor
    from jobstreaming.google import Google
    from jobstreaming.indeed import Indeed
    from jobstreaming.linkedin import LinkedIn
    from jobstreaming.model import Site
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
