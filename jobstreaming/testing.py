from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

from jobstreaming.events import AdapterCheckpoint
from jobstreaming.model import (
    AdapterIdentifier,
    AdapterIdentifierInput,
    JobResponse,
    SearchRequest,
    parse_adapter_identifier,
)
from jobstreaming.protocols import Adapter, AdapterFactory
from jobstreaming.registry import AdapterRegistry, _validate_adapter_instance
from jobstreaming.runtime import ScrapeContext


class AdapterContractViolation(AssertionError):
    """Raised when an adapter fails the public SDK contract test kit."""


@dataclass(frozen=True, slots=True)
class AdapterRun:
    response: JobResponse
    context: ScrapeContext


class AdapterTestKit:
    """Framework-neutral helpers for offline third-party adapter tests."""

    @staticmethod
    def assert_conforms(
        identifier: AdapterIdentifierInput,
        factory: AdapterFactory | Callable[..., Any],
    ) -> Adapter:
        try:
            expected = parse_adapter_identifier(identifier)
            registry = AdapterRegistry()
            registry.register(expected, factory)
            adapter = registry.create(expected)
        except (AttributeError, TypeError, ValueError) as exc:
            raise AdapterContractViolation(str(exc)) from exc
        return adapter

    @staticmethod
    def context(
        request: SearchRequest,
        *,
        identifier: AdapterIdentifierInput | None = None,
        resume_state: Mapping[str, Any] | None = None,
    ) -> ScrapeContext:
        try:
            adapter_id: AdapterIdentifier = (
                parse_adapter_identifier(identifier)
                if identifier is not None
                else request.sites[0]
            )
        except (AttributeError, TypeError, ValueError) as exc:
            raise AdapterContractViolation(str(exc)) from exc
        if adapter_id not in request.sites:
            raise AdapterContractViolation(
                f"{adapter_id.value} is not included in the search request"
            )
        try:
            checkpoint = (
                AdapterCheckpoint(site=adapter_id, state=dict(resume_state))
                if resume_state is not None
                else None
            )
            return ScrapeContext(
                site=adapter_id,
                request=request,
                checkpoint=checkpoint,
            )
        except (AttributeError, TypeError, ValueError) as exc:
            raise AdapterContractViolation(str(exc)) from exc

    @classmethod
    def run(
        cls,
        adapter: Adapter,
        request: SearchRequest,
        *,
        resume_state: Mapping[str, Any] | None = None,
    ) -> AdapterRun:
        try:
            validated, _ = _validate_adapter_instance(adapter)
        except (AttributeError, TypeError, ValueError) as exc:
            raise AdapterContractViolation(str(exc)) from exc
        context = cls.context(
            request,
            identifier=validated.site,
            resume_state=resume_state,
        )
        try:
            response = validated.scrape(request, context=context)
        except TypeError as exc:
            raise AdapterContractViolation(str(exc)) from exc
        if not isinstance(response, JobResponse):
            raise AdapterContractViolation("Adapter scrape must return JobResponse")
        return AdapterRun(
            response=response,
            context=context,
        )
