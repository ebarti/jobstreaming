from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Any, get_type_hints

import pytest
from pydantic import ValidationError

from jobstreaming import (
    Adapter,
    AdapterCapabilities,
    AdapterContractViolation,
    AdapterId,
    AdapterRegistry,
    AdapterTestKit,
    JobEvent,
    JobPost,
    JobResponse,
    MemoryCheckpointStore,
    NoResume,
    Resumable,
    ResumeGranularity,
    ScrapeContext,
    Scraper,
    SearchCheckpoint,
    SearchFilter,
    SearchRequest,
    SearchStream,
    Site,
    SqliteCheckpointStore,
    WarningEvent,
    collect_jobs,
    legacy_adapter,
    parse_adapter_identifier,
    stream_search,
)

CUSTOM_ID = AdapterId("acme.jobs")


class FixtureAdapter:
    capabilities = AdapterCapabilities(
        filters=frozenset({SearchFilter.SEARCH_TERM, SearchFilter.OFFSET}),
        resume=Resumable(granularity=ResumeGranularity.LISTING),
    )

    def __init__(
        self,
        proxies: list[str] | str | None = None,
        ca_cert: str | None = None,
        user_agent: str | None = None,
        **kwargs: Any,
    ) -> None:
        del proxies, ca_cert, user_agent, kwargs
        self.site = CUSTOM_ID

    def scrape(self, scraper_input, context=None) -> JobResponse:
        assert context is not None
        start = int(context.resume_state.get("listing", 0))
        job = JobPost(
            id=f"acme-{start + 1}",
            title=f"Fixture job {start + 1}",
            job_url=f"https://example.test/acme/{start + 1}",
        )
        context.emit_job(job, {"listing": start + 1})
        return JobResponse(jobs=(job,))


@pytest.mark.parametrize(
    "value",
    ["", "contains spaces", "/absolute/path", "UPPER CASE", "a" * 65],
)
def test_custom_adapter_identifier_rejects_unsafe_values(value: str) -> None:
    with pytest.raises(ValueError, match="adapter identifier"):
        AdapterId(value)


def test_built_in_identifiers_remain_site_values() -> None:
    assert parse_adapter_identifier("indeed") is Site.INDEED
    assert parse_adapter_identifier("ziprecruiter") is Site.ZIP_RECRUITER
    with pytest.raises(ValueError, match="built in"):
        AdapterId("indeed")


def test_custom_identifier_round_trips_through_request_json() -> None:
    request = SearchRequest(
        site_type=("acme.jobs",),
        search_term="engineer",
    )

    assert request.sites == (CUSTOM_ID,)
    restored = SearchRequest.model_validate_json(request.model_dump_json())
    assert restored == request
    assert restored.fingerprint() == request.fingerprint()


def test_structural_adapter_protocol_streams_a_custom_site() -> None:
    registry = AdapterRegistry()
    registry.register(CUSTOM_ID, FixtureAdapter)
    store = MemoryCheckpointStore()
    request = SearchRequest(site_type=(CUSTOM_ID,), results_wanted=1)

    adapter = registry.create(CUSTOM_ID)
    assert isinstance(adapter, Adapter)
    events = list(
        stream_search(
            request,
            registry=registry,
            checkpoint_store=store,
        )
    )

    job = next(event for event in events if isinstance(event, JobEvent))
    assert job.site == CUSTOM_ID
    assert job.job.id == "acme-1"
    checkpoint = store.load()
    assert checkpoint is not None
    assert checkpoint.adapters[CUSTOM_ID.value].site == CUSTOM_ID
    restored = SearchCheckpoint.model_validate_json(checkpoint.model_dump_json())
    assert restored.adapters[CUSTOM_ID.value].site == CUSTOM_ID


def test_custom_identifier_round_trips_through_sqlite_and_outcomes(tmp_path) -> None:
    registry = AdapterRegistry()
    registry.register(CUSTOM_ID, FixtureAdapter)
    store = SqliteCheckpointStore(tmp_path / "custom-adapter.sqlite3")
    request = SearchRequest(site_type=(CUSTOM_ID,), results_wanted=1)

    outcome = collect_jobs(
        request,
        registry=registry,
        checkpoint_store=store,
    )

    assert [(item.site, item.job.id) for item in outcome.jobs] == [
        (CUSTOM_ID, "acme-1")
    ]
    assert outcome.summary_for(CUSTOM_ID).completed is True
    checkpoint = store.load()
    assert checkpoint is not None
    assert checkpoint.adapters[CUSTOM_ID.value].site == CUSTOM_ID
    assert checkpoint.generation != "legacy"


def test_adapter_test_kit_runs_an_offline_resume_fixture() -> None:
    adapter = AdapterTestKit.assert_conforms(CUSTOM_ID, FixtureAdapter)
    request = SearchRequest(site_type=(CUSTOM_ID,), results_wanted=2)

    run = AdapterTestKit.run(
        adapter,
        request,
        resume_state={"listing": 7},
    )

    assert [job.id for job in run.response.jobs] == ["acme-8"]
    assert run.context.resume_state == {"listing": 8}


def test_context_can_check_a_stable_identity_before_job_construction() -> None:
    request = SearchRequest(site_type=(CUSTOM_ID,), results_wanted=2)
    context = AdapterTestKit.context(request, identifier=CUSTOM_ID)
    job = JobPost(
        id="acme-1",
        title="Fixture job",
        job_url="https://example.test/acme/1",
    )

    assert context.already_seen_identity("acme-1") is False

    assert context.emit_job(job) is True

    assert context.already_seen_identity("acme-1") is True
    assert context.already_seen(job) is True


def test_adapter_test_kit_reports_identifier_mismatches() -> None:
    with pytest.raises(AdapterContractViolation, match="produced acme.jobs"):
        AdapterTestKit.assert_conforms(AdapterId("other.jobs"), FixtureAdapter)


def test_legacy_signature_detection_is_deprecated_and_kept_out_of_runtime() -> None:
    class LegacyAdapter(Scraper):
        capabilities = AdapterCapabilities(resume=NoResume())

        def __init__(self, **kwargs: Any) -> None:
            del kwargs
            super().__init__(Site.INDEED)

        def scrape(self, scraper_input) -> JobResponse:
            del scraper_input
            return JobResponse(
                jobs=(
                    JobPost(
                        id="legacy-1",
                        title="Legacy job",
                        job_url="https://example.test/legacy/1",
                    ),
                )
            )

    implicit = AdapterRegistry()
    with pytest.warns(DeprecationWarning, match="Implicit legacy adapter"):
        implicit.register(Site.INDEED, LegacyAdapter)
    implicit_events = list(
        stream_search(
            SearchRequest(site_type=(Site.INDEED,), results_wanted=1),
            registry=implicit,
        )
    )
    assert any(isinstance(event, JobEvent) for event in implicit_events)

    explicit = AdapterRegistry()
    explicit.register(Site.INDEED, legacy_adapter(LegacyAdapter))
    assert explicit.create(Site.INDEED).capabilities.resume == NoResume()


def test_legacy_adapter_bridge_forwards_lifecycle_cleanup() -> None:
    close_count = 0

    class LegacyAdapter(Scraper):
        def __init__(self, **kwargs: Any) -> None:
            del kwargs
            super().__init__(Site.INDEED)

        def scrape(self, scraper_input) -> JobResponse:
            del scraper_input
            return JobResponse()

        def close(self) -> None:
            nonlocal close_count
            close_count += 1

    registry = AdapterRegistry()
    registry.register(Site.INDEED, legacy_adapter(LegacyAdapter))

    with stream_search(
        SearchRequest(site_type=(Site.INDEED,)),
        registry=registry,
    ) as stream:
        list(stream)
        diagnostics = stream.wait_closed(1)

    assert close_count == 1
    assert diagnostics.quiescent is True


def test_callable_factory_legacy_signature_is_adapted_on_first_creation() -> None:
    class LegacyAdapter(Scraper):
        def __init__(self) -> None:
            super().__init__(Site.INDEED)

        def scrape(self, scraper_input) -> JobResponse:
            del scraper_input
            return JobResponse()

    def factory(**kwargs: Any) -> LegacyAdapter:
        del kwargs
        return LegacyAdapter()

    registry = AdapterRegistry()
    registry.register(Site.INDEED, factory)

    with pytest.warns(DeprecationWarning, match="Implicit legacy adapter"):
        adapter = registry.create(Site.INDEED)

    assert (
        adapter.scrape(
            SearchRequest(site_type=(Site.INDEED,)),
            context=None,
        )
        == JobResponse()
    )
    assert registry.create(Site.INDEED).capabilities.resume == NoResume()


def test_legacy_capability_constructor_has_a_validated_migration_path() -> None:
    with pytest.warns(DeprecationWarning, match="deprecated"):
        capabilities = AdapterCapabilities(
            supports_resume=True,
            resume_granularity="cursor",
            cursor_schema_version=3,
        )

    assert capabilities.resume == Resumable(
        granularity=ResumeGranularity.CURSOR,
        cursor_schema_version=3,
    )

    with pytest.raises(ValidationError, match="requires supports_resume"):
        AdapterCapabilities(
            supports_resume=False,
            resume_granularity="page",
        )


@pytest.mark.parametrize(
    ("legacy", "canonical"),
    [
        ("continuation token", "continuation_token"),
        ("custom-batch window", "custom_batch_window"),
    ],
)
def test_legacy_resume_granularities_remain_open_and_normalized(
    legacy: str,
    canonical: str,
) -> None:
    with pytest.warns(DeprecationWarning, match="deprecated"):
        capabilities = AdapterCapabilities(
            supports_resume=True,
            resume_granularity=legacy,
        )

    assert capabilities.resume == Resumable(granularity=ResumeGranularity(canonical))
    restored = AdapterCapabilities.model_validate_json(capabilities.model_dump_json())
    assert restored == capabilities


def test_deferred_legacy_adaptation_preserves_cursor_and_filter_contracts() -> None:
    capabilities = AdapterCapabilities(
        filters=frozenset({SearchFilter.LOCATION}),
        resume=Resumable(
            granularity=ResumeGranularity.PAGE,
            cursor_schema_version=7,
        ),
    )

    class LegacyAdapter:
        def __init__(self) -> None:
            self.site = Site.INDEED
            self.capabilities = capabilities

        def scrape(self, scraper_input) -> JobResponse:
            del scraper_input
            return JobResponse(
                jobs=(
                    JobPost(
                        id="legacy-cursor-1",
                        title="Legacy cursor fixture",
                        job_url="https://example.test/legacy/cursor-1",
                    ),
                )
            )

    def factory(**kwargs: Any) -> LegacyAdapter:
        del kwargs
        return LegacyAdapter()

    registry = AdapterRegistry()
    with pytest.warns(DeprecationWarning, match="cursor_schema_version"):
        registry.register(
            Site.INDEED,
            factory,
            cursor_schema_version=3,
        )
    with pytest.warns(DeprecationWarning, match="Implicit legacy adapter"):
        adapter = registry.create(Site.INDEED)

    assert registry.cursor_schema_version(Site.INDEED) == 3
    assert adapter.capabilities.filters == frozenset({SearchFilter.LOCATION})
    assert adapter.capabilities.resume == NoResume()

    store = MemoryCheckpointStore()
    request = SearchRequest(
        site_type=(Site.INDEED,),
        location="Madrid",
        results_wanted=1,
    )
    events = list(
        stream_search(
            request,
            registry=registry,
            checkpoint_store=store,
        )
    )
    assert not any(
        isinstance(event, WarningEvent) and "Unsupported filters" in event.message
        for event in events
    )
    checkpoint = store.load()
    assert checkpoint is not None
    assert checkpoint.adapters[Site.INDEED.value].cursor_schema_version == 3
    list(
        stream_search(
            request,
            registry=registry,
            checkpoint_store=store,
        )
    )


def test_callable_factory_without_capabilities_discovers_a_safe_schema() -> None:
    constructed = False

    def factory(**kwargs: Any) -> FixtureAdapter:
        nonlocal constructed
        del kwargs
        constructed = True
        return FixtureAdapter()

    registry = AdapterRegistry()
    registry.register(CUSTOM_ID, factory)

    assert constructed is False
    assert registry.create(CUSTOM_ID).site == CUSTOM_ID
    assert constructed is True


def test_declared_factory_rejects_a_contradictory_cursor_override() -> None:
    capabilities = AdapterCapabilities(
        resume=Resumable(
            granularity=ResumeGranularity.PAGE,
            cursor_schema_version=7,
        )
    )

    class DeclaredAdapter:
        def __init__(self) -> None:
            self.site = CUSTOM_ID
            self.capabilities = capabilities

        def scrape(self, scraper_input, context=None) -> JobResponse:
            del scraper_input, context
            return JobResponse()

    DeclaredAdapter.capabilities = capabilities
    registry = AdapterRegistry()

    with pytest.raises(ValueError, match=r"override 3.*declared.*version 7"):
        registry.register(
            CUSTOM_ID,
            DeclaredAdapter,
            cursor_schema_version=3,
        )

    with pytest.warns(DeprecationWarning, match="cursor_schema_version"):
        registry.register(
            CUSTOM_ID,
            DeclaredAdapter,
            cursor_schema_version=7,
        )
    assert registry.cursor_schema_version(CUSTOM_ID) == 7


@pytest.mark.parametrize("explicit_bridge", [False, True])
def test_legacy_class_preserves_a_cursor_override(
    explicit_bridge: bool,
) -> None:
    capabilities = AdapterCapabilities(
        filters=frozenset({SearchFilter.LOCATION}),
        resume=Resumable(
            granularity=ResumeGranularity.PAGE,
            cursor_schema_version=7,
        ),
    )

    class LegacyClass:
        capabilities = AdapterCapabilities()

        def __init__(self, **kwargs: Any) -> None:
            del kwargs
            self.site = CUSTOM_ID
            self.capabilities = capabilities

        def scrape(self, scraper_input) -> JobResponse:
            del scraper_input
            return JobResponse()

    LegacyClass.capabilities = capabilities
    factory = legacy_adapter(LegacyClass) if explicit_bridge else LegacyClass
    registry = AdapterRegistry()

    with pytest.warns(DeprecationWarning) as captured:
        registry.register(
            CUSTOM_ID,
            factory,
            cursor_schema_version=3,
        )
    messages = [str(warning.message) for warning in captured]
    assert any("cursor_schema_version" in message for message in messages)
    if not explicit_bridge:
        assert any("Implicit legacy adapter" in message for message in messages)
    adapter = registry.create(CUSTOM_ID)

    assert registry.cursor_schema_version(CUSTOM_ID) == 3
    assert adapter.capabilities.filters == frozenset({SearchFilter.LOCATION})
    assert adapter.capabilities.resume == NoResume()


def test_concurrent_first_use_capability_discovery_has_one_winner() -> None:
    construction_barrier = threading.Barrier(2)
    release_conflicting_adapter = threading.Event()

    class DynamicAdapter:
        def __init__(self, granularity: ResumeGranularity) -> None:
            self.site = CUSTOM_ID
            self.capabilities = AdapterCapabilities(
                resume=Resumable(granularity=granularity)
            )

        def scrape(self, scraper_input, context=None) -> JobResponse:
            del scraper_input, context
            return JobResponse()

    def factory(
        *,
        granularity: ResumeGranularity,
        **kwargs: Any,
    ) -> DynamicAdapter:
        del kwargs
        construction_barrier.wait()
        if granularity == ResumeGranularity.CURSOR:
            assert release_conflicting_adapter.wait(timeout=5)
        return DynamicAdapter(granularity)

    registry = AdapterRegistry()
    registry.register(CUSTOM_ID, factory)

    with ThreadPoolExecutor(max_workers=2) as executor:
        accepted = executor.submit(
            registry.create,
            CUSTOM_ID,
            granularity=ResumeGranularity.PAGE,
        )
        rejected = executor.submit(
            registry.create,
            CUSTOM_ID,
            granularity=ResumeGranularity.CURSOR,
        )

        adapter = accepted.result(timeout=5)
        release_conflicting_adapter.set()
        with pytest.raises(TypeError, match="declared capabilities.*do not match"):
            rejected.result(timeout=5)

    assert adapter.capabilities.resume == Resumable(granularity=ResumeGranularity.PAGE)


@pytest.mark.parametrize("registered_version", [1, 3])
def test_undeclared_factory_rejects_discovered_resume_schema_mismatch(
    registered_version: int,
) -> None:
    class DynamicAdapter:
        capabilities = AdapterCapabilities(
            resume=Resumable(
                granularity=ResumeGranularity.CURSOR,
                cursor_schema_version=7,
            )
        )

        def __init__(self) -> None:
            self.site = CUSTOM_ID

        def scrape(self, scraper_input, context=None) -> JobResponse:
            del scraper_input, context
            return JobResponse()

    def factory(**kwargs: Any) -> DynamicAdapter:
        del kwargs
        return DynamicAdapter()

    registry = AdapterRegistry()
    if registered_version == 1:
        registry.register(CUSTOM_ID, factory)
    else:
        with pytest.warns(DeprecationWarning, match="cursor_schema_version"):
            registry.register(
                CUSTOM_ID,
                factory,
                cursor_schema_version=registered_version,
            )

    with pytest.raises(
        TypeError,
        match=(f"cursor schema version 7.*registration uses " f"{registered_version}"),
    ):
        registry.create(CUSTOM_ID)


def test_factory_and_instance_capabilities_must_match() -> None:
    declared = AdapterCapabilities(resume=NoResume())

    class DynamicAdapter:
        def __init__(self) -> None:
            self.site = CUSTOM_ID
            self.capabilities = AdapterCapabilities(
                resume=Resumable(
                    granularity=ResumeGranularity.CURSOR,
                    cursor_schema_version=7,
                )
            )

        def scrape(self, scraper_input, context=None) -> JobResponse:
            del scraper_input, context
            return JobResponse()

    def factory(**kwargs: Any) -> DynamicAdapter:
        del kwargs
        return DynamicAdapter()

    factory.capabilities = declared
    registry = AdapterRegistry()
    registry.register(CUSTOM_ID, factory)

    with pytest.raises(TypeError, match="do not match"):
        registry.create(CUSTOM_ID)


@pytest.mark.parametrize(
    ("factory", "message"),
    [
        (
            lambda: type(
                "RawSiteAdapter",
                (),
                {
                    "capabilities": AdapterCapabilities(),
                    "site": "acme.jobs",
                    "scrape": lambda self, request, context=None: JobResponse(),
                },
            ),
            "Site or AdapterId",
        ),
        (
            lambda: type(
                "WrongCapabilitiesAdapter",
                (),
                {
                    "capabilities": AdapterCapabilities(),
                    "__init__": lambda self: setattr(self, "capabilities", "resumable"),
                    "site": CUSTOM_ID,
                    "scrape": lambda self, request, context=None: JobResponse(),
                },
            ),
            "AdapterCapabilities",
        ),
        (
            lambda: type(
                "WrongFactoryCapabilitiesAdapter",
                (),
                {
                    "capabilities": "resumable",
                    "site": CUSTOM_ID,
                    "scrape": lambda self, request, context=None: JobResponse(),
                },
            ),
            "AdapterCapabilities",
        ),
        (
            lambda: type(
                "NonCallableScrapeAdapter",
                (),
                {
                    "capabilities": AdapterCapabilities(),
                    "site": CUSTOM_ID,
                    "scrape": None,
                },
            ),
            "callable scrape",
        ),
    ],
)
def test_adapter_test_kit_wraps_structural_contract_failures(
    factory,
    message: str,
) -> None:
    with pytest.raises(AdapterContractViolation, match=message):
        AdapterTestKit.assert_conforms(CUSTOM_ID, factory())


def test_adapter_test_kit_wraps_invalid_response_types() -> None:
    class WrongResponseAdapter:
        capabilities = AdapterCapabilities()
        site = CUSTOM_ID

        def scrape(self, scraper_input, context=None):
            del scraper_input, context
            return ()

    adapter = AdapterTestKit.assert_conforms(CUSTOM_ID, WrongResponseAdapter)
    with pytest.raises(AdapterContractViolation, match="return JobResponse"):
        AdapterTestKit.run(
            adapter,
            SearchRequest(site_type=(CUSTOM_ID,)),
        )


def test_public_protocol_annotations_resolve_at_runtime() -> None:
    adapter_hints = get_type_hints(Adapter.scrape)
    stream_hints = get_type_hints(SearchStream.__init__)

    assert adapter_hints["context"] == ScrapeContext | None
    assert stream_hints["registry"] == AdapterRegistry
