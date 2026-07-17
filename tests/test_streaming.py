from __future__ import annotations

from threading import Event

from jobstreaming import (
    AdapterRegistry,
    ErrorEvent,
    JobEvent,
    JobPost,
    JobResponse,
    MemoryCheckpointStore,
    Scraper,
    SearchCompleteEvent,
    SearchRequest,
    Site,
    TransientNetworkError,
    stream_search,
)


def _job(site: Site, number: int) -> JobPost:
    return JobPost(
        id=f"{site.value}-{number}",
        title=f"Job {number}",
        company_name="Acme",
        job_url=f"https://example.test/{site.value}/{number}",
    )


def test_fast_adapter_streams_before_slow_adapter_finishes() -> None:
    release_slow = Event()

    class FastAdapter(Scraper):
        def __init__(self, **_: object) -> None:
            super().__init__(Site.INDEED)

        def scrape(self, scraper_input, context=None) -> JobResponse:
            assert context is not None
            job = _job(self.site, 1)
            context.emit_job(job, {"page": 1})
            return JobResponse(jobs=[job])

    class SlowAdapter(Scraper):
        def __init__(self, **_: object) -> None:
            super().__init__(Site.LINKEDIN)

        def scrape(self, scraper_input, context=None) -> JobResponse:
            assert context is not None
            release_slow.wait(timeout=2)
            job = _job(self.site, 1)
            context.emit_job(job, {"start": 0})
            return JobResponse(jobs=[job])

    registry = AdapterRegistry()
    registry.register(Site.INDEED, FastAdapter)
    registry.register(Site.LINKEDIN, SlowAdapter)
    request = SearchRequest(site_type=(Site.INDEED, Site.LINKEDIN))

    with stream_search(request, registry=registry) as stream:
        first_job = next(event for event in stream if isinstance(event, JobEvent))
        assert first_job.site is Site.INDEED
        release_slow.set()
        assert any(
            isinstance(event, JobEvent) and event.site is Site.LINKEDIN
            for event in stream
        )


def test_adapter_failure_is_an_event_and_other_sites_continue() -> None:
    class WorkingAdapter(Scraper):
        def __init__(self, **_: object) -> None:
            super().__init__(Site.INDEED)

        def scrape(self, scraper_input, context=None) -> JobResponse:
            assert context is not None
            job = _job(self.site, 1)
            context.emit_job(job, {"page": 1})
            return JobResponse(jobs=[job])

    class BrokenAdapter(Scraper):
        def __init__(self, **_: object) -> None:
            super().__init__(Site.LINKEDIN)

        def scrape(self, scraper_input, context=None) -> JobResponse:
            raise RuntimeError("blocked")

    registry = AdapterRegistry()
    registry.register(Site.INDEED, WorkingAdapter)
    registry.register(Site.LINKEDIN, BrokenAdapter)
    request = SearchRequest(site_type=(Site.INDEED, Site.LINKEDIN))

    with stream_search(request, registry=registry) as stream:
        events = list(stream)

    assert any(isinstance(event, JobEvent) for event in events)
    assert any(isinstance(event, ErrorEvent) for event in events)
    complete = next(event for event in events if isinstance(event, SearchCompleteEvent))
    assert complete.completed is False


def test_transient_adapter_failure_retries_from_the_same_context() -> None:
    attempts = 0

    class FlakyAdapter(Scraper):
        def __init__(self, **_: object) -> None:
            super().__init__(Site.INDEED)

        def scrape(self, scraper_input, context=None) -> JobResponse:
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                raise TransientNetworkError("temporary outage")
            assert context is not None
            job = _job(self.site, 1)
            context.emit_job(job, {"page": 1})
            return JobResponse(jobs=[job])

    registry = AdapterRegistry()
    registry.register(Site.INDEED, FlakyAdapter)
    request = SearchRequest(site_type=(Site.INDEED,), results_wanted=1)

    with stream_search(
        request,
        registry=registry,
        max_retries=1,
        retry_backoff=0,
    ) as stream:
        events = list(stream)

    assert attempts == 2
    assert any(isinstance(event, JobEvent) for event in events)
    assert not any(isinstance(event, ErrorEvent) for event in events)
    complete = next(event for event in events if isinstance(event, SearchCompleteEvent))
    assert complete.completed is True


def test_acknowledged_jobs_are_not_reemitted_after_restart() -> None:
    class RestartableAdapter(Scraper):
        def __init__(self, **_: object) -> None:
            super().__init__(Site.INDEED)

        def scrape(self, scraper_input, context=None) -> JobResponse:
            assert context is not None
            jobs = [_job(self.site, 1), _job(self.site, 2)]
            emitted = []
            for index, job in enumerate(jobs):
                if context.emit_job(job, {"page": 1, "index": index}):
                    emitted.append(job)
            context.emit_progress({"page": 2}, "page complete")
            return JobResponse(jobs=emitted)

    registry = AdapterRegistry()
    registry.register(Site.INDEED, RestartableAdapter)
    request = SearchRequest(site_type=(Site.INDEED,), results_wanted=2)
    store = MemoryCheckpointStore()

    with stream_search(
        request, registry=registry, checkpoint_store=store
    ) as first_stream:
        first = next(event for event in first_stream if isinstance(event, JobEvent))
        assert first.job.id == "indeed-1"
        first_stream.ack(first)

    with stream_search(
        request, registry=registry, checkpoint_store=store, resume=True
    ) as resumed_stream:
        resumed_jobs = [
            event.job.id for event in resumed_stream if isinstance(event, JobEvent)
        ]

    assert resumed_jobs == ["indeed-2"]
