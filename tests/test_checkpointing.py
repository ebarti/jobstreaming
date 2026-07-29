from __future__ import annotations

from dataclasses import replace

import pytest

from jobstreaming import (
    AdapterRegistry,
    CheckpointMismatchError,
    JobEvent,
    JobPost,
    JobResponse,
    JsonFileCheckpointStore,
    MemoryCheckpointStore,
    Scraper,
    SearchCheckpoint,
    SearchCompleteEvent,
    SearchRequest,
    Site,
    WarningEvent,
    stream_search,
)


def _job(number: int) -> JobPost:
    return JobPost(
        id=f"job-{number}",
        title=f"Job {number}",
        job_url=f"https://example.test/jobs/{number}",
    )


class _RestartableAdapter(Scraper):
    def __init__(self, **_: object) -> None:
        super().__init__(Site.INDEED)

    def scrape(self, request, context=None) -> JobResponse:
        assert context is not None
        emitted = []
        for index in range(3):
            job = _job(index)
            if context.emit_job(job, {"page": 1, "index": index}):
                emitted.append(job)
        context.emit_progress({"page": 2})
        return JobResponse(jobs=emitted)


def _registry(adapter=_RestartableAdapter) -> AdapterRegistry:
    registry = AdapterRegistry()
    registry.register(Site.INDEED, adapter)
    return registry


def test_unacknowledged_job_is_replayed_after_restart() -> None:
    request = SearchRequest(site_type=(Site.INDEED,), results_wanted=3)
    store = MemoryCheckpointStore()
    first_stream = stream_search(request, registry=_registry(), checkpoint_store=store)
    first = next(event for event in first_stream if isinstance(event, JobEvent))
    assert first.job.id == "job-0"
    first_stream.close(acknowledge=False)

    checkpoint = store.load()
    assert checkpoint is not None
    assert checkpoint.adapters[Site.INDEED.value].seen_job_keys == ()

    with stream_search(
        request,
        registry=_registry(),
        checkpoint_store=store,
        resume=True,
    ) as resumed:
        replayed = [event.job.id for event in resumed if isinstance(event, JobEvent)]
    assert replayed == ["job-0", "job-1", "job-2"]


def test_clean_context_manager_exit_does_not_acknowledge_an_early_stop() -> None:
    request = SearchRequest(site_type=(Site.INDEED,), results_wanted=3)
    store = MemoryCheckpointStore()

    with stream_search(
        request, registry=_registry(), checkpoint_store=store
    ) as first_stream:
        first = next(event for event in first_stream if isinstance(event, JobEvent))
        assert first.job.id == "job-0"

    checkpoint = store.load()
    assert checkpoint is not None
    assert checkpoint.adapters[Site.INDEED.value].seen_job_keys == ()

    with stream_search(
        request,
        registry=_registry(),
        checkpoint_store=store,
        resume=True,
    ) as resumed:
        replayed = [event.job.id for event in resumed if isinstance(event, JobEvent)]

    assert replayed == ["job-0", "job-1", "job-2"]


def test_completed_adapter_is_not_run_again() -> None:
    runs: list[int] = []

    def factory(**_: object) -> _RestartableAdapter:
        runs.append(1)
        return _RestartableAdapter()

    request = SearchRequest(site_type=(Site.INDEED,), results_wanted=1)
    store = MemoryCheckpointStore()
    with stream_search(
        request, registry=_registry(factory), checkpoint_store=store
    ) as first:
        list(first)
    with stream_search(
        request,
        registry=_registry(factory),
        checkpoint_store=store,
        resume=True,
    ) as second:
        events = list(second)

    assert len(runs) == 1
    assert [type(event) for event in events] == [SearchCompleteEvent]


def test_acknowledged_result_limit_completes_without_restarting_adapter() -> None:
    runs: list[int] = []

    def factory(**_: object) -> _RestartableAdapter:
        runs.append(1)
        return _RestartableAdapter()

    request = SearchRequest(site_type=(Site.INDEED,), results_wanted=1)
    store = MemoryCheckpointStore()
    first_stream = stream_search(
        request,
        registry=_registry(factory),
        checkpoint_store=store,
    )
    first = next(event for event in first_stream if isinstance(event, JobEvent))
    first_stream.ack(first)
    first_stream.close()

    with stream_search(
        request,
        registry=_registry(factory),
        checkpoint_store=store,
        resume=True,
    ) as resumed:
        events = list(resumed)

    assert len(runs) == 1
    assert not any(isinstance(event, JobEvent) for event in events)
    assert events[-1].completed is True


def test_checkpoint_rejects_a_different_request() -> None:
    store = MemoryCheckpointStore()
    original = SearchRequest(site_type=(Site.INDEED,), search_term="python")
    store.save(SearchCheckpoint.for_request(original))
    changed = SearchRequest(site_type=(Site.INDEED,), search_term="rust")

    with pytest.raises(CheckpointMismatchError):
        stream_search(changed, registry=_registry(), checkpoint_store=store)


def test_resume_false_preserves_generic_store_clear_then_save_contract() -> None:
    calls: list[str] = []

    class GenericStore:
        checkpoint = SearchCheckpoint.for_request(
            SearchRequest(site_type=(Site.INDEED,), search_term="python")
        )

        def load(self) -> SearchCheckpoint | None:
            return self.checkpoint

        def save(self, checkpoint: SearchCheckpoint) -> None:
            calls.append("save")
            self.checkpoint = checkpoint

        def clear(self) -> None:
            calls.append("clear")
            self.checkpoint = None

    stream = stream_search(
        SearchRequest(site_type=(Site.INDEED,), search_term="rust"),
        registry=_registry(),
        checkpoint_store=GenericStore(),
        resume=False,
    )
    stream.close()

    assert calls[:2] == ["clear", "save"]


def test_json_checkpoint_round_trip(tmp_path) -> None:
    path = tmp_path / "nested" / "checkpoint.json"
    store = JsonFileCheckpointStore(path)
    request = SearchRequest(site_type=(Site.INDEED,), search_term="python")
    checkpoint = SearchCheckpoint.for_request(request)

    store.save(checkpoint)

    assert store.load() == checkpoint
    assert not list(path.parent.glob("*.tmp"))


def test_legacy_batch_adapter_is_still_supported() -> None:
    class LegacyAdapter(Scraper):
        def __init__(self, **_: object) -> None:
            super().__init__(Site.INDEED)

        def scrape(self, request) -> JobResponse:
            return JobResponse(jobs=[_job(1)])

    request = SearchRequest(site_type=(Site.INDEED,))
    with stream_search(request, registry=_registry(LegacyAdapter)) as stream:
        jobs = [event for event in stream if isinstance(event, JobEvent)]

    assert [event.job.id for event in jobs] == ["job-1"]


def test_unsupported_filter_is_visible_as_a_warning() -> None:
    request = SearchRequest(
        site_type=(Site.INDEED,), location="Madrid", results_wanted=1
    )
    with stream_search(request, registry=_registry()) as stream:
        warnings = [event for event in stream if isinstance(event, WarningEvent)]

    assert len(warnings) == 1
    assert "location" in warnings[0].message


def test_adapter_cannot_emit_more_than_the_per_site_limit() -> None:
    request = SearchRequest(site_type=(Site.INDEED,), results_wanted=2)
    with stream_search(request, registry=_registry()) as stream:
        jobs = [event for event in stream if isinstance(event, JobEvent)]

    assert [event.job.id for event in jobs] == ["job-0", "job-1"]


def test_event_resume_state_is_deeply_immutable_and_acknowledgeable() -> None:
    class NestedStateAdapter(Scraper):
        def __init__(self, **_: object) -> None:
            super().__init__(Site.INDEED)

        def scrape(self, request, context=None) -> JobResponse:
            assert context is not None
            context.emit_job(
                _job(1),
                {"page": 1, "nested": {"cursor": "next"}, "offsets": [1, 2]},
            )
            return JobResponse()

    request = SearchRequest(site_type=(Site.INDEED,), results_wanted=1)
    store = MemoryCheckpointStore()
    stream = stream_search(
        request,
        registry=_registry(NestedStateAdapter),
        checkpoint_store=store,
    )
    event = next(item for item in stream if isinstance(item, JobEvent))

    with pytest.raises(TypeError):
        event.resume_state["page"] = 2  # type: ignore[index]
    with pytest.raises(TypeError):
        event.resume_state["nested"]["cursor"] = "changed"
    with pytest.raises(TypeError):
        event.resume_state["offsets"][0] = 99

    stream.ack(event)
    stream.close()
    checkpoint = store.load()

    assert checkpoint is not None
    assert checkpoint.adapters[Site.INDEED.value].state == {
        "page": 1,
        "nested": {"cursor": "next"},
        "offsets": [1, 2],
    }


def test_ack_rejects_a_reconstructed_event_with_the_same_sequence() -> None:
    request = SearchRequest(site_type=(Site.INDEED,), results_wanted=1)
    stream = stream_search(request, registry=_registry())
    event = next(item for item in stream if isinstance(item, JobEvent))
    reconstructed = replace(event, resume_state={"page": 999})

    with pytest.raises(ValueError, match="delivery order"):
        stream.ack(reconstructed)

    stream.ack(event)
    stream.close()
