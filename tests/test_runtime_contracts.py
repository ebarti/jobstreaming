from __future__ import annotations

import threading
import time
from collections.abc import Callable

import pytest

from jobstreaming import (
    AdapterCapabilities,
    AdapterRegistry,
    AuthenticationConfigurationError,
    CheckpointCompatibilityError,
    CheckpointConflictError,
    CursorExpiredError,
    ErrorCode,
    ErrorEvent,
    InvalidRequestError,
    JobEvent,
    JobPost,
    JobResponse,
    MemoryCheckpointStore,
    RateLimitError,
    Resumable,
    ResumeGranularity,
    Scraper,
    SearchCheckpoint,
    SearchRequest,
    Site,
    StreamCancelledError,
    TransientNetworkError,
    UnacknowledgedEventError,
    WarningEvent,
    stream_jobs,
    stream_search,
)
from jobstreaming.exception import LinkedInException


def _job(number: int) -> JobPost:
    return JobPost(
        id=f"job-{number}",
        title=f"Job {number}",
        job_url=f"https://example.test/jobs/{number}",
    )


class _TwoJobAdapter(Scraper):
    def __init__(self, **_: object) -> None:
        super().__init__(Site.INDEED)

    def scrape(self, request, context=None) -> JobResponse:
        assert context is not None
        emitted = []
        for number in (1, 2):
            job = _job(number)
            if context.emit_job(job, {"page": 1}):
                emitted.append(job)
        context.emit_progress({"page": 2})
        return JobResponse(jobs=emitted)


def _registry(adapter: Callable[..., Scraper] = _TwoJobAdapter) -> AdapterRegistry:
    registry = AdapterRegistry()
    registry.register(Site.INDEED, adapter)
    return registry


def test_explicit_ack_mode_refuses_the_next_delivery_without_checkpointing() -> None:
    request = SearchRequest(site_type=(Site.INDEED,), results_wanted=2)
    store = MemoryCheckpointStore()
    stream = stream_search(
        request,
        registry=_registry(),
        checkpoint_store=store,
        ack_mode="explicit",
    )

    first = next(stream)
    assert isinstance(first, JobEvent)

    with pytest.raises(UnacknowledgedEventError):
        next(stream)

    checkpoint = store.load()
    assert checkpoint is not None
    assert checkpoint.revision == 0
    assert checkpoint.adapters[Site.INDEED.value].seen_job_keys == ()

    stream.ack(first)
    second = next(stream)
    assert isinstance(second, JobEvent)
    assert second.job.id == "job-2"
    stream.close()


def test_job_only_iterator_acknowledges_in_explicit_mode() -> None:
    jobs = list(
        stream_jobs(
            site_name=Site.INDEED,
            registry=_registry(),
            results_wanted=2,
            ack_mode="explicit",
        )
    )

    assert [job.id for job in jobs] == ["job-1", "job-2"]


def test_close_wakes_a_blocked_iterator_promptly() -> None:
    operation_started = threading.Event()
    release_operation = threading.Event()
    iterator_started = threading.Event()
    outcome: list[str] = []

    class BlockingAdapter(Scraper):
        def __init__(self, **_: object) -> None:
            super().__init__(Site.INDEED)

        def scrape(self, request, context=None) -> JobResponse:
            operation_started.set()
            release_operation.wait(timeout=5)
            return JobResponse()

    stream = stream_search(
        SearchRequest(site_type=(Site.INDEED,)),
        registry=_registry(BlockingAdapter),
    )
    assert operation_started.wait(timeout=1)

    def consume() -> None:
        iterator_started.set()
        try:
            next(stream)
        except StopIteration:
            outcome.append("closed")

    consumer = threading.Thread(target=consume)
    consumer.start()
    assert iterator_started.wait(timeout=1)
    started = time.monotonic()
    stream.close()
    consumer.join(timeout=0.5)
    elapsed = time.monotonic() - started
    release_operation.set()

    assert not consumer.is_alive()
    assert outcome == ["closed"]
    assert elapsed < 0.5


def test_external_event_interrupts_a_network_wait() -> None:
    operation_started = threading.Event()
    release_operation = threading.Event()
    cancel_event = threading.Event()
    outcome: list[Exception] = []

    class BlockingAdapter(Scraper):
        def __init__(self, **_: object) -> None:
            super().__init__(Site.INDEED)

        def scrape(self, request, context=None) -> JobResponse:
            operation_started.set()
            release_operation.wait(timeout=5)
            return JobResponse()

    stream = stream_search(
        SearchRequest(site_type=(Site.INDEED,)),
        registry=_registry(BlockingAdapter),
        cancel_event=cancel_event,
    )
    assert operation_started.wait(timeout=1)

    def consume() -> None:
        try:
            next(stream)
        except Exception as exc:
            outcome.append(exc)

    consumer = threading.Thread(target=consume)
    consumer.start()
    started = time.monotonic()
    cancel_event.set()
    consumer.join(timeout=0.5)
    elapsed = time.monotonic() - started
    release_operation.set()

    assert not consumer.is_alive()
    assert len(outcome) == 1
    assert isinstance(outcome[0], StreamCancelledError)
    assert outcome[0].code is ErrorCode.CANCELLED
    assert elapsed < 0.5


def test_cancellation_callback_interrupts_retry_backoff() -> None:
    attempts = 0
    cancelled = False

    class RetryingAdapter(Scraper):
        def __init__(self, **_: object) -> None:
            super().__init__(Site.INDEED)

        def scrape(self, request, context=None) -> JobResponse:
            nonlocal attempts
            attempts += 1
            raise TransientNetworkError("offline")

    stream = stream_search(
        SearchRequest(site_type=(Site.INDEED,)),
        registry=_registry(RetryingAdapter),
        max_retries=3,
        retry_backoff=30,
        cancel_callback=lambda: cancelled,
    )
    warning = next(stream)
    assert isinstance(warning, WarningEvent)
    stream.ack(warning)

    cancelled = True
    started = time.monotonic()
    with pytest.raises(StreamCancelledError):
        next(stream)

    assert time.monotonic() - started < 0.5
    assert attempts == 1


def test_overall_checkpoint_schema_is_validated() -> None:
    request = SearchRequest(site_type=(Site.INDEED,))
    store = MemoryCheckpointStore()
    checkpoint = SearchCheckpoint.for_request(request).model_copy(
        update={"version": 999}
    )
    store.save(checkpoint)

    with pytest.raises(CheckpointCompatibilityError, match="schema 999"):
        stream_search(request, registry=_registry(), checkpoint_store=store)


def test_adapter_cursor_schema_is_validated_before_workers_start() -> None:
    class UpgradedAdapter(_TwoJobAdapter):
        capabilities = AdapterCapabilities(
            resume=Resumable(
                granularity=ResumeGranularity.PAGE,
                cursor_schema_version=2,
            )
        )

    request = SearchRequest(site_type=(Site.INDEED,))
    store = MemoryCheckpointStore()
    store.save(SearchCheckpoint.for_request(request))

    with pytest.raises(CheckpointCompatibilityError, match="adapter schema 2"):
        stream_search(
            request,
            registry=_registry(UpgradedAdapter),
            checkpoint_store=store,
        )


def test_new_checkpoint_records_the_registered_adapter_cursor_schema() -> None:
    class UpgradedAdapter(_TwoJobAdapter):
        capabilities = AdapterCapabilities(
            resume=Resumable(
                granularity=ResumeGranularity.PAGE,
                cursor_schema_version=2,
            )
        )

    store = MemoryCheckpointStore()
    stream = stream_search(
        SearchRequest(site_type=(Site.INDEED,)),
        registry=_registry(UpgradedAdapter),
        checkpoint_store=store,
    )
    stream.close()

    checkpoint = store.load()
    assert checkpoint is not None
    assert checkpoint.adapters[Site.INDEED.value].cursor_schema_version == 2


class _CompareAndSwapStore:
    def __init__(self) -> None:
        self.checkpoint: SearchCheckpoint | None = None
        self.lock = threading.Lock()

    def load(self) -> SearchCheckpoint | None:
        with self.lock:
            return (
                self.checkpoint.model_copy(deep=True)
                if self.checkpoint is not None
                else None
            )

    def save(self, checkpoint: SearchCheckpoint) -> None:
        with self.lock:
            if self.checkpoint is None:
                if checkpoint.revision != 0:
                    raise CheckpointConflictError("initial revision must be zero")
            elif checkpoint.revision != self.checkpoint.revision + 1:
                raise CheckpointConflictError("stale checkpoint owner")
            self.checkpoint = checkpoint.model_copy(deep=True)

    def clear(self) -> None:
        with self.lock:
            self.checkpoint = None


def test_stale_checkpoint_owner_conflict_stops_the_stream() -> None:
    request = SearchRequest(site_type=(Site.INDEED,), results_wanted=2)
    store = _CompareAndSwapStore()
    first_stream = stream_search(
        request,
        registry=_registry(),
        checkpoint_store=store,
        ack_mode="explicit",
    )
    stale_stream = stream_search(
        request,
        registry=_registry(),
        checkpoint_store=store,
        ack_mode="explicit",
    )
    first = next(first_stream)
    stale = next(stale_stream)
    first_stream.ack(first)

    with pytest.raises(CheckpointConflictError, match="stale checkpoint owner"):
        stale_stream.ack(stale)
    with pytest.raises(StopIteration):
        next(stale_stream)

    assert stale_stream.checkpoint.revision == 0
    assert store.checkpoint is not None
    assert store.checkpoint.revision == 1
    first_stream.close()


@pytest.mark.parametrize(
    ("failure", "code", "retryable", "reset_checkpoint", "attempts"),
    [
        (
            TransientNetworkError("offline"),
            ErrorCode.TRANSIENT_NETWORK,
            True,
            False,
            3,
        ),
        (RateLimitError("slow down"), ErrorCode.RATE_LIMITED, True, False, 3),
        (
            InvalidRequestError("bad query"),
            ErrorCode.INVALID_REQUEST,
            False,
            False,
            1,
        ),
        (
            CursorExpiredError("stale cursor"),
            ErrorCode.CURSOR_EXPIRED,
            False,
            True,
            1,
        ),
        (
            AuthenticationConfigurationError("bad credentials"),
            ErrorCode.AUTHENTICATION_CONFIGURATION,
            False,
            False,
            1,
        ),
        (
            StreamCancelledError("cancelled"),
            ErrorCode.CANCELLED,
            False,
            False,
            1,
        ),
        (
            LinkedInException("LinkedIn returned HTTP 503"),
            ErrorCode.TRANSIENT_NETWORK,
            True,
            False,
            3,
        ),
        (RuntimeError("unknown"), ErrorCode.ADAPTER_FAILURE, False, False, 1),
    ],
)
def test_error_codes_and_retry_dispositions_are_stable(
    failure: Exception,
    code: ErrorCode,
    retryable: bool,
    reset_checkpoint: bool,
    attempts: int,
) -> None:
    attempt_count = 0

    class BrokenAdapter(Scraper):
        def __init__(self, **_: object) -> None:
            super().__init__(Site.INDEED)

        def scrape(self, request, context=None) -> JobResponse:
            nonlocal attempt_count
            attempt_count += 1
            raise failure

    with stream_search(
        SearchRequest(site_type=(Site.INDEED,)),
        registry=_registry(BrokenAdapter),
        max_retries=2,
        retry_backoff=0,
    ) as stream:
        events = list(stream)

    error = next(event for event in events if isinstance(event, ErrorEvent))
    assert attempt_count == attempts
    assert error.code is code
    assert error.retryable is retryable
    assert error.reset_checkpoint is reset_checkpoint
    assert error.recoverable is (retryable or reset_checkpoint)
