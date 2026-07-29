from __future__ import annotations

import gc
import threading
import time
import weakref
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


def test_close_winning_before_operation_start_never_invokes_adapter() -> None:
    registration_window = threading.Event()
    release_registration = threading.Event()
    scrape_started = threading.Event()

    class RegistrationWindowAdapter(Scraper):
        @property
        def capabilities(self) -> AdapterCapabilities:
            registration_window.set()
            release_registration.wait(timeout=1)
            return AdapterCapabilities()

        def __init__(self, **_: object) -> None:
            super().__init__(Site.INDEED)

        def scrape(self, request, context=None) -> JobResponse:
            scrape_started.set()
            return JobResponse()

    stream = stream_search(
        SearchRequest(site_type=(Site.INDEED,)),
        registry=_registry(RegistrationWindowAdapter),
    )
    assert registration_window.wait(timeout=1)

    stream.close()
    release_registration.set()
    diagnostics = stream.wait_closed(1)

    assert scrape_started.is_set() is False
    assert diagnostics.operations_started == 0
    assert diagnostics.cleanup_tasks_started == 1
    assert diagnostics.quiescent is True
    assert stream._managed_adapters == {}


def test_repeated_close_cannot_later_acknowledge_an_uncommitted_event() -> None:
    store = MemoryCheckpointStore()
    stream = stream_search(
        SearchRequest(site_type=(Site.INDEED,), results_wanted=1),
        registry=_registry(),
        checkpoint_store=store,
        ack_mode="explicit",
    )
    event = next(stream)
    assert isinstance(event, JobEvent)

    stream.close(acknowledge=False)
    stream.close(acknowledge=True)
    assert stream.wait_closed(1).quiescent is True

    checkpoint = store.load()
    assert checkpoint is not None
    assert checkpoint.revision == 0
    assert checkpoint.adapters[Site.INDEED.value].seen_job_keys == ()


def test_close_is_prompt_and_wait_closed_reports_lingering_resources() -> None:
    operation_started = threading.Event()
    close_started = threading.Event()
    release_operation = threading.Event()
    release_close = threading.Event()

    class BlockingLifecycleAdapter(Scraper):
        def __init__(self, **_: object) -> None:
            super().__init__(Site.INDEED)

        def scrape(self, request, context=None) -> JobResponse:
            operation_started.set()
            release_operation.wait(timeout=5)
            return JobResponse()

        def close(self) -> None:
            close_started.set()
            release_close.wait(timeout=5)

    stream = stream_search(
        SearchRequest(site_type=(Site.INDEED,)),
        registry=_registry(BlockingLifecycleAdapter),
    )
    assert operation_started.wait(timeout=1)

    started = time.monotonic()
    stream.close()
    elapsed = time.monotonic() - started
    assert elapsed < 0.5
    assert close_started.wait(timeout=1)

    diagnostics = stream.wait_closed(0.01)
    assert diagnostics.closed is True
    assert diagnostics.cancellation_requested is True
    assert diagnostics.quiescent is False
    assert diagnostics.active_operations == ("jobstreaming-network-indeed",)
    assert diagnostics.active_cleanup_tasks == ("jobstreaming-close-indeed",)
    assert diagnostics.open_adapters == ()
    assert all(
        thread.daemon
        for thread in threading.enumerate()
        if thread.name
        in diagnostics.active_operations + diagnostics.active_cleanup_tasks
    )

    release_operation.set()
    release_close.set()
    diagnostics = stream.wait_closed(1)
    assert diagnostics.quiescent is True
    assert diagnostics.workers_started == 1
    assert diagnostics.operations_started == 1
    assert diagnostics.cleanup_tasks_started == 1


def test_adapter_transports_are_closed_once_after_natural_completion() -> None:
    close_count = 0

    class Transport:
        def close(self) -> None:
            nonlocal close_count
            close_count += 1

    class TransportAdapter(Scraper):
        def __init__(self, **_: object) -> None:
            super().__init__(Site.INDEED)
            self.track_transport(Transport())

        def scrape(self, request, context=None) -> JobResponse:
            return JobResponse()

    with stream_search(
        SearchRequest(site_type=(Site.INDEED,)),
        registry=_registry(TransportAdapter),
    ) as stream:
        list(stream)
        diagnostics = stream.wait_closed(1)

    assert diagnostics.quiescent is True
    assert diagnostics.cleanup_errors == ()
    assert close_count == 1


def test_adapter_cleanup_failures_are_preserved_in_diagnostics() -> None:
    class BrokenCloseAdapter(Scraper):
        def __init__(self, **_: object) -> None:
            super().__init__(Site.INDEED)

        def scrape(self, request, context=None) -> JobResponse:
            return JobResponse()

        def close(self) -> None:
            raise RuntimeError("close failed")

    with stream_search(
        SearchRequest(site_type=(Site.INDEED,)),
        registry=_registry(BrokenCloseAdapter),
    ) as stream:
        list(stream)
        diagnostics = stream.wait_closed(1)

    assert diagnostics.quiescent is True
    assert diagnostics.cleanup_errors == ("indeed: RuntimeError: close failed",)


def test_custom_close_failure_is_not_hidden_by_transport_failures() -> None:
    class FailingTransport:
        def close(self) -> None:
            raise RuntimeError("transport close failed")

    class CustomCloseAdapter(Scraper):
        def __init__(self, **_: object) -> None:
            super().__init__(Site.INDEED)
            self.track_transport(FailingTransport())

        def scrape(self, request, context=None) -> JobResponse:
            return JobResponse()

        def close(self) -> None:
            try:
                super().close()
            except Exception:
                raise ValueError("custom close failed") from None

    with stream_search(
        SearchRequest(site_type=(Site.INDEED,)),
        registry=_registry(CustomCloseAdapter),
    ) as stream:
        list(stream)
        diagnostics = stream.wait_closed(1)

    assert diagnostics.quiescent is True
    assert diagnostics.cleanup_errors == (
        "indeed: ValueError: custom close failed",
        "indeed: transport cleanup: RuntimeError: transport close failed",
    )


def test_transport_created_after_adapter_close_is_closed_and_rejected() -> None:
    close_count = 0

    class Transport:
        def close(self) -> None:
            nonlocal close_count
            close_count += 1

    adapter = _TwoJobAdapter()
    adapter.close()

    with pytest.raises(RuntimeError, match="rejected late transport"):
        adapter.track_transport(Transport())
    adapter.close()

    assert close_count == 1


def test_late_transport_close_failure_survives_cancellation_diagnostics() -> None:
    operation_started = threading.Event()
    adapter_closed = threading.Event()
    register_late_transport = threading.Event()

    class FailingTransport:
        def close(self) -> None:
            raise RuntimeError("late close failed")

    class LateTransportAdapter(Scraper):
        def __init__(self, **_: object) -> None:
            super().__init__(Site.INDEED)

        def scrape(self, request, context=None) -> JobResponse:
            operation_started.set()
            register_late_transport.wait(timeout=5)
            self.track_transport(FailingTransport())
            return JobResponse()

        def close(self) -> None:
            super().close()
            adapter_closed.set()

    stream = stream_search(
        SearchRequest(site_type=(Site.INDEED,)),
        registry=_registry(LateTransportAdapter),
    )
    assert operation_started.wait(timeout=1)
    stream.close()
    assert adapter_closed.wait(timeout=1)

    register_late_transport.set()
    diagnostics = stream.wait_closed(1)

    assert diagnostics.quiescent is True
    assert diagnostics.cleanup_errors == (
        "indeed: transport cleanup: RuntimeError: late close failed",
    )


def test_transport_scopes_bound_detail_session_retention() -> None:
    closed = 0
    maximum_tracked = 0

    class Transport:
        def close(self) -> None:
            nonlocal closed
            closed += 1

    adapter = _TwoJobAdapter()
    adapter.track_transport(Transport())
    for _ in range(100):
        with adapter.transport_scope():
            for _ in range(8):
                adapter.track_transport(Transport())
                maximum_tracked = max(
                    maximum_tracked,
                    adapter.tracked_transport_count,
                )
        assert adapter.tracked_transport_count == 1

    adapter.close()
    assert maximum_tracked == 9
    assert closed == 801


def test_concurrent_transport_scopes_cannot_close_each_others_transports() -> None:
    first_entered = threading.Event()
    second_attempting = threading.Event()
    second_entered = threading.Event()
    release_first = threading.Event()
    failures: list[BaseException] = []

    class Transport:
        closed = False

        def close(self) -> None:
            self.closed = True

    adapter = _TwoJobAdapter()
    transports: dict[str, Transport] = {}

    def first_scope() -> None:
        try:
            with adapter.transport_scope():
                transport = Transport()
                transports["first"] = transport
                adapter.track_transport(transport)
                first_entered.set()
                release_first.wait(timeout=1)
                assert transport.closed is False
            assert transport.closed is True
        except BaseException as exc:
            failures.append(exc)

    def second_scope() -> None:
        try:
            assert first_entered.wait(timeout=1)
            second_attempting.set()
            with adapter.transport_scope():
                second_entered.set()
                transport = Transport()
                transports["second"] = transport
                adapter.track_transport(transport)
                assert transport.closed is False
            assert transport.closed is True
        except BaseException as exc:
            failures.append(exc)

    first = threading.Thread(target=first_scope)
    second = threading.Thread(target=second_scope)
    first.start()
    assert first_entered.wait(timeout=1)
    second.start()
    assert second_attempting.wait(timeout=1)
    assert second_entered.wait(timeout=0.05) is False

    release_first.set()
    assert second_entered.wait(timeout=1)
    first.join(timeout=1)
    second.join(timeout=1)

    assert first.is_alive() is False
    assert second.is_alive() is False
    assert failures == []
    assert transports["first"].closed is True
    assert transports["second"].closed is True
    assert adapter.tracked_transport_count == 0


def test_shutdown_winning_registration_retains_transport_failures() -> None:
    constructor_started = threading.Event()
    release_constructor = threading.Event()

    class FailingTransport:
        def close(self) -> None:
            raise RuntimeError("shutdown-win close failed")

    class LateAdapter(Scraper):
        def __init__(self, **_: object) -> None:
            super().__init__(Site.INDEED)
            self.track_transport(FailingTransport())
            constructor_started.set()
            release_constructor.wait(timeout=1)

        def scrape(self, request, context=None) -> JobResponse:
            raise AssertionError("closed adapters must not start scraping")

    stream = stream_search(
        SearchRequest(site_type=(Site.INDEED,)),
        registry=_registry(LateAdapter),
    )
    assert constructor_started.wait(timeout=1)
    stream.close()
    release_constructor.set()
    diagnostics = stream.wait_closed(1)

    assert diagnostics.quiescent is True
    assert diagnostics.cleanup_errors == (
        "indeed: transport cleanup: RuntimeError: shutdown-win close failed",
    )


def test_structural_adapter_without_close_hook_remains_compatible() -> None:
    class StructuralAdapter:
        capabilities = AdapterCapabilities()

        def __init__(self, **_: object) -> None:
            pass

        def scrape(self, request, context=None) -> JobResponse:
            return JobResponse(jobs=(_job(1),))

    registry = AdapterRegistry()
    registry.register(Site.INDEED, StructuralAdapter)  # type: ignore[arg-type]
    with stream_search(
        SearchRequest(site_type=(Site.INDEED,), results_wanted=1),
        registry=registry,
    ) as stream:
        events = list(stream)
        diagnostics = stream.wait_closed(1)

    assert any(isinstance(event, JobEvent) for event in events)
    assert diagnostics.quiescent is True
    assert diagnostics.cleanup_errors == ()


def test_repeated_cancellation_does_not_accumulate_managed_threads() -> None:
    class CooperativeAdapter(Scraper):
        def __init__(self, **_: object) -> None:
            super().__init__(Site.INDEED)
            self.started = threading.Event()
            self.release = threading.Event()

        def scrape(self, request, context=None) -> JobResponse:
            self.started.set()
            self.release.wait(timeout=5)
            return JobResponse()

        def close(self) -> None:
            self.release.set()

    adapters: list[CooperativeAdapter] = []

    def factory(**kwargs: object) -> CooperativeAdapter:
        adapter = CooperativeAdapter(**kwargs)
        adapters.append(adapter)
        return adapter

    for _ in range(8):
        expected_adapter_count = len(adapters) + 1
        stream = stream_search(
            SearchRequest(site_type=(Site.INDEED,)),
            registry=_registry(factory),
        )
        deadline = time.monotonic() + 1
        while len(adapters) < expected_adapter_count and time.monotonic() < deadline:
            time.sleep(0.001)
        assert len(adapters) == expected_adapter_count
        assert adapters[-1].started.wait(timeout=1)
        stream.close()
        assert stream.wait_closed(1).quiescent is True

    assert not [
        thread.name
        for thread in threading.enumerate()
        if thread.name.startswith(
            (
                "jobstreaming-indeed",
                "jobstreaming-network-indeed",
                "jobstreaming-close-indeed",
            )
        )
    ]


def test_completed_retry_adapters_are_collectible_without_losing_diagnostics() -> None:
    attempts = 0
    adapter_refs: list[weakref.ReferenceType[Scraper]] = []

    class RetryingAdapter(Scraper):
        def __init__(self, attempt: int, **_: object) -> None:
            super().__init__(Site.INDEED)
            self.attempt = attempt
            self.payload = bytearray(1_000_000)

        def scrape(self, request, context=None) -> JobResponse:
            raise TransientNetworkError(f"attempt {self.attempt}")

        def close(self) -> None:
            super().close()
            if self.attempt in (7, 19):
                raise RuntimeError(f"close failed {self.attempt}")

    def factory(**kwargs: object) -> RetryingAdapter:
        nonlocal attempts
        attempts += 1
        adapter = RetryingAdapter(attempts, **kwargs)
        adapter_refs.append(weakref.ref(adapter))
        return adapter

    with stream_search(
        SearchRequest(site_type=(Site.INDEED,)),
        registry=_registry(factory),
        max_retries=24,
        retry_backoff=0,
    ) as stream:
        list(stream)
        diagnostics = stream.wait_closed(2)

    assert attempts == 25
    assert diagnostics.quiescent is True
    assert diagnostics.cleanup_tasks_started == 25
    assert set(diagnostics.cleanup_errors) == {
        "indeed: RuntimeError: close failed 7",
        "indeed: RuntimeError: close failed 19",
    }
    assert stream._managed_adapters == {}

    gc.collect()
    assert all(adapter_ref() is None for adapter_ref in adapter_refs)


@pytest.mark.parametrize("timeout", [-1, float("inf"), float("nan"), True])
def test_wait_closed_requires_a_bounded_timeout(timeout) -> None:
    stream = stream_search(
        SearchRequest(site_type=(Site.INDEED,), results_wanted=0),
        registry=_registry(),
    )
    stream.close()

    with pytest.raises(ValueError, match="finite, non-negative"):
        stream.wait_closed(timeout)


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
            supports_resume=True,
            resume_granularity="page",
            cursor_schema_version=2,
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
            supports_resume=True,
            resume_granularity="page",
            cursor_schema_version=2,
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
            elif (
                checkpoint.generation != self.checkpoint.generation
                or checkpoint.revision != self.checkpoint.revision + 1
            ):
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
