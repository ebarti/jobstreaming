from __future__ import annotations

import inspect
import math
import queue
import threading
import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from functools import partial
from typing import Any, TypeVar, cast

from jobstreaming.checkpoint import (
    CheckpointCompatibilityError,
    CheckpointConflictError,
    CheckpointMismatchError,
    CheckpointStore,
    MemoryCheckpointStore,
)
from jobstreaming.events import (
    CHECKPOINT_VERSION,
    AdapterCheckpoint,
    ErrorEvent,
    JobEvent,
    ProgressEvent,
    SearchCheckpoint,
    SearchCompleteEvent,
    SearchEvent,
    SiteCompleteEvent,
    WarningEvent,
    freeze_state,
    thaw_state,
)
from jobstreaming.exception import (
    ErrorCode,
    StreamCancelledError,
    UnacknowledgedEventError,
    classify_exception,
)
from jobstreaming.model import JobPost, Scraper, SearchRequest, Site
from jobstreaming.registry import AdapterRegistry
from jobstreaming.result import normalize_job
from jobstreaming.util import stable_job_key


class _MessageType(str, Enum):
    JOB = "job"
    PROGRESS = "progress"
    WARNING = "warning"
    ERROR = "error"
    SITE_COMPLETE = "site_complete"
    WORKER_DONE = "worker_done"


class AckMode(str, Enum):
    IMPLICIT = "implicit"
    EXPLICIT = "explicit"


@dataclass(frozen=True, slots=True)
class _AdapterMessage:
    type: _MessageType
    site: Site
    state: dict[str, Any]
    job: JobPost | None = None
    job_key: str | None = None
    message: str | None = None
    error_type: str | None = None
    error_code: ErrorCode = ErrorCode.ADAPTER_FAILURE
    retryable: bool = False
    retry_after: float | None = None
    reset_checkpoint: bool = False
    emitted_count: int = 0


MessageSink = Callable[[_AdapterMessage], bool]
CancellationCallback = Callable[[], bool]
_Result = TypeVar("_Result")
_WAKE = object()


@dataclass(frozen=True, slots=True)
class StreamDiagnostics:
    """Immutable snapshot of the stream's resource lifecycle."""

    closed: bool
    cancellation_requested: bool
    workers_started: int
    operations_started: int
    cleanup_tasks_started: int
    active_workers: tuple[str, ...]
    active_operations: tuple[str, ...]
    active_cleanup_tasks: tuple[str, ...]
    open_adapters: tuple[Site, ...]
    cleanup_errors: tuple[str, ...]

    @property
    def quiescent(self) -> bool:
        """Whether no adapter or managed background thread remains active."""

        return not (
            self.active_workers
            or self.active_operations
            or self.active_cleanup_tasks
            or self.open_adapters
        )


class _ThreadTracker:
    """Own daemon threads and expose bounded, race-safe lifecycle snapshots."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._threads: list[threading.Thread] = []

    def start(self, *, name: str, target: Callable[[], None]) -> threading.Thread:
        thread = threading.Thread(target=target, name=name, daemon=True)
        with self._lock:
            self._threads.append(thread)
        try:
            thread.start()
        except BaseException:
            with self._lock:
                self._threads.remove(thread)
            raise
        return thread

    @property
    def started_count(self) -> int:
        with self._lock:
            return len(self._threads)

    def active_threads(self) -> tuple[threading.Thread, ...]:
        with self._lock:
            return tuple(thread for thread in self._threads if thread.is_alive())

    def active_names(self) -> tuple[str, ...]:
        return tuple(thread.name for thread in self.active_threads())


class _CancellationController:
    """Combines stream close, a caller event, and a caller callback."""

    def __init__(
        self,
        *,
        stop_event: threading.Event | None = None,
        cancel_event: threading.Event | None = None,
        cancel_callback: CancellationCallback | None = None,
    ) -> None:
        self._stop_event = stop_event or threading.Event()
        self._cancel_event = cancel_event
        self._cancel_callback = cancel_callback
        self._external_cancelled = threading.Event()
        self._callback_lock = threading.Lock()

    @property
    def externally_cancelled(self) -> bool:
        if self._external_cancelled.is_set():
            return True
        if self._cancel_event is not None and self._cancel_event.is_set():
            self._external_cancelled.set()
            return True
        if self._cancel_callback is None:
            return False
        with self._callback_lock:
            if self._external_cancelled.is_set():
                return True
            if self._cancel_callback():
                self._external_cancelled.set()
        return self._external_cancelled.is_set()

    @property
    def cancelled(self) -> bool:
        return self._stop_event.is_set() or self.externally_cancelled

    @property
    def requested(self) -> bool:
        """Read cancellation state without invoking a caller callback."""

        return (
            self._stop_event.is_set()
            or self._external_cancelled.is_set()
            or (self._cancel_event is not None and self._cancel_event.is_set())
        )

    def stop(self) -> None:
        self._stop_event.set()

    def wait(self, seconds: float) -> bool:
        """Return true if cancellation occurs before the timeout."""

        deadline = time.monotonic() + max(0, seconds)
        while not self.cancelled:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return False
            self._stop_event.wait(min(remaining, 0.05))
        return True


class ScrapeContext:
    """Per-adapter execution boundary for emission, deduplication, and resume."""

    def __init__(
        self,
        *,
        site: Site,
        request: SearchRequest,
        checkpoint: AdapterCheckpoint | None = None,
        sink: MessageSink | None = None,
        stop_event: threading.Event | None = None,
        cancel_event: threading.Event | None = None,
        cancel_callback: CancellationCallback | None = None,
        cancellation: _CancellationController | None = None,
        operation_threads: _ThreadTracker | None = None,
    ) -> None:
        self.site = site
        self.request = request
        self._sink = sink
        self._cancellation = cancellation or _CancellationController(
            stop_event=stop_event,
            cancel_event=cancel_event,
            cancel_callback=cancel_callback,
        )
        self._state = thaw_state(freeze_state(checkpoint.state)) if checkpoint else {}
        self._seen = set(checkpoint.seen_job_keys) if checkpoint else set()
        self._lock = threading.Lock()
        self._operation_threads = operation_threads or _ThreadTracker()

    @classmethod
    def local(
        cls,
        site: Site,
        request: SearchRequest,
        context: ScrapeContext | None,
    ) -> ScrapeContext:
        return context or cls(site=site, request=request)

    @property
    def resume_state(self) -> dict[str, Any]:
        with self._lock:
            return thaw_state(freeze_state(self._state))

    @property
    def seen_job_keys(self) -> frozenset[str]:
        with self._lock:
            return frozenset(self._seen)

    @property
    def emitted_count(self) -> int:
        with self._lock:
            return len(self._seen)

    @property
    def cancelled(self) -> bool:
        return self._cancellation.cancelled

    @property
    def should_continue(self) -> bool:
        return not self.cancelled and self.emitted_count < self.request.results_wanted

    def wait(self, seconds: float) -> bool:
        """Wait between requests, waking immediately when the stream is closed."""
        if seconds <= 0:
            return self.should_continue
        return not self._cancellation.wait(seconds) and self.should_continue

    def run_interruptibly(self, operation: Callable[[], _Result]) -> _Result:
        """Run a potentially blocking adapter operation behind cancellation."""

        completed: queue.Queue[tuple[bool, object]] = queue.Queue(maxsize=1)

        def invoke() -> None:
            try:
                completed.put((True, operation()))
            except BaseException as exc:
                completed.put((False, exc))

        self._operation_threads.start(
            name=f"jobstreaming-network-{self.site.value}",
            target=invoke,
        )
        while not self.cancelled:
            try:
                succeeded, result = completed.get(timeout=0.05)
            except queue.Empty:
                continue
            if succeeded:
                return cast(_Result, result)
            raise cast(BaseException, result)
        raise StreamCancelledError("Search stream was cancelled")

    def already_seen(self, job: JobPost) -> bool:
        key = stable_job_key(self.site.value, job.id or job.job_url)
        with self._lock:
            return key in self._seen

    def emit_job(
        self, job: JobPost, resume_state: dict[str, Any] | None = None
    ) -> bool:
        job = normalize_job(job, self.request)
        key = stable_job_key(self.site.value, job.id or job.job_url)
        with self._lock:
            if (
                self.cancelled
                or key in self._seen
                or len(self._seen) >= self.request.results_wanted
            ):
                return False
            self._seen.add(key)
            if resume_state is not None:
                self._state = thaw_state(freeze_state(resume_state))
            state = thaw_state(freeze_state(self._state))

        if self._sink is None:
            return True
        accepted = self._sink(
            _AdapterMessage(
                type=_MessageType.JOB,
                site=self.site,
                state=state,
                job=job,
                job_key=key,
                emitted_count=self.emitted_count,
            )
        )
        if not accepted:
            with self._lock:
                self._seen.discard(key)
            return False
        return True

    def emit_progress(
        self, resume_state: dict[str, Any], message: str | None = None
    ) -> None:
        with self._lock:
            self._state = thaw_state(freeze_state(resume_state))
            state = thaw_state(freeze_state(self._state))
        if self._sink is not None and not self.cancelled:
            self._sink(
                _AdapterMessage(
                    type=_MessageType.PROGRESS,
                    site=self.site,
                    state=state,
                    message=message,
                    emitted_count=self.emitted_count,
                )
            )

    def emit_warning(self, message: str) -> None:
        if self._sink is not None and not self.cancelled:
            self._sink(
                _AdapterMessage(
                    type=_MessageType.WARNING,
                    site=self.site,
                    state=self.resume_state,
                    message=message,
                    emitted_count=self.emitted_count,
                )
            )


class SearchStream(Iterator[SearchEvent]):
    """
    Concurrent, acknowledged event stream.

    In implicit mode, requesting the next event acknowledges the previous event.
    Explicit mode refuses another delivery until ``ack`` persists the current event.
    """

    def __init__(
        self,
        request: SearchRequest,
        *,
        registry: AdapterRegistry,
        checkpoint_store: CheckpointStore | None = None,
        resume: bool = True,
        proxies: list[str] | str | None = None,
        ca_cert: str | None = None,
        user_agent: str | None = None,
        queue_size: int = 128,
        max_retries: int = 1,
        retry_backoff: float = 0.5,
        ack_mode: str | AckMode = AckMode.IMPLICIT,
        cancel_event: threading.Event | None = None,
        cancel_callback: CancellationCallback | None = None,
    ) -> None:
        if queue_size < 1:
            raise ValueError("queue_size must be positive")
        if max_retries < 0:
            raise ValueError("max_retries cannot be negative")
        if retry_backoff < 0:
            raise ValueError("retry_backoff cannot be negative")
        try:
            parsed_ack_mode = AckMode(ack_mode)
        except ValueError as exc:
            raise ValueError("ack_mode must be 'implicit' or 'explicit'") from exc
        self.request = request
        self.registry = registry
        self.checkpoint_store = checkpoint_store or MemoryCheckpointStore()
        self.proxies = proxies
        self.ca_cert = ca_cert
        self.user_agent = user_agent
        self.max_retries = max_retries
        self.retry_backoff = retry_backoff
        self.ack_mode = parsed_ack_mode
        self._queue: queue.Queue[object] = queue.Queue(queue_size)
        self._cancellation = _CancellationController(
            cancel_event=cancel_event,
            cancel_callback=cancel_callback,
        )
        self._worker_threads = _ThreadTracker()
        self._operation_threads = _ThreadTracker()
        self._cleanup_threads = _ThreadTracker()
        self._lifecycle_lock = threading.Lock()
        self._open_adapters: dict[int, tuple[Site, Scraper]] = {}
        self._cleanup_errors: list[str] = []
        self._active_workers = 0
        self._sequence = 0
        self._total_jobs = 0
        self._total_errors = 0
        self._last_delivered: SearchEvent | None = None
        self._last_acknowledged = True
        self._terminal_delivered = False
        self._closed = False
        self._checkpoint = self._load_checkpoint(resume=resume)
        self._start_workers()

    def _load_checkpoint(self, *, resume: bool) -> SearchCheckpoint:
        cursor_versions = {
            site: self.registry.cursor_schema_version(site)
            for site in self.request.sites
        }
        loaded = self.checkpoint_store.load() if resume else None
        if loaded is None:
            checkpoint = SearchCheckpoint.for_request(self.request, cursor_versions)
            self.checkpoint_store.save(checkpoint)
            return checkpoint
        if loaded.version != CHECKPOINT_VERSION:
            raise CheckpointCompatibilityError(
                f"Checkpoint schema {loaded.version} is incompatible with "
                f"schema {CHECKPOINT_VERSION}"
            )
        if loaded.request_fingerprint != self.request.fingerprint():
            raise CheckpointMismatchError(
                "Checkpoint belongs to a different search request"
            )
        adapters = dict(loaded.adapters)
        for site in self.request.sites:
            expected_version = cursor_versions[site]
            adapter_checkpoint = adapters.get(site.value)
            if adapter_checkpoint is None:
                adapters[site.value] = AdapterCheckpoint(
                    site=site,
                    cursor_schema_version=expected_version,
                )
                continue
            if adapter_checkpoint.site is not site:
                raise CheckpointCompatibilityError(
                    f"Checkpoint adapter key {site.value!r} contains "
                    f"state for {adapter_checkpoint.site.value!r}"
                )
            if adapter_checkpoint.cursor_schema_version != expected_version:
                raise CheckpointCompatibilityError(
                    f"{site.value} cursor schema "
                    f"{adapter_checkpoint.cursor_schema_version} is incompatible "
                    f"with adapter schema {expected_version}"
                )
        loaded = loaded.model_copy(update={"adapters": adapters})
        return loaded

    def _start_workers(self) -> None:
        pending_sites = [
            site
            for site in self.request.sites
            if not self._checkpoint.adapters[site.value].completed
        ]
        self._active_workers = len(pending_sites)
        for site in pending_sites:
            self._worker_threads.start(
                name=f"jobstreaming-{site.value}",
                target=partial(self._run_adapter, site),
            )

    def _register_adapter(self, site: Site, scraper: Scraper) -> bool:
        with self._lifecycle_lock:
            if self._closed:
                should_close = True
            else:
                should_close = False
                self._open_adapters[id(scraper)] = (site, scraper)
        if should_close:
            self._schedule_adapter_close(site, scraper)
            return False
        return True

    def _release_adapter(self, scraper: Scraper) -> None:
        with self._lifecycle_lock:
            owned = self._open_adapters.pop(id(scraper), None)
        if owned is not None:
            self._schedule_adapter_close(*owned)

    def _schedule_adapter_close(self, site: Site, scraper: Scraper) -> None:
        def close_adapter() -> None:
            try:
                scraper.close()
            except Exception as exc:
                message = f"{site.value}: {type(exc).__name__}: {exc}"
                with self._lifecycle_lock:
                    self._cleanup_errors.append(message)

        self._cleanup_threads.start(
            name=f"jobstreaming-close-{site.value}",
            target=close_adapter,
        )

    def _put_message(self, message: _AdapterMessage) -> bool:
        while not self._cancellation.cancelled:
            try:
                self._queue.put(message, timeout=0.05)
                return True
            except queue.Full:
                continue
        return False

    def _run_adapter(self, site: Site) -> None:
        checkpoint = self._checkpoint.adapters[site.value]
        context = ScrapeContext(
            site=site,
            request=self.request,
            checkpoint=checkpoint,
            sink=self._put_message,
            cancellation=self._cancellation,
            operation_threads=self._operation_threads,
        )
        try:
            if not context.should_continue:
                self._emit_site_complete(site, context)
                return
            for attempt in range(self.max_retries + 1):
                scraper: Scraper | None = None
                try:
                    scraper = self.registry.create(
                        site,
                        proxies=self.proxies,
                        ca_cert=self.ca_cert,
                        user_agent=self.user_agent,
                    )
                    if not self._register_adapter(site, scraper):
                        return
                    if attempt == 0:
                        self._emit_capability_warnings(scraper, context)
                    parameters = inspect.signature(scraper.scrape).parameters
                    operation = (
                        partial(scraper.scrape, self.request, context=context)
                        if "context" in parameters
                        else partial(scraper.scrape, self.request)
                    )
                    response = context.run_interruptibly(operation)
                    for job in response.jobs:
                        context.emit_job(job, context.resume_state)
                    if not context.cancelled:
                        self._emit_site_complete(site, context)
                    return
                except Exception as exc:
                    if context.cancelled:
                        return
                    if not context.should_continue:
                        self._emit_site_complete(site, context)
                        return

                    error = classify_exception(exc)
                    retries_left = self.max_retries - attempt
                    if not error.retryable or retries_left <= 0:
                        self._put_message(
                            _AdapterMessage(
                                type=_MessageType.ERROR,
                                site=site,
                                state=context.resume_state,
                                message=str(exc),
                                error_type=type(exc).__name__,
                                error_code=error.code,
                                retryable=error.retryable,
                                retry_after=error.retry_after,
                                reset_checkpoint=error.reset_checkpoint,
                                emitted_count=context.emitted_count,
                            )
                        )
                        return

                    delay = max(
                        self.retry_backoff * (2**attempt),
                        error.retry_after or 0,
                    )
                    context.emit_warning(
                        f"{type(exc).__name__}: {exc}; retrying "
                        f"({attempt + 1}/{self.max_retries}) in {delay:g}s"
                    )
                    if not context.wait(delay):
                        return
                finally:
                    if scraper is not None:
                        self._release_adapter(scraper)
        finally:
            self._put_message(
                _AdapterMessage(
                    type=_MessageType.WORKER_DONE,
                    site=site,
                    state=context.resume_state,
                    emitted_count=context.emitted_count,
                )
            )

    def _emit_capability_warnings(self, scraper, context: ScrapeContext) -> None:
        requested_filters = {
            name
            for name, enabled in {
                "location": bool(self.request.location),
                "distance": self.request.distance != 50,
                "is_remote": self.request.is_remote,
                "job_type": self.request.job_type is not None,
                "easy_apply": bool(self.request.easy_apply),
                "offset": self.request.offset > 0,
                "hours_old": self.request.hours_old is not None,
                "description_format": self.request.description_format.value
                != "markdown",
            }.items()
            if enabled
        }
        unsupported = requested_filters - scraper.capabilities.filters
        if unsupported:
            context.emit_warning(
                "Unsupported filters were ignored: " + ", ".join(sorted(unsupported))
            )
        supported_job_types = scraper.capabilities.supported_job_types
        if (
            self.request.job_type is not None
            and "job_type" in scraper.capabilities.filters
            and supported_job_types is not None
            and self.request.job_type not in supported_job_types
        ):
            context.emit_warning(
                "Unsupported job_type value was ignored: "
                f"{self.request.job_type.canonical}"
            )

    def _emit_site_complete(self, site: Site, context: ScrapeContext) -> None:
        self._put_message(
            _AdapterMessage(
                type=_MessageType.SITE_COMPLETE,
                site=site,
                state=context.resume_state,
                emitted_count=context.emitted_count,
            )
        )

    @property
    def checkpoint(self) -> SearchCheckpoint:
        return self._checkpoint.model_copy(deep=True)

    @property
    def diagnostics(self) -> StreamDiagnostics:
        """Return a side-effect-free snapshot of owned runtime resources."""

        with self._lifecycle_lock:
            closed = self._closed
            open_adapters = tuple(
                sorted(
                    (site for site, _ in self._open_adapters.values()),
                    key=lambda site: site.value,
                )
            )
            cleanup_errors = tuple(self._cleanup_errors)
        return StreamDiagnostics(
            closed=closed,
            cancellation_requested=self._cancellation.requested,
            workers_started=self._worker_threads.started_count,
            operations_started=self._operation_threads.started_count,
            cleanup_tasks_started=self._cleanup_threads.started_count,
            active_workers=self._worker_threads.active_names(),
            active_operations=self._operation_threads.active_names(),
            active_cleanup_tasks=self._cleanup_threads.active_names(),
            open_adapters=open_adapters,
            cleanup_errors=cleanup_errors,
        )

    def wait_closed(self, timeout: float) -> StreamDiagnostics:
        """Wait at most ``timeout`` seconds for all owned resources to stop.

        This method never initiates cancellation. Call ``close()`` first when
        stopping an active stream, then inspect the returned diagnostics to learn
        whether a transport operation outlived the bounded wait.
        """

        if (
            isinstance(timeout, bool)
            or not isinstance(timeout, (int, float))
            or not math.isfinite(timeout)
            or timeout < 0
        ):
            raise ValueError("timeout must be a finite, non-negative number")

        deadline = time.monotonic() + timeout
        current = threading.current_thread()
        while True:
            tracked = (
                self._worker_threads.active_threads()
                + self._operation_threads.active_threads()
                + self._cleanup_threads.active_threads()
            )
            joinable = tuple(thread for thread in tracked if thread is not current)
            if not joinable:
                return self.diagnostics
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return self.diagnostics
            for thread in joinable:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return self.diagnostics
                thread.join(remaining)

    def __iter__(self) -> SearchStream:
        return self

    def __next__(self) -> SearchEvent:
        if self._closed:
            raise StopIteration
        if self._cancellation.externally_cancelled:
            self.close()
            raise StreamCancelledError("Search stream was cancelled")
        if self._last_delivered is not None and not self._last_acknowledged:
            if self.ack_mode is AckMode.EXPLICIT:
                raise UnacknowledgedEventError(
                    "Acknowledge the previously delivered event before requesting "
                    "another event"
                )
            self.ack(self._last_delivered)
        if self._terminal_delivered:
            self.close()
            raise StopIteration

        while True:
            if self._closed:
                raise StopIteration
            if self._cancellation.externally_cancelled:
                self.close()
                raise StreamCancelledError("Search stream was cancelled")
            if self._active_workers == 0:
                self._sequence += 1
                completed = all(
                    checkpoint.completed
                    for checkpoint in self._checkpoint.adapters.values()
                )
                event = SearchCompleteEvent(
                    sequence=self._sequence,
                    emitted_at=datetime.now(timezone.utc),
                    total_jobs=self._total_jobs,
                    total_errors=self._total_errors,
                    completed=completed,
                )
                self._terminal_delivered = True
                self._remember(event)
                return event

            try:
                queued = self._queue.get(timeout=0.05)
            except queue.Empty:
                continue
            if queued is _WAKE:
                continue
            if self._closed:
                raise StopIteration
            message = cast(_AdapterMessage, queued)
            if message.type is _MessageType.WORKER_DONE:
                self._active_workers -= 1
                continue

            self._sequence += 1
            now = datetime.now(timezone.utc)
            if message.type is _MessageType.JOB:
                assert message.job is not None and message.job_key is not None
                event: SearchEvent = JobEvent(
                    sequence=self._sequence,
                    emitted_at=now,
                    site=message.site,
                    job=message.job,
                    job_key=message.job_key,
                    resume_state=freeze_state(message.state),
                )
                self._total_jobs += 1
            elif message.type is _MessageType.PROGRESS:
                event = ProgressEvent(
                    sequence=self._sequence,
                    emitted_at=now,
                    site=message.site,
                    resume_state=freeze_state(message.state),
                    message=message.message,
                )
            elif message.type is _MessageType.WARNING:
                event = WarningEvent(
                    sequence=self._sequence,
                    emitted_at=now,
                    site=message.site,
                    message=message.message or "adapter warning",
                    resume_state=freeze_state(message.state),
                )
            elif message.type is _MessageType.ERROR:
                event = ErrorEvent(
                    sequence=self._sequence,
                    emitted_at=now,
                    site=message.site,
                    message=message.message or "adapter failure",
                    error_type=message.error_type or "Exception",
                    recoverable=message.retryable or message.reset_checkpoint,
                    resume_state=freeze_state(message.state),
                    code=message.error_code,
                    retryable=message.retryable,
                    retry_after=message.retry_after,
                    reset_checkpoint=message.reset_checkpoint,
                )
                self._total_errors += 1
            else:
                event = SiteCompleteEvent(
                    sequence=self._sequence,
                    emitted_at=now,
                    site=message.site,
                    emitted_count=message.emitted_count,
                    resume_state=freeze_state(message.state),
                )
            self._remember(event)
            return event

    def _remember(self, event: SearchEvent) -> None:
        self._last_delivered = event
        self._last_acknowledged = False

    def ack(self, event: SearchEvent | None = None) -> None:
        event = event or self._last_delivered
        if event is None:
            return
        if self._last_delivered is None or event is not self._last_delivered:
            raise ValueError("Events must be acknowledged in delivery order")
        if self._last_acknowledged:
            return

        adapters = dict(self._checkpoint.adapters)
        if isinstance(
            event,
            (JobEvent, ProgressEvent, WarningEvent, ErrorEvent, SiteCompleteEvent),
        ):
            existing = adapters[event.site.value]
            seen = list(existing.seen_job_keys)
            if isinstance(event, JobEvent) and event.job_key not in seen:
                seen.append(event.job_key)
            adapters[event.site.value] = existing.model_copy(
                update={
                    "state": thaw_state(event.resume_state),
                    "seen_job_keys": tuple(seen),
                    "emitted_count": len(seen),
                    "completed": isinstance(event, SiteCompleteEvent),
                    "updated_at": datetime.now(timezone.utc),
                }
            )

        completed = (
            event.completed
            if isinstance(event, SearchCompleteEvent)
            else self._checkpoint.completed
        )
        checkpoint = self._checkpoint.model_copy(
            update={
                "adapters": adapters,
                "completed": completed,
                "revision": self._checkpoint.revision + 1,
                "updated_at": datetime.now(timezone.utc),
            }
        )
        try:
            self.checkpoint_store.save(checkpoint)
        except CheckpointConflictError:
            self.close()
            raise
        self._checkpoint = checkpoint
        self._last_acknowledged = True

    def close(self, *, acknowledge: bool = False) -> None:
        if acknowledge and self._last_delivered is not None:
            self.ack(self._last_delivered)
        with self._lifecycle_lock:
            if self._closed:
                return
            self._closed = True
            adapters = tuple(self._open_adapters.values())
            self._open_adapters.clear()
        self._cancellation.stop()
        self._wake_iterator()
        for site, scraper in adapters:
            self._schedule_adapter_close(site, scraper)

    def _wake_iterator(self) -> None:
        while True:
            try:
                self._queue.put_nowait(_WAKE)
                return
            except queue.Full:
                try:
                    self._queue.get_nowait()
                except queue.Empty:
                    continue

    def __enter__(self) -> SearchStream:
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        self.close(acknowledge=False)
