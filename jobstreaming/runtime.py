from __future__ import annotations

import math
import queue
import threading
import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from functools import partial
from typing import cast

from jobstreaming.checkpoint import (
    AtomicCheckpointStore,
    CheckpointCompatibilityError,
    CheckpointConflictError,
    CheckpointMismatchError,
    CheckpointStore,
    CheckpointWrite,
    IncrementalCheckpointStore,
    MemoryCheckpointStore,
)
from jobstreaming.context import (
    CancellationCallback,
    ScrapeContext,
    _AdapterMessage,
    _CancellationController,
    _MessageType,
    _ThreadTracker,
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
    StreamCancelledError,
    UnacknowledgedEventError,
    _TransportCleanupError,
    classify_exception,
)
from jobstreaming.model import (
    AdapterIdentifier,
    JobResponse,
    SearchFilter,
    SearchRequest,
)
from jobstreaming.protocols import Adapter
from jobstreaming.registry import AdapterRegistry


class AckMode(str, Enum):
    IMPLICIT = "implicit"
    EXPLICIT = "explicit"


@dataclass(slots=True)
class _ManagedAdapter:
    token: int
    site: AdapterIdentifier
    scraper: object
    admitted: bool
    released: bool = False
    operation_started: bool = False
    operation_complete: bool = False
    cleanup_complete: bool = False


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
    open_adapters: tuple[AdapterIdentifier, ...]
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
        self._close_lock = threading.RLock()
        self._open_adapters: dict[int, _ManagedAdapter] = {}
        self._managed_adapters: dict[int, _ManagedAdapter] = {}
        self._next_adapter_token = 0
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
        self._acknowledged_seen_keys = {
            site_name: list(adapter.seen_job_keys)
            for site_name, adapter in self._checkpoint.adapters.items()
        }
        self._acknowledged_seen_key_sets = {
            site_name: set(keys)
            for site_name, keys in self._acknowledged_seen_keys.items()
        }
        self._start_workers()

    def _load_checkpoint(self, *, resume: bool) -> SearchCheckpoint:
        cursor_versions = {
            site: self.registry.cursor_schema_version(site)
            for site in self.request.sites
        }
        if not resume:
            checkpoint = SearchCheckpoint.for_request(self.request, cursor_versions)
            if isinstance(self.checkpoint_store, AtomicCheckpointStore):
                checkpoint = self.checkpoint_store.replace(checkpoint)
            else:
                self.checkpoint_store.clear()
                self.checkpoint_store.save(checkpoint)
            return checkpoint
        loaded = self.checkpoint_store.load()
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
            if adapter_checkpoint.site != site:
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

    def _register_adapter(
        self, site: AdapterIdentifier, scraper: object
    ) -> _ManagedAdapter:
        with self._lifecycle_lock:
            self._next_adapter_token += 1
            registration = _ManagedAdapter(
                token=self._next_adapter_token,
                site=site,
                scraper=scraper,
                admitted=not self._closed,
            )
            self._managed_adapters[registration.token] = registration
            if registration.admitted:
                self._open_adapters[id(scraper)] = registration
        if not registration.admitted:
            self._schedule_adapter_close(registration)
        return registration

    def _release_adapter(self, registration: _ManagedAdapter) -> None:
        schedule_cleanup = False
        with self._lifecycle_lock:
            registration.released = True
            scraper_id = id(registration.scraper)
            if self._open_adapters.get(scraper_id) is registration:
                self._open_adapters.pop(scraper_id)
                schedule_cleanup = True
        if schedule_cleanup:
            self._schedule_adapter_close(registration)
        self._retire_adapter_registration(registration)

    def _complete_adapter_operation(self, registration: _ManagedAdapter) -> None:
        with self._lifecycle_lock:
            registration.operation_complete = True
        self._retire_adapter_registration(registration)

    def _mark_adapter_operation_started(
        self,
        registration: _ManagedAdapter,
    ) -> None:
        with self._lifecycle_lock:
            registration.operation_started = True

    def _retire_adapter_registration(
        self,
        registration: _ManagedAdapter,
    ) -> None:
        with self._lifecycle_lock:
            ready = (
                registration.released
                and registration.operation_complete
                and registration.cleanup_complete
            )
        if not ready:
            return
        self._capture_transport_cleanup_errors(registration)
        with self._lifecycle_lock:
            if (
                registration.released
                and registration.operation_complete
                and registration.cleanup_complete
            ):
                self._managed_adapters.pop(registration.token, None)

    def _capture_transport_cleanup_errors(
        self,
        registration: _ManagedAdapter,
    ) -> None:
        for error in getattr(
            registration.scraper,
            "transport_cleanup_errors",
            (),
        ):
            message = f"{registration.site.value}: transport cleanup: {error}"
            with self._lifecycle_lock:
                self._cleanup_errors.append(message)

    def _schedule_adapter_close(self, registration: _ManagedAdapter) -> None:
        def close_adapter() -> None:
            try:
                close = getattr(registration.scraper, "close", None)
                if callable(close):
                    close()
            except _TransportCleanupError:
                pass
            except Exception as exc:
                message = f"{registration.site.value}: " f"{type(exc).__name__}: {exc}"
                with self._lifecycle_lock:
                    self._cleanup_errors.append(message)
            finally:
                self._capture_transport_cleanup_errors(registration)
                with self._lifecycle_lock:
                    registration.cleanup_complete = True
                self._retire_adapter_registration(registration)

        self._cleanup_threads.start(
            name=f"jobstreaming-close-{registration.site.value}",
            target=close_adapter,
        )

    def _run_managed_adapter_operation(
        self,
        registration: _ManagedAdapter,
        operation: Callable[[], JobResponse],
    ) -> JobResponse:
        try:
            return operation()
        finally:
            self._complete_adapter_operation(registration)

    def _put_message(self, message: _AdapterMessage) -> bool:
        while not self._cancellation.cancelled:
            try:
                self._queue.put(message, timeout=0.05)
                return True
            except queue.Full:
                continue
        return False

    def _run_adapter(self, site: AdapterIdentifier) -> None:
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
                scraper: Adapter | None = None
                registration: _ManagedAdapter | None = None
                try:
                    scraper = self.registry.create(
                        site,
                        proxies=self.proxies,
                        ca_cert=self.ca_cert,
                        user_agent=self.user_agent,
                    )
                    registration = self._register_adapter(site, scraper)
                    if not registration.admitted:
                        return
                    if attempt == 0:
                        self._emit_capability_warnings(scraper, context)
                    operation = partial(
                        scraper.scrape,
                        self.request,
                        context=context,
                    )

                    response = context.run_interruptibly(
                        partial(
                            self._run_managed_adapter_operation,
                            registration,
                            operation,
                        ),
                        on_started=partial(
                            self._mark_adapter_operation_started,
                            registration,
                        ),
                    )
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
                    if registration is not None:
                        if not registration.operation_started:
                            self._complete_adapter_operation(registration)
                        self._release_adapter(registration)
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
            search_filter
            for search_filter, enabled in {
                SearchFilter.LOCATION: bool(self.request.location),
                SearchFilter.DISTANCE: self.request.distance != 50,
                SearchFilter.IS_REMOTE: self.request.is_remote,
                SearchFilter.JOB_TYPE: self.request.job_type is not None,
                SearchFilter.EASY_APPLY: bool(self.request.easy_apply),
                SearchFilter.OFFSET: self.request.offset > 0,
                SearchFilter.HOURS_OLD: self.request.hours_old is not None,
                SearchFilter.DESCRIPTION_FORMAT: self.request.description_format.value
                != "markdown",
            }.items()
            if enabled
        }
        unsupported = requested_filters - scraper.capabilities.filters
        if unsupported:
            context.emit_warning(
                "Unsupported filters were ignored: "
                + ", ".join(sorted(item.value for item in unsupported))
            )
        supported_job_types = scraper.capabilities.supported_job_types
        if (
            self.request.job_type is not None
            and SearchFilter.JOB_TYPE in scraper.capabilities.filters
            and supported_job_types is not None
            and self.request.job_type not in supported_job_types
        ):
            context.emit_warning(
                "Unsupported job_type value was ignored: "
                f"{self.request.job_type.canonical}"
            )

    def _emit_site_complete(
        self, site: AdapterIdentifier, context: ScrapeContext
    ) -> None:
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
        adapters = {
            site_name: adapter.model_copy(
                update={
                    "seen_job_keys": tuple(
                        self._acknowledged_seen_keys.get(site_name, ())
                    )
                }
            )
            for site_name, adapter in self._checkpoint.adapters.items()
        }
        return self._checkpoint.model_copy(
            update={"adapters": adapters},
            deep=True,
        )

    @property
    def diagnostics(self) -> StreamDiagnostics:
        """Return a side-effect-free snapshot of owned runtime resources."""

        with self._lifecycle_lock:
            closed = self._closed
            open_adapters = tuple(
                sorted(
                    (
                        registration.site
                        for registration in self._open_adapters.values()
                    ),
                    key=lambda site: site.value,
                )
            )
            cleanup_errors = list(self._cleanup_errors)
            managed_adapters = tuple(
                (registration.site, registration.scraper)
                for registration in self._managed_adapters.values()
            )
        for site, scraper in managed_adapters:
            for error in getattr(scraper, "transport_cleanup_errors", ()):
                cleanup_errors.append(f"{site.value}: transport cleanup: {error}")
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
            cleanup_errors=tuple(dict.fromkeys(cleanup_errors)),
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
                complete_event = SearchCompleteEvent(
                    sequence=self._sequence,
                    emitted_at=datetime.now(timezone.utc),
                    total_jobs=self._total_jobs,
                    total_errors=self._total_errors,
                    completed=completed,
                )
                self._terminal_delivered = True
                self._remember(complete_event)
                return complete_event

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
            stream_event: SearchEvent
            if message.type is _MessageType.JOB:
                assert message.job is not None and message.job_key is not None
                stream_event = JobEvent(
                    sequence=self._sequence,
                    emitted_at=now,
                    site=message.site,
                    job=message.job,
                    job_key=message.job_key,
                    resume_state=freeze_state(message.state),
                )
                self._total_jobs += 1
            elif message.type is _MessageType.PROGRESS:
                stream_event = ProgressEvent(
                    sequence=self._sequence,
                    emitted_at=now,
                    site=message.site,
                    resume_state=freeze_state(message.state),
                    message=message.message,
                )
            elif message.type is _MessageType.WARNING:
                stream_event = WarningEvent(
                    sequence=self._sequence,
                    emitted_at=now,
                    site=message.site,
                    message=message.message or "adapter warning",
                    resume_state=freeze_state(message.state),
                )
            elif message.type is _MessageType.ERROR:
                stream_event = ErrorEvent(
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
                stream_event = SiteCompleteEvent(
                    sequence=self._sequence,
                    emitted_at=now,
                    site=message.site,
                    emitted_count=message.emitted_count,
                    resume_state=freeze_state(message.state),
                )
            self._remember(stream_event)
            return stream_event

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

        incremental_store = (
            self.checkpoint_store
            if isinstance(self.checkpoint_store, IncrementalCheckpointStore)
            else None
        )
        adapters = dict(self._checkpoint.adapters)
        adapter_site: AdapterIdentifier | None = None
        new_seen_job_key: str | None = None
        if isinstance(
            event,
            (JobEvent, ProgressEvent, WarningEvent, ErrorEvent, SiteCompleteEvent),
        ):
            adapter_site = event.site
            existing = adapters[event.site.value]
            seen_keys = self._acknowledged_seen_keys[event.site.value]
            seen_key_set = self._acknowledged_seen_key_sets[event.site.value]
            if isinstance(event, JobEvent) and event.job_key not in seen_key_set:
                new_seen_job_key = event.job_key
            emitted_count = existing.emitted_count + (
                1 if new_seen_job_key is not None else 0
            )
            adapters[event.site.value] = existing.model_copy(
                update={
                    "state": thaw_state(event.resume_state),
                    "seen_job_keys": (
                        existing.seen_job_keys
                        if incremental_store is not None
                        else tuple(
                            [
                                *seen_keys,
                                *(
                                    (new_seen_job_key,)
                                    if new_seen_job_key is not None
                                    else ()
                                ),
                            ]
                        )
                    ),
                    "emitted_count": emitted_count,
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
            if incremental_store is None:
                self.checkpoint_store.save(checkpoint)
            else:
                incremental_store.save_incremental(
                    CheckpointWrite(
                        checkpoint=checkpoint,
                        adapter_site=adapter_site,
                        new_seen_job_key=new_seen_job_key,
                    )
                )
        except CheckpointConflictError:
            self.close()
            raise
        if new_seen_job_key is not None and adapter_site is not None:
            self._acknowledged_seen_keys[adapter_site.value].append(new_seen_job_key)
            self._acknowledged_seen_key_sets[adapter_site.value].add(new_seen_job_key)
        self._checkpoint = checkpoint
        self._last_acknowledged = True

    def close(self, *, acknowledge: bool = False) -> None:
        with self._close_lock:
            with self._lifecycle_lock:
                if self._closed:
                    return
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
            for registration in adapters:
                self._schedule_adapter_close(registration)

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
