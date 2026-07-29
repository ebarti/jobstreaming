from __future__ import annotations

import queue
import threading
from collections.abc import Iterator
from datetime import datetime, timezone
from enum import Enum
from functools import partial
from typing import cast

from jobstreaming.checkpoint import (
    CheckpointCompatibilityError,
    CheckpointConflictError,
    CheckpointMismatchError,
    CheckpointStore,
    MemoryCheckpointStore,
)
from jobstreaming.context import (
    CancellationCallback,
    ScrapeContext,
    _AdapterMessage,
    _CancellationController,
    _MessageType,
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
    classify_exception,
)
from jobstreaming.model import (
    AdapterIdentifier,
    SearchFilter,
    SearchRequest,
)
from jobstreaming.registry import AdapterRegistry


class AckMode(str, Enum):
    IMPLICIT = "implicit"
    EXPLICIT = "explicit"


_WAKE = object()


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
        self._threads: list[threading.Thread] = []
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
            thread = threading.Thread(
                target=self._run_adapter,
                args=(site,),
                name=f"jobstreaming-{site.value}",
                daemon=True,
            )
            self._threads.append(thread)
            thread.start()

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
        )
        try:
            if not context.should_continue:
                self._emit_site_complete(site, context)
                return
            for attempt in range(self.max_retries + 1):
                try:
                    scraper = self.registry.create(
                        site,
                        proxies=self.proxies,
                        ca_cert=self.ca_cert,
                        user_agent=self.user_agent,
                    )
                    if attempt == 0:
                        self._emit_capability_warnings(scraper, context)
                    operation = partial(
                        scraper.scrape,
                        self.request,
                        context=context,
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
        return self._checkpoint.model_copy(deep=True)

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
        if self._closed:
            return
        if acknowledge and self._last_delivered is not None:
            self.ack(self._last_delivered)
        self._closed = True
        self._cancellation.stop()
        self._wake_iterator()

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
