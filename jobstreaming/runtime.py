from __future__ import annotations

import inspect
import queue
import threading
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any

from jobstreaming.checkpoint import (
    CheckpointMismatchError,
    CheckpointStore,
    MemoryCheckpointStore,
)
from jobstreaming.events import (
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
from jobstreaming.model import JobPost, SearchRequest, Site
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


@dataclass(frozen=True, slots=True)
class _AdapterMessage:
    type: _MessageType
    site: Site
    state: dict[str, Any]
    job: JobPost | None = None
    job_key: str | None = None
    message: str | None = None
    error_type: str | None = None
    recoverable: bool = True
    emitted_count: int = 0


MessageSink = Callable[[_AdapterMessage], bool]


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
    ) -> None:
        self.site = site
        self.request = request
        self._sink = sink
        self._stop_event = stop_event or threading.Event()
        self._state = thaw_state(freeze_state(checkpoint.state)) if checkpoint else {}
        self._seen = set(checkpoint.seen_job_keys) if checkpoint else set()
        self._lock = threading.Lock()

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
        return self._stop_event.is_set()

    @property
    def should_continue(self) -> bool:
        return not self.cancelled and self.emitted_count < self.request.results_wanted

    def wait(self, seconds: float) -> bool:
        """Wait between requests, waking immediately when the stream is closed."""
        if seconds <= 0:
            return self.should_continue
        return not self._stop_event.wait(seconds) and self.should_continue

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

    Requesting the next event implicitly acknowledges the previous event. A crash
    while handling an event therefore replays that event after restart instead of
    silently losing it. Call ``ack`` explicitly before intentionally stopping.
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
    ) -> None:
        if queue_size < 1:
            raise ValueError("queue_size must be positive")
        if max_retries < 0:
            raise ValueError("max_retries cannot be negative")
        if retry_backoff < 0:
            raise ValueError("retry_backoff cannot be negative")
        self.request = request
        self.registry = registry
        self.checkpoint_store = checkpoint_store or MemoryCheckpointStore()
        self.proxies = proxies
        self.ca_cert = ca_cert
        self.user_agent = user_agent
        self.max_retries = max_retries
        self.retry_backoff = retry_backoff
        self._queue: queue.Queue[_AdapterMessage] = queue.Queue(queue_size)
        self._stop_event = threading.Event()
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
        loaded = self.checkpoint_store.load() if resume else None
        if loaded is None:
            checkpoint = SearchCheckpoint.for_request(self.request)
            self.checkpoint_store.save(checkpoint)
            return checkpoint
        if loaded.request_fingerprint != self.request.fingerprint():
            raise CheckpointMismatchError(
                "Checkpoint belongs to a different search request"
            )
        missing_sites = [
            site for site in self.request.sites if site.value not in loaded.adapters
        ]
        if missing_sites:
            adapters = dict(loaded.adapters)
            for site in missing_sites:
                adapters[site.value] = AdapterCheckpoint(site=site)
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
        while not self._stop_event.is_set():
            try:
                self._queue.put(message, timeout=0.1)
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
            stop_event=self._stop_event,
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
                    parameters = inspect.signature(scraper.scrape).parameters
                    if "context" in parameters:
                        response = scraper.scrape(self.request, context=context)
                    else:
                        response = scraper.scrape(self.request)
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

                    retryable = not isinstance(exc, (TypeError, ValueError))
                    retries_left = self.max_retries - attempt
                    if not retryable or retries_left <= 0:
                        self._put_message(
                            _AdapterMessage(
                                type=_MessageType.ERROR,
                                site=site,
                                state=context.resume_state,
                                message=str(exc),
                                error_type=type(exc).__name__,
                                recoverable=retryable,
                                emitted_count=context.emitted_count,
                            )
                        )
                        return

                    delay = self.retry_backoff * (2**attempt)
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

    def __iter__(self) -> SearchStream:
        return self

    def __next__(self) -> SearchEvent:
        if self._closed:
            raise StopIteration
        if self._last_delivered is not None and not self._last_acknowledged:
            self.ack(self._last_delivered)
        if self._terminal_delivered:
            self.close()
            raise StopIteration

        while True:
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

            message = self._queue.get()
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
                    recoverable=message.recoverable,
                    resume_state=freeze_state(message.state),
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
        self._checkpoint = self._checkpoint.model_copy(
            update={
                "adapters": adapters,
                "completed": completed,
                "updated_at": datetime.now(timezone.utc),
            }
        )
        self.checkpoint_store.save(self._checkpoint)
        self._last_acknowledged = True

    def close(self, *, acknowledge: bool = False) -> None:
        if self._closed:
            return
        if acknowledge and self._last_delivered is not None:
            self.ack(self._last_delivered)
        self._closed = True
        self._stop_event.set()

    def __enter__(self) -> SearchStream:
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        self.close(acknowledge=False)
