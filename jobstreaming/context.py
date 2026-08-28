from __future__ import annotations

import queue
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum
from typing import Any, TypeVar, cast

from jobstreaming.events import (
    AdapterCheckpoint,
    ProgressPhase,
    ProgressUnit,
    ProviderProgress,
    freeze_state,
    thaw_state,
)
from jobstreaming.exception import ErrorCode, StreamCancelledError
from jobstreaming.model import AdapterIdentifier, JobPost, SearchRequest
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
    site: AdapterIdentifier
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
    progress: ProviderProgress | None = None


MessageSink = Callable[[_AdapterMessage], bool]
CancellationCallback = Callable[[], bool]
_Result = TypeVar("_Result")


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
        self._operation_start_lock = threading.RLock()

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
        with self._operation_start_lock:
            self._stop_event.set()

    def start_unless_cancelled(
        self,
        start: Callable[[], _Result],
    ) -> _Result | None:
        """Linearize owned operation starts against stream shutdown."""

        with self._operation_start_lock:
            if self.cancelled or self._stop_event.is_set():
                return None
            return start()

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
        site: AdapterIdentifier,
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
        site: AdapterIdentifier,
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

    def run_interruptibly(
        self,
        operation: Callable[[], _Result],
        *,
        on_started: Callable[[], None] | None = None,
    ) -> _Result:
        """Run a potentially blocking adapter operation behind cancellation."""

        completed: queue.Queue[tuple[bool, object]] = queue.Queue(maxsize=1)

        def invoke() -> None:
            try:
                completed.put((True, operation()))
            except BaseException as exc:
                completed.put((False, exc))

        def start() -> threading.Thread:
            thread = self._operation_threads.start(
                name=f"jobstreaming-network-{self.site.value}",
                target=invoke,
            )
            if on_started is not None:
                on_started()
            return thread

        if self._cancellation.start_unless_cancelled(start) is None:
            raise StreamCancelledError("Search stream was cancelled")
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
        return self.already_seen_identity(job.id or job.job_url)

    def already_seen_identity(self, identity: str) -> bool:
        """Check a provider identity before constructing or enriching a job."""

        key = stable_job_key(self.site.value, identity)
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
        self,
        resume_state: dict[str, Any],
        *,
        completed_units: int,
        raw_items_seen: int | None,
        has_more: bool | None,
        message: str | None = None,
        phase: ProgressPhase = ProgressPhase.SEARCH,
        unit: ProgressUnit = ProgressUnit.PAGE,
        total_units: int | None = None,
    ) -> None:
        with self._lock:
            self._state = thaw_state(freeze_state(resume_state))
            state = thaw_state(freeze_state(self._state))
            progress = ProviderProgress(
                phase=phase,
                unit=unit,
                completed_units=completed_units,
                total_units=total_units,
                raw_items_seen=raw_items_seen,
                jobs_emitted=len(self._seen),
                has_more=has_more,
            )
        if self._sink is not None and not self.cancelled:
            self._sink(
                _AdapterMessage(
                    type=_MessageType.PROGRESS,
                    site=self.site,
                    state=state,
                    message=message,
                    emitted_count=self.emitted_count,
                    progress=progress,
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
