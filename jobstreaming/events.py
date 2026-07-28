from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from types import MappingProxyType
from typing import Any, TypeAlias

from pydantic import Field

from jobstreaming.exception import ErrorCode
from jobstreaming.model import FrozenModel, JobPost, SearchRequest, Site

CHECKPOINT_VERSION = 1


def freeze_state(state: Mapping[str, Any]) -> Mapping[str, Any]:
    """Return a deeply read-only snapshot of adapter resume state."""

    def freeze(value: Any) -> Any:
        if isinstance(value, Mapping):
            return MappingProxyType(
                {str(key): freeze(item) for key, item in value.items()}
            )
        if isinstance(value, (list, tuple)):
            return tuple(freeze(item) for item in value)
        if isinstance(value, (set, frozenset)):
            return frozenset(freeze(item) for item in value)
        return value

    return freeze(state)


def thaw_state(state: Mapping[str, Any]) -> dict[str, Any]:
    """Return a detached, checkpoint-serializable copy of resume state."""

    def thaw(value: Any) -> Any:
        if isinstance(value, Mapping):
            return {str(key): thaw(item) for key, item in value.items()}
        if isinstance(value, tuple):
            return [thaw(item) for item in value]
        if isinstance(value, frozenset):
            return [thaw(item) for item in sorted(value, key=repr)]
        return value

    return thaw(state)


class EventType(str, Enum):
    JOB = "job"
    PROGRESS = "progress"
    WARNING = "warning"
    ERROR = "error"
    SITE_COMPLETE = "site_complete"
    SEARCH_COMPLETE = "search_complete"


@dataclass(frozen=True, slots=True)
class JobEvent:
    sequence: int
    emitted_at: datetime
    site: Site
    job: JobPost
    job_key: str
    resume_state: Mapping[str, Any]
    type: EventType = EventType.JOB


@dataclass(frozen=True, slots=True)
class ProgressEvent:
    sequence: int
    emitted_at: datetime
    site: Site
    resume_state: Mapping[str, Any]
    message: str | None = None
    type: EventType = EventType.PROGRESS


@dataclass(frozen=True, slots=True)
class WarningEvent:
    sequence: int
    emitted_at: datetime
    site: Site
    message: str
    resume_state: Mapping[str, Any]
    type: EventType = EventType.WARNING


@dataclass(frozen=True, slots=True)
class ErrorEvent:
    sequence: int
    emitted_at: datetime
    site: Site
    message: str
    error_type: str
    recoverable: bool
    resume_state: Mapping[str, Any]
    code: ErrorCode = ErrorCode.ADAPTER_FAILURE
    retryable: bool = False
    reset_checkpoint: bool = False
    type: EventType = EventType.ERROR
    retry_after: float | None = None


@dataclass(frozen=True, slots=True)
class SiteCompleteEvent:
    sequence: int
    emitted_at: datetime
    site: Site
    emitted_count: int
    resume_state: Mapping[str, Any]
    type: EventType = EventType.SITE_COMPLETE


@dataclass(frozen=True, slots=True)
class SearchCompleteEvent:
    sequence: int
    emitted_at: datetime
    total_jobs: int
    total_errors: int
    completed: bool
    type: EventType = EventType.SEARCH_COMPLETE


SearchEvent: TypeAlias = (
    JobEvent
    | ProgressEvent
    | WarningEvent
    | ErrorEvent
    | SiteCompleteEvent
    | SearchCompleteEvent
)


class AdapterCheckpoint(FrozenModel):
    site: Site
    cursor_schema_version: int = Field(default=1, ge=1)
    state: dict[str, Any] = Field(default_factory=dict)
    seen_job_keys: tuple[str, ...] = ()
    emitted_count: int = 0
    completed: bool = False
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class SearchCheckpoint(FrozenModel):
    version: int = CHECKPOINT_VERSION
    revision: int = Field(default=0, ge=0)
    request_fingerprint: str
    adapters: dict[str, AdapterCheckpoint] = Field(default_factory=dict)
    completed: bool = False
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    @classmethod
    def for_request(
        cls,
        request: SearchRequest,
        cursor_schema_versions: Mapping[Site, int] | None = None,
    ) -> SearchCheckpoint:
        versions = cursor_schema_versions or {}
        return cls(
            request_fingerprint=request.fingerprint(),
            adapters={
                site.value: AdapterCheckpoint(
                    site=site,
                    cursor_schema_version=versions.get(site, 1),
                )
                for site in request.sites
            },
        )
