from __future__ import annotations

import math
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from enum import Enum

from requests import exceptions as requests_exceptions

MAX_RETRY_AFTER_SECONDS = 300.0


def parse_retry_after(value: str | int | float | None) -> float | None:
    """Parse an HTTP Retry-After value and bound coordinator sleep time."""

    if value is None:
        return None
    try:
        seconds = float(value)
    except (TypeError, ValueError):
        try:
            retry_at = parsedate_to_datetime(str(value))
        except (TypeError, ValueError, OverflowError):
            return None
        if retry_at.tzinfo is None:
            retry_at = retry_at.replace(tzinfo=timezone.utc)
        seconds = (retry_at - datetime.now(timezone.utc)).total_seconds()
    if not math.isfinite(seconds):
        return None
    return min(max(0.0, seconds), MAX_RETRY_AFTER_SECONDS)


class ErrorCode(str, Enum):
    """Stable, machine-readable failure categories exposed by the stream."""

    TRANSIENT_NETWORK = "transient_network"
    RATE_LIMITED = "rate_limited"
    INVALID_REQUEST = "invalid_request"
    CURSOR_EXPIRED = "cursor_expired"
    AUTHENTICATION_CONFIGURATION = "authentication_configuration"
    CANCELLED = "cancelled"
    ADAPTER_FAILURE = "adapter_failure"


class JobStreamingError(RuntimeError):
    code = ErrorCode.ADAPTER_FAILURE
    retryable = False
    reset_checkpoint = False

    def __init__(
        self,
        message: str = "",
        *,
        retry_after: str | int | float | None = None,
    ) -> None:
        super().__init__(message)
        self.retry_after = parse_retry_after(retry_after)


class TransientNetworkError(JobStreamingError):
    code = ErrorCode.TRANSIENT_NETWORK
    retryable = True


class RateLimitError(JobStreamingError):
    code = ErrorCode.RATE_LIMITED
    retryable = True


class InvalidRequestError(JobStreamingError):
    code = ErrorCode.INVALID_REQUEST


class CursorExpiredError(JobStreamingError):
    code = ErrorCode.CURSOR_EXPIRED
    reset_checkpoint = True


class AuthenticationConfigurationError(JobStreamingError):
    code = ErrorCode.AUTHENTICATION_CONFIGURATION


class MissingOptionalDependencyError(JobStreamingError):
    """A public API requires an optional package extra that is not installed."""

    def __init__(self, *, extra: str, dependency: str) -> None:
        self.extra = extra
        self.dependency = dependency
        self.install_spec = f"jobstreaming[{extra}]"
        super().__init__(
            f"{dependency} is required for this operation; install "
            f"{self.install_spec!r} (for example, "
            f'pip install "{self.install_spec}")'
        )


class StreamCancelledError(JobStreamingError):
    code = ErrorCode.CANCELLED


class UnacknowledgedEventError(RuntimeError):
    """Raised when explicit acknowledgement is required before another delivery."""


class AdapterFailureError(JobStreamingError):
    pass


class LinkedInException(AdapterFailureError):
    def __init__(self, message=None):
        super().__init__(message or "An error occurred with LinkedIn")


class IndeedException(AdapterFailureError):
    def __init__(self, message=None):
        super().__init__(message or "An error occurred with Indeed")


class ZipRecruiterException(AdapterFailureError):
    def __init__(self, message=None):
        super().__init__(message or "An error occurred with ZipRecruiter")


class GlassdoorException(AdapterFailureError):
    def __init__(self, message=None):
        super().__init__(message or "An error occurred with Glassdoor")


class GoogleJobsException(AdapterFailureError):
    def __init__(self, message=None):
        super().__init__(message or "An error occurred with Google Jobs")


class BaytException(AdapterFailureError):
    def __init__(self, message=None):
        super().__init__(message or "An error occurred with Bayt")


class NaukriException(AdapterFailureError):
    def __init__(self, message=None):
        super().__init__(message or "An error occurred with Naukri")


class BDJobsException(AdapterFailureError):
    def __init__(self, message=None):
        super().__init__(message or "An error occurred with BDJobs")


@dataclass(frozen=True, slots=True)
class ErrorInfo:
    code: ErrorCode
    retryable: bool
    reset_checkpoint: bool
    retry_after: float | None = None


_HTTP_STATUS = re.compile(r"\bHTTP\s+(\d{3})\b", re.IGNORECASE)


def error_for_http_status(
    board: str,
    status_code: int,
    *,
    cursor_active: bool = False,
    retry_after: str | int | float | None = None,
) -> JobStreamingError:
    """Translate an HTTP status at an adapter boundary into a stable failure."""

    message = f"{board} returned HTTP {status_code}"
    if status_code == 429:
        return RateLimitError(
            f"{board} rate limited the search",
            retry_after=retry_after,
        )
    if status_code in (401, 403):
        return AuthenticationConfigurationError(message)
    if cursor_active and status_code == 410:
        return CursorExpiredError(f"{board} rejected an expired cursor")
    if status_code in (408, 425) or 500 <= status_code < 600:
        return TransientNetworkError(message, retry_after=retry_after)
    if 400 <= status_code < 500:
        return InvalidRequestError(message)
    return AdapterFailureError(message)


def error_for_response_message(
    board: str,
    message: str,
    *,
    cursor_active: bool = False,
) -> JobStreamingError:
    """Classify structured board/API errors that do not expose an HTTP status."""

    normalized = message.casefold()
    cursor_terms = ("cursor", "continuation", "continue token", "page token")
    expiry_terms = ("expired", "invalid", "stale", "no longer valid")
    if (
        cursor_active
        and any(term in normalized for term in cursor_terms)
        and any(term in normalized for term in expiry_terms)
    ):
        return CursorExpiredError(f"{board}: {message}")
    if "rate limit" in normalized or "too many requests" in normalized:
        return RateLimitError(f"{board}: {message}")
    if any(
        term in normalized
        for term in ("unauthorized", "forbidden", "authentication", "api key")
    ):
        return AuthenticationConfigurationError(f"{board}: {message}")
    return AdapterFailureError(f"{board}: {message}")


def classify_exception(exc: Exception) -> ErrorInfo:
    """Classify an adapter exception without guessing that unknowns are retryable."""

    if isinstance(exc, JobStreamingError) and exc.code is not ErrorCode.ADAPTER_FAILURE:
        return ErrorInfo(
            exc.code,
            exc.retryable,
            exc.reset_checkpoint,
            exc.retry_after,
        )

    if isinstance(
        exc,
        (
            requests_exceptions.SSLError,
            requests_exceptions.ProxyError,
            requests_exceptions.InvalidProxyURL,
        ),
    ):
        return ErrorInfo(ErrorCode.AUTHENTICATION_CONFIGURATION, False, False)

    if isinstance(
        exc,
        (
            requests_exceptions.Timeout,
            requests_exceptions.ConnectionError,
            requests_exceptions.RetryError,
        ),
    ) or type(exc).__module__.startswith("tls_client"):
        return ErrorInfo(ErrorCode.TRANSIENT_NETWORK, True, False)

    if isinstance(exc, requests_exceptions.HTTPError):
        response = exc.response
        status_code = response.status_code if response is not None else None
        if status_code is not None:
            headers = getattr(response, "headers", {}) or {}
            classified = error_for_http_status(
                "Adapter",
                status_code,
                retry_after=headers.get("Retry-After"),
            )
            return ErrorInfo(
                classified.code,
                classified.retryable,
                classified.reset_checkpoint,
                classified.retry_after,
            )

    if isinstance(exc, (TypeError, ValueError)):
        return ErrorInfo(ErrorCode.INVALID_REQUEST, False, False)

    message = str(exc)
    match = _HTTP_STATUS.search(message)
    if match:
        classified = error_for_http_status("Adapter", int(match.group(1)))
        return ErrorInfo(
            classified.code,
            classified.retryable,
            classified.reset_checkpoint,
            classified.retry_after,
        )
    normalized = message.casefold()
    if "rate limit" in normalized or "too many requests" in normalized:
        return ErrorInfo(ErrorCode.RATE_LIMITED, True, False)

    return ErrorInfo(ErrorCode.ADAPTER_FAILURE, False, False)
