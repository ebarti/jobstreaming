from __future__ import annotations

from itertools import cycle
from typing import Any

import httpcloak

from jobstreaming.exception import (
    AdapterFailureError,
    AuthenticationConfigurationError,
    InvalidRequestError,
    JobStreamingError,
    TransientNetworkError,
)


def _proxy_url(proxy: str) -> str:
    if not proxy.startswith(("http://", "https://", "socks5://", "socks5h://")):
        proxy = f"http://{proxy}"
    return "" if proxy == "http://localhost" else proxy


def _request_error(error: httpcloak.HTTPCloakError) -> JobStreamingError:
    # The Python binding carries native transport errors as strings. Keep unknown
    # failures non-retryable and avoid surfacing URLs or proxy credentials.
    details = str(error).casefold()
    if any(
        marker in details
        for marker in (
            "x509:",
            "certificate verify failed",
            "failed to verify certificate",
            "proxyconnect",
            "proxy connect",
            "proxy authentication",
            "invalid proxy",
        )
    ):
        return AuthenticationConfigurationError("HTTP certificate or proxy failure")
    if any(
        marker in details
        for marker in (
            "invalid url",
            "missing protocol scheme",
            "unsupported protocol scheme",
        )
    ):
        return InvalidRequestError("Invalid HTTP request URL")
    if (
        details == "eof"
        or details.endswith(": eof")
        or any(
            marker in details
            for marker in (
                "i/o timeout",
                "timeout exceeded",
                "timed out",
                "deadline exceeded",
                "connection refused",
                "connection reset",
                "connection closed",
                "broken pipe",
                "no such host",
                "network is unreachable",
                "unexpected eof",
            )
        )
    ):
        return TransientNetworkError("HTTP connection or timeout failure")
    return AdapterFailureError("HTTP transport request failed")


class BoardSession:
    """Synchronous transport shared by the built-in adapters."""

    def __init__(
        self,
        *,
        proxies: list[str] | str | None,
        clear_cookies: bool,
        allow_redirects: bool,
    ) -> None:
        proxy_list = [proxies] if isinstance(proxies, str) else proxies
        self._proxies = cycle(map(_proxy_url, proxy_list)) if proxy_list else None
        self._active_proxy = ""
        self._clear_cookies = clear_cookies
        self._session = httpcloak.Session(
            preset="chrome-158",
            verify=True,
            retry=0,
            allow_redirects=allow_redirects,
            max_redirects=30,
            without_conditional_cache=True,
        )
        self.headers = self._session.headers

    def get(self, url: str, **kwargs: Any) -> httpcloak.Response:
        return self.request("GET", url, **kwargs)

    def post(self, url: str, **kwargs: Any) -> httpcloak.Response:
        return self.request("POST", url, **kwargs)

    def request(
        self,
        method: str,
        url: str,
        *,
        headers: dict[str, str] | None = None,
        **kwargs: Any,
    ) -> httpcloak.Response:
        if self._clear_cookies:
            self._session.clear_cookies()
        if self._proxies is not None:
            proxy = next(self._proxies)
            if proxy != self._active_proxy:
                try:
                    self._session.set_proxy(proxy)
                except httpcloak.HTTPCloakError:
                    raise AuthenticationConfigurationError(
                        "Invalid HTTP proxy"
                    ) from None
                self._active_proxy = proxy

        custom_user_agent = any(
            name.lower() == "user-agent" for name in (self.headers | (headers or {}))
        )
        try:
            response = self._session.request(
                method,
                url,
                headers=headers,
                disable_client_hints=custom_user_agent,
                **kwargs,
            )
        except httpcloak.HTTPCloakError as error:
            raise _request_error(error) from None

        retry_after = next(
            (
                value
                for name, value in response.headers.items()
                if name.lower() == "retry-after"
            ),
            None,
        )
        if isinstance(retry_after, list):
            retry_after = retry_after[0] if retry_after else None
        if retry_after is not None:
            response.headers["Retry-After"] = retry_after
        return response

    def close(self) -> None:
        self._session.close()
