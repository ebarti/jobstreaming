from types import SimpleNamespace
from unittest.mock import Mock
from urllib.parse import parse_qsl

import pytest

from jobstreaming.exception import error_for_http_status
from jobstreaming.model import SearchRequest, Site
from jobstreaming.util import create_session
from jobstreaming.ziprecruiter import ZipRecruiter
from jobstreaming.ziprecruiter.constant import get_cookie_data


def test_httpcloak_session_preserves_request_options(monkeypatch: pytest.MonkeyPatch):
    native_session = Mock(headers={})
    native_session.request.side_effect = lambda *args, **kwargs: SimpleNamespace(
        headers={"retry-after": ["3"]}
    )
    open_session = Mock(return_value=native_session)
    monkeypatch.setattr("jobstreaming.transport.httpcloak.Session", open_session)

    session = create_session(
        proxies=["proxy.example.test:3128", "localhost"],
        clear_cookies=True,
        allow_redirects=False,
    )
    session.headers["User-Agent"] = "Test app"
    redirect = session.get("https://example.test/redirect", timeout=0.25)
    session.get("https://example.test/final", timeout=2.5, allow_redirects=True)
    session.close()

    assert open_session.call_args.kwargs["verify"] is True
    assert open_session.call_args.kwargs["retry"] == 0
    assert open_session.call_args.kwargs["allow_redirects"] is False
    assert [call.args[0] for call in native_session.set_proxy.call_args_list] == [
        "http://proxy.example.test:3128",
        "",
    ]
    assert native_session.clear_cookies.call_count == 2
    requests = native_session.request.call_args_list
    assert [call.kwargs["timeout"] for call in requests] == [0.25, 2.5]
    assert requests[1].kwargs["allow_redirects"] is True
    assert all(call.kwargs["disable_client_hints"] for call in requests)
    error = error_for_http_status(
        "Test", 429, retry_after=redirect.headers["Retry-After"]
    )
    assert error.retry_after == 3
    native_session.close.assert_called_once_with()


def test_ziprecruiter_preserves_repeated_form_fields(monkeypatch: pytest.MonkeyPatch):
    scraper = ZipRecruiter(authorization="offline-test")
    try:
        scraper.scraper_input = SearchRequest(site_type=(Site.ZIP_RECRUITER,))
        post = Mock()
        monkeypatch.setattr(scraper.session, "post", post)
        scraper._get_cookies()
        post.assert_called_once()
        call = post.call_args
        assert parse_qsl(call.kwargs["data"]) == get_cookie_data
        assert (
            call.kwargs["headers"]["content-type"]
            == "application/x-www-form-urlencoded"
        )
    finally:
        scraper.close()
