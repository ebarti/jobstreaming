import json
from urllib.parse import parse_qs, urlsplit

import pytest
from requests import Response, Session

from jobstreaming import (
    ErrorEvent,
    JobEvent,
    MemoryCheckpointStore,
    SearchRequest,
    Site,
    SiteCompleteEvent,
    SiteTerminationReason,
    WarningEvent,
    stream_search,
)
from jobstreaming.lennysjobs import LISTINGS_URL_ENV


def card(key, title, location="Barcelona, Catalonia, Spain", **fields):
    return {
        "job_id": key,
        "title": title,
        "company_name": "Example",
        "location": location,
        **fields,
    }


def page(hits, *, number=0, pages=1, query="", **metadata):
    return {"hits": hits, "query": query, "page": number, "nbPages": pages, **metadata}


def html(data):
    return (
        '<html><script type="application/json" id="__NEXT_DATA__">'
        + json.dumps({"props": {"pageProps": {"initialJobs": data}}})
        + "</script></html>"
    )


@pytest.fixture
def connection(monkeypatch):
    monkeypatch.setenv(LISTINGS_URL_ENV, "https://listings.example.test/search")
    calls = []

    def serve(provider):
        def send(session, request, **kwargs):
            url = urlsplit(request.url)
            assert (request.method, url.netloc, url.path) == (
                "GET",
                "listings.example.test",
                "/search",
            )
            query = dict(
                (k, v[0])
                for k, v in parse_qs(url.query, keep_blank_values=True).items()
            )
            calls.append(query)
            result = provider(query)
            response = Response()
            response.status_code = 200
            if isinstance(result, tuple):
                response.status_code, response.headers, result = result
            response._content = (
                result if isinstance(result, str) else json.dumps(result)
            ).encode()
            response.encoding = "utf-8"
            return response

        monkeypatch.setattr(Session, "send", send)
        return calls

    return serve


def search(query, **filters):
    request = SearchRequest(
        site_type=(Site.LENNYSJOBS,), search_term=query, results_wanted=None, **filters
    )
    with stream_search(request, max_retries=0) as stream:
        return list(stream)


def ids(events):
    return [event.job.id for event in events if isinstance(event, JobEvent)]


def test_captured_page_shape_filters_literal_query_and_location(connection):
    data = page(
        [
            card(
                "eng",
                "Director of Engineering",
                salary_min=80,
                salary_max=100,
                salary_currency="EUR",
                salary_period="hour",
                employment_type="full_time",
                updated_at="2026-01-01T00:00:00Z",
            ),
            card("pm", "Product Manager"),
            card(
                "wrong-place",
                "Director of Engineering",
                "Barcelona, Venezuela; Madrid, Spain",
            ),
            None,
            {"job_id": "bad", "title": ""},
        ]
    )
    calls = connection(lambda _: html(data))
    events = search("Director of Engineering", location="Barcelona, Spain")
    assert ids(events) == ["eng"]
    job = next(event.job for event in events if isinstance(event, JobEvent))
    assert job.job_url == "https://www.lennysjobs.com/job/eng"
    assert job.location.display_location() == "Barcelona, Catalonia, Spain"
    assert job.job_type[0].canonical == "fulltime"
    assert job.compensation.currency == "EUR"
    assert job.date_posted is None and job.description is None
    assert sum(isinstance(event, WarningEvent) for event in events) == 2
    assert ids(search("pROducT Manager", location="Barcelona, Spain")) == ["pm"]
    assert ids(search("missing")) == []
    assert calls[0] == {
        "page": "0",
        "query": "Director of Engineering",
        "location": "Barcelona, Spain",
    }


def test_native_query_filters_and_provider_page_cap(connection):
    def provider(params):
        number = int(params["page"])
        return page(
            [card(str(number), "Engineering lead", "Remote")],
            number=number,
            pages=2,
            nbHits=80,
            query=params["query"],
            params="location=Spain&is_remote=true&hours_old=72",
        )

    calls = connection(provider)
    events = search(
        "Director of Engineering", location="Spain", is_remote=True, hours_old=72
    )
    assert ids(events) == ["0", "1"]
    assert calls == [
        {
            "page": str(n),
            "query": "Director of Engineering",
            "location": "Spain",
            "is_remote": "true",
            "hours_old": "72",
        }
        for n in range(2)
    ]
    assert (
        next(e.reason for e in events if isinstance(e, SiteCompleteEvent))
        == SiteTerminationReason.PROVIDER_LIMIT
    )


@pytest.mark.parametrize(
    ("payload", "filters", "message"),
    [
        (page([], query="different"), {}, "different query"),
        (page([], number=1), {}, "pagination metadata"),
        ("<html>Sign in</html>", {}, "unreadable listing page"),
        ({"status": "invalid"}, {}, "pagination metadata"),
        (page([]), {"hours_old": 24}, "hours_old filter"),
        (page([]), {"is_remote": True}, "is_remote filter"),
    ],
)
def test_unusable_or_unconfirmed_response_is_not_an_empty_success(
    connection, payload, filters, message
):
    connection(lambda _: payload)
    events = search("Engineer", **filters)
    assert ids(events) == []
    assert message in next(e.message for e in events if isinstance(e, ErrorEvent))


def test_resumes_from_last_acknowledged_card_without_losing_page_siblings(connection):
    def provider(params):
        number = int(params["page"])
        return page(
            [
                card(key, "Engineer")
                for key in (["one", "two"] if number == 0 else ["two", "three"])
            ],
            number=number,
            pages=2,
            query="Engineer",
        )

    connection(provider)
    store = MemoryCheckpointStore()
    request = SearchRequest(
        site_type=(Site.LENNYSJOBS,), search_term="Engineer", results_wanted=None
    )
    with stream_search(
        request, checkpoint_store=store, ack_mode="explicit", max_retries=0
    ) as stream:
        for event in stream:
            stream.ack(event)
            if isinstance(event, JobEvent):
                assert event.job.id == "one"
                break
    with stream_search(request, checkpoint_store=store, max_retries=0) as stream:
        assert ids(list(stream)) == ["two", "three"]


def test_missing_connection_and_throttling_are_distinct(monkeypatch, connection):
    calls = connection(lambda _: (429, {"Retry-After": "19"}, ""))
    error = next(e for e in search("Engineer") if isinstance(e, ErrorEvent))
    assert error.code.value == "rate_limited" and error.retry_after == 19
    monkeypatch.delenv(LISTINGS_URL_ENV)
    error = next(e for e in search("Engineer") if isinstance(e, ErrorEvent))
    assert (
        error.code.value == "authentication_configuration"
        and LISTINGS_URL_ENV in error.message
    )
    assert len(calls) == 1
