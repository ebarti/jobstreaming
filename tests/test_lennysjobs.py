import json
from datetime import datetime, timezone
from urllib.parse import urlencode, urlsplit

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
from jobstreaming.lennysjobs.native import CATEGORY_IDS, scope

BARCELONA = "🇪🇸 Barcelona, Spain"
REMOTE_SPAIN = "🇪🇸 Spain (remote)"


def card(key, title="Engineer", **fields):
    return {"job_id": key, "title": title, "company_name": "Example", **fields}


def page(request_params, hits, *, pages=1, **overrides):
    flat = ",".join(
        value for group in request_params.get("facetFilters", []) for value in group
    )
    return {
        "results": [
            {
                "hits": hits,
                "query": request_params["query"],
                "page": request_params["page"],
                "hitsPerPage": 15,
                "nbPages": pages,
                "params": (
                    urlencode({"facetFilters": flat}) if flat else "query=Engineer"
                ),
                **overrides,
            },
            {"hits": [], "hitsPerPage": 0, "nbHits": 3000},
        ]
    }


def places(*labels):
    return {
        "results": [{"facetHits": [{"value": label, "count": 246} for label in labels]}]
    }


@pytest.fixture
def connection(monkeypatch):
    monkeypatch.setenv(LISTINGS_URL_ENV, "https://listings.example.test/jobs/search")
    calls = []

    def serve(provider):
        def send(session, request, **kwargs):
            url = urlsplit(request.url)
            assert (request.method, url.netloc, url.path, url.query) == (
                "POST",
                "listings.example.test",
                "/jobs/search",
                "",
            )
            assert request.headers["Content-Type"] == "application/json"
            batch = json.loads(request.body)
            assert len(batch) == 1 and batch[0]["indexName"] == "job"
            params = batch[0]["params"]
            assert {
                k: params[k]
                for k in (
                    "trueupPartnerId",
                    "trueupRequestVersion",
                    "trueupCompanyPreferencesKey",
                )
            } == {
                "trueupPartnerId": "lenny",
                "trueupRequestVersion": 2,
                "trueupCompanyPreferencesKey": "public",
            }
            calls.append(params)
            result = provider(params)
            response = Response()
            response.status_code = 200
            if isinstance(result, tuple):
                response.status_code, response.headers, result = result
            response._content = (
                result if isinstance(result, str) else json.dumps(result)
            ).encode()
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


def test_real_form_separates_category_location_autocomplete_and_job_hits(connection):
    def provider(p):
        if p["hitsPerPage"] == 0:
            return places("🇻🇪 Barcelona, Venezuela", BARCELONA)
        return page(
            p,
            [
                card(
                    "eng",
                    "Engineering lead",
                    salary_min=80,
                    salary_max=100,
                    salary_currency="EUR",
                    salary_period="hour",
                    employment_type="full_time",
                    location="Barcelona, Catalonia, Spain",
                ),
                None,
                card("bad", ""),
            ],
        )

    calls = connection(provider)
    events = search("Software Engineering", location="Barcelona, Spain")
    assert ids(events) == ["eng"]
    suggest, listing = calls
    assert suggest["query"] == listing["query"] == ""
    assert (suggest["facetName"], suggest["facetQuery"], suggest["hitsPerPage"]) == (
        "job_locations_combined",
        "Barcelona",
        0,
    )
    assert suggest["facetFilters"] == [["job_category_ids_all:11"]]
    assert listing["facetFilters"] == [
        ["job_category_ids_all:11"],
        [f"job_locations_combined:{BARCELONA}"],
    ]
    assert listing["hitsPerPage"] == 15 and listing["page"] == 0
    assert "facetName" not in listing and "location" not in listing
    job = next(e.job for e in events if isinstance(e, JobEvent))
    assert job.job_url == "https://www.lennysjobs.com/job/eng"
    assert (
        job.job_type[0].canonical == "fulltime" and job.compensation.currency == "EUR"
    )
    assert job.date_posted is None and job.description is None
    assert sum(isinstance(e, WarningEvent) for e in events) == 2


def test_picker_names_are_literal_and_a_title_stays_a_query(connection):
    assert scope("SeCuRiTy") == ("", [["job_category_ids_all:138"]])
    assert scope("Product Design") == ("", [["job_category_ids_all:109"]])
    assert scope("UX & Product Design") == ("", [["job_category_ids_all:7"]])
    assert len(CATEGORY_IDS) == len(set(CATEGORY_IDS.values()))
    calls = connection(lambda p: page(p, [card("title", "VP of Engineering")]))
    assert ids(search("Director of Engineering")) == ["title"]
    assert (
        len(calls) == 1
        and calls[0]["query"] == "Director of Engineering"
        and "facetFilters" not in calls[0]
    )


@pytest.mark.parametrize(
    ("place", "remote", "options", "expected"),
    [
        ("Spain", True, ["🇪🇸 Spain", REMOTE_SPAIN], REMOTE_SPAIN),
        ("Spain", False, ["🇪🇸 Spain", REMOTE_SPAIN], "🇪🇸 Spain"),
        (None, True, ["🌎 Remote", REMOTE_SPAIN], "🌎 Remote"),
        ("Barcelona", False, [BARCELONA], BARCELONA),
    ],
)
def test_location_and_remote_use_exact_board_values(
    connection, place, remote, options, expected
):
    calls = connection(
        lambda p: places(*options) if "facetName" in p else page(p, [card("job")])
    )
    events = search("Lead", location=place, is_remote=remote)
    assert ids(events) == ["job"]
    assert calls[1]["facetFilters"] == [[f"job_locations_combined:{expected}"]]
    if remote:
        assert next(e.job for e in events if isinstance(e, JobEvent)).is_remote is True


@pytest.mark.parametrize(
    ("place", "remote", "options", "ambiguous"),
    [
        ("Barcelona", False, [BARCELONA, "🇻🇪 Barcelona, Venezuela"], True),
        ("Barcelona, Spain", True, [BARCELONA, REMOTE_SPAIN], False),
        ("Madrid", False, [], False),
    ],
)
def test_unknown_or_ambiguous_place_never_broadens_the_listing(
    connection, place, remote, options, ambiguous
):
    calls = connection(lambda p: places(*options))
    events = search("Engineer", location=place, is_remote=remote)
    assert len(calls) == 1 and not ids(events)
    if ambiguous:
        assert "more specific location" in next(
            e.message for e in events if isinstance(e, ErrorEvent)
        )
    else:
        assert (
            next(e.reason for e in events if isinstance(e, SiteCompleteEvent))
            == SiteTerminationReason.SOURCE_EXHAUSTED
        )
        assert any(isinstance(e, WarningEvent) for e in events)


def test_update_lookback_uses_card_timestamps_and_excludes_unknown_dates(
    connection, monkeypatch
):
    now = 1800000000
    monkeypatch.setattr("jobstreaming.lennysjobs.time", lambda: now)

    def stamp(seconds):
        return datetime.fromtimestamp(now - seconds, timezone.utc).isoformat()

    calls = connection(
        lambda p: page(
            p,
            [
                card("edge", updated_at=stamp(3600)),
                card("old", updated_at=stamp(3601)),
                card("unknown"),
                card("bad", updated_at="yesterday"),
                card("naive", updated_at="2026-01-01"),
            ],
        )
    )
    events = search("Engineer", hours_old=1)
    assert ids(events) == ["edge"] and "numericFilters" not in calls[0]
    assert next(e.job for e in events if isinstance(e, JobEvent)).date_posted is None
    assert sum(isinstance(e, WarningEvent) for e in events) == 1


@pytest.mark.parametrize(
    ("body", "location", "message"),
    [
        (lambda p: page(p, [], query="other"), None, "different query"),
        (lambda p: page(p, [], page=1), None, "pagination metadata"),
        (lambda p: places(BARCELONA), None, "pagination metadata"),
        (lambda p: {"hits": []}, None, "unreadable search response"),
        (lambda p: "<html>Sign in</html>", None, "unreadable search response"),
        (
            lambda p: {"results": [{"hits": []}]},
            "Spain",
            "invalid location suggestions",
        ),
        (
            lambda p: page(p, [], params="facetFilters=job_category_ids_all%3A22"),
            None,
            "confirm the selected filters",
        ),
    ],
)
def test_wrong_scope_facet_only_and_unusable_results_fail_explicitly(
    connection, body, location, message
):
    connection(body)
    events = search("Engineer", location=location)
    assert not ids(events) and message in next(
        e.message for e in events if isinstance(e, ErrorEvent)
    )


def test_pagination_and_recovery_reuse_location_cutoff_and_processed_ids(
    connection, monkeypatch
):
    instant = [1800000000]
    monkeypatch.setattr("jobstreaming.lennysjobs.time", lambda: instant[0])
    stamp = datetime.fromtimestamp(instant[0], timezone.utc).isoformat()

    def provider(p):
        if "facetName" in p:
            return places(BARCELONA)
        keys = ["one", "two"] if p["page"] == 0 else ["two", "three"]
        return page(p, [card(k, updated_at=stamp) for k in keys], pages=2, nbHits=80)

    calls = connection(provider)
    store = MemoryCheckpointStore()
    request = SearchRequest(
        site_type=(Site.LENNYSJOBS,),
        search_term="Engineer",
        location="Barcelona, Spain",
        hours_old=1,
        results_wanted=None,
    )
    with stream_search(
        request, checkpoint_store=store, ack_mode="explicit", max_retries=0
    ) as stream:
        for e in stream:
            stream.ack(e)
            if isinstance(e, JobEvent):
                break
    instant[0] += 7200
    with stream_search(request, checkpoint_store=store, max_retries=0) as stream:
        events = list(stream)
    assert ids(events) == ["two", "three"]
    assert sum("facetName" in p for p in calls) == 1
    assert calls[-1]["page"] == 1
    assert (
        next(e.reason for e in events if isinstance(e, SiteCompleteEvent))
        == SiteTerminationReason.PROVIDER_LIMIT
    )


def test_missing_connection_throttling_and_access_denial_are_distinct(
    monkeypatch, connection
):
    calls = connection(lambda p: (429, {"Retry-After": "19"}, ""))
    error = next(e for e in search("Engineer") if isinstance(e, ErrorEvent))
    assert error.code.value == "rate_limited" and error.retry_after == 19
    connection(lambda p: (403, {}, ""))
    error = next(e for e in search("Engineer") if isinstance(e, ErrorEvent))
    assert error.code.value == "authentication_configuration" and "403" in error.message
    monkeypatch.delenv(LISTINGS_URL_ENV)
    error = next(e for e in search("Engineer") if isinstance(e, ErrorEvent))
    assert (
        error.code.value == "authentication_configuration"
        and LISTINGS_URL_ENV in error.message
    )
    assert len(calls) == 2
