"""Lenny's public search form vocabulary and native TrueUp search responses."""

from __future__ import annotations

import unicodedata
from urllib.parse import parse_qs

from jobstreaming.exception import AdapterFailureError

BOARD = "Lenny's Jobs"
CATEGORY_FACET = "job_category_ids_all"
LOCATION_FACET = "job_locations_combined"
PAGE_SIZE = 15

# Exact labels and numeric values from the public Job category picker.
CATEGORY_IDS = {
    "ai engineering": "108",
    "artificial intelligence": "6",
    "backend": "132",
    "communications": "158",
    "content design": "111",
    "content marketing": "159",
    "data engineering": "139",
    "devops/sre": "137",
    "developer relations": "157",
    "embedded/firmware": "140",
    "event marketing": "160",
    "forward deployed": "143",
    "frontend": "133",
    "full-stack": "134",
    "general marketing": "163",
    "general software engineering": "144",
    "growth": "154",
    "infrastructure/platform": "136",
    "ml engineering": "106",
    "ml infrastructure": "107",
    "marketing & growth": "16",
    "mobile": "135",
    "networking": "142",
    "product design": "109",
    "product management": "22",
    "product marketing": "155",
    "public relations": "156",
    "qa/test": "141",
    "research scientist": "105",
    "security": "138",
    "social media": "162",
    "software engineering": "11",
    "ux & product design": "7",
    "ux research": "110",
}


def scope(term: str) -> tuple[str, list[list[str]]]:
    category = CATEGORY_IDS.get(term.casefold())
    if category:
        return "", [[f"{CATEGORY_FACET}:{category}"]]
    return term, []


def native_request(
    query: str, filters: list[list[str]], **params: object
) -> list[dict]:
    search = {
        "query": query,
        "trueupPartnerId": "lenny",
        "trueupRequestVersion": 2,
        "trueupCompanyPreferencesKey": "public",
        **params,
    }
    if filters:
        search["facetFilters"] = filters
    return [{"indexName": "job", "params": search}]


def _label(value: str) -> str:
    # Ignore only the display flag or globe preceding a name.
    value = unicodedata.normalize("NFC", value).strip()
    prefix, space, rest = value.partition(" ")
    if space and (
        prefix in {"🌎", "🌍", "🌏"} or all("🇦" <= ch <= "🇿" for ch in prefix)
    ):
        value = rest
    return " ".join(value.casefold().split())


def select_location(place: str, remote: bool, data: dict) -> str | None:
    hits = data.get("facetHits")
    if not isinstance(hits, list) or any(
        not isinstance(hit, dict) or not isinstance(hit.get("value"), str)
        for hit in hits
    ):
        raise AdapterFailureError(f"{BOARD} returned invalid location suggestions")
    name = _label(place)
    remote_suffix = " (remote)"
    if remote and name != "remote" and not name.endswith(remote_suffix):
        name += remote_suffix
    values = {hit["value"] for hit in hits}
    exact = {value for value in values if _label(value) == name}
    # A bare city is usable only when the picker identifies it unambiguously.
    if not exact and "," not in place:
        exact = {value for value in values if _label(value).partition(",")[0] == name}
        if exact and len(hits) >= 10:
            raise AdapterFailureError(f"{BOARD} needs a more specific location")
    if len(exact) > 1:
        raise AdapterFailureError(f"{BOARD} needs a more specific location")
    return next(iter(exact), None)


def check_page(data: dict, query: str, filters: list[list[str]], page: int) -> None:
    if (
        not isinstance(data.get("hits"), list)
        or type(data.get("page")) is not int
        or data["page"] != page
        or type(data.get("nbPages")) is not int
        or data["nbPages"] < 0
        or data.get("hitsPerPage") != PAGE_SIZE
    ):
        raise AdapterFailureError(
            f"{BOARD} returned invalid listing or pagination metadata"
        )
    if data.get("query") != query:
        raise AdapterFailureError(f"{BOARD} returned results for a different query")
    # The service echoes selected values as a single comma-joined string.
    # Compare the entire string: an individual city label may contain a comma.
    params = data.get("params")
    if not isinstance(params, str):
        raise AdapterFailureError(f"{BOARD} did not confirm the selected filters")
    echoed = parse_qs(params).get("facetFilters", [])
    expected = ",".join(v for group in filters for v in group)
    if echoed != ([expected] if expected else []):
        raise AdapterFailureError(f"{BOARD} did not confirm the selected filters")
