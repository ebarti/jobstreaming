from __future__ import annotations

from collections.abc import Iterator

import jobstreaming


def build_request() -> jobstreaming.SearchRequest:
    return jobstreaming.SearchRequest(
        site_type=(jobstreaming.Site.GOOGLE,),
        results_wanted=1,
    )


def stream(request: jobstreaming.SearchRequest) -> Iterator[jobstreaming.JobPost]:
    return jobstreaming.stream_jobs(request)


def batch_api_remains_visible() -> object:
    return jobstreaming.scrape_jobs(
        site_name=jobstreaming.Site.GOOGLE,
        results_wanted=1,
    )
