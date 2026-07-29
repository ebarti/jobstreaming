from __future__ import annotations

from jobstreaming import (
    AdapterCapabilities,
    AdapterId,
    AdapterRegistry,
    JobPost,
    JobResponse,
    NoResume,
    scrape_jobs,
)

IDENTIFIER = AdapterId("artifact.fixture")


class FixtureAdapter:
    capabilities = AdapterCapabilities(resume=NoResume())

    def __init__(self, **kwargs):
        del kwargs
        self.site = IDENTIFIER

    def scrape(self, request, context=None):
        del request
        job = JobPost(
            id="artifact-1",
            title="Artifact fixture",
            job_url="https://example.test/artifact/1",
        )
        if context is not None:
            context.emit_job(job)
        return JobResponse(jobs=(job,))


registry = AdapterRegistry()
registry.register(IDENTIFIER, FixtureAdapter)
frame = scrape_jobs(
    site_name=IDENTIFIER,
    registry=registry,
    results_wanted=1,
)
assert frame["id"].tolist() == ["artifact-1"]
