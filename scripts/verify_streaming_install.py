from __future__ import annotations

import importlib.util
import sys

assert importlib.util.find_spec("pandas") is None

jobstreaming = __import__("jobstreaming")

assert "pandas" not in sys.modules

try:
    jobstreaming.scrape_jobs()
except jobstreaming.MissingOptionalDependencyError as exc:
    assert exc.extra == "batch"
    assert exc.dependency == "pandas"
else:
    raise AssertionError("scrape_jobs did not require the batch extra")

request = jobstreaming.SearchRequest(
    site_type=(jobstreaming.Site.GOOGLE,),
    description_salary_policy=jobstreaming.DescriptionSalaryPolicy.CONSERVATIVE,
)
assert request.sites == (jobstreaming.Site.GOOGLE,)
assert (
    request.description_salary_policy
    is jobstreaming.DescriptionSalaryPolicy.CONSERVATIVE
)
provenance = jobstreaming.SalaryProvenance(
    source=jobstreaming.SalarySource.DESCRIPTION,
    confidence=jobstreaming.SalaryConfidence.MEDIUM,
    evidence="USD 80,000 - 100,000 per year",
)
assert provenance.evidence == "USD 80,000 - 100,000 per year"
