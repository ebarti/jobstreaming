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

request = jobstreaming.SearchRequest(site_type=(jobstreaming.Site.GOOGLE,))
assert request.sites == (jobstreaming.Site.GOOGLE,)
