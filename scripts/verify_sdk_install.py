from __future__ import annotations

from pathlib import Path
from typing import get_type_hints

import jobstreaming

package_dir = Path(jobstreaming.__file__).parent
assert (package_dir / "py.typed").is_file()

adapter_hints = get_type_hints(jobstreaming.Adapter.scrape)
stream_hints = get_type_hints(jobstreaming.SearchStream.__init__)
assert adapter_hints["context"] == jobstreaming.ScrapeContext | None
assert stream_hints["registry"] == jobstreaming.AdapterRegistry

request = jobstreaming.SearchRequest(site_type=(jobstreaming.Site.GOOGLE,))
assert request.sites == (jobstreaming.Site.GOOGLE,)
