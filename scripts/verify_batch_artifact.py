from __future__ import annotations

import sys
from email.parser import BytesParser
from pathlib import Path
from zipfile import ZipFile

EXPECTED_KEYWORDS = {
    "data-pipeline",
    "glassdoor",
    "indeed",
    "job-boards",
    "job-search",
    "jobs-scraper",
    "linkedin",
    "resumable",
    "streaming",
    "ziprecruiter",
}


def main(wheel_path: str) -> int:
    wheel = Path(wheel_path)
    with ZipFile(wheel) as archive:
        metadata_path = next(
            name for name in archive.namelist() if name.endswith(".dist-info/METADATA")
        )
        metadata = BytesParser().parsebytes(archive.read(metadata_path))

    extras = metadata.get_all("Provides-Extra", [])
    requirements = metadata.get_all("Requires-Dist", [])
    pandas_requirements = [
        requirement
        for requirement in requirements
        if requirement.casefold().startswith("pandas")
    ]
    keywords = {
        keyword.strip()
        for keyword in metadata.get("Keywords", "").split(",")
        if keyword.strip()
    }

    assert extras == ["batch"], f"wheel exposes unexpected extras: {extras}"
    assert keywords == EXPECTED_KEYWORDS, "wheel did not preserve project keywords"
    assert len(pandas_requirements) == 1, "wheel must declare pandas exactly once"
    assert (
        'extra == "batch"' in pandas_requirements[0]
    ), "pandas must be guarded by the batch extra"
    print(f"{wheel.name}: optional batch metadata is valid")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1]))
