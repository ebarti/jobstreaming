#!/usr/bin/env python3
"""Install built distributions into clean environments and verify public imports."""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import tempfile
from pathlib import Path

SMOKE_TEST = """
import sys
from importlib.metadata import metadata, version
from pathlib import Path

import jobstreaming
from jobstreaming import SearchRequest, Site

expected_version = sys.argv[1]
assert version("jobstreaming") == expected_version
assert metadata("jobstreaming")["Name"] == "jobstreaming"

package_path = Path(jobstreaming.__file__).resolve()
environment_path = Path(sys.prefix).resolve()
assert package_path.is_relative_to(environment_path), (
    f"import resolved outside the consumer environment: {package_path}"
)

request = SearchRequest(site_type=(Site.GOOGLE,))
assert request.sites == (Site.GOOGLE,)
"""


def _single_artifact(dist_dir: Path, pattern: str) -> Path:
    matches = sorted(dist_dir.glob(pattern))
    if len(matches) != 1:
        names = ", ".join(path.name for path in matches) or "none"
        raise RuntimeError(
            f"expected exactly one {pattern!r} artifact in {dist_dir}, found {names}"
        )
    return matches[0]


def _consumer_python(environment: Path) -> Path:
    if os.name == "nt":
        return environment / "Scripts" / "python.exe"
    return environment / "bin" / "python"


def _run(*args: str | Path, cwd: Path | None = None) -> None:
    subprocess.run(
        [str(arg) for arg in args],
        cwd=cwd,
        check=True,
    )


def verify_artifact(artifact: Path, expected_version: str) -> None:
    with tempfile.TemporaryDirectory(prefix="jobstreaming-consumer-") as temporary:
        consumer = Path(temporary)
        _run(sys.executable, "-m", "venv", consumer)
        python = _consumer_python(consumer)
        _run(
            python,
            "-m",
            "pip",
            "install",
            "--disable-pip-version-check",
            "--no-cache-dir",
            artifact.resolve(),
        )
        _run(python, "-m", "pip", "check")
        _run(
            python,
            "-c",
            SMOKE_TEST,
            expected_version,
            cwd=consumer,
        )
        print(f"Verified installed artifact: {artifact.name}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dist-dir", type=Path, default=Path("dist"))
    parser.add_argument("--expected-version", required=True)
    args = parser.parse_args()

    dist_dir = args.dist_dir.resolve()
    artifacts = (
        _single_artifact(dist_dir, "jobstreaming-*.whl"),
        _single_artifact(dist_dir, "jobstreaming-*.tar.gz"),
    )
    for artifact in artifacts:
        verify_artifact(artifact, args.expected_version)


if __name__ == "__main__":
    main()
