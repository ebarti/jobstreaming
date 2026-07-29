from __future__ import annotations

import builtins
import subprocess
import sys

import pytest

from jobstreaming import MissingOptionalDependencyError, scrape_jobs


def test_default_import_surface_does_not_import_pandas() -> None:
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "import sys; import jobstreaming; " "assert 'pandas' not in sys.modules",
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr


def test_batch_api_fails_before_adapter_work_when_extra_is_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_import = builtins.__import__
    sys.modules.pop("jobstreaming.batch", None)

    def block_pandas(name, *args, **kwargs):
        if name == "pandas":
            raise ModuleNotFoundError("No module named 'pandas'", name="pandas")
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", block_pandas)

    with pytest.raises(MissingOptionalDependencyError) as raised:
        scrape_jobs(site_name="indeed")

    assert raised.value.extra == "batch"
    assert raised.value.dependency == "pandas"
    assert raised.value.install_spec == "jobstreaming[batch]"
    assert 'pip install "jobstreaming[batch]"' in str(raised.value)
