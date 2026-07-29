from __future__ import annotations

from collections.abc import Iterable

import pandas as pd

from jobstreaming.model import AdapterIdentifier, JobPost, SearchRequest
from jobstreaming.result import job_to_row, normalize_job
from jobstreaming.util import desired_order


def jobs_to_dataframe(
    jobs: Iterable[tuple[AdapterIdentifier, JobPost]],
    request: SearchRequest,
) -> pd.DataFrame:
    rows = [job_to_row(site, normalize_job(job, request)) for site, job in jobs]
    frame = pd.DataFrame.from_records(rows)
    for column in desired_order:
        if column not in frame.columns:
            frame[column] = None
    frame = frame[desired_order]
    if frame.empty:
        return frame
    return frame.sort_values(
        by=["site", "date_posted"],
        ascending=[True, False],
        na_position="last",
        kind="stable",
    ).reset_index(drop=True)
