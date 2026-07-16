from __future__ import annotations

import logging
from datetime import datetime, timedelta

from jobstream import CompensationInterval
from jobstream.google.util import parse_relative_date
from jobstream.util import (
    create_logger,
    extract_salary,
    set_logger_level,
    stable_job_id,
)


def test_enforced_annual_salary_changes_the_interval() -> None:
    interval, minimum, maximum, currency = extract_salary(
        "$20 - $30 per hour", enforce_annual_salary=True
    )

    assert interval == CompensationInterval.YEARLY.value
    assert minimum == 41_600
    assert maximum == 62_400
    assert currency == "USD"


def test_google_relative_hours_do_not_become_days() -> None:
    now = datetime(2026, 7, 15, 12, 0, 0)

    assert parse_relative_date("2 hours ago", now=now) == now.date()
    assert parse_relative_date("today", now=now) == now.date()
    assert (
        parse_relative_date("3 days ago", now=now) == (now - timedelta(days=3)).date()
    )


def test_stable_job_id_is_process_independent() -> None:
    assert stable_job_id("bayt", "https://example.test/jobs/1") == (
        "bayt-a8a440e5823115f1ce822c35e654d047"
    )


def test_verbosity_applies_to_loggers_created_later() -> None:
    set_logger_level(0)
    try:
        assert create_logger("created-after-config").level == logging.ERROR
    finally:
        set_logger_level(2)
