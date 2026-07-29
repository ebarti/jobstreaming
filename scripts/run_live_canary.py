from __future__ import annotations

import os

from jobstreaming import ErrorEvent, JobEvent, Site, stream_search


def _configured_sites() -> tuple[Site, ...]:
    raw = os.getenv("JOBSTREAMING_CANARY_SITES", "")
    if not raw.strip():
        return ()
    return tuple(
        dict.fromkeys(
            Site.from_string(value) for value in raw.split(",") if value.strip()
        )
    )


def main() -> int:
    if os.getenv("JOBSTREAMING_CANARY_ENABLED", "").casefold() != "true":
        print("Live adapter canaries are disabled; nothing was queried.")
        return 0
    sites = _configured_sites()
    if not sites:
        print("No live adapter canary sites are configured; nothing was queried.")
        return 0

    query = os.getenv("JOBSTREAMING_CANARY_QUERY", "software engineer")
    location = os.getenv("JOBSTREAMING_CANARY_LOCATION") or None
    failed: list[str] = []
    for site in sites:
        jobs = 0
        errors = 0
        with stream_search(
            site_name=site,
            search_term=query,
            location=location,
            results_wanted=1,
            max_pages=1,
            request_timeout=15,
            max_retries=0,
            resume=False,
        ) as stream:
            for event in stream:
                if isinstance(event, JobEvent):
                    jobs += 1
                elif isinstance(event, ErrorEvent):
                    errors += 1
                stream.ack(event)
        status = "healthy" if jobs and not errors else "failed"
        print(f"{site.value}: {status} (jobs={jobs}, errors={errors})")
        if status == "failed":
            failed.append(site.value)
    if failed:
        print("Failed adapter canaries: " + ", ".join(failed))
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
