from __future__ import annotations

import argparse
import json
import tempfile
import time
from pathlib import Path

from jobstreaming import (
    CheckpointWrite,
    SearchCheckpoint,
    SearchRequest,
    Site,
    SqliteCheckpointStore,
)


def run_scenario(acknowledgements: int, path: Path) -> dict[str, int | float]:
    """Run one deterministic acknowledgement workload and verify its facts."""

    if acknowledgements < 1:
        raise ValueError("acknowledgements must be positive")
    request = SearchRequest(
        site_type=(Site.INDEED,),
        search_term="checkpoint benchmark",
        results_wanted=acknowledgements,
    )
    store = SqliteCheckpointStore(path)
    checkpoint = SearchCheckpoint.for_request(request)
    store.save(checkpoint)

    started = time.perf_counter()
    for index in range(acknowledgements):
        emitted_count = index + 1
        adapter = checkpoint.adapters[Site.INDEED.value].model_copy(
            update={
                "state": {"acknowledged": emitted_count},
                "emitted_count": emitted_count,
            }
        )
        checkpoint = checkpoint.model_copy(
            update={
                "revision": emitted_count,
                "adapters": {Site.INDEED.value: adapter},
            }
        )
        store.save_incremental(
            CheckpointWrite(
                checkpoint=checkpoint,
                adapter_site=Site.INDEED,
                new_seen_job_key=f"benchmark-{index:06d}",
            )
        )
    elapsed = time.perf_counter() - started

    stats = store.stats()
    if stats.revision != acknowledgements:
        raise RuntimeError(f"revision mismatch: {stats.revision} != {acknowledgements}")
    if stats.seen_job_key_count != acknowledgements:
        raise RuntimeError(
            "seen-key mismatch: " f"{stats.seen_job_key_count} != {acknowledgements}"
        )
    return {
        "acknowledgements": acknowledgements,
        "database_bytes": stats.database_bytes,
        "elapsed_seconds": round(elapsed, 6),
        "final_revision": stats.revision,
        "seen_job_keys": stats.seen_job_key_count,
    }


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Benchmark incremental SQLite checkpoint acknowledgements."
    )
    parser.add_argument(
        "--counts",
        nargs="+",
        type=int,
        default=[10_000, 100_000],
        help="Deterministic acknowledgement counts (default: 10000 100000).",
    )
    parser.add_argument(
        "--directory",
        type=Path,
        help="Directory for benchmark databases; a temporary directory is default.",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    if any(count < 1 for count in args.counts):
        raise SystemExit("all counts must be positive")

    if args.directory is not None:
        args.directory.mkdir(parents=True, exist_ok=True)
        results = [
            run_scenario(
                count,
                args.directory / f"checkpoint-{count}.sqlite3",
            )
            for count in args.counts
        ]
    else:
        with tempfile.TemporaryDirectory(
            prefix="jobstreaming-checkpoint-benchmark-"
        ) as directory:
            root = Path(directory)
            results = [
                run_scenario(count, root / f"checkpoint-{count}.sqlite3")
                for count in args.counts
            ]

    print(json.dumps({"scenarios": results}, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
