from __future__ import annotations

from pathlib import Path

from coverage import Coverage

MINIMUMS = {
    "jobstreaming/bdjobs/__init__.py": 85.0,
    "jobstreaming/google/__init__.py": 70.0,
    "jobstreaming/glassdoor/__init__.py": 72.0,
}
TOTAL_MINIMUM = 78.0


def main() -> int:
    coverage = Coverage()
    coverage.load()
    failures: list[str] = []
    total = coverage.report(show_missing=False, skip_covered=True)
    if total + 1e-9 < TOTAL_MINIMUM:
        failures.append(f"total coverage {total:.1f}% is below {TOTAL_MINIMUM:.1f}%")
    for filename, minimum in MINIMUMS.items():
        _, statements, _, missing, _ = coverage.analysis2(filename)
        percentage = (
            100.0
            if not statements
            else 100.0 * (len(statements) - len(missing)) / len(statements)
        )
        print(
            f"{Path(filename).as_posix()}: {percentage:.1f}% (minimum {minimum:.1f}%)"
        )
        if percentage + 1e-9 < minimum:
            failures.append(
                f"{filename} coverage {percentage:.1f}% is below {minimum:.1f}%"
            )
    if failures:
        for failure in failures:
            print(f"error: {failure}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
