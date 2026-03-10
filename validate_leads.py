from __future__ import annotations

import csv
from collections import Counter
from pathlib import Path
from urllib.parse import urlparse

BASE_DIR = Path(__file__).resolve().parent
INPUT_PATH = BASE_DIR / "workspace" / "leads" / "all_realtor_leads.csv"


def clean_text(value: str) -> str:
    return " ".join((value or "").split()).strip()


def is_valid_url(value: str) -> bool:
    if not value:
        return False
    parsed = urlparse(value)
    return parsed.scheme in {"http", "https"} and bool(parsed.netloc)


def load_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        rows: list[dict[str, str]] = []
        for row in reader:
            rows.append({
                "Name": clean_text(row.get("Name", "")),
                "Brokerage": clean_text(row.get("Brokerage", "")),
                "ProfileURL": clean_text(row.get("ProfileURL", "")),
                "Source": clean_text(row.get("Source", "")),
            })
        return rows


def validate_rows(rows: list[dict[str, str]]) -> dict[str, int]:
    profile_counter = Counter(
        row["ProfileURL"].lower() for row in rows if row["ProfileURL"].strip()
    )
    duplicate_profile_urls = sum(count - 1 for count in profile_counter.values() if count > 1)
    blank_name_count = sum(1 for row in rows if not row["Name"].strip())
    blank_brokerage_count = sum(1 for row in rows if not row["Brokerage"].strip())
    invalid_url_count = sum(1 for row in rows if not is_valid_url(row["ProfileURL"]))

    return {
        "row_count": len(rows),
        "duplicate_profile_url_count": duplicate_profile_urls,
        "blank_name_count": blank_name_count,
        "blank_brokerage_count": blank_brokerage_count,
        "invalid_url_count": invalid_url_count,
    }


def print_report(report: dict[str, int], path: Path = INPUT_PATH) -> None:
    print(f"Validation report for {path}")
    print(f"- Row count: {report['row_count']}")
    print(f"- Duplicate ProfileURL count: {report['duplicate_profile_url_count']}")
    print(f"- Blank Name count: {report['blank_name_count']}")
    print(f"- Blank Brokerage count: {report['blank_brokerage_count']}")
    print(f"- Invalid URL count: {report['invalid_url_count']}")


def has_blocking_errors(report: dict[str, int]) -> bool:
    return report["duplicate_profile_url_count"] > 0 or report["blank_name_count"] > 0


def main() -> int:
    rows = load_rows(INPUT_PATH)
    report = validate_rows(rows)
    print_report(report, INPUT_PATH)
    if has_blocking_errors(report):
        print("Validation failed: duplicate URLs or blank names detected.")
        return 1
    print("Validation passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
