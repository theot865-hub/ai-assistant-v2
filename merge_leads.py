from __future__ import annotations

import csv
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
LEADS_DIR = BASE_DIR / "workspace" / "leads"
RANKMYAGENT_PATH = LEADS_DIR / "rankmyagent_victoria.csv"
REW_PATH = LEADS_DIR / "rew_victoria.csv"
OUTPUT_PATH = LEADS_DIR / "all_realtor_leads.csv"

COLUMNS = ["Name", "Brokerage", "ProfileURL", "Source"]


def clean_text(value: str) -> str:
    return " ".join((value or "").split()).strip()


def read_rows(path: Path) -> list[dict[str, str]]:
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


def merge_rows(*row_groups: list[dict[str, str]]) -> list[dict[str, str]]:
    merged: list[dict[str, str]] = []
    seen_profile_urls: set[str] = set()
    seen_name_brokerage: set[tuple[str, str]] = set()

    for rows in row_groups:
        for row in rows:
            profile_url = row["ProfileURL"].strip()
            profile_key = profile_url.lower()
            name_key = row["Name"].lower()
            brokerage_key = row["Brokerage"].lower()
            pair_key = (name_key, brokerage_key)

            if profile_url and profile_key in seen_profile_urls:
                continue
            if pair_key in seen_name_brokerage:
                continue

            merged.append(row)
            if profile_url:
                seen_profile_urls.add(profile_key)
            seen_name_brokerage.add(pair_key)

    return merged


def write_rows(rows: list[dict[str, str]], output_path: Path = OUTPUT_PATH) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=COLUMNS)
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    rank_rows = read_rows(RANKMYAGENT_PATH)
    rew_rows = read_rows(REW_PATH)
    merged = merge_rows(rank_rows, rew_rows)
    write_rows(merged, OUTPUT_PATH)
    print(
        f"Merged {len(rank_rows)} RankMyAgent + {len(rew_rows)} REW rows into "
        f"{len(merged)} unique leads at {OUTPUT_PATH}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
