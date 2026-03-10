from __future__ import annotations

import argparse
import csv
import subprocess
import sys
from collections import Counter
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent
LEADS_PATH = BASE_DIR / "workspace" / "leads" / "all_realtor_leads.csv"


def run_step(script_name: str) -> int:
    script_path = BASE_DIR / script_name
    cmd = [sys.executable, str(script_path)]
    result = subprocess.run(cmd)
    return result.returncode


def read_counts(csv_path: Path) -> tuple[int, int, int]:
    if not csv_path.exists():
        return 0, 0, 0

    total = 0
    source_counts: Counter[str] = Counter()

    with csv_path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            total += 1
            source = (row.get("Source") or "").strip()
            source_counts[source] += 1

    rew_count = source_counts.get("REW", 0)
    rankmyagent_count = source_counts.get("RankMyAgent", 0)
    return total, rew_count, rankmyagent_count


def main() -> int:
    parser = argparse.ArgumentParser(description="Run realtor lead pipeline.")
    parser.add_argument("goal", nargs="*", help="Optional goal text (accepted but not used yet).")
    _args = parser.parse_args()

    steps = [
        "rankmyagent_scraper.py",
        "rew_scraper.py",
        "merge_leads.py",
        "validate_leads.py",
    ]

    for step in steps:
        code = run_step(step)
        if code != 0:
            return code

    total, rew_count, rankmyagent_count = read_counts(LEADS_PATH)
    print("Final summary:")
    print(f"- total leads: {total}")
    print(f"- REW count: {rew_count}")
    print(f"- RankMyAgent count: {rankmyagent_count}")
    print(f"- output file path: {LEADS_PATH}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
