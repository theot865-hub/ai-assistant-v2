from __future__ import annotations

import csv
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent.parent
DEFAULT_INPUT_PATH = BASE_DIR / "workspace" / "leads" / "all_realtor_leads.csv"
OUTPUT_PATH = BASE_DIR / "workspace" / "leads" / "all_realtor_outreach.csv"


def _resolve_input_path(input_path: str | None) -> Path:
    if not input_path:
        return DEFAULT_INPUT_PATH

    candidate = Path(input_path)
    if candidate.is_absolute():
        return candidate
    return BASE_DIR / candidate


def _build_message(name: str, brokerage: str, source: str) -> str:
    safe_name = name or "there"
    safe_brokerage = brokerage or "your team"
    safe_source = source or "your profile"
    return (
        f"Hi {safe_name}, I work with local Victoria listing prep and noticed {safe_source}. "
        f"If helpful for {safe_brokerage}, I can share quick pricing and availability."
    )


def run(input_path: str | None = None) -> str:
    source_path = _resolve_input_path(input_path)
    if not source_path.exists():
        return f"outreach worker failed: input file not found at {source_path}"

    with source_path.open("r", encoding="utf-8", newline="") as infile:
        reader = csv.DictReader(infile)
        rows = list(reader)

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with OUTPUT_PATH.open("w", encoding="utf-8", newline="") as outfile:
        writer = csv.writer(outfile)
        writer.writerow(["Name", "Brokerage", "ProfileURL", "Source", "Message"])
        for row in rows:
            name = (row.get("Name") or "").strip()
            brokerage = (row.get("Brokerage") or "").strip()
            profile_url = (row.get("ProfileURL") or "").strip()
            source = (row.get("Source") or "").strip()
            message = _build_message(name, brokerage, source)
            writer.writerow([name, brokerage, profile_url, source, message])

    return f"outreach worker completed: wrote {len(rows)} rows to {OUTPUT_PATH}"

