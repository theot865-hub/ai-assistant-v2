from __future__ import annotations

import csv
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent
INPUT_PATH = BASE_DIR / "workspace" / "leads" / "rankmyagent_victoria.txt"
OUTPUT_PATH = BASE_DIR / "workspace" / "leads" / "rankmyagent_outreach.txt"
CSV_OUTPUT_PATH = BASE_DIR / "workspace" / "leads" / "rankmyagent_outreach.csv"


def parse_leads(path: Path) -> list[tuple[str, str, str]]:
    leads: list[tuple[str, str, str]] = []
    if not path.exists():
        return leads

    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line:
            continue
        parts = [p.strip() for p in line.split(" | ", 2)]
        if len(parts) != 3:
            continue
        name, brokerage, profile_url = parts
        leads.append((name, brokerage, profile_url))
    return leads


def build_message(name: str, brokerage: str, idx: int) -> str:
    templates = [
        (
            "Hi {name}, I run a local Victoria pressure washing service and wanted to connect "
            "with {brokerage}. We help with listing prep by cleaning driveways, walkways, and "
            "home exteriors to improve curb appeal and first impressions. If useful, I can send "
            "quick pricing and availability for your next listing."
        ),
        (
            "Hi {name}, I work with a local Victoria pressure washing team and thought this may "
            "help {brokerage}. Before listings go live, we clean driveways, front walkways, and "
            "exterior surfaces so properties show cleaner from the street. Happy to share simple "
            "rates and near-term availability."
        ),
        (
            "Hi {name}, I provide pressure washing in Victoria and wanted to reach out to "
            "{brokerage}. We support listing prep with driveway, walkway, and exterior cleaning "
            "to strengthen curb appeal and overall presentation. If you want, I can send a quick "
            "quote range for upcoming listings."
        ),
    ]
    template = templates[idx % len(templates)]
    message = template.format(name=name, brokerage=brokerage)
    return " ".join(message.split())


def write_outreach_csv(leads: list[tuple[str, str, str]], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["Name", "Brokerage", "ProfileURL", "Message"])
        for idx, (name, brokerage, profile_url) in enumerate(leads):
            writer.writerow([name, brokerage, profile_url, build_message(name, brokerage, idx)])


def write_outreach(leads: list[tuple[str, str, str]], output_path: Path) -> None:
    # Legacy text output kept for compatibility.
    output_path.parent.mkdir(parents=True, exist_ok=True)
    lines: list[str] = []
    for idx, (name, brokerage, _profile_url) in enumerate(leads):
        message = build_message(name, brokerage, idx)
        lines.append(f"{name} | {brokerage} | {message}")
    output_path.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")


if __name__ == "__main__":
    leads_data = parse_leads(INPUT_PATH)
    write_outreach(leads_data, OUTPUT_PATH)
    write_outreach_csv(leads_data, CSV_OUTPUT_PATH)
    print(f"Wrote {len(leads_data)} outreach messages to {OUTPUT_PATH}")
    print(f"Wrote {len(leads_data)} outreach rows to {CSV_OUTPUT_PATH}")
