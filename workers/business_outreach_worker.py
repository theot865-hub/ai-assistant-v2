from __future__ import annotations

import csv
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent.parent
DEFAULT_INPUT_PATH = BASE_DIR / "workspace" / "leads" / "business_discovery_enriched.csv"
OUTPUT_PATH = BASE_DIR / "workspace" / "leads" / "business_discovery_outreach.csv"


def _resolve_input_path(input_path: str | None) -> Path:
    if not input_path:
        return DEFAULT_INPUT_PATH
    candidate = Path(input_path)
    if candidate.is_absolute():
        return candidate
    return BASE_DIR / candidate


def _build_message(title: str, domain: str, idx: int) -> str:
    safe_title = title or "your business"
    safe_domain = domain or "your website"
    templates = [
        (
            "Hi, I came across {title} ({domain}) and wanted to connect. "
            "I help local businesses with practical growth workflows and can share a quick plan."
        ),
        (
            "Hi there, I found {title} via {domain}. "
            "If useful, I can share a short outreach and lead-handling setup for local business growth."
        ),
        (
            "Hello, I noticed {title} on {domain}. "
            "I work on simple demand-gen systems and can send a concise idea tailored to your business."
        ),
    ]
    return templates[idx % len(templates)].format(title=safe_title, domain=safe_domain)


def run(input_path: str | None = None) -> str:
    source_path = _resolve_input_path(input_path)
    if not source_path.exists():
        return f"business outreach worker failed: input file not found at {source_path}"

    with source_path.open("r", encoding="utf-8", newline="") as infile:
        reader = csv.DictReader(infile)
        rows = list(reader)

    output_rows: list[dict[str, str]] = []
    for idx, row in enumerate(rows):
        title = (row.get("Title") or "").strip()
        url = (row.get("URL") or "").strip()
        snippet = (row.get("Snippet") or "").strip()
        query = (row.get("Query") or "").strip()
        domain = (row.get("Domain") or "").strip()
        email = (row.get("Email") or "").strip()
        phone = (row.get("Phone") or "").strip()
        source_page = (row.get("SourcePage") or "").strip()
        message = _build_message(title, domain, idx)

        output_rows.append(
            {
                "Title": title,
                "URL": url,
                "Snippet": snippet,
                "Query": query,
                "Domain": domain,
                "Email": email,
                "Phone": phone,
                "SourcePage": source_page,
                "Message": message,
            }
        )

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with OUTPUT_PATH.open("w", encoding="utf-8", newline="") as outfile:
        writer = csv.DictWriter(
            outfile,
            fieldnames=[
                "Title",
                "URL",
                "Snippet",
                "Query",
                "Domain",
                "Email",
                "Phone",
                "SourcePage",
                "Message",
            ],
        )
        writer.writeheader()
        writer.writerows(output_rows)

    return f"business outreach worker completed: wrote {len(output_rows)} rows to {OUTPUT_PATH}"

