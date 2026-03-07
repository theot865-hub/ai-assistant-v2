from __future__ import annotations

import csv
import json
from pathlib import Path

import tools


BASE_DIR = Path(__file__).resolve().parent.parent
OUTPUT_PATH = BASE_DIR / "workspace" / "leads" / "business_discovery.csv"


def run(query: str | None = None) -> str:
    normalized_query = (query or "").strip()
    if not normalized_query:
        return (
            'business discovery worker error: missing query. '
            'Usage: python run_worker.py business_discovery "your query"'
        )

    try:
        raw_results = tools.search_web(normalized_query, 8)
        parsed_results = json.loads(raw_results)
        if not isinstance(parsed_results, list):
            parsed_results = []
    except Exception as exc:
        return f"business discovery worker failed: {exc}"

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with OUTPUT_PATH.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["Title", "URL", "Snippet", "Query"])
        for result in parsed_results:
            if not isinstance(result, dict):
                continue
            title = (result.get("title") or "").strip()
            url = (result.get("url") or "").strip()
            snippet = (result.get("snippet") or "").strip()
            writer.writerow([title, url, snippet, normalized_query])

    return (
        "business discovery worker completed: "
        f"wrote {len(parsed_results)} rows to {OUTPUT_PATH}"
    )

