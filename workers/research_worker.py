from __future__ import annotations

import json

import tools


def _build_summary(query: str, results: list[dict]) -> str:
    lines = [f"query: {query}", f"result_count: {len(results)}", "top_results:"]
    for idx, item in enumerate(results[:3], start=1):
        title = (item.get("title") or "(no title)").strip()
        url = (item.get("url") or "(no url)").strip()
        snippet = (item.get("snippet") or "").strip().replace("\n", " ")
        if len(snippet) > 180:
            snippet = snippet[:177] + "..."
        lines.append(f"{idx}. {title}")
        lines.append(f"   url: {url}")
        lines.append(f"   snippet: {snippet or '(no snippet)'}")
    return "\n".join(lines)


def run(query: str | None = None) -> str:
    normalized_query = (query or "").strip()
    if not normalized_query:
        return 'research worker error: missing query. Usage: python run_worker.py research "your query"'

    try:
        raw_results = tools.search_web(normalized_query, 5)
        parsed_results = json.loads(raw_results)
        if not isinstance(parsed_results, list):
            parsed_results = []
    except Exception as exc:
        return f"research worker failed: {exc}"

    summary = _build_summary(normalized_query, parsed_results)
    tools.write_file("jobs/latest_research.txt", summary)
    return (
        f"research worker completed ({len(parsed_results)} results); "
        "summary saved to workspace/jobs/latest_research.txt"
    )

