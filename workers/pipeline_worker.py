from __future__ import annotations

from workers import enrichment_worker
from workers import leads_worker
from workers import outreach_worker
from workers import research_worker


DEFAULT_QUERY = "victoria local businesses"


def run(query: str | None = None) -> str:
    normalized_query = (query or "").strip() or DEFAULT_QUERY

    leads_result = leads_worker.run()
    research_result = research_worker.run(normalized_query)
    outreach_result = outreach_worker.run()
    enrichment_result = enrichment_worker.run()

    return (
        "pipeline worker completed: "
        f"leads=[{leads_result}] "
        f"research=[{research_result}] "
        f"outreach=[{outreach_result}] "
        f"enrichment=[{enrichment_result}]"
    )
