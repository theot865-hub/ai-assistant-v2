from __future__ import annotations

from workers import business_enrichment_worker
from workers import business_discovery_worker
from workers import business_outreach_worker
from workers import enrichment_worker
from workers import leads_worker
from workers import outreach_worker
from workers import pipeline_worker
from workers import research_worker


WORKER_REGISTRY = {
    "business_enrichment": business_enrichment_worker.run,
    "business_discovery": business_discovery_worker.run,
    "business_outreach": business_outreach_worker.run,
    "enrichment": enrichment_worker.run,
    "leads": leads_worker.run,
    "research": research_worker.run,
    "outreach": outreach_worker.run,
    "pipeline": pipeline_worker.run,
}
