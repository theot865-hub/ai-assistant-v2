from __future__ import annotations

from datetime import datetime, timezone

from core.campaign_service import CampaignService
from db.repository import record_run
from services.outreach_config import DEFAULT_SENDER_PROFILE_KEY, resolve_sender_profile


DEFAULT_QUERY = "victoria local businesses"


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def run(query: str | None = None) -> str:
    started_at = _utc_now()
    normalized_query = (query or "").strip() or DEFAULT_QUERY
    service = CampaignService()

    sender_key, sender_profile = resolve_sender_profile(DEFAULT_SENDER_PROFILE_KEY)
    campaign_payload = {
        "campaign_name": f"pipeline {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        "discovery_query": normalized_query,
        "audience": "local businesses",
        "location": "Victoria BC",
        "sender_name": str(sender_profile.get("name", "")).strip(),
        "business_name": str(sender_profile.get("business", "")).strip(),
        "city": str(sender_profile.get("city", "")).strip(),
        "phone": str(sender_profile.get("phone", "")).strip(),
        "services_offered": ", ".join(
            str(item).strip() for item in sender_profile.get("services", []) if str(item).strip()
        ),
        "unique_angle": str(sender_profile.get("angle", "")).strip(),
        "sender_profile_key": sender_key,
    }

    pipeline_status = "ok"
    campaign_id = ""
    summary: dict[str, object] = {}
    error_text = ""
    try:
        campaign_id = service.create_campaign(0, campaign_payload)
        summary = service.run_campaign(campaign_id)
    except Exception as exc:
        pipeline_status = "error"
        error_text = str(exc)

    finished_at = _utc_now()
    record_run(
        worker="pipeline",
        args=[normalized_query],
        status=pipeline_status,
        started_at=started_at,
        finished_at=finished_at,
    )
    if pipeline_status != "ok":
        return f"pipeline worker failed: {error_text}"
    return (
        "pipeline worker completed: "
        f"campaign_id={campaign_id}; "
        f"summary={summary}"
    )
