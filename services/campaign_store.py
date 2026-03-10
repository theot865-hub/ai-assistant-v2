from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


BASE_DIR = Path(__file__).resolve().parent.parent
CAMPAIGNS_STORE_PATH = BASE_DIR / "workspace" / "config" / "campaigns.json"


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _load_campaigns() -> dict[str, dict[str, Any]]:
    if not CAMPAIGNS_STORE_PATH.exists():
        return {}
    try:
        parsed = json.loads(CAMPAIGNS_STORE_PATH.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return {}
    if not isinstance(parsed, dict):
        return {}
    out: dict[str, dict[str, Any]] = {}
    for campaign_id, payload in parsed.items():
        if not isinstance(campaign_id, str) or not campaign_id.strip():
            continue
        if not isinstance(payload, dict):
            continue
        out[campaign_id] = payload
    return out


def _save_campaigns(campaigns: dict[str, dict[str, Any]]) -> None:
    CAMPAIGNS_STORE_PATH.parent.mkdir(parents=True, exist_ok=True)
    CAMPAIGNS_STORE_PATH.write_text(
        json.dumps(campaigns, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


def save_campaign_record(record: dict[str, Any]) -> dict[str, Any]:
    campaign_id = str(record.get("id", "")).strip()
    if not campaign_id:
        raise ValueError("Campaign record is missing id")

    campaigns = _load_campaigns()
    payload = dict(record)
    payload.setdefault("created_at", _utc_now())
    payload["updated_at"] = _utc_now()
    campaigns[campaign_id] = payload
    _save_campaigns(campaigns)
    return payload


def get_campaign_record(campaign_id: str) -> dict[str, Any] | None:
    key = (campaign_id or "").strip()
    if not key:
        return None
    campaigns = _load_campaigns()
    value = campaigns.get(key)
    if not isinstance(value, dict):
        return None
    return value


def get_campaign_record_for_user(campaign_id: str, user_id: int) -> dict[str, Any] | None:
    campaign = get_campaign_record(campaign_id)
    if campaign is None:
        return None
    campaign_user_id = int(campaign.get("user_id") or 0)
    if campaign_user_id != int(user_id):
        return None
    return campaign


def list_campaigns_for_user(user_id: int, limit: int = 200) -> list[dict[str, Any]]:
    normalized_limit = max(1, min(int(limit), 1000))
    campaigns = _load_campaigns()
    rows: list[dict[str, Any]] = []
    for payload in campaigns.values():
        if not isinstance(payload, dict):
            continue
        campaign_user_id = int(payload.get("user_id") or 0)
        if campaign_user_id != int(user_id):
            continue
        rows.append(payload)
    rows.sort(key=lambda item: str(item.get("created_at", "")), reverse=True)
    return rows[:normalized_limit]
