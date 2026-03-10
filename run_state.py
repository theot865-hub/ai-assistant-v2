from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


BASE_DIR = Path(__file__).resolve().parent
STATE_PATH = BASE_DIR / "workspace" / "logs" / "run_state.json"

ARTIFACTS_BY_WORKER: dict[str, list[str]] = {
    "business_enrichment": ["workspace/leads/business_discovery_enriched.csv"],
    "business_discovery": ["workspace/leads/business_discovery.csv"],
    "business_outreach": ["workspace/leads/business_discovery_outreach.csv"],
    "enrichment": ["workspace/leads/all_realtor_leads_enriched.csv"],
    "leads": ["workspace/leads/all_realtor_leads.csv"],
    "research": ["workspace/jobs/latest_research.txt"],
    "outreach": ["workspace/leads/all_realtor_outreach.csv"],
    "pipeline": [
        "workspace/leads/business_discovery.csv",
        "workspace/leads/business_discovery_enriched.csv",
        "workspace/leads/business_discovery_outreach.csv",
    ],
}


def _empty_state() -> dict[str, Any]:
    return {"latest_success": {}}


def _load_state() -> dict[str, Any]:
    if not STATE_PATH.exists():
        return _empty_state()
    try:
        parsed = json.loads(STATE_PATH.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return _empty_state()
    if not isinstance(parsed, dict):
        return _empty_state()
    latest = parsed.get("latest_success")
    if not isinstance(latest, dict):
        parsed["latest_success"] = {}
    return parsed


def _save_state(state: dict[str, Any]) -> None:
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    STATE_PATH.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


def update_run_state(worker: str, ok: bool, result: str, error: str) -> None:
    state = _load_state()
    latest = state.setdefault("latest_success", {})
    if not isinstance(latest, dict):
        latest = {}
        state["latest_success"] = latest

    if ok and worker:
        latest[worker] = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "worker": worker,
            "ok": True,
            "result": result,
            "error": "",
            "artifacts": ARTIFACTS_BY_WORKER.get(worker, []),
        }

    _save_state(state)


def get_latest_success_by_worker() -> dict[str, dict[str, Any]]:
    state = _load_state()
    latest = state.get("latest_success", {})
    if not isinstance(latest, dict):
        return {}
    return latest


def get_artifacts_for_worker(worker: str) -> list[str]:
    latest = get_latest_success_by_worker()
    entry = latest.get(worker, {})
    if not isinstance(entry, dict):
        return []
    artifacts = entry.get("artifacts", [])
    if not isinstance(artifacts, list):
        return []
    return [str(item) for item in artifacts]


def get_latest_artifacts_all_workers() -> dict[str, list[str]]:
    latest = get_latest_success_by_worker()
    out: dict[str, list[str]] = {}
    for worker in ARTIFACTS_BY_WORKER:
        entry = latest.get(worker, {})
        if isinstance(entry, dict) and isinstance(entry.get("artifacts"), list):
            out[worker] = [str(item) for item in entry.get("artifacts", [])]
        else:
            out[worker] = []
    return out
