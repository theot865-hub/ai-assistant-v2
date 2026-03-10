from __future__ import annotations

import csv
import json
from datetime import datetime, timezone
from pathlib import Path
import re
from typing import Any
from uuid import uuid4

from db.repository import (
    assign_draft_owner_for_new_records,
    get_latest_draft_id,
)
from services.campaign_store import (
    get_campaign_record,
    save_campaign_record,
)
from services.outreach_config import (
    DEFAULT_CAMPAIGN_PROMPT_PATH,
    DEFAULT_SENDER_PROFILE_KEY,
    campaign_path_display,
    ensure_default_files,
    resolve_campaign_prompt,
    resolve_sender_profile,
    save_sender_profile,
    validate_sender_profile,
)
from workers import business_discovery_worker
from workers import business_enrichment_worker
from workers import business_outreach_worker


BASE_DIR = Path(__file__).resolve().parent.parent
WORKSPACE_DIR = BASE_DIR / "workspace"
CAMPAIGN_ARTIFACT_PATHS = [
    "workspace/leads/business_discovery.csv",
    "workspace/leads/business_discovery_enriched.csv",
    "workspace/leads/business_discovery_outreach.csv",
]


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _generate_campaign_id(campaign_name: str) -> str:
    base = re.sub(r"[^a-z0-9]+", "-", (campaign_name or "").lower()).strip("-")
    if not base:
        base = "campaign"
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    token = uuid4().hex[:6]
    return f"{base}-{timestamp}-{token}"


def _parse_services(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    if isinstance(value, str):
        return [item.strip() for item in value.split(",") if item.strip()]
    return []


def _build_campaign_prompt_text(campaign_data: dict[str, Any]) -> str:
    audience = str(campaign_data.get("audience", "")).strip()
    location = str(campaign_data.get("location", "")).strip()
    city = str(campaign_data.get("city", "")).strip()
    services = str(campaign_data.get("services_offered", "")).strip()
    angle = str(campaign_data.get("unique_angle", "")).strip()
    offer = str(campaign_data.get("offer", "")).strip()
    tone = str(campaign_data.get("tone", "")).strip()
    cta = str(campaign_data.get("call_to_action", "")).strip()

    return (
        f"Goal:\n{offer or 'Offer relevant local services with clear value.'}\n\n"
        f"Audience:\n{audience} in {location}\n\n"
        f"Tone:\n{tone or 'Friendly, concise, local.'}\n\n"
        f"Offer:\n{offer or 'Provide a quick quote.'}\n\n"
        f"CallToAction:\n{cta or 'Reply to get a quick quote.'}\n\n"
        f"SenderContext:\n"
        f"City: {city}\n"
        f"Services: {services}\n"
        f"Angle: {angle}\n"
    )


def _build_discovery_query(campaign_data: dict[str, Any]) -> str:
    explicit = str(campaign_data.get("discovery_query", "")).strip()
    if explicit:
        return explicit
    audience = str(campaign_data.get("audience", "")).strip()
    location = str(campaign_data.get("location", "")).strip()
    extra_notes = str(campaign_data.get("extra_notes", "")).strip()
    max_leads = str(campaign_data.get("max_leads", "")).strip()
    query = f"find {audience} businesses in {location}".strip()
    if extra_notes:
        query += f"; focus: {extra_notes}"
    if max_leads:
        query += f"; target around {max_leads} leads"
    return query


def _csv_row_count(path: Path) -> int:
    if not path.exists() or not path.is_file():
        return 0
    with path.open("r", encoding="utf-8", newline="") as infile:
        reader = csv.DictReader(infile)
        return sum(1 for _ in reader)


def _enriched_coverage(path: Path) -> tuple[int, int]:
    if not path.exists() or not path.is_file():
        return 0, 0
    with path.open("r", encoding="utf-8", newline="") as infile:
        reader = csv.DictReader(infile)
        emails = 0
        phones = 0
        for row in reader:
            if str(row.get("Email", "")).strip():
                emails += 1
            if str(row.get("Phone", "")).strip():
                phones += 1
    return emails, phones


def _enriched_breakdown(path: Path) -> tuple[int, int, int, int, int]:
    if not path.exists() or not path.is_file():
        return 0, 0, 0, 0, 0
    total = 0
    with_email = 0
    with_phone = 0
    with_both = 0
    with_neither = 0
    with path.open("r", encoding="utf-8", newline="") as infile:
        reader = csv.DictReader(infile)
        for row in reader:
            total += 1
            has_email = bool(str(row.get("Email", "")).strip())
            has_phone = bool(str(row.get("Phone", "")).strip())
            if has_email:
                with_email += 1
            if has_phone:
                with_phone += 1
            if has_email and has_phone:
                with_both += 1
            if not has_email and not has_phone:
                with_neither += 1
    return total, with_email, with_phone, with_both, with_neither


def _resolve_artifact_path(path_value: str) -> Path:
    raw = str(path_value or "").strip()
    if not raw:
        return BASE_DIR / "workspace" / "leads" / "missing.csv"
    candidate = Path(raw)
    if candidate.is_absolute():
        return candidate
    return BASE_DIR / raw


def _extract_int_metric(text: str, metric_key: str) -> int | None:
    match = re.search(rf"{re.escape(metric_key)}=(\d+)", text)
    if not match:
        return None
    try:
        return int(match.group(1))
    except ValueError:
        return None


def _read_json_file(path: Path) -> dict[str, Any]:
    if not path.exists() or not path.is_file():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _top_reject_reasons(path: Path, limit: int = 5) -> list[dict[str, Any]]:
    if not path.exists() or not path.is_file():
        return []
    counts: dict[str, int] = {}
    with path.open("r", encoding="utf-8", newline="") as infile:
        reader = csv.DictReader(infile)
        for row in reader:
            reason = str(row.get("ValidationReason", "")).strip() or "unspecified"
            counts[reason] = counts.get(reason, 0) + 1
    ranked = sorted(counts.items(), key=lambda item: item[1], reverse=True)
    return [
        {"reason": reason, "count": count}
        for reason, count in ranked[: max(1, int(limit))]
    ]


class CampaignService:
    def __init__(self) -> None:
        ensure_default_files()

    def create_campaign(self, user_id, campaign_data):
        user_id_int = int(user_id or 0)
        payload = dict(campaign_data or {})
        errors: list[str] = []

        discovery_query = _build_discovery_query(payload)
        audience = str(payload.get("audience", "")).strip()
        location = str(payload.get("location", "")).strip()
        if not discovery_query:
            if not audience:
                errors.append("Audience / business type is required.")
            if not location:
                errors.append("Location is required.")
        max_leads_value = str(payload.get("max_leads", "")).strip()
        if max_leads_value:
            try:
                if int(max_leads_value) <= 0:
                    raise ValueError
            except ValueError:
                errors.append("Max leads must be a positive number when provided.")

        sender_profile_key = str(payload.get("sender_profile_key", "")).strip()
        campaign_prompt_path = str(payload.get("campaign_prompt_path", "")).strip()
        sender_profile: dict[str, Any]
        if sender_profile_key:
            sender_profile_key, sender_profile = resolve_sender_profile(sender_profile_key)
        else:
            sender_profile = {
                "name": str(payload.get("sender_name", "")).strip(),
                "business": str(payload.get("business_name", "")).strip(),
                "city": str(payload.get("city", "")).strip(),
                "phone": str(payload.get("phone", "")).strip(),
                "services": _parse_services(payload.get("services_offered", "")),
                "angle": str(payload.get("unique_angle", "")).strip(),
                "email": str(payload.get("email", "")).strip(),
                "website": str(payload.get("website", "")).strip(),
            }
            sender_profile_key = ""

        sender_validation = validate_sender_profile(sender_profile)
        if not sender_validation.get("valid", False):
            errors.extend(str(item) for item in sender_validation.get("errors", []))

        if errors:
            raise ValueError(" ".join(errors))

        campaign_name = str(payload.get("campaign_name", "")).strip() or (
            f"{audience or 'businesses'} in {location or 'local area'}"
        )
        campaign_id = _generate_campaign_id(campaign_name)
        campaign_sender_key = sender_profile_key or f"campaign_{campaign_id}"
        save_sender_profile(campaign_sender_key, sender_profile)

        prompt_path: str
        if campaign_prompt_path:
            prompt_path = campaign_prompt_path
        else:
            campaign_prompt_text = _build_campaign_prompt_text(payload)
            prompt_file = WORKSPACE_DIR / "campaigns" / f"{campaign_id}.txt"
            prompt_file.parent.mkdir(parents=True, exist_ok=True)
            prompt_file.write_text(campaign_prompt_text, encoding="utf-8")
            prompt_path = str(prompt_file)

        resolved_prompt_path, _ = resolve_campaign_prompt(prompt_path)
        record = {
            "id": campaign_id,
            "name": campaign_name,
            "user_id": user_id_int,
            "user_email": str(payload.get("user_email", "")).strip().lower(),
            "status": "created",
            "lead_request": {
                "audience": audience,
                "location": location,
                "max_leads": max_leads_value,
                "extra_notes": str(payload.get("extra_notes", "")).strip(),
                "discovery_query": discovery_query,
            },
            "sender": sender_profile,
            "sender_profile_key": campaign_sender_key,
            "campaign_prompt_path": resolved_prompt_path,
            "worker_results": [],
            "assigned_draft_count": 0,
            "artifacts": list(CAMPAIGN_ARTIFACT_PATHS),
            "summary": {},
            "diagnostics": {},
        }
        save_campaign_record(record)
        return campaign_id

    def run_campaign(self, campaign_id):
        campaign = get_campaign_record(campaign_id)
        if campaign is None:
            raise ValueError(f"Campaign not found: {campaign_id}")

        lead_request = campaign.get("lead_request", {})
        if not isinstance(lead_request, dict):
            lead_request = {}
        discovery_query = _build_discovery_query(lead_request)
        if not discovery_query:
            raise ValueError("Campaign missing discovery query inputs.")

        sender_profile_key = str(campaign.get("sender_profile_key", "")).strip() or DEFAULT_SENDER_PROFILE_KEY
        prompt_path = str(campaign.get("campaign_prompt_path", "")).strip()
        if not prompt_path:
            prompt_path = campaign_path_display(DEFAULT_CAMPAIGN_PROMPT_PATH)

        before_draft_id = get_latest_draft_id()
        worker_results: list[dict[str, Any]] = []
        status = "completed"

        workflow = [
            ("business_discovery", lambda: business_discovery_worker.run(discovery_query)),
            ("business_enrichment", lambda: business_enrichment_worker.run()),
            (
                "business_outreach",
                lambda: business_outreach_worker.run(
                    sender_profile_key=sender_profile_key,
                    campaign_prompt_path=prompt_path,
                ),
            ),
        ]
        for worker_name, runner in workflow:
            try:
                result_text = str(runner())
                ok = not any(token in result_text.lower() for token in ("failed", "error"))
                worker_results.append(
                    {
                        "worker": worker_name,
                        "args": [],
                        "ok": ok,
                        "result": result_text,
                        "error": "",
                    }
                )
                if not ok:
                    status = "failed"
                    break
            except Exception as exc:
                status = "failed"
                worker_results.append(
                    {
                        "worker": worker_name,
                        "args": [],
                        "ok": False,
                        "result": "",
                        "error": str(exc),
                    }
                )
                break

        user_id = int(campaign.get("user_id") or 0)
        assigned_draft_count = 0
        if user_id > 0:
            assigned_draft_count = assign_draft_owner_for_new_records(before_draft_id, user_id)

        diagnostics = self._build_campaign_diagnostics(campaign, worker_results)
        summary = self.get_campaign_summary(campaign_id)
        updated_record = dict(campaign)
        updated_record["status"] = status
        updated_record["worker_results"] = worker_results
        updated_record["assigned_draft_count"] = assigned_draft_count
        updated_record["summary"] = dict(summary)
        updated_record["diagnostics"] = diagnostics
        updated_record["last_run_at"] = _utc_now()
        save_campaign_record(updated_record)
        return summary

    def _build_campaign_diagnostics(
        self,
        campaign: dict[str, Any],
        worker_results: list[dict[str, Any]],
    ) -> dict[str, Any]:
        artifacts = campaign.get("artifacts", CAMPAIGN_ARTIFACT_PATHS)
        if not isinstance(artifacts, list):
            artifacts = CAMPAIGN_ARTIFACT_PATHS
        artifact_paths = [_resolve_artifact_path(str(path)) for path in artifacts]
        discovery_path = artifact_paths[0] if len(artifact_paths) > 0 else BASE_DIR / CAMPAIGN_ARTIFACT_PATHS[0]
        enrichment_path = artifact_paths[1] if len(artifact_paths) > 1 else BASE_DIR / CAMPAIGN_ARTIFACT_PATHS[1]
        outreach_path = artifact_paths[2] if len(artifact_paths) > 2 else BASE_DIR / CAMPAIGN_ARTIFACT_PATHS[2]

        discovery_result_text = ""
        for item in worker_results:
            if str(item.get("worker", "")).strip() == "business_discovery":
                discovery_result_text = str(item.get("result", "")).strip()
                break
        rejected_path = Path(
            getattr(
                business_discovery_worker,
                "REJECTED_OUTPUT_PATH",
                BASE_DIR / "workspace" / "leads" / "business_discovery_rejected.csv",
            )
        )
        raw_candidates = _extract_int_metric(discovery_result_text, "raw_candidates")
        if raw_candidates is None:
            raw_candidates = _csv_row_count(discovery_path) + _csv_row_count(rejected_path)
        validated_saved = _extract_int_metric(discovery_result_text, "validated_saved")
        if validated_saved is None:
            validated_saved = _csv_row_count(discovery_path)
        rejected_count = _csv_row_count(rejected_path)

        summary_path = Path(getattr(business_enrichment_worker, "SUMMARY_OUTPUT_PATH", ""))
        enrichment_summary = _read_json_file(summary_path)
        total_leads = int(enrichment_summary.get("total_leads") or 0)
        leads_with_email = int(enrichment_summary.get("leads_with_email") or 0)
        leads_with_phone = int(enrichment_summary.get("leads_with_phone") or 0)
        leads_with_both = int(enrichment_summary.get("leads_with_both") or 0)
        leads_with_neither = int(enrichment_summary.get("leads_with_neither") or 0)
        if total_leads <= 0:
            (
                total_leads,
                leads_with_email,
                leads_with_phone,
                leads_with_both,
                leads_with_neither,
            ) = _enriched_breakdown(enrichment_path)

        outreach_result_text = ""
        for item in worker_results:
            if str(item.get("worker", "")).strip() == "business_outreach":
                outreach_result_text = str(item.get("result", "")).strip()
                break
        outreach_match = re.search(r"wrote\s+(\d+)\s+rows", outreach_result_text)
        drafts_generated = int(outreach_match.group(1)) if outreach_match else None
        if drafts_generated is None:
            drafts_generated = _csv_row_count(outreach_path)

        return {
            "discovery": {
                "raw_candidates": int(raw_candidates),
                "validated_saved": int(validated_saved),
                "rejected_count": int(rejected_count),
                "top_reject_reasons": _top_reject_reasons(rejected_path, limit=5),
            },
            "enrichment": {
                "total_leads": int(total_leads),
                "leads_with_email": int(leads_with_email),
                "leads_with_phone": int(leads_with_phone),
                "leads_with_both": int(leads_with_both),
                "leads_with_neither": int(leads_with_neither),
            },
            "outreach": {
                "drafts_generated": int(drafts_generated),
            },
        }

    def get_campaign_summary(self, campaign_id):
        campaign = get_campaign_record(campaign_id)
        if campaign is None:
            raise ValueError(f"Campaign not found: {campaign_id}")

        artifacts = campaign.get("artifacts", CAMPAIGN_ARTIFACT_PATHS)
        if not isinstance(artifacts, list):
            artifacts = CAMPAIGN_ARTIFACT_PATHS
        artifact_paths = [_resolve_artifact_path(str(path)) for path in artifacts]
        discovery_path = artifact_paths[0] if len(artifact_paths) > 0 else BASE_DIR / CAMPAIGN_ARTIFACT_PATHS[0]
        enrichment_path = artifact_paths[1] if len(artifact_paths) > 1 else BASE_DIR / CAMPAIGN_ARTIFACT_PATHS[1]
        outreach_path = artifact_paths[2] if len(artifact_paths) > 2 else BASE_DIR / CAMPAIGN_ARTIFACT_PATHS[2]

        leads_found = _csv_row_count(discovery_path)
        emails_found, phones_found = _enriched_coverage(enrichment_path)
        drafts_generated = _csv_row_count(outreach_path)

        return {
            "campaign_id": str(campaign_id),
            "leads_found": leads_found,
            "emails_found": emails_found,
            "phones_found": phones_found,
            "drafts_generated": drafts_generated,
        }
