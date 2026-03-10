from __future__ import annotations

import csv
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

from db.repository import (
    create_draft,
    create_lead,
    get_contacts_by_lead,
    get_leads,
    record_run,
    update_lead_status,
)
from services.outreach_config import (
    DEFAULT_CAMPAIGN_PROMPT_PATH,
    DEFAULT_SENDER_PROFILE_KEY,
    campaign_path_display,
    ensure_default_files,
    load_sender_profiles,
    resolve_campaign_prompt,
    resolve_sender_profile,
    validate_sender_profile,
)
from services import source_strategist


BASE_DIR = Path(__file__).resolve().parent.parent
DEFAULT_INPUT_PATH = BASE_DIR / "workspace" / "leads" / "business_discovery_enriched.csv"
OUTPUT_PATH = BASE_DIR / "workspace" / "leads" / "business_discovery_outreach.csv"


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _record_outreach_run(
    *,
    status: str,
    started_at: str,
    input_path: str,
    sender_key: str,
    campaign_path: str,
) -> None:
    record_run(
        worker="business_outreach",
        args=[input_path, sender_key, campaign_path],
        status=status,
        started_at=started_at,
        finished_at=_utc_now(),
    )


def _resolve_input_path(input_path: str | None) -> Path:
    if not input_path:
        return DEFAULT_INPUT_PATH
    candidate = Path(input_path)
    if candidate.is_absolute():
        return candidate
    return BASE_DIR / candidate


def _derive_domain(url: str) -> str:
    host = urlparse(url).netloc.lower().strip()
    if host.startswith("www."):
        host = host[4:]
    return host


def _normalize_services(profile: dict) -> list[str]:
    raw_services = profile.get("services", [])
    if not isinstance(raw_services, list):
        return []
    out: list[str] = []
    for item in raw_services:
        value = str(item or "").strip()
        if value:
            out.append(value)
    return out


def _parse_campaign_prompt(prompt_text: str) -> dict[str, str]:
    sections = {"goal": "", "audience": "", "tone": "", "offer": ""}
    current = ""
    for line in prompt_text.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        lowered = stripped.lower()
        if lowered.startswith("goal:"):
            current = "goal"
            sections[current] = stripped[5:].strip()
            continue
        if lowered.startswith("audience:"):
            current = "audience"
            sections[current] = stripped[9:].strip()
            continue
        if lowered.startswith("tone:"):
            current = "tone"
            sections[current] = stripped[5:].strip()
            continue
        if lowered.startswith("offer:"):
            current = "offer"
            sections[current] = stripped[6:].strip()
            continue
        if current:
            existing = sections[current]
            sections[current] = f"{existing} {stripped}".strip()
    return sections


def _build_subject(title: str, services: list[str], offer: str, discovery_mode: str) -> str:
    business_label = title or "your team"
    if len(business_label) > 36:
        compact = business_label[:40].rsplit(" ", 1)[0].strip(" -:,")
        business_label = compact or business_label[:40]
    service_hint = services[0] if services else "exterior cleaning"

    if discovery_mode == "company_mode":
        if offer:
            subject = f"Quick quote for {business_label}"
        else:
            subject = f"{service_hint.title()} support for {business_label}"
    else:
        if offer:
            subject = f"{business_label}: {service_hint} quote"
        else:
            subject = f"{business_label}: quick {service_hint} quote"

    if len(subject) <= 72:
        return subject.strip()
    trimmed = subject[:72].rsplit(" ", 1)[0].strip(" -:,")
    return trimmed or subject[:72].strip()


def _build_message(
    *,
    title: str,
    sender_name: str,
    sender_business: str,
    sender_city: str,
    sender_phone: str,
    services: list[str],
    sender_angle: str,
    campaign_offer: str,
    campaign_goal: str,
    campaign_tone: str,
    discovery_mode: str,
) -> str:
    lead_label = title or "there"
    service_line = ", ".join(services[:3]) if services else "exterior cleaning work"
    offer_line = campaign_offer or "I can share a quick quote if timing is helpful."
    goal_line = campaign_goal or "I help with curb-appeal prep before listings."
    tone_line = campaign_tone or "Friendly and local."

    if discovery_mode == "company_mode":
        return (
            f"Hi {lead_label},\n\n"
            f"I'm {sender_name} from {sender_business} in {sender_city}. "
            f"We help local businesses with {service_line}.\n"
            f"{offer_line}\n"
            f"If useful, I can send a fast estimate and availability this week.\n\n"
            f"{sender_name}\n"
            f"{sender_business}\n"
            f"{sender_phone}"
        )

    return (
        f"Hi {lead_label},\n\n"
        f"I'm {sender_name} with {sender_business} in {sender_city}. "
        f"We handle {service_line}. {sender_angle}.\n"
        f"{goal_line}\n"
        f"{offer_line}\n"
        f"{tone_line}\n\n"
        f"{sender_name}\n"
        f"{sender_business}\n"
        f"{sender_phone}"
    )


def _fallback_business_label(domain: str, url: str) -> str:
    domain_value = (domain or "").strip().lower()
    if not domain_value and url:
        domain_value = _derive_domain(url)
    if not domain_value:
        return "your team"
    root = domain_value.split(".")[0].replace("-", " ").replace("_", " ").strip()
    if not root:
        return "your team"
    if " " not in root and root.isalpha() and len(root) >= 12:
        lowered = root.lower()
        for token in ["roofing", "dental", "dentistry", "landscaping", "painting", "services", "group", "clinic"]:
            lowered = lowered.replace(token, f" {token} ")
        root = " ".join(lowered.split())
    return " ".join(part.capitalize() for part in root.split())


def _clean_lead_title(raw_title: str, domain: str, url: str) -> str:
    title = (raw_title or "").strip()
    if not title:
        return _fallback_business_label(domain, url)

    title = re.sub(r"\s+at\s+[A-Za-z0-9.-]+\.[A-Za-z]{2,}.*$", "", title, flags=re.IGNORECASE)
    title = title.split("|")[0].strip()
    title = re.sub(
        r"\s*-\s*(find.*|profile.*|linkedin.*|facebook.*|instagram.*|yelp.*|coroflot.*)$",
        "",
        title,
        flags=re.IGNORECASE,
    ).strip()
    title = " ".join(title.split())

    noisy_markers = (
        ".com",
        "http://",
        "https://",
        "/",
        "search results",
        "directory",
        "best ",
        "top ",
        "find ",
        " in ",
        " near ",
    )
    lowered = title.lower()
    weak_category = any(
        term in lowered
        for term in [
            "companies in",
            "dentists in",
            "roofers in",
            "realtors in",
            "service providers",
        ]
    )
    is_noisy = any(marker in lowered for marker in noisy_markers) or len(title.split()) > 10 or weak_category
    if not title or is_noisy:
        return _fallback_business_label(domain, url)
    return title


def _rows_from_db() -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for lead in get_leads(limit=5000):
        request_profile = source_strategist.classify_request(lead.query or "")
        contacts = get_contacts_by_lead(lead.id)
        if contacts:
            for contact in contacts:
                rows.append(
                    {
                        "LeadID": str(lead.id),
                        "Title": lead.name,
                        "URL": lead.website,
                        "Snippet": "",
                        "Query": lead.query,
                        "Domain": lead.domain or _derive_domain(lead.website),
                        "Email": contact.email,
                        "Phone": contact.phone,
                        "SourcePage": contact.source_page or lead.website,
                        "DiscoveryMode": request_profile.get("discovery_mode", "company_mode"),
                        "ModeReason": request_profile.get("mode_reason", ""),
                    }
                )
        else:
            rows.append(
                {
                    "LeadID": str(lead.id),
                    "Title": lead.name,
                    "URL": lead.website,
                    "Snippet": "",
                    "Query": lead.query,
                    "Domain": lead.domain or _derive_domain(lead.website),
                    "Email": "",
                    "Phone": "",
                    "SourcePage": lead.website,
                    "DiscoveryMode": request_profile.get("discovery_mode", "company_mode"),
                    "ModeReason": request_profile.get("mode_reason", ""),
                }
            )
    return rows


def _rows_from_csv(input_path: Path) -> list[dict[str, str]]:
    with input_path.open("r", encoding="utf-8", newline="") as infile:
        reader = csv.DictReader(infile)
        csv_rows = list(reader)

    rows: list[dict[str, str]] = []
    for row in csv_rows:
        lead_id_raw = (row.get("LeadID") or "").strip()
        title = (row.get("Title") or "").strip()
        url = (row.get("URL") or "").strip()
        snippet = (row.get("Snippet") or "").strip()
        query = (row.get("Query") or "").strip()
        domain = (row.get("Domain") or "").strip() or _derive_domain(url)
        email = (row.get("Email") or "").strip()
        phone = (row.get("Phone") or "").strip()
        source_page = (row.get("SourcePage") or "").strip()

        if lead_id_raw.isdigit():
            lead_id = int(lead_id_raw)
        else:
            lead = create_lead(
                name=title or domain or url or "Unknown Lead",
                domain=domain,
                website=url,
                source="csv_import",
                query=query,
                status="enriched",
            )
            lead_id = lead.id

        rows.append(
            {
                "LeadID": str(lead_id),
                "Title": title,
                "URL": url,
                "Snippet": snippet,
                "Query": query,
                "Domain": domain,
                "Email": email,
                "Phone": phone,
                "SourcePage": source_page,
                "DiscoveryMode": (row.get("DiscoveryMode") or "").strip(),
                "ModeReason": (row.get("ModeReason") or "").strip(),
            }
        )
    return rows


def _parse_compound_input(
    input_path: str | None,
    sender_profile_key: str | None,
    campaign_prompt_path: str | None,
) -> tuple[str | None, str | None, str | None]:
    normalized_input = (input_path or "").strip()
    if not normalized_input:
        return None, sender_profile_key, campaign_prompt_path

    if normalized_input.startswith("{") and normalized_input.endswith("}"):
        try:
            payload = json.loads(normalized_input)
        except json.JSONDecodeError:
            return normalized_input, sender_profile_key, campaign_prompt_path
        if isinstance(payload, dict):
            parsed_input = str(payload.get("input_path", "")).strip() or None
            parsed_sender = str(payload.get("sender_profile_key", "")).strip() or None
            parsed_campaign = str(payload.get("campaign_prompt_path", "")).strip() or None
            return (
                parsed_input,
                parsed_sender or sender_profile_key,
                parsed_campaign or campaign_prompt_path,
            )
    return normalized_input, sender_profile_key, campaign_prompt_path


def run(
    input_path: str | None = None,
    sender_profile_key: str | None = None,
    campaign_prompt_path: str | None = None,
) -> str:
    ensure_default_files()
    started_at = _utc_now()
    normalized_input_path, normalized_sender_key, normalized_campaign_path = _parse_compound_input(
        input_path,
        sender_profile_key,
        campaign_prompt_path,
    )

    sender_key, sender_profile = resolve_sender_profile(
        normalized_sender_key or DEFAULT_SENDER_PROFILE_KEY
    )
    campaign_path_resolved, campaign_prompt_text = resolve_campaign_prompt(
        normalized_campaign_path or campaign_path_display(DEFAULT_CAMPAIGN_PROMPT_PATH)
    )
    campaign_sections = _parse_campaign_prompt(campaign_prompt_text)

    profiles = load_sender_profiles()
    requested_sender_key = (normalized_sender_key or sender_key).strip()
    if requested_sender_key and requested_sender_key not in profiles:
        _record_outreach_run(
            status="error",
            started_at=started_at,
            input_path=normalized_input_path or "",
            sender_key=requested_sender_key,
            campaign_path=campaign_path_resolved,
        )
        available = ", ".join(sorted(profiles.keys()))
        return (
            "business outreach worker failed: unknown sender profile key "
            f'"{requested_sender_key}". Available profiles: {available}'
        )

    sender_validation = validate_sender_profile(sender_profile)
    if not sender_validation.get("valid", False):
        _record_outreach_run(
            status="error",
            started_at=started_at,
            input_path=normalized_input_path or "",
            sender_key=sender_key,
            campaign_path=campaign_path_resolved,
        )
        return (
            "business outreach worker failed: invalid sender profile "
            f'"{sender_key}". '
            + " ".join(str(err) for err in sender_validation.get("errors", []))
            + f" Fix {BASE_DIR / 'workspace' / 'config' / 'senders.json'}."
        )

    sender_name = str(sender_profile.get("name") or "REPLACE_ME").strip()
    sender_business = str(sender_profile.get("business") or "REPLACE_ME").strip()
    sender_city = str(sender_profile.get("city") or "Victoria BC").strip()
    sender_phone = str(sender_profile.get("phone") or "REPLACE_ME").strip()
    sender_angle = str(sender_profile.get("angle") or "").strip() or "Local service with quick quotes."
    sender_services = _normalize_services(sender_profile)

    rows: list[dict[str, str]] = []
    source_mode = "db"
    try:
        if normalized_input_path:
            source_path = _resolve_input_path(normalized_input_path)
            if not source_path.exists():
                _record_outreach_run(
                    status="error",
                    started_at=started_at,
                    input_path=normalized_input_path,
                    sender_key=sender_key,
                    campaign_path=campaign_path_resolved,
                )
                return (
                    f"business outreach worker failed: input file not found at {source_path}"
                )
            rows = _rows_from_csv(source_path)
            source_mode = f"csv:{source_path}"
        else:
            rows = _rows_from_db()
            if not rows and DEFAULT_INPUT_PATH.exists():
                rows = _rows_from_csv(DEFAULT_INPUT_PATH)
                source_mode = f"csv-fallback:{DEFAULT_INPUT_PATH}"
    except Exception as exc:
        _record_outreach_run(
            status="error",
            started_at=started_at,
            input_path=normalized_input_path or "",
            sender_key=sender_key,
            campaign_path=campaign_path_resolved,
        )
        return f"business outreach worker failed: {exc}"

    output_rows: list[dict[str, str]] = []
    draft_count = 0
    for row in rows:
        lead_id = int((row.get("LeadID") or "0").strip() or "0")
        title = (row.get("Title") or "").strip()
        url = (row.get("URL") or "").strip()
        snippet = (row.get("Snippet") or "").strip()
        query = (row.get("Query") or "").strip()
        domain = (row.get("Domain") or "").strip()
        email = (row.get("Email") or "").strip()
        phone = (row.get("Phone") or "").strip()
        source_page = (row.get("SourcePage") or "").strip()
        discovery_mode = (row.get("DiscoveryMode") or "").strip()
        if not discovery_mode:
            profile = source_strategist.classify_request(query)
            discovery_mode = profile.get("discovery_mode", "company_mode")
        cleaned_title = _clean_lead_title(title, domain, url)

        subject = _build_subject(
            title=cleaned_title,
            services=sender_services,
            offer=campaign_sections.get("offer", ""),
            discovery_mode=discovery_mode,
        )
        message = _build_message(
            title=cleaned_title,
            sender_name=sender_name,
            sender_business=sender_business,
            sender_city=sender_city,
            sender_phone=sender_phone,
            services=sender_services,
            sender_angle=sender_angle,
            campaign_offer=campaign_sections.get("offer", ""),
            campaign_goal=campaign_sections.get("goal", ""),
            campaign_tone=campaign_sections.get("tone", ""),
            discovery_mode=discovery_mode,
        )

        if lead_id > 0:
            create_draft(
                lead_id=lead_id,
                email=email,
                subject=subject,
                body=message,
                status="draft",
                sender_profile=sender_key,
                campaign_prompt=campaign_path_resolved,
            )
            update_lead_status(lead_id, "drafted")
            draft_count += 1

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
                "DiscoveryMode": discovery_mode,
                "Subject": subject,
                "Message": message,
                "SenderProfile": sender_key,
                "CampaignPrompt": campaign_path_resolved,
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
                "DiscoveryMode",
                "Subject",
                "Message",
                "SenderProfile",
                "CampaignPrompt",
            ],
        )
        writer.writeheader()
        writer.writerows(output_rows)

    _record_outreach_run(
        status="ok",
        started_at=started_at,
        input_path=normalized_input_path or "",
        sender_key=sender_key,
        campaign_path=campaign_path_resolved,
    )
    return (
        "business outreach worker completed: "
        f"wrote {len(output_rows)} rows to {OUTPUT_PATH}; "
        f"stored {draft_count} draft rows in workspace/data/assistant.db "
        f"(source={source_mode}, sender_profile={sender_key}, "
        f"campaign_prompt={campaign_path_resolved})"
    )
