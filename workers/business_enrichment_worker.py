from __future__ import annotations

import csv
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from urllib.error import URLError
from urllib.parse import urljoin, urlparse
from urllib.request import Request, urlopen

from db.repository import (
    create_contact,
    create_lead,
    get_leads,
    record_run,
    update_lead_status,
)
from services import source_strategist


BASE_DIR = Path(__file__).resolve().parent.parent
DEFAULT_INPUT_PATH = BASE_DIR / "workspace" / "leads" / "business_discovery.csv"
OUTPUT_PATH = BASE_DIR / "workspace" / "leads" / "business_discovery_enriched.csv"
SUMMARY_OUTPUT_PATH = BASE_DIR / "workspace" / "leads" / "business_discovery_summary.json"

EMAIL_RE = re.compile(r"[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}", re.IGNORECASE)
PHONE_RE = re.compile(
    r"(?:\+?1[\s\-.]?)?(?:\(?\d{3}\)?[\s\-.]?)\d{3}[\s\-.]?\d{4}"
)
COMPANY_NAME_WEAK_PATTERNS = [
    re.compile(r"\b(best|top|find|directory|list|listing|search)\b", re.IGNORECASE),
    re.compile(r"\b(in|near)\s+[A-Za-z\s,.-]+$", re.IGNORECASE),
    re.compile(r"\b(dentists?|roofers?|realtors?|companies?)\b", re.IGNORECASE),
    re.compile(r"\b(conditions addressed|readers'? choice award|service area|our team|contact us|about us)\b", re.IGNORECASE),
    re.compile(r"\b(roofing repair|roof contractors|dental conditions)\b", re.IGNORECASE),
]
NAME_SPLIT_TOKENS = [
    "roofing",
    "dental",
    "dentistry",
    "landscaping",
    "painting",
    "contracting",
    "services",
    "solutions",
    "clinic",
    "group",
    "ltd",
    "inc",
]


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


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


def _fetch_page(url: str) -> tuple[str, str]:
    req = Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urlopen(req, timeout=8) as resp:
        charset = resp.headers.get_content_charset() or "utf-8"
        html = resp.read().decode(charset, errors="ignore")
        source_page = resp.geturl() or url
    return html, source_page


def _extract_email(text: str) -> str:
    mailto = re.search(r'href=["\']mailto:([^"\'>\s]+)', text, re.IGNORECASE)
    if mailto:
        return mailto.group(1).strip()
    match = EMAIL_RE.search(text)
    return (match.group(0).strip() if match else "")


def _extract_phone(text: str) -> str:
    tel = re.search(r'href=["\']tel:([^"\'>\s]+)', text, re.IGNORECASE)
    if tel:
        value = tel.group(1).strip()
        value = value.replace("%20", "").replace("-", "").replace(".", "")
        digits = re.sub(r"\D", "", value)
        if len(digits) == 11 and digits.startswith("1"):
            digits = digits[1:]
        if len(digits) == 10:
            return f"{digits[:3]}-{digits[3:6]}-{digits[6:]}"
        return value
    match = PHONE_RE.search(text)
    if not match:
        return ""
    raw = " ".join(match.group(0).split())
    digits = re.sub(r"\D", "", raw)
    if len(digits) == 11 and digits.startswith("1"):
        digits = digits[1:]
    if len(digits) == 10:
        return f"{digits[:3]}-{digits[3:6]}-{digits[6:]}"
    return raw


def _extract_candidate_contact_links(base_url: str, html: str) -> list[str]:
    links: list[str] = []
    seen: set[str] = set()
    base_root = _derive_domain(base_url)
    preferred_tokens = ("contact", "about", "team", "our-team", "staff")

    # direct high-probability paths first
    for path in ["/contact", "/about", "/team", "/our-team", "/contact-us", "/about-us"]:
        candidate = urljoin(base_url, path)
        key = candidate.rstrip("/").lower()
        if key not in seen:
            seen.add(key)
            links.append(candidate)

    for match in re.finditer(r"<a[^>]+href=[\"']([^\"']+)[\"'][^>]*>(.*?)</a>", html, re.IGNORECASE | re.DOTALL):
        href = (match.group(1) or "").strip()
        anchor_text = re.sub(r"<[^>]+>", " ", match.group(2) or "")
        anchor_text = " ".join(anchor_text.split()).lower()
        if not href:
            continue
        if href.startswith(("mailto:", "tel:", "javascript:")):
            continue
        full_url = urljoin(base_url, href)
        domain = _derive_domain(full_url)
        if base_root and domain and domain != base_root:
            continue
        path_lower = urlparse(full_url).path.lower()
        if not any(token in path_lower or token in anchor_text for token in preferred_tokens):
            continue
        key = full_url.rstrip("/").lower()
        if key in seen:
            continue
        seen.add(key)
        links.append(full_url)
        if len(links) >= 12:
            break

    return links


def _contact_confidence(email: str, phone: str, source_page: str, html: str) -> float:
    confidence = 0.15
    if email:
        confidence += 0.45
    if phone:
        confidence += 0.30
    path = urlparse(source_page).path.lower()
    if any(token in path for token in ["/contact", "/about", "/team", "/our-team"]):
        confidence += 0.08
    if "mailto:" in html.lower():
        confidence += 0.03
    if "tel:" in html.lower():
        confidence += 0.03
    return max(0.0, min(0.99, confidence))


def _looks_weak_company_name(name: str) -> bool:
    value = " ".join((name or "").split())
    if not value:
        return True
    if len(value) < 3 or len(value.split()) > 10:
        return True
    lowered = value.lower()
    if lowered in {"contact us", "about us", "home", "our team"}:
        return True
    return any(pattern.search(value) for pattern in COMPANY_NAME_WEAK_PATTERNS)


def _extract_company_name_candidates(html: str, fallback_domain: str) -> list[str]:
    candidates: list[str] = []

    title_match = re.search(r"<title[^>]*>(.*?)</title>", html, re.IGNORECASE | re.DOTALL)
    if title_match:
        title = re.sub(r"<[^>]+>", " ", title_match.group(1))
        candidates.append(" ".join(title.split()))

    og_match = re.search(
        r"<meta[^>]+property=[\"']og:site_name[\"'][^>]+content=[\"'](.*?)[\"']",
        html,
        re.IGNORECASE | re.DOTALL,
    )
    if og_match:
        candidates.append(" ".join(og_match.group(1).split()))

    for h_match in re.finditer(r"<h1[^>]*>(.*?)</h1>", html, re.IGNORECASE | re.DOTALL):
        h = re.sub(r"<[^>]+>", " ", h_match.group(1))
        candidates.append(" ".join(h.split()))
        if len(candidates) >= 6:
            break

    if fallback_domain:
        root = fallback_domain.split(".")[0].replace("-", " ").replace("_", " ")
        candidates.append(" ".join(part.capitalize() for part in root.split()))

    cleaned: list[str] = []
    seen: set[str] = set()
    for c in candidates:
        value = c.strip(" -|:,")
        value = re.split(r"\s+[|\-:]\s+", value)[0].strip()
        if not value:
            continue
        key = value.lower()
        if key in seen:
            continue
        seen.add(key)
        cleaned.append(value)
    return cleaned


def _choose_company_name(raw_title: str, candidates: list[str], domain: str) -> str:
    def _humanize_name(name: str) -> str:
        value = " ".join((name or "").split())
        if not value:
            return value
        if " " not in value and value.isalpha() and len(value) >= 12:
            lowered = value.lower()
            for token in NAME_SPLIT_TOKENS:
                lowered = lowered.replace(token, f" {token} ")
            value = " ".join(lowered.split()).strip()
        return " ".join(part.capitalize() for part in value.split())

    raw = " ".join((raw_title or "").split())
    if raw and not _looks_weak_company_name(raw):
        return _humanize_name(raw)
    for candidate in candidates:
        if not _looks_weak_company_name(candidate):
            return _humanize_name(candidate)
    if domain:
        root = domain.split(".")[0].replace("-", " ").replace("_", " ").strip()
        if root:
            return _humanize_name(root)
    return _humanize_name(raw) if raw else "Unknown Business"


def _rows_from_db() -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for lead in get_leads(limit=5000):
        request_profile = source_strategist.classify_request(lead.query or "")
        rows.append(
            {
                "LeadID": str(lead.id),
                "Title": lead.name,
                "URL": lead.website,
                "Snippet": "",
                "Query": lead.query,
                "Domain": lead.domain or _derive_domain(lead.website),
                "TargetType": request_profile.get("target_type", ""),
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
        title = (row.get("Title") or "").strip()
        url = (row.get("URL") or "").strip()
        snippet = (row.get("Snippet") or "").strip()
        query = (row.get("Query") or "").strip()
        domain = _derive_domain(url) if url else ""
        lead_name = title or domain or url or "Unknown Lead"
        lead = create_lead(
            name=lead_name,
            domain=domain,
            website=url,
            source="csv_import",
            query=query,
            status="discovered",
        )
        rows.append(
            {
                "LeadID": str(lead.id),
                "Title": title,
                "URL": url,
                "Snippet": snippet,
                "Query": query,
                "Domain": domain,
                "TargetType": (row.get("TargetType") or "").strip(),
                "DiscoveryMode": (row.get("DiscoveryMode") or "").strip(),
                "ModeReason": (row.get("ModeReason") or "").strip(),
            }
        )
    return rows


def run(input_path: str | None = None) -> str:
    started_at = _utc_now()
    rows: list[dict[str, str]] = []
    source_mode = "db"

    try:
        if input_path:
            source_path = _resolve_input_path(input_path)
            if not source_path.exists():
                finished_at = _utc_now()
                record_run(
                    worker="business_enrichment",
                    args=[input_path],
                    status="error",
                    started_at=started_at,
                    finished_at=finished_at,
                )
                return (
                    "business enrichment worker failed: "
                    f"input file not found at {source_path}"
                )
            rows = _rows_from_csv(source_path)
            source_mode = f"csv:{source_path}"
        else:
            rows = _rows_from_db()
            if not rows and DEFAULT_INPUT_PATH.exists():
                rows = _rows_from_csv(DEFAULT_INPUT_PATH)
                source_mode = f"csv-fallback:{DEFAULT_INPUT_PATH}"
    except Exception as exc:
        finished_at = _utc_now()
        record_run(
            worker="business_enrichment",
            args=[input_path or ""],
            status="error",
            started_at=started_at,
            finished_at=finished_at,
        )
        return f"business enrichment worker failed: {exc}"

    output_rows: list[dict[str, str]] = []
    created_contacts = 0
    leads_with_email = 0
    leads_with_phone = 0
    leads_with_both = 0
    leads_with_neither = 0
    for row in rows:
        lead_id = int((row.get("LeadID") or "0").strip() or "0")
        title = (row.get("Title") or "").strip()
        url = (row.get("URL") or "").strip()
        snippet = (row.get("Snippet") or "").strip()
        query = (row.get("Query") or "").strip()
        domain = (row.get("Domain") or "").strip() or (_derive_domain(url) if url else "")
        target_type = (row.get("TargetType") or "").strip()
        discovery_mode = (row.get("DiscoveryMode") or "").strip()
        mode_reason = (row.get("ModeReason") or "").strip()

        if not discovery_mode:
            profile = source_strategist.classify_request(query)
            discovery_mode = profile.get("discovery_mode", "company_mode")
            mode_reason = profile.get("mode_reason", "")
            if not target_type:
                target_type = profile.get("target_type", "")

        email = ""
        phone = ""
        source_page = url
        chosen_confidence = 0.2
        cleaned_title = title

        if url:
            try:
                html, resolved_url = _fetch_page(url)
                base_page = resolved_url or url
                source_page = base_page

                # Prefer direct company identity from on-site metadata when company_mode.
                name_candidates = _extract_company_name_candidates(html, domain or _derive_domain(base_page))
                cleaned_title = _choose_company_name(title, name_candidates, domain or _derive_domain(base_page))

                candidate_pages = [base_page]
                if discovery_mode == "company_mode":
                    candidate_pages.extend(_extract_candidate_contact_links(base_page, html))

                best_email = ""
                best_phone = ""
                best_source = base_page
                best_confidence = 0.2
                checked_pages = set()
                for candidate_page in candidate_pages:
                    key = candidate_page.rstrip("/").lower()
                    if key in checked_pages:
                        continue
                    checked_pages.add(key)
                    try:
                        page_html, page_resolved = _fetch_page(candidate_page)
                    except (URLError, TimeoutError, ValueError, OSError):
                        continue
                    resolved = page_resolved or candidate_page
                    page_email = _extract_email(page_html)
                    page_phone = _extract_phone(page_html)
                    page_conf = _contact_confidence(page_email, page_phone, resolved, page_html)

                    if page_conf > best_confidence or (page_email and page_phone and not (best_email and best_phone)):
                        best_email = page_email or best_email
                        best_phone = page_phone or best_phone
                        best_source = resolved
                        best_confidence = page_conf

                    if best_email and best_phone and page_conf >= 0.90:
                        break

                email = best_email
                phone = best_phone
                source_page = best_source
                chosen_confidence = best_confidence
            except (URLError, TimeoutError, ValueError, OSError):
                pass

        confidence = chosen_confidence if (email or phone) else 0.2
        if lead_id > 0:
            create_contact(
                lead_id=lead_id,
                email=email,
                phone=phone,
                source_page=source_page,
                confidence=confidence,
            )
            update_lead_status(lead_id, "enriched")
            created_contacts += 1

        has_email = bool(email)
        has_phone = bool(phone)
        if has_email:
            leads_with_email += 1
        if has_phone:
            leads_with_phone += 1
        if has_email and has_phone:
            leads_with_both += 1
        if not has_email and not has_phone:
            leads_with_neither += 1

        output_rows.append(
            {
                "LeadID": str(lead_id),
                "Title": cleaned_title,
                "OriginalTitle": title,
                "URL": url,
                "Snippet": snippet,
                "Query": query,
                "Domain": domain,
                "TargetType": target_type,
                "DiscoveryMode": discovery_mode,
                "ModeReason": mode_reason,
                "Email": email,
                "Phone": phone,
                "SourcePage": source_page,
                "Confidence": f"{confidence:.2f}",
            }
        )

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with OUTPUT_PATH.open("w", encoding="utf-8", newline="") as outfile:
        writer = csv.DictWriter(
            outfile,
            fieldnames=[
                "LeadID",
                "Title",
                "OriginalTitle",
                "URL",
                "Snippet",
                "Query",
                "Domain",
                "TargetType",
                "DiscoveryMode",
                "ModeReason",
                "Email",
                "Phone",
                "SourcePage",
                "Confidence",
            ],
        )
        writer.writeheader()
        writer.writerows(output_rows)

    summary_payload = {
        "generated_at": _utc_now(),
        "source_mode": source_mode,
        "total_leads": len(output_rows),
        "leads_with_email": leads_with_email,
        "leads_with_phone": leads_with_phone,
        "leads_with_both": leads_with_both,
        "leads_with_neither": leads_with_neither,
        "contact_coverage_rate": round(
            (len(output_rows) - leads_with_neither) / len(output_rows), 4
        )
        if output_rows
        else 0.0,
    }
    SUMMARY_OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with SUMMARY_OUTPUT_PATH.open("w", encoding="utf-8") as summary_file:
        json.dump(summary_payload, summary_file, indent=2)

    finished_at = _utc_now()
    record_run(
        worker="business_enrichment",
        args=[input_path or ""],
        status="ok",
        started_at=started_at,
        finished_at=finished_at,
    )
    return (
        "business enrichment worker completed: "
        f"wrote {len(output_rows)} rows to {OUTPUT_PATH}; "
        f"stored {created_contacts} contact rows in workspace/data/assistant.db "
        f"(source={source_mode}); "
        f"coverage total={len(output_rows)} email={leads_with_email} "
        f"phone={leads_with_phone} both={leads_with_both} neither={leads_with_neither}; "
        f"summary={SUMMARY_OUTPUT_PATH}"
    )
