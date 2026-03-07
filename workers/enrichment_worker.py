from __future__ import annotations

import csv
import re
from html import unescape
from pathlib import Path
from urllib.error import URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen


BASE_DIR = Path(__file__).resolve().parent.parent
DEFAULT_INPUT_PATH = BASE_DIR / "workspace" / "leads" / "all_realtor_leads.csv"
OUTPUT_PATH = BASE_DIR / "workspace" / "leads" / "all_realtor_leads_enriched.csv"

EMAIL_RE = re.compile(r"[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}", re.IGNORECASE)
PHONE_RE = re.compile(
    r"(?:\+?1[\s\-.]?)?(?:\(?\d{3}\)?[\s\-.]?)\d{3}[\s\-.]?\d{4}"
)


def _resolve_input_path(input_path: str | None) -> Path:
    if not input_path:
        return DEFAULT_INPUT_PATH
    candidate = Path(input_path)
    if candidate.is_absolute():
        return candidate
    return BASE_DIR / candidate


def _fetch_page(url: str) -> str:
    req = Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urlopen(req, timeout=8) as resp:
        charset = resp.headers.get_content_charset() or "utf-8"
        data = resp.read()
    return data.decode(charset, errors="ignore")


def _clean_text(text: str) -> str:
    return " ".join(unescape(text).replace("\n", " ").split())


def _extract_website(html: str, profile_url: str) -> str:
    profile_host = urlparse(profile_url).netloc.lower().replace("www.", "")
    for match in re.finditer(r'href=["\'](https?://[^"\']+)["\']', html, re.IGNORECASE):
        candidate = match.group(1).strip()
        if not candidate:
            continue
        host = urlparse(candidate).netloc.lower().replace("www.", "")
        if host and host != profile_host:
            return candidate
    return ""


def _extract_email(html: str) -> str:
    mailto = re.search(r'href=["\']mailto:([^"\'>\s]+)', html, re.IGNORECASE)
    if mailto:
        return mailto.group(1).strip()
    match = EMAIL_RE.search(html)
    return match.group(0).strip() if match else ""


def _extract_phone(html: str) -> str:
    tel = re.search(r'href=["\']tel:([^"\'>\s]+)', html, re.IGNORECASE)
    if tel:
        return tel.group(1).strip()
    match = PHONE_RE.search(html)
    if not match:
        return ""
    return _clean_text(match.group(0))


def _enrich_row(row: dict[str, str]) -> dict[str, str]:
    name = (row.get("Name") or "").strip()
    brokerage = (row.get("Brokerage") or "").strip()
    profile_url = (row.get("ProfileURL") or "").strip()
    source = (row.get("Source") or "").strip()

    website = ""
    email = ""
    phone = ""

    if profile_url:
        try:
            html = _fetch_page(profile_url)
            website = _extract_website(html, profile_url)
            email = _extract_email(html)
            phone = _extract_phone(html)
        except (URLError, TimeoutError, ValueError, OSError):
            # Keep fields blank if enrichment fetch/parsing fails.
            pass

    return {
        "Name": name,
        "Brokerage": brokerage,
        "ProfileURL": profile_url,
        "Source": source,
        "Website": website,
        "Email": email,
        "Phone": phone,
    }


def run(input_path: str | None = None) -> str:
    source_path = _resolve_input_path(input_path)
    if not source_path.exists():
        return f"enrichment worker failed: input file not found at {source_path}"

    with source_path.open("r", encoding="utf-8", newline="") as infile:
        reader = csv.DictReader(infile)
        rows = list(reader)

    enriched_rows = [_enrich_row(row) for row in rows]

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with OUTPUT_PATH.open("w", encoding="utf-8", newline="") as outfile:
        writer = csv.DictWriter(
            outfile,
            fieldnames=["Name", "Brokerage", "ProfileURL", "Source", "Website", "Email", "Phone"],
        )
        writer.writeheader()
        writer.writerows(enriched_rows)

    return f"enrichment worker completed: wrote {len(enriched_rows)} rows to {OUTPUT_PATH}"
