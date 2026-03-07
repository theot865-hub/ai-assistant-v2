from __future__ import annotations

import csv
import re
from pathlib import Path
from urllib.error import URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen


BASE_DIR = Path(__file__).resolve().parent.parent
DEFAULT_INPUT_PATH = BASE_DIR / "workspace" / "leads" / "business_discovery.csv"
OUTPUT_PATH = BASE_DIR / "workspace" / "leads" / "business_discovery_enriched.csv"

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
        return tel.group(1).strip()
    match = PHONE_RE.search(text)
    if not match:
        return ""
    return " ".join(match.group(0).split())


def run(input_path: str | None = None) -> str:
    source_path = _resolve_input_path(input_path)
    if not source_path.exists():
        return f"business enrichment worker failed: input file not found at {source_path}"

    with source_path.open("r", encoding="utf-8", newline="") as infile:
        reader = csv.DictReader(infile)
        rows = list(reader)

    output_rows: list[dict[str, str]] = []
    for row in rows:
        title = (row.get("Title") or "").strip()
        url = (row.get("URL") or "").strip()
        snippet = (row.get("Snippet") or "").strip()
        query = (row.get("Query") or "").strip()
        domain = _derive_domain(url) if url else ""
        email = ""
        phone = ""
        source_page = url

        if url:
            try:
                html, resolved_url = _fetch_page(url)
                source_page = resolved_url or url
                email = _extract_email(html)
                phone = _extract_phone(html)
            except (URLError, TimeoutError, ValueError, OSError):
                # Keep enrichment fields blank on fetch/parse failures.
                pass

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
            ],
        )
        writer.writeheader()
        writer.writerows(output_rows)

    return f"business enrichment worker completed: wrote {len(output_rows)} rows to {OUTPUT_PATH}"

