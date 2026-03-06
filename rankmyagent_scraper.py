from __future__ import annotations

import csv
import re
from pathlib import Path
from urllib.parse import urljoin

from playwright.sync_api import sync_playwright

URL = "https://rankmyagent.com/city/victoria-bc-real-estate-agent-reviews-ratings"
BASE_URL = "https://rankmyagent.com"
SOURCE = "RankMyAgent"
OUTPUT_PATH = Path(__file__).resolve().parent / "workspace" / "leads" / "rankmyagent_victoria.csv"

SLUG_RE = re.compile(r"^/[a-z0-9][a-z0-9-]*$", re.IGNORECASE)
AGENT_PATH_RE = re.compile(r"^/agent/[a-z0-9-]+$", re.IGNORECASE)
BLOCKED_PATHS = {
    "/password-forget",
    "/register",
    "/rankmyagent",
    "/realestate",
    "/find-an-agent",
    "/rank-an-agent",
}


def clean_text(value: str) -> str:
    return " ".join((value or "").split()).strip()


def parse_brokerage(card_text: str, name: str) -> str:
    text = clean_text(card_text)
    if not text:
        return ""

    if text.lower().startswith(name.lower()):
        text = text[len(name):].strip()

    # Brokerage usually appears between the name and rating/details text.
    text = re.split(r"\bRated\b", text, maxsplit=1)[0].strip()
    text = re.split(r"\baward winner\b", text, flags=re.IGNORECASE, maxsplit=1)[0].strip()
    return text


def is_profile_href(href: str) -> bool:
    if href in BLOCKED_PATHS:
        return False
    return bool(SLUG_RE.match(href) or AGENT_PATH_RE.match(href))


def scrape_rankmyagent(url: str = URL, headless: bool = True, limit: int | None = None) -> list[dict[str, str]]:
    leads: list[dict[str, str]] = []
    seen_profile_urls: set[str] = set()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        page = browser.new_page()
        page.goto(url, wait_until="domcontentloaded", timeout=90000)
        page.wait_for_timeout(5000)

        links = page.locator("a[href]")
        for i in range(links.count()):
            a = links.nth(i)
            href = clean_text(a.get_attribute("href") or "")
            if not is_profile_href(href):
                continue

            name = clean_text(a.inner_text())
            if not name or " " not in name:
                continue

            profile_url = urljoin(BASE_URL, href)
            if profile_url in seen_profile_urls:
                continue

            card_text = a.evaluate(
                """
                el => {
                    const card = el.closest("article, li, div");
                    return card ? card.innerText : "";
                }
                """
            )
            brokerage = parse_brokerage(card_text, name)
            if not brokerage:
                continue

            seen_profile_urls.add(profile_url)
            leads.append(
                {
                    "Name": name,
                    "Brokerage": brokerage,
                    "ProfileURL": profile_url,
                    "Source": SOURCE,
                }
            )
            if limit is not None and len(leads) >= limit:
                break

        browser.close()

    return leads


def write_csv(rows: list[dict[str, str]], output_path: Path = OUTPUT_PATH) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["Name", "Brokerage", "ProfileURL", "Source"])
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    rows = scrape_rankmyagent()
    write_csv(rows, OUTPUT_PATH)
    print(f"Wrote {len(rows)} RankMyAgent leads to {OUTPUT_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
