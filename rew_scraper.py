from __future__ import annotations

import csv
import re
from pathlib import Path
from urllib.parse import urljoin

from playwright.sync_api import sync_playwright

URL = "https://www.rew.ca/agents/areas/victoria-bc"
BASE_URL = "https://www.rew.ca"
SOURCE = "REW"
OUTPUT_PATH = Path(__file__).resolve().parent / "workspace" / "leads" / "rew_victoria.csv"
PROFILE_RE = re.compile(r"^/agents/\d+/[a-z0-9-]+$", re.IGNORECASE)


def clean_text(value: str) -> str:
    return " ".join((value or "").split()).strip()


def scrape_rew(url: str = URL, headless: bool = True, limit: int | None = None) -> list[dict[str, str]]:
    leads: list[dict[str, str]] = []
    seen_profile_urls: set[str] = set()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        page = browser.new_page()
        page.goto(url, wait_until="domcontentloaded", timeout=90000)
        page.wait_for_timeout(5000)

        cards = page.locator("div.agenttile")
        for i in range(cards.count()):
            card = cards.nth(i)
            href = clean_text(card.locator("a.agenttile-link").first.get_attribute("href") or "")
            if not PROFILE_RE.match(href):
                continue

            profile_url = urljoin(BASE_URL, href)
            if profile_url in seen_profile_urls:
                continue

            name = clean_text(card.locator(".agenttile-title").first.inner_text())
            brokerage = clean_text(card.locator(".agenttile-subtitle").first.inner_text())
            if not name:
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
    rows = scrape_rew()
    write_csv(rows, OUTPUT_PATH)
    print(f"Wrote {len(rows)} REW leads to {OUTPUT_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
