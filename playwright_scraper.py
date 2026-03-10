from __future__ import annotations

import re
from pathlib import Path

from playwright.sync_api import sync_playwright


URL = "https://rankmyagent.com/city/victoria-bc-real-estate-agent-reviews-ratings"
BASE_URL = "https://rankmyagent.com"
OUTPUT_PATH = Path(__file__).resolve().parent / "workspace" / "leads" / "rankmyagent_victoria.txt"


def clean(values: list[str]) -> list[str]:
    seen = set()
    out: list[str] = []
    for value in values:
        item = " ".join(value.split()).strip()
        if not item:
            continue
        lowered = item.lower()
        if lowered in seen:
            continue
        seen.add(lowered)
        out.append(item)
    return out


def parse_brokerage(card_text: str, name: str) -> str:
    text = " ".join(card_text.split()).strip()
    if not text:
        return ""
    if text.lower().startswith(name.lower()):
        text = text[len(name):].strip()
    text = re.split(r"\bRated\b", text, maxsplit=1)[0].strip()
    return text


def scrape_leads(url: str = URL, headless: bool = True, limit: int = 50) -> list[tuple[str, str, str]]:
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        page = browser.new_page()
        page.goto(url, wait_until="domcontentloaded")
        page.wait_for_timeout(4000)

        # Agent profiles on this page are one-segment slugs like /aprilspackman.
        slug_pattern = re.compile(r"^/[a-z0-9][a-z0-9-]*$")
        blocked_slugs = {
            "/password-forget",
            "/register",
            "/rankmyagent",
            "/realestate",
        }

        leads: list[tuple[str, str, str]] = []
        seen_profiles: set[str] = set()
        links = page.locator("a[href]")
        for i in range(links.count()):
            a = links.nth(i)
            href = (a.get_attribute("href") or "").strip()
            if not slug_pattern.match(href) or href in blocked_slugs:
                continue
            name = " ".join(a.inner_text().split()).strip()
            if not name or " " not in name:
                continue
            profile_url = f"{BASE_URL}{href}"
            if profile_url in seen_profiles:
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
            seen_profiles.add(profile_url)
            leads.append((name, brokerage, profile_url))
        browser.close()

    return leads[:limit]


def write_leads(leads: list[tuple[str, str, str]], output_path: Path = OUTPUT_PATH) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    lines = [f"{name} | {brokerage} | {url}" for name, brokerage, url in leads]
    output_path.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")


if __name__ == "__main__":
    leads = scrape_leads()
    write_leads(leads)
    print(f"Wrote {len(leads)} leads to {OUTPUT_PATH}")
