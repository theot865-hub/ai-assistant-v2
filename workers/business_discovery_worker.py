from __future__ import annotations

import csv
import json
import re
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from urllib.parse import urljoin, urlparse
from urllib.request import Request, urlopen

from db.repository import create_lead, record_run
from services import source_strategist
import tools


BASE_DIR = Path(__file__).resolve().parent.parent
OUTPUT_PATH = BASE_DIR / "workspace" / "leads" / "business_discovery.csv"
REJECTED_OUTPUT_PATH = BASE_DIR / "workspace" / "leads" / "business_discovery_rejected.csv"

QUALITY_THRESHOLD = 56.0
REALTOR_CONFIDENCE_THRESHOLD = 0.60
TARGET_VALIDATED_LEADS = 30
MAX_RESULTS_PER_QUERY = 10
MIN_QUERY_VARIANTS = 8
MAX_QUERY_VARIANTS = 24
MAX_RAW_CANDIDATES = 300
MAX_CHILD_LINKS_PER_DIRECTORY = 10
MAX_DIRECTORY_PAGES_TO_FOLLOW = 8
MAX_EVALUATION_CANDIDATES = 320
MAX_ENTITY_HTML_FETCHES = 85
MAX_PERSON_LEADS_PER_ROOT_DOMAIN = 3
MAX_TEAM_LEADS_PER_ROOT_DOMAIN = 2
MAX_PARENT_PAGES_TO_EXPAND = 20
MAX_CHILD_LINKS_PER_PARENT = 20

LOCATION_CLUSTER_MAP = {
    "victoria bc": [
        "Victoria BC",
        "Sidney BC",
        "Langford BC",
        "Saanich BC",
        "Oak Bay BC",
        "Esquimalt BC",
        "Colwood BC",
        "View Royal BC",
        "North Saanich BC",
        "Central Saanich BC",
        "Sooke BC",
    ]
}

BROADER_REGION_TERMS = {
    "victoria bc": [
        "greater victoria",
        "capital regional district",
        "crd",
        "vancouver island",
        "south island",
    ]
}

BLOCKED_DOMAINS = {
    "tiktok.com",
    "twitter.com",
    "x.com",
    "youtube.com",
    "instagram.com",
    "linkedin.com",
    "facebook.com",
    "target.com",
    "uhaul.com",
    "pinterest.com",
    "reddit.com",
    "yelp.com",
    "yellowpages.ca",
    "411.ca",
    "bing.com",
    "google.com",
    "yahoo.com",
    "duckduckgo.com",
}

MIRROR_DOMAINS = {
    "provenexpert.com",
    "ec21.com",
    "straight.com",
    "rate-my-agent.com",
    "agentpronto.com",
}

SPAM_TERMS = {
    "email list",
    "mailing list",
    "lead list",
    "buy leads",
    "data broker",
    "people finder",
    "people search",
    "skip tracing",
    "bulk emails",
}

DIRECTORY_TERMS = {
    "directory",
    "find a",
    "search",
    "list of",
    "top ",
    "best ",
    "near me",
}

AGGREGATOR_TERMS = {
    "zillow",
    "realtor.ca",
    "trulia",
    "homes.com",
    "rankmyagent",
    "rate my agent",
    "realtylink",
}

PROFILE_TERMS = {
    "agent",
    "realtor",
    "broker",
    "profile",
    "team",
    "contact",
    "about",
    "our agents",
}

PARENT_CHILD_REJECT_TOKENS = {
    "join",
    "career",
    "careers",
    "jobs",
    "blog",
    "listing",
    "listings",
    "market",
    "report",
    "reports",
    "news",
    "property",
    "properties",
    "for-sale",
    "for_sale",
    "mls",
    "join-our-team",
    "membership",
    "help",
    "neighbourhood",
    "privacy",
    "terms",
    "login",
    "register",
    "sitemap",
    "feed",
}

BROAD_CATEGORY_PATTERNS = [
    re.compile(r"\bagents\s+in\b", re.IGNORECASE),
    re.compile(r"\breal\s+estate\s+agents\b", re.IGNORECASE),
    re.compile(r"\brealtors?\s+in\b", re.IGNORECASE),
    re.compile(r"\bhomes\s+for\s+sale\b", re.IGNORECASE),
    re.compile(r"\blistings?\b", re.IGNORECASE),
    re.compile(r"\bbrokerages?\b", re.IGNORECASE),
    re.compile(r"\bfind\s+a\s+realtor\b", re.IGNORECASE),
    re.compile(r"\btop\s+real\s+estate\s+agents\b", re.IGNORECASE),
    re.compile(r"\bbest\s+real\s+estate\s+agents\b", re.IGNORECASE),
]

GENERIC_NAME_PATTERNS = [
    re.compile(r"^find\s+(a\s+)?", re.IGNORECASE),
    re.compile(r"^(top|best)\s+", re.IGNORECASE),
    re.compile(r"(directory|listing|search results)", re.IGNORECASE),
    re.compile(r"email list", re.IGNORECASE),
    re.compile(r"join our team", re.IGNORECASE),
    re.compile(r"for sale", re.IGNORECASE),
    re.compile(r"^\d+\s+\d+\s+[A-Za-z].*", re.IGNORECASE),
]

BUSINESS_TERMS = {
    "team",
    "group",
    "associates",
    "realty",
    "real",
    "estate",
    "brokerage",
    "properties",
    "homes",
    "collective",
    "realtors",
}

PERSON_STOPWORDS = {
    "log",
    "in",
    "signup",
    "sign",
    "up",
    "market",
    "updates",
    "home",
    "worth",
    "skip",
    "to",
    "content",
    "why",
    "use",
    "your",
    "lifetime",
    "membership",
    "advantage",
    "contact",
    "listings",
    "projects",
    "follow",
    "me",
    "guide",
    "review",
    "highlights",
    "top",
    "best",
    "application",
    "form",
    "apartment",
    "apartments",
    "manor",
}

GENERIC_BUSINESS_NAME_TERMS = {
    "real estate",
    "realtors in",
    "agents in",
    "homes for sale",
    "market updates",
    "projects",
}

LISTING_TITLE_TERMS = {
    "townhome",
    "condo",
    "condominium",
    "bedroom",
    "bedrooms",
    "bath",
    "baths",
    "sqft",
    "square feet",
    "for sale",
    "mls",
    "retreat",
    "open house",
}

NON_PERSON_TOKENS = {
    "reviews",
    "review",
    "serviced",
    "services",
    "service",
    "areas",
    "recent",
    "project",
    "projects",
    "developments",
    "development",
    "removal",
    "international",
    "coast",
    "capital",
    "realty",
    "realtors",
    "realtor",
    "estate",
    "homes",
}

REALTOR_POSITIVE_TERMS = [
    "realtor",
    "realtors",
    "real estate agent",
    "real estate broker",
    "licensed realtor",
    "realtor profile",
    "agent profile",
    "broker",
]

REALTOR_NEGATIVE_TERMS = [
    "appraiser",
    "appraisal",
    "development centre",
    "development center",
    "neighborhood",
    "community page",
    "news",
    "blog",
    "article",
    "travel",
    "tourism",
    "directory",
    "explore",
    "open house",
    "project developments",
]


@dataclass
class CandidateInput:
    raw_title: str
    url: str
    snippet: str
    query: str
    parent_source_url: str = ""
    parent_entity_type: str = ""


@dataclass
class CandidateResult:
    raw_title: str
    normalized_name: str
    url: str
    domain: str
    snippet: str
    query: str
    source_type: str
    entity_type: str
    extraction_source: str
    parent_source_url: str
    parent_entity_type: str
    child_profile_extracted: int
    root_domain: str
    source_preference_rank: int
    diversity_decision: str
    matched_location: str
    location_match_type: str
    quality_score: float
    realtor_confidence: float
    target_type: str
    discovery_mode: str
    mode_reason: str
    strategist_decision: str
    strategist_reason: str
    role_validation_reason: str
    validation_reason: str
    location_match: bool
    audience_match: bool


@dataclass
class DirectoryStats:
    pages_followed: int = 0
    child_links_extracted: int = 0


@dataclass
class ParentExpansionStats:
    parent_pages_expanded: int = 0
    child_profile_links_followed: int = 0


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _clean_space(value: str) -> str:
    return " ".join((value or "").strip().split())


def _derive_domain(url: str) -> str:
    host = urlparse(url).netloc.lower().strip()
    if host.startswith("www."):
        host = host[4:]
    return host


def _root_domain(domain: str) -> str:
    value = (domain or "").strip().lower()
    if not value:
        return ""
    parts = [p for p in value.split(".") if p]
    if len(parts) <= 2:
        return value
    if parts[-2] in {"co", "com", "org", "net"} and len(parts) >= 3:
        return ".".join(parts[-3:])
    return ".".join(parts[-2:])


def _strip_html_tags(text: str) -> str:
    cleaned = re.sub(r"<[^>]+>", " ", text or "")
    return _clean_space(unescape(cleaned))


def _fetch_html(url: str) -> tuple[str, str]:
    if not (url or "").strip():
        return "", ""
    try:
        req = Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urlopen(req, timeout=8) as resp:
            charset = resp.headers.get_content_charset() or "utf-8"
            html = resp.read().decode(charset, errors="ignore")
            resolved = resp.geturl() or url
            return html, resolved
    except Exception:
        return "", ""


def _split_request(query: str) -> tuple[str, str, str]:
    normalized = _clean_space(query)
    if not normalized:
        return "businesses", "", ""

    main_part = normalized.split(";", 1)[0].strip()
    notes = ""
    if ";" in normalized:
        notes = normalized.split(";", 1)[1].strip()

    audience = ""
    location = ""

    match = re.search(r"find\s+(.+?)\s+in\s+([A-Za-z0-9 ,.-]+)$", main_part, re.IGNORECASE)
    if match:
        audience = _clean_space(match.group(1))
        location = _clean_space(match.group(2))
    else:
        match = re.search(r"(.+?)\s+in\s+([A-Za-z0-9 ,.-]+)$", main_part, re.IGNORECASE)
        if match:
            audience = _clean_space(match.group(1))
            location = _clean_space(match.group(2))

    if not audience:
        audience = main_part
    audience = re.sub(
        r"\b(find|individual|their|email|emails|phone|numbers?|businesses?)\b",
        " ",
        audience,
        flags=re.IGNORECASE,
    )
    audience = _clean_space(audience) or "businesses"

    if not location:
        location = "Victoria BC" if "victoria" in normalized.lower() else ""

    return audience, location, notes


def _audience_variants(audience: str) -> list[str]:
    lower = audience.lower()
    if "realtor" in lower or "real estate" in lower or "broker" in lower:
        return [
            "realtors",
            "real estate agents",
            "real estate brokers",
            "brokerage agents",
        ]
    singular = lower[:-1].strip() if lower.endswith("s") else lower
    return [lower, singular, f"{singular} company".strip()]


def _build_query_variants(user_query: str) -> list[str]:
    audience, location, notes = _split_request(user_query)
    location_options = _location_variants(location) or [location or ""]
    audience_options = _audience_variants(audience)
    note_tokens = notes.lower()
    contact_focus = "contact" in note_tokens or "email" in note_tokens or "phone" in note_tokens

    primary_templates = [
        "{location} {audience}",
        "{location} {audience} profiles",
        "{location} {audience} contact",
        "{location} {audience} bio",
        "{location} {audience} agents",
        "{location} {audience} realtors",
    ]
    secondary_templates = [
        "{location} {audience} team",
        "{location} {audience} brokerage",
        "{location} {audience} about",
        "{location} {audience} phone email",
        "{location} local {audience}",
        "{audience} in {location}",
        "{audience} profile in {location}",
        "{audience} agent in {location}",
        "{audience} contact in {location}",
    ]
    if contact_focus:
        secondary_templates.extend(
            [
                "{location} {audience} email",
                "{location} {audience} phone",
                "{location} {audience} contact information",
            ]
        )

    queries: list[str] = []
    for location_part in location_options:
        for audience_value in audience_options[:2]:
            for template in primary_templates[:3]:
                q = _clean_space(template.format(location=location_part, audience=audience_value))
                if q and q not in queries:
                    queries.append(q)
                if len(queries) >= MAX_QUERY_VARIANTS:
                    break
            if len(queries) >= MAX_QUERY_VARIANTS:
                break
        if len(queries) >= MAX_QUERY_VARIANTS:
            break

    for location_part in location_options:
        for audience_value in audience_options:
            for template in primary_templates + secondary_templates:
                q = _clean_space(template.format(location=location_part, audience=audience_value))
                if q and q not in queries:
                    queries.append(q)
                if len(queries) >= MAX_QUERY_VARIANTS:
                    break
            if len(queries) >= MAX_QUERY_VARIANTS:
                break
        if len(queries) >= MAX_QUERY_VARIANTS:
            break

    if len(queries) < MIN_QUERY_VARIANTS:
        base = _clean_space(user_query)
        while len(queries) < MIN_QUERY_VARIANTS:
            fallback = f"{base} profile {len(queries) + 1}".strip()
            if fallback not in queries:
                queries.append(fallback)

    return queries[:MAX_QUERY_VARIANTS]


def _contains_any(text: str, patterns: set[str]) -> bool:
    lowered = text.lower()
    return any(pattern in lowered for pattern in patterns)


def _audience_tokens(audience: str) -> list[str]:
    raw = re.split(r"[^a-z0-9]+", audience.lower())
    tokens = [token for token in raw if len(token) > 2]
    if "realtor" in audience.lower() or "real estate" in audience.lower():
        tokens.extend(["realtor", "agent", "broker", "realty", "real estate"])
    return list(dict.fromkeys(tokens))


def _is_realtor_audience(audience: str) -> bool:
    lowered = (audience or "").lower()
    return any(token in lowered for token in ["realtor", "real estate", "broker", "brokerage"])


def _has_realtor_signal(text: str) -> bool:
    lowered = (text or "").lower()
    return any(
        token in lowered
        for token in [
            "realtor",
            "real estate",
            "broker",
            "brokerage",
            "mls",
            "listing",
            "home buyer",
            "home seller",
        ]
    )


def _realtor_role_validation(
    *,
    title: str,
    snippet: str,
    url: str,
    html: str,
    source_type: str,
    entity_type: str,
) -> tuple[float, str]:
    text = f"{title} {snippet} {url} {_strip_html_tags(html[:3000])}".lower()
    path = urlparse(url).path.lower()

    confidence = 0.0
    reasons: list[str] = []

    positive_hits = [term for term in REALTOR_POSITIVE_TERMS if term in text]
    if positive_hits:
        confidence += min(0.55, 0.25 + (0.12 * min(3, len(positive_hits))))
        reasons.append(f"positive_terms={','.join(positive_hits[:3])}")

    if any(token in path for token in ["/agent", "/realtor", "/profile", "/bio"]):
        confidence += 0.20
        reasons.append("profile_path_signal")

    if source_type == "individual_profile":
        confidence += 0.20
        reasons.append("individual_profile_source")
    elif source_type in {"brokerage_site", "company_profile"}:
        confidence += 0.18
        reasons.append("brokerage_or_company_source")

    if entity_type in {"person", "team_or_brokerage"}:
        confidence += 0.12
        reasons.append(f"entity_type={entity_type}")

    if any(token in text for token in ["realty", "brokerage", "royal lepage", "re/max", "exp realty", "macdonald"]):
        confidence += 0.12
        reasons.append("brokerage_affiliation_signal")

    negative_hits = [term for term in REALTOR_NEGATIVE_TERMS if term in text]
    if negative_hits:
        confidence -= min(0.55, 0.18 + (0.08 * min(4, len(negative_hits))))
        reasons.append(f"negative_terms={','.join(negative_hits[:3])}")

    if source_type in {"directory_list", "aggregator"}:
        confidence -= 0.22
        reasons.append("directory_or_aggregator_penalty")

    confidence = max(0.0, min(1.0, confidence))
    if not reasons:
        reasons.append("no_role_evidence")
    return confidence, "; ".join(reasons)


def _location_tokens(location: str) -> list[str]:
    raw = re.split(r"[^a-z0-9]+", location.lower())
    tokens = [token for token in raw if len(token) > 1]
    return list(dict.fromkeys(tokens))


def _normalize_location_key(location: str) -> str:
    return _clean_space(location).lower()


def _cluster_locations(location: str) -> list[str]:
    key = _normalize_location_key(location)
    if key in LOCATION_CLUSTER_MAP:
        return LOCATION_CLUSTER_MAP[key]
    value = _clean_space(location)
    return [value] if value else []


def _location_variants(location: str) -> list[str]:
    cluster = _cluster_locations(location)
    if not cluster:
        return []
    seen: set[str] = set()
    out: list[str] = []
    for item in cluster:
        cleaned = _clean_space(item)
        if cleaned and cleaned.lower() not in seen:
            seen.add(cleaned.lower())
            out.append(cleaned)
    return out


def _location_match_profile(text: str, requested_location: str) -> tuple[str, str]:
    combined = _clean_space(text).lower()
    req_key = _normalize_location_key(requested_location)
    cluster = _location_variants(requested_location)
    broader_terms = BROADER_REGION_TERMS.get(req_key, [])

    if not combined:
        return "outside_region", ""

    exact = _clean_space(requested_location)
    exact_aliases = [exact.lower(), exact.lower().replace(" bc", "")]
    for alias in exact_aliases:
        if alias and alias in combined:
            return "exact", exact

    for area in cluster:
        if area.lower() == exact.lower():
            continue
        aliases = [area.lower(), area.lower().replace(" bc", "")]
        for alias in aliases:
            if alias and alias in combined:
                return "nearby_cluster", area

    for term in broader_terms:
        if term in combined:
            return "broader_region", term

    return "outside_region", ""


def _classify_source(title: str, snippet: str, url: str, audience: str, location: str) -> tuple[str, str]:
    domain = _derive_domain(url)
    path = urlparse(url).path.lower()
    combined = f"{title} {snippet} {url}".lower()

    if not url or not domain:
        return "irrelevant", "missing URL or domain"

    if domain in BLOCKED_DOMAINS or any(domain.endswith(f".{d}") for d in BLOCKED_DOMAINS):
        return "spam_or_junk", "blocked domain"

    if _contains_any(combined, SPAM_TERMS):
        return "spam_or_junk", "email-list/data-broker pattern"

    if any(brand in combined for brand in ["tiktok", "target", "u-haul", "uhaul", "walmart"]):
        return "irrelevant", "unrelated brand"

    if _contains_any(combined, DIRECTORY_TERMS):
        return "directory_list", "directory/listing page pattern"

    if _contains_any(combined, AGGREGATOR_TERMS):
        return "aggregator", "aggregator pattern"

    if any(token in path for token in ["/agent", "/realtor", "/profile", "/team", "/our-agents", "/about"]):
        return "individual_profile", "profile-like URL path"

    if any(word in combined for word in ["brokerage", "realty", "real estate", "our agents"]):
        return "brokerage_site", "brokerage/business context"

    audience_tokens = _audience_tokens(audience)
    if audience_tokens and any(token in combined for token in audience_tokens):
        return "company_profile", "audience keyword match"

    location_tokens = _location_tokens(location)
    if location_tokens and not any(token in combined for token in location_tokens):
        return "irrelevant", "location mismatch"

    return "company_profile", "generic business profile"


def _score_result(
    *,
    source_type: str,
    title: str,
    snippet: str,
    url: str,
    audience: str,
    location: str,
    duplicate_count: int,
) -> tuple[float, bool, str, str, str]:
    combined = f"{title} {snippet} {url}".lower()
    path_lower = urlparse(url).path.lower()
    audience_tokens = _audience_tokens(audience)
    location_match_type, matched_location = _location_match_profile(combined, location)

    score = 0.0
    audience_match = bool(audience_tokens) and any(token in combined for token in audience_tokens)
    location_match = location_match_type in {"exact", "nearby_cluster", "broader_region"}

    if source_type in {"individual_profile", "company_profile", "brokerage_site"}:
        score += 30
    if source_type == "individual_profile":
        score += 12
    if source_type == "brokerage_site":
        score += 8

    if audience_match:
        score += 22
    else:
        score -= 10

    if location_match_type == "exact":
        score += 28
    elif location_match_type == "nearby_cluster":
        score += 20
    elif location_match_type == "broader_region":
        score += 8
    else:
        score -= 28

    if any(term in combined for term in PROFILE_TERMS):
        score += 10
    if any(token in path_lower for token in ["/agent", "/realtor", "/profile", "/bio"]):
        score += 10
    if re.search(r"\b[A-Z][a-z]+\s+[A-Z][a-z]+\b", title):
        score += 8

    domain = _derive_domain(url)
    if domain.endswith(".ca"):
        score += 6

    if source_type in {"directory_list", "aggregator"}:
        score -= 18
    if any(token in combined for token in ["team", "brokerage", "our agents"]) and source_type != "individual_profile":
        score -= 5
    if source_type in {"spam_or_junk", "irrelevant"}:
        score -= 40

    if duplicate_count > 2:
        score -= (duplicate_count - 2) * 3

    if re.search(r"[^\x00-\x7F]", title) and not location_match:
        score -= 8

    score = max(0.0, min(100.0, score))
    reason = (
        "audience_match="
        f"{audience_match}, location_match_type={location_match_type}, source_type={source_type}"
    )
    return score, audience_match, location_match_type, matched_location, reason


def _is_broad_category_phrase(text: str) -> bool:
    value = _clean_space(text)
    if not value:
        return True
    lowered = value.lower()
    if lowered in {
        "victoria real estate",
        "real estate agents in victoria bc",
        "realtors in victoria bc",
        "find a realtor",
    }:
        return True
    return any(pattern.search(value) for pattern in BROAD_CATEGORY_PATTERNS)


def _is_noise_phrase(text: str) -> bool:
    value = _clean_space(text)
    lowered = value.lower()
    if not value:
        return True
    if len(value) < 3:
        return True
    if lowered in {"log in", "sign up", "skip to content", "contact us", "home"}:
        return True
    if any(phrase in lowered for phrase in ["our story", "get help with", "benefits of", "join our team"]):
        return True
    words = [re.sub(r"[^a-z]", "", token) for token in lowered.split()]
    words = [w for w in words if w]
    if words and all(word in PERSON_STOPWORDS for word in words):
        return True
    if any(term in lowered for term in ["cookie", "privacy policy", "terms of service"]):
        return True
    return False


def _has_noisy_identity_shape(text: str) -> bool:
    value = _clean_space(text)
    if not value:
        return True
    if len(re.findall(r"[^A-Za-z0-9\s&'\-]", value)) >= 4:
        return True
    if re.search(r"[a-z][A-Z]{2,}", value):
        return True
    if re.search(r"[A-Za-z]{18,}", value):
        return True
    lowered = value.lower()
    if lowered.count("real estate") > 1 or lowered.count("realtor") > 1:
        return True
    if sum(token in lowered for token in ["victoria", "bc", "realtor", "real estate", "agents"]) >= 4:
        return True
    return False


def _normalize_person_name(text: str) -> str:
    value = _clean_space(text)
    if not value:
        return ""
    value = re.sub(
        r"\b(victoria|bc|british columbia|local|top|best)\b",
        " ",
        value,
        flags=re.IGNORECASE,
    )
    value = re.sub(
        r"\b(realtor[s]?|real estate agent[s]?|agent[s]?|broker[s]?)\b",
        " ",
        value,
        flags=re.IGNORECASE,
    )
    value = _clean_space(value)
    tokens = [re.sub(r"[^A-Za-z'\-]", "", t) for t in value.split()]
    tokens = [t for t in tokens if t and t.lower() not in PERSON_STOPWORDS]
    if len(tokens) < 2:
        return ""
    if len(tokens) > 4:
        tokens = tokens[:4]
    rebuilt = " ".join(token.capitalize() for token in tokens)
    if not _looks_person_name(rebuilt):
        return ""
    return rebuilt


def _source_preference_rank(
    *,
    source_type: str,
    domain: str,
    parent_source_url: str,
    extraction_source: str,
) -> int:
    root = _root_domain(domain)
    if root in MIRROR_DOMAINS:
        return 4
    if source_type == "aggregator":
        return 6
    if source_type == "directory_list":
        return 5
    if source_type == "individual_profile" and not parent_source_url:
        return 1
    if source_type in {"company_profile", "brokerage_site"} and not parent_source_url:
        return 2
    if source_type == "individual_profile":
        return 3
    if extraction_source in {"visible_person_pattern", "visible_team_business_pattern"}:
        return 3
    return 4


def _looks_person_name(text: str) -> bool:
    name = _clean_space(text)
    if not name or _is_noise_phrase(name):
        return False
    if any(ch.isdigit() for ch in name):
        return False
    lowered_name = name.lower()
    if any(term in lowered_name for term in LISTING_TITLE_TERMS):
        return False
    words = [w for w in re.split(r"\s+", name) if w]
    if len(words) < 2 or len(words) > 4:
        return False
    generic_hits = 0
    title_case_words = 0
    long_word_count = 0
    for word in words:
        clean_word = re.sub(r"[^A-Za-z'\-]", "", word)
        if not clean_word or len(clean_word) < 2:
            return False
        if clean_word.lower() in PERSON_STOPWORDS:
            return False
        if clean_word.lower() in NON_PERSON_TOKENS:
            return False
        if clean_word.lower() in BUSINESS_TERMS:
            generic_hits += 1
        if clean_word[0].isalpha() and not clean_word[0].isupper():
            return False
        if clean_word.isupper() and len(clean_word) > 2:
            return False
        if re.fullmatch(r"[A-Z][a-z'\-]+", clean_word):
            title_case_words += 1
        if len(clean_word) >= 3:
            long_word_count += 1
    if generic_hits > 0:
        return False
    return title_case_words >= 2 and long_word_count >= 2


def _looks_team_or_brokerage(text: str) -> bool:
    value = _clean_space(text)
    lowered = value.lower()
    if not value or _is_broad_category_phrase(value) or _is_noise_phrase(value):
        return False
    if any(term in lowered for term in GENERIC_BUSINESS_NAME_TERMS):
        if "&" not in value and "team" not in lowered and "group" not in lowered:
            return False
    if any(token in lowered for token in ["log in", "sign up", "market updates", "home worth"]):
        return False
    has_business_token = any(
        token in lowered
        for token in [
            " team",
            " group",
            "associates",
            "realty",
            "brokerage",
            "properties",
            "collective",
            "& associates",
        ]
    )
    if not has_business_token:
        return False
    words = [re.sub(r"[^A-Za-z]", "", w) for w in value.split()]
    distinctive = [
        w
        for w in words
        if w
        and w.lower() not in BUSINESS_TERMS
        and w.lower() not in {"victoria", "bc", "british", "columbia", "canada", "real", "estate"}
    ]
    return len(distinctive) >= 1


def _extract_header_candidates(html: str) -> list[str]:
    values: list[str] = []

    title_match = re.search(r"<title[^>]*>(.*?)</title>", html, re.IGNORECASE | re.DOTALL)
    if title_match:
        values.append(_strip_html_tags(title_match.group(1)))

    og_matches = re.findall(
        r"<meta[^>]+property=[\"']og:title[\"'][^>]+content=[\"'](.*?)[\"']",
        html,
        re.IGNORECASE | re.DOTALL,
    )
    for match in og_matches:
        values.append(_strip_html_tags(match))

    h_matches = re.findall(r"<h[12][^>]*>(.*?)</h[12]>", html, re.IGNORECASE | re.DOTALL)
    for match in h_matches[:6]:
        values.append(_strip_html_tags(match))

    ld_names = re.findall(r'"name"\s*:\s*"([^"]{3,120})"', html, re.IGNORECASE)
    for match in ld_names[:6]:
        values.append(_clean_space(match))

    uniq: list[str] = []
    seen: set[str] = set()
    for value in values:
        if _is_noise_phrase(value):
            continue
        key = value.lower()
        if value and key not in seen:
            seen.add(key)
            uniq.append(value)
    return uniq


def _clean_title_for_fallback(raw_title: str, domain: str) -> str:
    text = _clean_space(raw_title)
    if not text:
        text = domain.split(".")[0].replace("-", " ").replace("_", " ").strip()

    text = re.sub(r"\s+at\s+[A-Za-z0-9.-]+\.[A-Za-z]{2,}.*$", "", text, flags=re.IGNORECASE)
    text = re.split(r"\s+[\-|–|—|\|:]\s+", text)[0].strip()
    text = re.sub(r"\([^)]*\)", "", text).strip()
    text = _clean_space(text)

    if any(pattern.search(text) for pattern in GENERIC_NAME_PATTERNS):
        return ""
    if _is_noise_phrase(text):
        return ""
    if len(text) < 3 or len(text.split()) > 8 or sum(ch.isdigit() for ch in text) >= 3:
        return ""
    if _is_broad_category_phrase(text):
        return ""
    return text


def _title_segments(raw_title: str) -> list[str]:
    parts = re.split(r"[|:–—]", raw_title or "")
    out: list[str] = []
    for part in parts:
        value = _clean_space(part)
        if value:
            out.append(value)
    return out


def _strip_role_tokens(value: str) -> str:
    cleaned = _clean_space(value)
    cleaned = re.sub(r"^(contact|about)\\s+", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\\brealtor[s]?\\b", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\\breal\\s+estate\\s+agent\\b", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\\bvictoria\\b", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\\bbc\\b", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\\s+", " ", cleaned).strip(" -|:,")
    return _clean_space(cleaned)


def _extract_entity_name(raw_title: str, domain: str, html: str) -> tuple[str, str, str]:
    headers = _extract_header_candidates(html)

    for value in headers:
        if _looks_person_name(value):
            return value, "person", "visible_person_pattern"

    for value in headers:
        if _looks_team_or_brokerage(value):
            return value, "team_or_brokerage", "visible_team_business_pattern"

    for segment in _title_segments(raw_title):
        stripped = _strip_role_tokens(segment)
        if _looks_person_name(stripped):
            return stripped, "person", "cleaned_title_segment"

    for segment in _title_segments(raw_title):
        stripped = _strip_role_tokens(segment)
        if _looks_team_or_brokerage(stripped):
            return stripped, "team_or_brokerage", "cleaned_title_segment"

    if headers:
        header_value = headers[0]
        if not _is_broad_category_phrase(header_value):
            if _looks_person_name(header_value):
                return header_value, "person", "hero_header"
            if _looks_team_or_brokerage(header_value):
                return header_value, "team_or_brokerage", "hero_header"

    for value in headers:
        if not _is_broad_category_phrase(value) and len(value) >= 3:
            return value, "unknown", "structured_metadata"

    fallback = _clean_title_for_fallback(raw_title, domain)
    if fallback:
        if _looks_person_name(fallback):
            return fallback, "person", "cleaned_title"
        if _looks_team_or_brokerage(fallback):
            return fallback, "team_or_brokerage", "cleaned_title"
        if _is_broad_category_phrase(fallback):
            return "", "category_page", "cleaned_title"
        return fallback, "unknown", "cleaned_title"

    return "", "unknown", "none"


def _normalize_entity_name(name: str, entity_type: str) -> str:
    value = _clean_space(name)
    if not value:
        return ""
    if entity_type == "person":
        return _normalize_person_name(value)
    return value


def _extract_child_links(parent_url: str, html: str, audience: str, location: str) -> list[CandidateInput]:
    out: list[CandidateInput] = []
    seen: set[str] = set()
    location_tokens = _location_tokens(location)
    audience_tokens = _audience_tokens(audience)

    link_matches = re.findall(r"<a[^>]+href=[\"']([^\"']+)[\"'][^>]*>(.*?)</a>", html, re.IGNORECASE | re.DOTALL)
    for href, text in link_matches:
        href_clean = _clean_space(href)
        if not href_clean:
            continue
        if href_clean.startswith("javascript:") or href_clean.startswith("mailto:") or href_clean.startswith("tel:"):
            continue
        full_url = urljoin(parent_url, href_clean)
        parsed = urlparse(full_url)
        if parsed.scheme not in {"http", "https"}:
            continue
        path_lower = parsed.path.lower()
        if any(token in path_lower for token in ["/search", "/category", "/tag", "/feed", "/privacy", "/terms"]):
            continue

        anchor_text = _strip_html_tags(text)
        combined = f"{anchor_text} {full_url}".lower()
        profile_signal = any(
            token in combined
            for token in ["agent", "realtor", "team", "broker", "profile", "about", "contact", "associates"]
        )
        if not profile_signal:
            continue

        if location_tokens and not any(token in combined for token in location_tokens):
            if audience_tokens and not any(token in combined for token in audience_tokens):
                continue

        if full_url in seen:
            continue
        seen.add(full_url)

        out.append(
            CandidateInput(
                raw_title=anchor_text,
                url=full_url,
                snippet="",
                query="",
                parent_source_url=parent_url,
                parent_entity_type="category_page",
            )
        )
        if len(out) >= MAX_CHILD_LINKS_PER_DIRECTORY:
            break

    return out


def _extract_profile_child_links_from_parent(
    *,
    parent_url: str,
    parent_entity_type: str,
    html: str,
) -> list[CandidateInput]:
    out: list[CandidateInput] = []
    seen: set[str] = set()
    parent_root = _root_domain(_derive_domain(parent_url))

    link_matches = re.findall(
        r"<a[^>]+href=[\"']([^\"']+)[\"'][^>]*>(.*?)</a>",
        html,
        re.IGNORECASE | re.DOTALL,
    )
    for href, text in link_matches:
        href_clean = _clean_space(href)
        if not href_clean:
            continue
        if href_clean.startswith("javascript:") or href_clean.startswith("mailto:") or href_clean.startswith("tel:"):
            continue

        full_url = urljoin(parent_url, href_clean)
        parsed = urlparse(full_url)
        if parsed.scheme not in {"http", "https"}:
            continue
        path_lower = parsed.path.lower()

        if any(token in path_lower for token in ["/#", "#", "/?"]):
            continue
        if any(token in path_lower for token in PARENT_CHILD_REJECT_TOKENS):
            continue

        child_root = _root_domain(_derive_domain(full_url))
        if parent_root and child_root and parent_root != child_root:
            continue

        anchor_text = _strip_html_tags(text)
        if _is_noise_phrase(anchor_text):
            continue

        combined = f"{anchor_text} {path_lower}".lower()
        positive_signal = any(
            token in combined
            for token in [
                "agent",
                "realtor",
                "bio",
                "profile",
                "associate",
                "team-member",
                "team_member",
                "our-agents",
            ]
        )
        if not positive_signal and not _looks_person_name(anchor_text):
            continue

        if full_url in seen:
            continue
        seen.add(full_url)

        out.append(
            CandidateInput(
                raw_title=anchor_text,
                url=full_url,
                snippet="",
                query="",
                parent_source_url=parent_url,
                parent_entity_type=parent_entity_type,
            )
        )
        if len(out) >= MAX_CHILD_LINKS_PER_PARENT:
            break

    return out


def _expand_parent_entities(
    validated_parents: list[CandidateResult],
) -> tuple[list[CandidateInput], ParentExpansionStats]:
    stats = ParentExpansionStats()
    expanded_children: list[CandidateInput] = []

    parent_candidates = [
        item
        for item in validated_parents
        if (
            item.entity_type == "team_or_brokerage"
            or item.source_type in {"brokerage_site", "company_profile", "directory_list"}
        )
        and item.strategist_decision in {"accept_for_expansion", "accept_direct"}
    ]
    parent_candidates = sorted(parent_candidates, key=lambda x: (-x.quality_score, x.source_preference_rank))

    for parent in parent_candidates[:MAX_PARENT_PAGES_TO_EXPAND]:
        html, resolved_url = _fetch_html(parent.url)
        if not html:
            continue
        parent_url = resolved_url or parent.url
        stats.parent_pages_expanded += 1

        children = _extract_profile_child_links_from_parent(
            parent_url=parent_url,
            parent_entity_type=parent.entity_type,
            html=html,
        )
        stats.child_profile_links_followed += len(children)
        for child in children:
            expanded_children.append(
                CandidateInput(
                    raw_title=child.raw_title,
                    url=child.url,
                    snippet=child.snippet,
                    query=parent.query,
                    parent_source_url=parent_url,
                    parent_entity_type=parent.entity_type,
                )
            )

    return expanded_children, stats


def _fetch_candidates(query_variants: list[str]) -> list[CandidateInput]:
    candidates: list[CandidateInput] = []
    seen_urls: set[str] = set()
    for variant in query_variants:
        try:
            raw_results = tools.search_web(variant, MAX_RESULTS_PER_QUERY)
            parsed_results = json.loads(raw_results)
            if not isinstance(parsed_results, list):
                continue
            for item in parsed_results:
                if not isinstance(item, dict):
                    continue
                url = _clean_space(str(item.get("url") or ""))
                if not url:
                    continue
                normalized_url = url.rstrip("/").lower()
                if normalized_url in seen_urls:
                    continue
                seen_urls.add(normalized_url)
                candidates.append(
                    CandidateInput(
                        raw_title=_clean_space(str(item.get("title") or "")),
                        url=url,
                        snippet=_clean_space(str(item.get("snippet") or "")),
                        query=variant,
                    )
                )
                if len(candidates) >= MAX_RAW_CANDIDATES:
                    return candidates
        except Exception:
            continue
    return candidates


def _expand_directory_candidates(
    user_query: str,
    raw_candidates: list[CandidateInput],
) -> tuple[list[CandidateInput], DirectoryStats]:
    audience, location, _ = _split_request(user_query)
    stats = DirectoryStats()
    expanded: list[CandidateInput] = []

    for candidate in raw_candidates:
        source_type, _ = _classify_source(candidate.raw_title, candidate.snippet, candidate.url, audience, location)
        expanded.append(candidate)

        if source_type != "directory_list":
            continue
        if stats.pages_followed >= MAX_DIRECTORY_PAGES_TO_FOLLOW:
            continue

        html, resolved_url = _fetch_html(candidate.url)
        if not html:
            continue
        stats.pages_followed += 1
        parent_url = resolved_url or candidate.url
        children = _extract_child_links(parent_url, html, audience, location)
        stats.child_links_extracted += len(children)
        for child in children:
            expanded.append(
                CandidateInput(
                    raw_title=child.raw_title,
                    url=child.url,
                    snippet=child.snippet,
                    query=candidate.query,
                    parent_source_url=parent_url,
                )
            )
            if len(expanded) >= MAX_EVALUATION_CANDIDATES:
                break
        if len(expanded) >= MAX_EVALUATION_CANDIDATES:
            break

    return expanded, stats


def _evaluate_candidates(
    user_query: str,
    expanded_candidates: list[CandidateInput],
    *,
    apply_diversity: bool = True,
    quality_threshold: float = QUALITY_THRESHOLD,
    enforce_role_validation: bool = True,
    request_profile: dict[str, str] | None = None,
) -> tuple[list[CandidateResult], list[CandidateResult], int]:
    audience, location, _ = _split_request(user_query)
    profile = request_profile or source_strategist.classify_request(user_query)
    strategist_target_type = profile.get("target_type", "generic_local_business")
    discovery_mode = profile.get("discovery_mode", "company_mode")
    mode_reason = profile.get("mode_reason", "")
    strategist_location = profile.get("location", "") or location
    domain_counts = Counter(_derive_domain(candidate.url) for candidate in expanded_candidates)

    kept_after_source: list[CandidateResult] = []
    rejected: list[CandidateResult] = []
    source_kept_count = 0
    seen_keys: set[tuple[str, str]] = set()

    html_fetches = 0
    for candidate in expanded_candidates[:MAX_EVALUATION_CANDIDATES]:
        raw_title = candidate.raw_title
        url = candidate.url
        snippet = candidate.snippet
        query = candidate.query
        parent_source_url = candidate.parent_source_url
        parent_entity_type = candidate.parent_entity_type
        domain = _derive_domain(url)

        source_type, source_reason = _classify_source(raw_title, snippet, url, audience, location)
        combined_text = f"{raw_title} {snippet} {url}"
        realtor_audience = _is_realtor_audience(audience)
        realtor_signal = _has_realtor_signal(combined_text)
        strategist = source_strategist.score_candidate_page(
            url=url,
            title=raw_title,
            snippet=snippet,
            target_type=strategist_target_type,
            location=strategist_location,
            discovery_mode=discovery_mode,
        )
        strategist_decision = str(strategist.get("decision") or "reject")
        strategist_reason = str(strategist.get("reason") or "strategist_no_reason")
        score, audience_match, location_match_type, matched_location, score_reason = _score_result(
            source_type=source_type,
            title=raw_title,
            snippet=snippet,
            url=url,
            audience=audience,
            location=location,
            duplicate_count=domain_counts.get(domain, 0),
        )

        if strategist_decision == "reject":
            realtor_confidence, role_validation_reason = _realtor_role_validation(
                title=raw_title,
                snippet=snippet,
                url=url,
                html="",
                source_type=source_type,
                entity_type="unknown",
            )
            rejected.append(
                CandidateResult(
                    raw_title=raw_title,
                    normalized_name="",
                    url=url,
                    domain=domain,
                    snippet=snippet,
                    query=query,
                    source_type=source_type,
                    entity_type="unknown",
                    extraction_source="none",
                    parent_source_url=parent_source_url,
                    parent_entity_type=parent_entity_type,
                    child_profile_extracted=(1 if parent_source_url else 0),
                    root_domain=_root_domain(domain),
                    source_preference_rank=_source_preference_rank(
                        source_type=source_type,
                        domain=domain,
                        parent_source_url=parent_source_url,
                        extraction_source="none",
                    ),
                    diversity_decision="",
                    matched_location=matched_location or _clean_space(location),
                    location_match_type=location_match_type,
                    quality_score=score,
                    realtor_confidence=realtor_confidence,
                    target_type=strategist_target_type,
                    discovery_mode=discovery_mode,
                    mode_reason=mode_reason,
                    strategist_decision=strategist_decision,
                    strategist_reason=strategist_reason,
                    role_validation_reason=role_validation_reason,
                    validation_reason=f"strategist_reject: {strategist_reason}",
                    location_match=(location_match_type != "outside_region"),
                    audience_match=audience_match,
                )
            )
            continue

        html = ""
        extraction_source = "cleaned_title"
        should_fetch_entity_html = (
            candidate.parent_source_url != ""
            or (
                source_type in {"individual_profile", "company_profile", "brokerage_site"}
                and score >= (quality_threshold - 8)
            )
        )
        if should_fetch_entity_html and html_fetches < MAX_ENTITY_HTML_FETCHES:
            html, resolved = _fetch_html(url)
            if resolved:
                url = resolved
                domain = _derive_domain(url)
            html_fetches += 1

        normalized_name, entity_type, extraction_source = _extract_entity_name(raw_title, domain, html)
        normalized_name = _normalize_entity_name(normalized_name, entity_type)
        profile_path_signal = False
        if source_type == "directory_list" and not parent_source_url:
            entity_type = "category_page"
            extraction_source = "directory_page"
        else:
            parsed_url = urlparse(url)
            path_lower = (parsed_url.path or "/").lower()
            profile_path_signal = any(
                token in path_lower
                for token in ["/agent", "/realtor", "/profile", "/bio", "/our-agents", "/team/"]
            )
            rootish_path = path_lower in {"", "/"} or path_lower.count("/") <= 2
            if (
                entity_type == "person"
                and source_type in {"brokerage_site", "company_profile"}
                and not parent_source_url
                and rootish_path
                and not profile_path_signal
            ):
                entity_type = "team_or_brokerage"
                extraction_source = "root_company_page_coercion"

        source_rank = _source_preference_rank(
            source_type=source_type,
            domain=domain,
            parent_source_url=parent_source_url,
            extraction_source=extraction_source,
        )
        root_domain = _root_domain(domain)
        realtor_confidence, role_validation_reason = _realtor_role_validation(
            title=raw_title,
            snippet=snippet,
            url=url,
            html=html,
            source_type=source_type,
            entity_type=entity_type,
        )

        validation_reason = source_reason
        if source_type not in {"spam_or_junk", "irrelevant"} and score >= quality_threshold:
            source_kept_count += 1

        if source_type in {"spam_or_junk", "irrelevant"}:
            validation_reason = f"rejected source: {source_reason}"
        elif strategist_decision == "reject":
            validation_reason = f"strategist_reject: {strategist_reason}"
        elif source_type == "directory_list" and not parent_source_url:
            validation_reason = "directory/category page expanded via child links"
        elif location_match_type == "outside_region":
            validation_reason = "outside location cluster"
        elif score < quality_threshold:
            validation_reason = f"low quality score ({score:.1f})"
        elif not normalized_name:
            validation_reason = "rejected generic/noisy lead name"
        elif _has_noisy_identity_shape(normalized_name):
            validation_reason = "rejected noisy/machine-assembled identity"
        elif entity_type in {"category_page", "unknown"}:
            validation_reason = f"entity type not specific enough ({entity_type})"
        elif _is_broad_category_phrase(normalized_name):
            validation_reason = "rejected broad category phrase"
        elif source_rank >= 4 and score < 88:
            validation_reason = "rejected low-trust mirror/directory source"
        elif root_domain in {"provenexpert.com", "ec21.com"} and score < 95:
            validation_reason = "rejected mirror domain unless very strong"
        elif not audience_match:
            validation_reason = "audience mismatch"
        elif realtor_audience and not realtor_signal:
            validation_reason = "missing explicit realtor/real-estate signal"
        elif (
            enforce_role_validation
            and realtor_audience
            and realtor_confidence < REALTOR_CONFIDENCE_THRESHOLD
        ):
            validation_reason = (
                "role_mismatch: realtor confidence below threshold "
                f"({realtor_confidence:.2f} < {REALTOR_CONFIDENCE_THRESHOLD:.2f})"
            )
        else:
            key = (normalized_name.lower(), domain)
            if key in seen_keys:
                validation_reason = "duplicate name/domain"
            else:
                seen_keys.add(key)
                validation_reason = f"accepted ({score_reason})"

        result = CandidateResult(
            raw_title=raw_title,
            normalized_name=normalized_name,
            url=url,
            domain=domain,
            snippet=snippet,
            query=query,
            source_type=source_type,
            entity_type=entity_type,
            extraction_source=extraction_source,
            parent_source_url=parent_source_url,
            parent_entity_type=parent_entity_type,
            child_profile_extracted=(1 if parent_source_url else 0),
            root_domain=root_domain,
            source_preference_rank=source_rank,
            diversity_decision="",
            matched_location=matched_location or _clean_space(location),
            location_match_type=location_match_type,
            quality_score=score,
            realtor_confidence=realtor_confidence,
            target_type=strategist_target_type,
            discovery_mode=discovery_mode,
            mode_reason=mode_reason,
            strategist_decision=strategist_decision,
            strategist_reason=strategist_reason,
            role_validation_reason=role_validation_reason,
            validation_reason=validation_reason,
            location_match=(location_match_type != "outside_region"),
            audience_match=audience_match,
        )

        if validation_reason.startswith("accepted"):
            kept_after_source.append(result)
        else:
            rejected.append(result)

    prelim_validated = [item for item in kept_after_source if item.quality_score >= quality_threshold]
    if not apply_diversity:
        return prelim_validated, rejected, source_kept_count
    validated, diversity_rejected = _apply_domain_diversity(prelim_validated)
    rejected.extend(diversity_rejected)
    return validated, rejected, source_kept_count


def _apply_domain_diversity(validated: list[CandidateResult]) -> tuple[list[CandidateResult], list[CandidateResult]]:
    kept: list[CandidateResult] = []
    rejected: list[CandidateResult] = []
    person_counts: Counter[str] = Counter()
    team_counts: Counter[str] = Counter()

    for item in sorted(validated, key=lambda x: (-x.quality_score, x.source_preference_rank, x.normalized_name.lower())):
        root = item.root_domain or _root_domain(item.domain)
        if item.entity_type == "person":
            if person_counts[root] >= MAX_PERSON_LEADS_PER_ROOT_DOMAIN:
                item.diversity_decision = "rejected:person_domain_cap"
                item.validation_reason = "rejected by domain diversity cap (person)"
                rejected.append(item)
                continue
            person_counts[root] += 1
            item.diversity_decision = "kept:person_domain_cap_ok"
            kept.append(item)
            continue

        if item.entity_type == "team_or_brokerage":
            if team_counts[root] >= MAX_TEAM_LEADS_PER_ROOT_DOMAIN:
                item.diversity_decision = "rejected:team_domain_cap"
                item.validation_reason = "rejected by domain diversity cap (team_or_brokerage)"
                rejected.append(item)
                continue
            team_counts[root] += 1
            item.diversity_decision = "kept:team_domain_cap_ok"
            kept.append(item)
            continue

        item.diversity_decision = "rejected:unknown_entity_type"
        item.validation_reason = "rejected by diversity pass: unknown entity type"
        rejected.append(item)

    return kept, rejected


def _apply_strategist_gate(
    candidates: list[CandidateResult],
    request_profile: dict[str, str],
) -> tuple[list[CandidateResult], list[CandidateResult]]:
    discovery_mode = request_profile.get("discovery_mode", "company_mode")
    kept: list[CandidateResult] = []
    rejected: list[CandidateResult] = []
    for item in candidates:
        if item.strategist_decision == "reject":
            item.validation_reason = f"strategist_reject: {item.strategist_reason}"
            rejected.append(item)
            continue
        if (
            item.strategist_decision == "accept_for_expansion"
            and not item.parent_source_url
            and item.entity_type != "person"
        ):
            if discovery_mode == "person_mode":
                item.validation_reason = "person_mode expansion_only: parent/list page not saved directly"
                rejected.append(item)
                continue
            # Company mode allows business pages directly if they passed quality.
            kept.append(item)
            continue
        kept.append(item)
    return kept, rejected


def _apply_realtor_role_gate(
    request_profile: dict[str, str],
    candidates: list[CandidateResult],
) -> tuple[list[CandidateResult], list[CandidateResult]]:
    target_type = request_profile.get("target_type", "")
    discovery_mode = request_profile.get("discovery_mode", "company_mode")
    if target_type != "realtor":
        return candidates, []

    threshold = REALTOR_CONFIDENCE_THRESHOLD
    if discovery_mode == "person_mode":
        threshold = min(0.85, REALTOR_CONFIDENCE_THRESHOLD + 0.08)
    elif discovery_mode == "company_mode":
        threshold = max(0.45, REALTOR_CONFIDENCE_THRESHOLD - 0.08)

    kept: list[CandidateResult] = []
    rejected: list[CandidateResult] = []
    for item in candidates:
        if item.realtor_confidence >= threshold:
            kept.append(item)
            continue
        item.validation_reason = (
            "role_mismatch: realtor confidence below threshold "
            f"({item.realtor_confidence:.2f} < {threshold:.2f})"
        )
        rejected.append(item)
    return kept, rejected


def _apply_discovery_mode_gate(
    request_profile: dict[str, str],
    candidates: list[CandidateResult],
) -> tuple[list[CandidateResult], list[CandidateResult]]:
    discovery_mode = request_profile.get("discovery_mode", "company_mode")
    kept: list[CandidateResult] = []
    rejected: list[CandidateResult] = []

    for item in candidates:
        if discovery_mode == "person_mode":
            if item.entity_type != "person":
                item.validation_reason = "person_mode requires individual person leads"
                rejected.append(item)
                continue
            if item.source_type in {"directory_list", "aggregator"} and not item.parent_source_url:
                item.validation_reason = "person_mode rejects direct directory/aggregator entries"
                rejected.append(item)
                continue
            if item.quality_score < (QUALITY_THRESHOLD + 2):
                item.validation_reason = "person_mode stricter quality threshold"
                rejected.append(item)
                continue
            kept.append(item)
            continue

        # Company mode: allow person + business entities and favor volume.
        if item.entity_type in {"person", "team_or_brokerage"}:
            kept.append(item)
            continue
        item.validation_reason = "company_mode rejected non-person/non-business entity"
        rejected.append(item)

    return kept, rejected


def _prefer_children_over_parents(
    parent_validated: list[CandidateResult],
    child_validated: list[CandidateResult],
) -> tuple[list[CandidateResult], list[CandidateResult]]:
    demoted: list[CandidateResult] = []
    if not child_validated:
        return parent_validated, demoted

    parent_with_person_child = {
        item.parent_source_url.rstrip("/")
        for item in child_validated
        if item.entity_type == "person" and item.parent_source_url and item.quality_score >= (QUALITY_THRESHOLD + 2)
    }
    person_child_root_domains = {
        _root_domain(_derive_domain(item.url))
        for item in child_validated
        if item.entity_type == "person" and item.quality_score >= (QUALITY_THRESHOLD + 2)
    }
    if not parent_with_person_child and not person_child_root_domains:
        return parent_validated, demoted

    kept_parents: list[CandidateResult] = []
    for parent in parent_validated:
        parent_key = parent.url.rstrip("/")
        parent_root = _root_domain(_derive_domain(parent.url))
        if parent_key in parent_with_person_child:
            parent.validation_reason = "deprioritized: replaced by extracted child person profiles from same parent page"
            parent.diversity_decision = "rejected:parent_replaced_by_children"
            demoted.append(parent)
            continue
        if parent.entity_type == "team_or_brokerage" and parent_root in person_child_root_domains:
            parent.validation_reason = "deprioritized: same-domain person profiles available"
            parent.diversity_decision = "rejected:parent_replaced_by_children"
            demoted.append(parent)
            continue
        kept_parents.append(parent)
    return kept_parents, demoted


def _write_outputs(validated: list[CandidateResult], rejected: list[CandidateResult], request_query: str) -> int:
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    saved_count = 0

    with OUTPUT_PATH.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "LeadID",
                "RawTitle",
                "NormalizedName",
                "Title",
                "URL",
                "Domain",
                "Snippet",
                "Query",
                "SourceType",
                "TargetType",
                "DiscoveryMode",
                "ModeReason",
                "EntityType",
                "ExtractionSource",
                "ParentSourceURL",
                "ParentEntityType",
                "ChildProfileExtracted",
                "RootDomain",
                "SourcePreferenceRank",
                "DiversityDecision",
                "MatchedLocation",
                "LocationMatchType",
                "QualityScore",
                "StrategistDecision",
                "StrategistReason",
                "RealtorConfidence",
                "RoleValidationReason",
                "ValidationReason",
            ],
        )
        writer.writeheader()
        for item in validated:
            lead = create_lead(
                name=item.normalized_name,
                raw_title=item.raw_title,
                normalized_name=item.normalized_name,
                domain=item.domain,
                website=item.url,
                source="search_web",
                query=request_query,
                source_type=item.source_type,
                entity_type=item.entity_type,
                extraction_source=item.extraction_source,
                parent_source_url=item.parent_source_url,
                parent_entity_type=item.parent_entity_type,
                child_profile_extracted=item.child_profile_extracted,
                quality_score=item.quality_score,
                realtor_confidence=item.realtor_confidence,
                role_validation_reason=item.role_validation_reason,
                validation_reason=item.validation_reason,
                status="discovered",
            )
            saved_count += 1
            writer.writerow(
                {
                    "LeadID": str(lead.id),
                    "RawTitle": item.raw_title,
                    "NormalizedName": item.normalized_name,
                    "Title": item.normalized_name,
                    "URL": item.url,
                    "Domain": item.domain,
                    "Snippet": item.snippet,
                    "Query": item.query,
                    "SourceType": item.source_type,
                    "TargetType": item.target_type,
                    "DiscoveryMode": item.discovery_mode,
                    "ModeReason": item.mode_reason,
                    "EntityType": item.entity_type,
                    "ExtractionSource": item.extraction_source,
                    "ParentSourceURL": item.parent_source_url,
                    "ParentEntityType": item.parent_entity_type,
                    "ChildProfileExtracted": str(item.child_profile_extracted),
                    "RootDomain": item.root_domain,
                    "SourcePreferenceRank": str(item.source_preference_rank),
                    "DiversityDecision": item.diversity_decision,
                    "MatchedLocation": item.matched_location,
                    "LocationMatchType": item.location_match_type,
                    "QualityScore": f"{item.quality_score:.1f}",
                    "StrategistDecision": item.strategist_decision,
                    "StrategistReason": item.strategist_reason,
                    "RealtorConfidence": f"{item.realtor_confidence:.2f}",
                    "RoleValidationReason": item.role_validation_reason,
                    "ValidationReason": item.validation_reason,
                }
            )

    with REJECTED_OUTPUT_PATH.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "RawTitle",
                "URL",
                "Domain",
                "Snippet",
                "Query",
                "SourceType",
                "TargetType",
                "DiscoveryMode",
                "ModeReason",
                "EntityType",
                "ExtractionSource",
                "ParentSourceURL",
                "ParentEntityType",
                "ChildProfileExtracted",
                "RootDomain",
                "SourcePreferenceRank",
                "DiversityDecision",
                "MatchedLocation",
                "LocationMatchType",
                "QualityScore",
                "StrategistDecision",
                "StrategistReason",
                "RealtorConfidence",
                "RoleValidationReason",
                "ValidationReason",
            ],
        )
        writer.writeheader()
        for item in rejected:
            writer.writerow(
                {
                    "RawTitle": item.raw_title,
                    "URL": item.url,
                    "Domain": item.domain,
                    "Snippet": item.snippet,
                    "Query": item.query,
                    "SourceType": item.source_type,
                    "TargetType": item.target_type,
                    "DiscoveryMode": item.discovery_mode,
                    "ModeReason": item.mode_reason,
                    "EntityType": item.entity_type,
                    "ExtractionSource": item.extraction_source,
                    "ParentSourceURL": item.parent_source_url,
                    "ParentEntityType": item.parent_entity_type,
                    "ChildProfileExtracted": str(item.child_profile_extracted),
                    "RootDomain": item.root_domain,
                    "SourcePreferenceRank": str(item.source_preference_rank),
                    "DiversityDecision": item.diversity_decision,
                    "MatchedLocation": item.matched_location,
                    "LocationMatchType": item.location_match_type,
                    "QualityScore": f"{item.quality_score:.1f}",
                    "StrategistDecision": item.strategist_decision,
                    "StrategistReason": item.strategist_reason,
                    "RealtorConfidence": f"{item.realtor_confidence:.2f}",
                    "RoleValidationReason": item.role_validation_reason,
                    "ValidationReason": item.validation_reason,
                }
            )

    return saved_count


def run(query: str | None = None) -> str:
    started_at = _utc_now()
    normalized_query = _clean_space(query or "")
    if not normalized_query:
        finished_at = _utc_now()
        record_run(
            worker="business_discovery",
            args=[],
            status="error",
            started_at=started_at,
            finished_at=finished_at,
        )
        return (
            'business discovery worker error: missing query. '
            'Usage: python run_worker.py business_discovery "your query"'
        )

    query_variants = _build_query_variants(normalized_query)
    request_profile = source_strategist.classify_request(normalized_query)
    discovery_mode = request_profile.get("discovery_mode", "company_mode")
    if discovery_mode == "person_mode":
        parent_quality_threshold = QUALITY_THRESHOLD + 4.0
        child_quality_threshold = QUALITY_THRESHOLD + 1.0
    else:
        parent_quality_threshold = max(QUALITY_THRESHOLD - 5.0, 46.0)
        child_quality_threshold = max(QUALITY_THRESHOLD - 7.0, 44.0)
    raw_candidates = _fetch_candidates(query_variants)
    expanded_candidates, dir_stats = _expand_directory_candidates(normalized_query, raw_candidates)

    parent_validated, parent_rejected, kept_after_source = _evaluate_candidates(
        normalized_query,
        expanded_candidates,
        apply_diversity=False,
        quality_threshold=parent_quality_threshold,
        enforce_role_validation=False,
        request_profile=request_profile,
    )
    child_candidates, parent_stats = _expand_parent_entities(parent_validated)
    child_validated: list[CandidateResult] = []
    child_rejected: list[CandidateResult] = []
    if child_candidates:
        child_validated, child_rejected, _ = _evaluate_candidates(
            normalized_query,
            child_candidates,
            apply_diversity=False,
            quality_threshold=child_quality_threshold,
            enforce_role_validation=False,
            request_profile=request_profile,
        )

    parent_kept, parent_demoted = _prefer_children_over_parents(parent_validated, child_validated)
    pre_diversity = parent_kept + child_validated
    strategist_validated, strategist_rejected = _apply_strategist_gate(pre_diversity, request_profile)
    mode_validated, mode_rejected = _apply_discovery_mode_gate(request_profile, strategist_validated)
    role_validated, role_rejected = _apply_realtor_role_gate(request_profile, mode_validated)
    validated, diversity_rejected = _apply_domain_diversity(role_validated)
    rejected = (
        parent_rejected
        + child_rejected
        + parent_demoted
        + strategist_rejected
        + mode_rejected
        + role_rejected
        + diversity_rejected
    )

    saved_count = _write_outputs(validated, rejected, normalized_query)
    next_action = source_strategist.decide_next_action(
        current_stats={"raw_candidates": len(raw_candidates)},
        accepted_count=saved_count,
        rejected_count=len(rejected),
    )

    finished_at = _utc_now()
    status = "ok" if saved_count > 0 else "error"
    record_run(
        worker="business_discovery",
        args=[normalized_query],
        status=status,
        started_at=started_at,
        finished_at=finished_at,
    )

    return (
        "business discovery worker completed: "
        f"queries={len(query_variants)}, raw_candidates={len(raw_candidates)}, "
        f"directory_pages_followed={dir_stats.pages_followed}, "
        f"child_profile_links_extracted={dir_stats.child_links_extracted}, "
        f"parent_pages_expanded={parent_stats.parent_pages_expanded}, "
        f"parent_child_profile_links_followed={parent_stats.child_profile_links_followed}, "
        f"kept_after_source={kept_after_source}, validated_saved={saved_count}; "
        f"target_type={request_profile.get('target_type','')}, "
        f"discovery_mode={discovery_mode}, "
        f"next_action={next_action.get('action','')}; "
        f"target_validated={TARGET_VALIDATED_LEADS}; "
        f"output={OUTPUT_PATH}; rejected={REJECTED_OUTPUT_PATH}"
    )
