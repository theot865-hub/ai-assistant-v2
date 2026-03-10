from __future__ import annotations

import re
from urllib.parse import urlparse


AGGREGATOR_DOMAINS = {
    "realtor.ca",
    "realtor.com",
    "zillow.com",
    "trulia.com",
    "homes.com",
    "rate-my-agent.com",
    "rankmyagent.com",
    "yellowpages.ca",
}

LOW_TRUST_DOMAINS = {
    "tiktok.com",
    "youtube.com",
    "x.com",
    "twitter.com",
    "instagram.com",
    "facebook.com",
    "linkedin.com",
}

GENERIC_REJECT_TERMS = {
    "article",
    "blog",
    "news",
    "travel",
    "tourism",
    "directory",
    "community",
    "project",
}

ROLE_REJECT_TERMS = {
    "realtor": {"appraiser", "appraisal", "development centre", "development center"},
    "dentist": {"veterinary", "pet clinic"},
    "roofer": {"roof rack", "solar panel only"},
}

TARGET_TERMS = {
    "realtor": {"realtor", "real estate", "broker", "agent", "mls", "listing"},
    "dentist": {"dentist", "dental", "orthodont", "clinic", "hygienist"},
    "roofer": {"roofer", "roofing", "roof", "shingle", "gutter", "contractor"},
    "generic_local_business": {"service", "company", "business", "local"},
}

SOURCE_CLASS_PATH_RULES = {
    "profile_page": ["/agent", "/realtor", "/profile", "/bio", "/staff"],
    "roster_page": ["/team", "/agents", "/our-agents", "/roster", "/directory"],
    "directory_list": ["/search", "/find", "/listing", "/listings", "/category"],
}

SOURCE_CLASS_PREFERENCE = {
    "realtor": ["company_site", "roster_page", "directory_list", "profile_page", "aggregator"],
    "dentist": ["company_site", "profile_page", "directory_list", "aggregator", "roster_page"],
    "roofer": ["company_site", "directory_list", "profile_page", "aggregator", "roster_page"],
    "generic_local_business": ["company_site", "directory_list", "profile_page", "aggregator", "roster_page"],
}

PERSON_FOCUS_TERMS = {
    "individual",
    "individuals",
    "person",
    "people",
    "agents",
    "agent",
    "advisor",
    "advisors",
    "emails of agents",
    "email of agents",
}

COMPANY_FOCUS_TERMS = {
    "roofing",
    "roofer",
    "roofers",
    "dentist",
    "dentists",
    "painter",
    "painters",
    "contractor",
    "contractors",
    "landscaping",
    "landscaper",
    "business",
    "company",
    "companies",
}


def _clean(value: str) -> str:
    return " ".join((value or "").strip().split())


def _extract_location(goal_text: str) -> str:
    text = _clean(goal_text)
    if not text:
        return ""
    match = re.search(r"\bin\s+([A-Za-z0-9 ,.-]+)$", text, re.IGNORECASE)
    if match:
        return _clean(match.group(1))
    return ""


def classify_request(goal_text: str) -> dict[str, str]:
    lowered = _clean(goal_text).lower()
    location = _extract_location(goal_text)

    target_type = "generic_local_business"
    if any(token in lowered for token in ["realtor", "real estate", "brokerage", "broker"]):
        target_type = "realtor"
    elif any(token in lowered for token in ["dentist", "dental", "orthodont"]):
        target_type = "dentist"
    elif any(token in lowered for token in ["roofer", "roofing", "roofers", "roof"]):
        target_type = "roofer"

    entity_goal = "companies"
    if any(token in lowered for token in ["individual", "person", "people", "agent", "realtor"]):
        entity_goal = "individual_people"
    elif any(token in lowered for token in ["team", "brokerage", "group"]):
        entity_goal = "teams"

    discovery_mode = "company_mode"
    mode_reason = "default_company_mode_for_local_business_discovery"
    has_person_focus = any(term in lowered for term in PERSON_FOCUS_TERMS)
    has_company_focus = any(term in lowered for term in COMPANY_FOCUS_TERMS)
    has_realtor_context = any(token in lowered for token in ["realtor", "real estate", "broker", "agent", "advisor"])

    if has_realtor_context and has_person_focus:
        discovery_mode = "person_mode"
        mode_reason = "realtor_or_agent_request_with_individual_people_focus"
    elif has_company_focus:
        discovery_mode = "company_mode"
        mode_reason = "service_business_or_company_request"
    elif entity_goal == "individual_people" and has_realtor_context:
        discovery_mode = "person_mode"
        mode_reason = "entity_goal_individual_people_in_realtor_context"

    return {
        "target_type": target_type,
        "entity_goal": entity_goal,
        "location": location,
        "discovery_mode": discovery_mode,
        "mode_reason": mode_reason,
    }


def choose_source_classes(target_type: str, discovery_mode: str = "company_mode") -> list[str]:
    normalized = (target_type or "").strip().lower()
    mode = (discovery_mode or "").strip().lower() or "company_mode"
    if mode == "person_mode":
        if normalized == "realtor":
            return ["profile_page", "roster_page", "company_site", "directory_list", "aggregator"]
        return ["profile_page", "roster_page", "company_site", "directory_list", "aggregator"]
    return SOURCE_CLASS_PREFERENCE.get(normalized, SOURCE_CLASS_PREFERENCE["generic_local_business"])


def _classify_source_class(url: str, title: str, snippet: str) -> str:
    domain = urlparse(url).netloc.lower().lstrip("www.")
    path = urlparse(url).path.lower()
    combined = f"{title} {snippet} {path}".lower()

    if domain in AGGREGATOR_DOMAINS:
        return "aggregator"
    for source_class, patterns in SOURCE_CLASS_PATH_RULES.items():
        if any(pattern in path for pattern in patterns):
            return source_class
    if any(term in combined for term in ["directory", "find ", "top ", "best ", "list of"]):
        return "directory_list"
    return "company_site"


def score_candidate_page(
    url: str,
    title: str,
    snippet: str,
    target_type: str,
    location: str,
    discovery_mode: str = "company_mode",
) -> dict[str, str | float]:
    domain = urlparse(url).netloc.lower().lstrip("www.")
    combined = f"{title} {snippet} {url}".lower()
    source_class = _classify_source_class(url, title, snippet)
    mode = (discovery_mode or "").strip().lower() or "company_mode"
    preferred = choose_source_classes(target_type, mode)
    pref_index = preferred.index(source_class) if source_class in preferred else len(preferred)

    trust_score = max(0.1, 1.0 - (pref_index * 0.14))
    relevance_score = 0.0
    reasons: list[str] = []

    if domain in LOW_TRUST_DOMAINS:
        trust_score -= 0.45
        reasons.append("low_trust_domain")

    target_terms = TARGET_TERMS.get(target_type, TARGET_TERMS["generic_local_business"])
    hits = [term for term in target_terms if term in combined]
    if hits:
        relevance_score += min(0.65, 0.20 + 0.10 * len(hits))
        reasons.append(f"target_terms={','.join(hits[:3])}")

    if location and location.lower().replace(" bc", "") in combined:
        relevance_score += 0.20
        reasons.append("location_match")

    generic_negative_hits = [term for term in GENERIC_REJECT_TERMS if term in combined]
    if generic_negative_hits:
        relevance_score -= min(0.45, 0.10 * len(generic_negative_hits))
        reasons.append(f"generic_negative={','.join(generic_negative_hits[:2])}")

    role_negative_hits = [term for term in ROLE_REJECT_TERMS.get(target_type, set()) if term in combined]
    if role_negative_hits:
        relevance_score -= 0.40
        reasons.append(f"role_negative={','.join(role_negative_hits[:2])}")

    trust_score = max(0.0, min(1.0, trust_score))
    relevance_score = max(0.0, min(1.0, relevance_score))

    decision = "reject"
    if mode == "person_mode":
        if trust_score >= 0.55 and relevance_score >= 0.55:
            decision = "accept_direct"
        elif source_class in {"directory_list", "roster_page"} and trust_score >= 0.45 and relevance_score >= 0.40:
            decision = "accept_for_expansion"
    else:
        if trust_score >= 0.38 and relevance_score >= 0.35:
            decision = "accept_direct"
        elif source_class in {"directory_list", "roster_page"} and trust_score >= 0.30 and relevance_score >= 0.25:
            decision = "accept_for_expansion"

    if decision == "reject":
        reason = f"strategist_reject mode={mode} trust={trust_score:.2f} relevance={relevance_score:.2f}"
    else:
        reason = f"strategist_{decision} mode={mode} trust={trust_score:.2f} relevance={relevance_score:.2f}"
    if reasons:
        reason = f"{reason}; " + "; ".join(reasons)

    return {
        "source_class": source_class,
        "trust_score": round(trust_score, 2),
        "relevance_score": round(relevance_score, 2),
        "decision": decision,
        "reason": reason,
    }


def decide_next_action(current_stats: dict[str, int], accepted_count: int, rejected_count: int) -> dict[str, str]:
    raw = int(current_stats.get("raw_candidates", 0))
    if accepted_count < 10 and raw < 120:
        return {"action": "expand_queries", "reason": "low accepted volume and low raw candidates"}
    if rejected_count > accepted_count * 3:
        return {"action": "tighten_sources", "reason": "high rejection ratio"}
    return {"action": "proceed", "reason": "source mix is acceptable"}
