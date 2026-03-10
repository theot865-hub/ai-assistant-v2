from __future__ import annotations

import json
from pathlib import Path
from typing import Any


BASE_DIR = Path(__file__).resolve().parent.parent
CONFIG_DIR = BASE_DIR / "workspace" / "config"
CAMPAIGNS_DIR = BASE_DIR / "workspace" / "campaigns"
SENDER_CONFIG_PATH = CONFIG_DIR / "senders.json"

DEFAULT_SENDER_PROFILE_KEY = "pressure_washing_buddy"
DEFAULT_CAMPAIGN_PROMPT_PATH = CAMPAIGNS_DIR / "realtor_pressure_washing.txt"

DEFAULT_VALIDATION_RULES = {
    "required_fields": ["name", "business", "city", "phone"],
    "disallowed_values": ["REPLACE_ME", "", "null"],
}

DEFAULT_SENDERS = {
    "_validation": DEFAULT_VALIDATION_RULES,
    "pressure_washing_buddy": {
        "name": "Theo Taylor",
        "business": "Pressure Washing Buddy",
        "city": "Victoria BC",
        "phone": "250-555-0134",
        "services": [
            "driveway cleaning",
            "walkway cleaning",
            "patio cleaning",
            "siding wash",
        ],
        "angle": "local pressure washing service with quick quotes, reliable scheduling, and clean finishes",
    },
}

DEFAULT_CAMPAIGN_TEXT = """Goal:
Offer pressure washing for driveways, walkways, and exterior presentation before listings.

Audience:
Realtors and property professionals in Victoria BC.

Tone:
Friendly, short, local, respectful.

Offer:
Quick quote for driveway or walkway cleaning before showings or listing photos.
"""


def ensure_default_files() -> None:
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    CAMPAIGNS_DIR.mkdir(parents=True, exist_ok=True)

    if not SENDER_CONFIG_PATH.exists():
        SENDER_CONFIG_PATH.write_text(
            json.dumps(DEFAULT_SENDERS, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )

    if not DEFAULT_CAMPAIGN_PROMPT_PATH.exists():
        DEFAULT_CAMPAIGN_PROMPT_PATH.write_text(DEFAULT_CAMPAIGN_TEXT, encoding="utf-8")


def _load_sender_config_raw() -> dict[str, Any]:
    ensure_default_files()
    try:
        parsed = json.loads(SENDER_CONFIG_PATH.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return DEFAULT_SENDERS.copy()
    if not isinstance(parsed, dict):
        return DEFAULT_SENDERS.copy()
    return parsed


def load_sender_validation_rules() -> dict[str, Any]:
    raw = _load_sender_config_raw()
    candidate = raw.get("_validation", {})
    if not isinstance(candidate, dict):
        return DEFAULT_VALIDATION_RULES.copy()

    required_fields = candidate.get("required_fields", DEFAULT_VALIDATION_RULES["required_fields"])
    disallowed_values = candidate.get(
        "disallowed_values", DEFAULT_VALIDATION_RULES["disallowed_values"]
    )

    if not isinstance(required_fields, list) or not required_fields:
        required_fields = DEFAULT_VALIDATION_RULES["required_fields"]
    if not isinstance(disallowed_values, list):
        disallowed_values = DEFAULT_VALIDATION_RULES["disallowed_values"]

    return {
        "required_fields": [str(item).strip() for item in required_fields if str(item).strip()],
        "disallowed_values": [str(item) for item in disallowed_values],
    }


def load_sender_profiles() -> dict[str, dict]:
    raw = _load_sender_config_raw()
    valid: dict[str, dict] = {}
    for key, value in raw.items():
        if not isinstance(key, str) or not key.strip():
            continue
        if key.startswith("_"):
            continue
        if not isinstance(value, dict):
            continue
        valid[key.strip()] = value

    if not valid:
        fallback = {
            key: value
            for key, value in DEFAULT_SENDERS.items()
            if not key.startswith("_") and isinstance(value, dict)
        }
        return fallback
    return valid


def save_sender_profile(profile_key: str, profile: dict[str, Any]) -> None:
    normalized_key = (profile_key or "").strip()
    if not normalized_key:
        raise ValueError("profile_key is required")
    raw = _load_sender_config_raw()
    raw[normalized_key] = dict(profile)
    if "_validation" not in raw:
        raw["_validation"] = DEFAULT_VALIDATION_RULES
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    SENDER_CONFIG_PATH.write_text(
        json.dumps(raw, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


def resolve_sender_profile(profile_key: str | None) -> tuple[str, dict]:
    profiles = load_sender_profiles()
    normalized_key = (profile_key or "").strip()
    if normalized_key and normalized_key in profiles:
        return normalized_key, profiles[normalized_key]

    if DEFAULT_SENDER_PROFILE_KEY in profiles:
        return DEFAULT_SENDER_PROFILE_KEY, profiles[DEFAULT_SENDER_PROFILE_KEY]

    first_key = sorted(profiles.keys())[0]
    return first_key, profiles[first_key]


def validate_sender_profile(profile: dict) -> dict[str, Any]:
    rules = load_sender_validation_rules()
    required_fields = rules.get("required_fields", DEFAULT_VALIDATION_RULES["required_fields"])
    disallowed_values_raw = rules.get(
        "disallowed_values", DEFAULT_VALIDATION_RULES["disallowed_values"]
    )
    disallowed_values = {
        str(item).strip().lower() for item in disallowed_values_raw if str(item).strip()
    }

    missing_fields: list[str] = []
    placeholder_fields: list[str] = []
    errors: list[str] = []

    for field in required_fields:
        value = profile.get(field)
        normalized = str(value).strip() if value is not None else ""
        normalized_lc = normalized.lower()
        if value is None or not normalized:
            missing_fields.append(field)
            continue
        if normalized_lc in disallowed_values:
            placeholder_fields.append(field)

    if missing_fields:
        errors.append(
            "Missing required sender fields: " + ", ".join(missing_fields)
        )
    if placeholder_fields:
        errors.append(
            "Sender fields still contain placeholder/disallowed values: "
            + ", ".join(placeholder_fields)
        )

    return {
        "valid": not errors,
        "missing_fields": missing_fields,
        "placeholder_fields": placeholder_fields,
        "errors": errors,
        "rules": rules,
    }


def resolve_campaign_prompt(campaign_prompt_path: str | None) -> tuple[str, str]:
    ensure_default_files()
    path_value = (campaign_prompt_path or "").strip()
    candidate: Path
    if path_value:
        possible = Path(path_value)
        if possible.is_absolute():
            candidate = possible
        else:
            candidate = BASE_DIR / possible
    else:
        candidate = DEFAULT_CAMPAIGN_PROMPT_PATH

    if not candidate.exists():
        candidate = DEFAULT_CAMPAIGN_PROMPT_PATH

    try:
        text = candidate.read_text(encoding="utf-8", errors="ignore").strip()
    except OSError:
        candidate = DEFAULT_CAMPAIGN_PROMPT_PATH
        text = DEFAULT_CAMPAIGN_TEXT.strip()

    return str(candidate), text


def campaign_path_display(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(BASE_DIR.resolve()))
    except ValueError:
        return str(path)
