from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from urllib.error import URLError

GOOGLE_AUTH_URL = "https://accounts.google.com/o/oauth2/v2/auth"
GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token"
GMAIL_SCOPE = "https://www.googleapis.com/auth/gmail.compose"


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _to_iso(dt: datetime) -> str:
    return dt.isoformat()


def oauth_config() -> dict[str, str]:
    return {
        "client_id": os.getenv("GMAIL_CLIENT_ID", "").strip(),
        "client_secret": os.getenv("GMAIL_CLIENT_SECRET", "").strip(),
        "redirect_uri": os.getenv("GMAIL_REDIRECT_URI", "http://127.0.0.1:8000/gmail/callback").strip(),
    }


def is_oauth_configured() -> bool:
    conf = oauth_config()
    return bool(conf["client_id"] and conf["client_secret"] and conf["redirect_uri"])


def build_gmail_auth_url(state: str) -> str:
    conf = oauth_config()
    params = {
        "client_id": conf["client_id"],
        "redirect_uri": conf["redirect_uri"],
        "response_type": "code",
        "scope": GMAIL_SCOPE,
        "access_type": "offline",
        "include_granted_scopes": "true",
        "prompt": "consent",
        "state": state,
    }
    return f"{GOOGLE_AUTH_URL}?{urlencode(params)}"


def _post_form(url: str, payload: dict[str, str]) -> dict[str, Any]:
    data = urlencode(payload).encode("utf-8")
    request = Request(
        url,
        data=data,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        method="POST",
    )
    try:
        with urlopen(request, timeout=20) as resp:
            body = resp.read().decode("utf-8", errors="ignore")
    except URLError as exc:
        return {"ok": False, "error": f"Network error: {exc}"}
    try:
        parsed = json.loads(body)
    except json.JSONDecodeError:
        return {"ok": False, "error": "Token endpoint returned invalid JSON."}
    if isinstance(parsed, dict) and parsed.get("error"):
        return {"ok": False, "error": str(parsed.get("error_description") or parsed.get("error"))}
    if not isinstance(parsed, dict):
        return {"ok": False, "error": "Token endpoint response had unexpected format."}
    return {"ok": True, "payload": parsed}


def exchange_code_for_tokens(code: str) -> dict[str, Any]:
    conf = oauth_config()
    if not is_oauth_configured():
        return {"ok": False, "error": "Gmail OAuth is not configured. Set GMAIL_CLIENT_ID, GMAIL_CLIENT_SECRET, and GMAIL_REDIRECT_URI."}
    payload = {
        "code": (code or "").strip(),
        "client_id": conf["client_id"],
        "client_secret": conf["client_secret"],
        "redirect_uri": conf["redirect_uri"],
        "grant_type": "authorization_code",
    }
    response = _post_form(GOOGLE_TOKEN_URL, payload)
    if not response.get("ok"):
        return response
    parsed = response.get("payload", {})
    expires_in = int(parsed.get("expires_in") or 0)
    token_expiry = _to_iso(_utc_now() + timedelta(seconds=max(expires_in, 0)))
    return {
        "ok": True,
        "access_token": str(parsed.get("access_token") or "").strip(),
        "refresh_token": str(parsed.get("refresh_token") or "").strip(),
        "scope": str(parsed.get("scope") or "").strip(),
        "token_expiry": token_expiry,
    }


def refresh_access_token(refresh_token: str) -> dict[str, Any]:
    conf = oauth_config()
    if not is_oauth_configured():
        return {"ok": False, "error": "Gmail OAuth is not configured."}
    if not (refresh_token or "").strip():
        return {"ok": False, "error": "Missing refresh token."}

    payload = {
        "refresh_token": (refresh_token or "").strip(),
        "client_id": conf["client_id"],
        "client_secret": conf["client_secret"],
        "grant_type": "refresh_token",
    }
    response = _post_form(GOOGLE_TOKEN_URL, payload)
    if not response.get("ok"):
        return response

    parsed = response.get("payload", {})
    expires_in = int(parsed.get("expires_in") or 0)
    token_expiry = _to_iso(_utc_now() + timedelta(seconds=max(expires_in, 0)))
    return {
        "ok": True,
        "access_token": str(parsed.get("access_token") or "").strip(),
        "scope": str(parsed.get("scope") or "").strip(),
        "token_expiry": token_expiry,
    }
