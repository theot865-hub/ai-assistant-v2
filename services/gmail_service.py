from __future__ import annotations

import base64
import json
from typing import Any
from urllib.error import URLError
from urllib.request import Request, urlopen

from db.models import GmailConnection


GMAIL_API_BASE = "https://gmail.googleapis.com/gmail/v1/users/me"


def _encode_message(to_email: str, subject: str, body: str) -> str:
    raw = (
        f"To: {(to_email or '').strip()}\r\n"
        f"Subject: {(subject or '').strip()}\r\n"
        "Content-Type: text/plain; charset=\"UTF-8\"\r\n"
        "\r\n"
        f"{(body or '').strip()}"
    )
    encoded = base64.urlsafe_b64encode(raw.encode("utf-8")).decode("ascii")
    return encoded.rstrip("=")


def _gmail_request(
    *,
    method: str,
    path: str,
    access_token: str,
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    data = None
    headers = {
        "Authorization": f"Bearer {(access_token or '').strip()}",
        "Accept": "application/json",
    }
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"

    request = Request(
        f"{GMAIL_API_BASE}{path}",
        data=data,
        headers=headers,
        method=method,
    )
    try:
        with urlopen(request, timeout=20) as resp:
            body = resp.read().decode("utf-8", errors="ignore")
    except URLError as exc:
        return {"ok": False, "error": f"Gmail network error: {exc}"}

    try:
        parsed = json.loads(body)
    except json.JSONDecodeError:
        return {"ok": False, "error": "Gmail API returned invalid JSON."}

    if not isinstance(parsed, dict):
        return {"ok": False, "error": "Unexpected Gmail API response format."}
    if parsed.get("error"):
        err = parsed.get("error")
        if isinstance(err, dict):
            message = str(err.get("message") or "Gmail API request failed.")
        else:
            message = str(err)
        return {"ok": False, "error": message}
    return {"ok": True, "payload": parsed}


def get_gmail_profile_email(connection: GmailConnection) -> dict[str, Any]:
    response = _gmail_request(
        method="GET",
        path="/profile",
        access_token=connection.access_token,
    )
    if not response.get("ok"):
        return response
    payload = response.get("payload", {})
    return {
        "ok": True,
        "email": str(payload.get("emailAddress") or "").strip(),
    }


def create_gmail_draft(
    *,
    connection: GmailConnection,
    to_email: str,
    subject: str,
    body: str,
    thread_id: str | None = None,
) -> dict[str, Any]:
    if not (to_email or "").strip():
        return {"ok": False, "error": "Draft recipient email is required."}

    message_payload: dict[str, Any] = {
        "message": {
            "raw": _encode_message(to_email, subject, body),
        }
    }
    if (thread_id or "").strip():
        message_payload["message"]["threadId"] = thread_id.strip()

    response = _gmail_request(
        method="POST",
        path="/drafts",
        access_token=connection.access_token,
        payload=message_payload,
    )
    if not response.get("ok"):
        return {
            "ok": False,
            "provider": "gmail",
            "action": "create_draft",
            "draft_id": "",
            "thread_id": (thread_id or "").strip(),
            "error": str(response.get("error") or "Failed to create Gmail draft."),
        }

    payload = response.get("payload", {})
    draft_obj = payload.get("id") if isinstance(payload, dict) else ""
    msg_obj = payload.get("message") if isinstance(payload, dict) else {}
    msg_id = ""
    thread_value = ""
    if isinstance(msg_obj, dict):
        msg_id = str(msg_obj.get("id") or "").strip()
        thread_value = str(msg_obj.get("threadId") or "").strip()

    return {
        "ok": True,
        "provider": "gmail",
        "action": "create_draft",
        "draft_id": str(draft_obj or "").strip(),
        "message_id": msg_id,
        "thread_id": thread_value,
        "error": "",
    }


def send_gmail_message(*, connection: GmailConnection, draft_id: str) -> dict[str, Any]:
    normalized_draft_id = (draft_id or "").strip()
    if not normalized_draft_id:
        return {
            "ok": False,
            "provider": "gmail",
            "action": "send_draft",
            "draft_id": "",
            "message_id": "",
            "error": "draft_id is required.",
        }

    response = _gmail_request(
        method="POST",
        path="/drafts/send",
        access_token=connection.access_token,
        payload={"id": normalized_draft_id},
    )
    if not response.get("ok"):
        return {
            "ok": False,
            "provider": "gmail",
            "action": "send_draft",
            "draft_id": normalized_draft_id,
            "message_id": "",
            "error": str(response.get("error") or "Failed to send Gmail draft."),
        }

    payload = response.get("payload", {})
    msg_id = str(payload.get("id") or "").strip() if isinstance(payload, dict) else ""
    return {
        "ok": True,
        "provider": "gmail",
        "action": "send_draft",
        "draft_id": normalized_draft_id,
        "message_id": msg_id,
        "error": "",
    }
