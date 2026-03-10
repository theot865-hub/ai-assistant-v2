from __future__ import annotations

import csv
import json
import os
import re
from collections import deque
from datetime import datetime
from hashlib import sha256
from pathlib import Path
import secrets
from threading import Lock
from uuid import uuid4
from urllib.error import URLError
from urllib.parse import quote_plus
from urllib.request import Request as URLRequest, urlopen

from dotenv import load_dotenv
import os

load_dotenv()

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.middleware.base import BaseHTTPMiddleware

from core.campaign_service import CampaignService
from db.database import DB_PATH, init_db
from db.repository import (
    assign_draft_owner_for_new_records,
    create_user,
    get_contacts_by_lead,
    get_draft_by_id,
    get_drafts,
    get_gmail_connection_by_user,
    get_leads,
    get_latest_draft_id,
    get_user_by_email,
    get_user_by_id,
    upsert_gmail_connection,
    update_draft_status,
    verify_password,
)
from services.outreach_config import (
    DEFAULT_CAMPAIGN_PROMPT_PATH,
    DEFAULT_SENDER_PROFILE_KEY,
    campaign_path_display,
    ensure_default_files,
    load_sender_profiles,
    validate_sender_profile,
)
from services.campaign_store import (
    get_campaign_record_for_user,
    list_campaigns_for_user,
)
from services.gmail_oauth import (
    build_gmail_auth_url,
    exchange_code_for_tokens,
    is_oauth_configured,
)
from services.gmail_service import create_gmail_draft, get_gmail_profile_email, send_gmail_message
from .capabilities import (
    HOME_EXAMPLES,
    build_capabilities_page_context,
    classify_command,
    get_output_paths_for_workers,
)


APP_DIR = Path(__file__).resolve().parent
TEMPLATES_DIR = APP_DIR / "templates"
STATIC_DIR = APP_DIR / "static"
BACKEND_URL = os.getenv("ASSISTANT_SERVER_URL", "http://127.0.0.1:8787")
WORKSPACE_DIR = APP_DIR.parent / "workspace"
ALLOWED_WORKSPACE_DIRS = {"leads", "jobs", "logs"}
TEXT_PREVIEW_SUFFIXES = {".txt", ".json", ".jsonl", ".log", ".md"}
MAX_TEXT_PREVIEW_CHARS = 20000
MAX_CSV_PREVIEW_ROWS = 50
MAX_CHAT_INTERACTIONS = 10
SESSION_SECRET = os.getenv("ASSISTANT_SESSION_SECRET", "assistant-v2-dev-session-secret")
SESSION_USER_ID_KEY = "user_id"
SESSION_USER_EMAIL_KEY = "user_email"
SESSION_GMAIL_STATE_KEY = "gmail_oauth_state"
SESSION_FLASH_KEY = "flash_message"
SESSION_COOKIE_NAME = "assistant_v2_session"
SESSION_COOKIE_TTL_SECONDS = 60 * 60 * 24 * 14
CAMPAIGN_ARTIFACT_PATHS = [
    "workspace/leads/business_discovery.csv",
    "workspace/leads/business_discovery_enriched.csv",
    "workspace/leads/business_discovery_outreach.csv",
]

init_db()
ensure_default_files()

app = FastAPI(title="Assistant v2 Web")
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))
INTERACTION_HISTORY: deque[dict] = deque(maxlen=MAX_CHAT_INTERACTIONS)
INTERACTION_LOCK = Lock()
SESSION_STORE: dict[str, dict] = {}
SESSION_LOCK = Lock()
CAMPAIGN_SERVICE = CampaignService()


class LocalSessionMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        session_id = str(request.cookies.get(SESSION_COOKIE_NAME, "")).strip()
        session_data: dict = {}
        if session_id:
            with SESSION_LOCK:
                session_data = dict(SESSION_STORE.get(session_id, {}))

        request.scope["session"] = session_data
        request.scope["session_id"] = session_id
        before = json.dumps(session_data, sort_keys=True, ensure_ascii=False)
        response = await call_next(request)

        after_data = request.scope.get("session", {})
        if not isinstance(after_data, dict):
            after_data = {}
        after = json.dumps(after_data, sort_keys=True, ensure_ascii=False)

        if after_data:
            if not session_id:
                session_id = secrets.token_urlsafe(24)
            with SESSION_LOCK:
                SESSION_STORE[session_id] = dict(after_data)
            if before != after or not request.cookies.get(SESSION_COOKIE_NAME):
                response.set_cookie(
                    key=SESSION_COOKIE_NAME,
                    value=session_id,
                    max_age=SESSION_COOKIE_TTL_SECONDS,
                    httponly=True,
                    samesite="lax",
                )
        elif session_id:
            with SESSION_LOCK:
                SESSION_STORE.pop(session_id, None)
            response.delete_cookie(SESSION_COOKIE_NAME)

        return response


app.add_middleware(LocalSessionMiddleware)


def fetch_json(path: str) -> dict:
    url = f"{BACKEND_URL}{path}"
    try:
        with urlopen(url, timeout=8) as resp:
            payload = resp.read().decode("utf-8", errors="ignore")
        parsed = json.loads(payload)
        if isinstance(parsed, dict):
            return parsed
        return {"ok": False, "error": "Invalid response payload."}
    except URLError as exc:
        return {"ok": False, "error": f"Backend unavailable: {exc}"}
    except json.JSONDecodeError:
        return {"ok": False, "error": "Backend returned non-JSON response."}


def run_worker_via_backend(worker: str, args: list[str]) -> dict:
    payload = json.dumps({"worker": worker, "args": args}).encode("utf-8")
    req = URLRequest(
        f"{BACKEND_URL}/run",
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urlopen(req, timeout=30) as resp:
            body = resp.read().decode("utf-8", errors="ignore")
        parsed = json.loads(body)
        if isinstance(parsed, dict):
            return parsed
        return {"ok": False, "error": "Invalid response payload."}
    except URLError as exc:
        return {"ok": False, "error": f"Backend unavailable: {exc}"}
    except json.JSONDecodeError:
        return {"ok": False, "error": "Backend returned non-JSON response."}


def _get_backend_workers() -> tuple[set[str], str]:
    workers_data = fetch_json("/workers")
    if not workers_data.get("ok"):
        return set(), str(workers_data.get("error", "Backend unavailable"))
    workers = workers_data.get("workers", [])
    if not isinstance(workers, list):
        return set(), "Backend /workers endpoint returned an invalid payload"
    return {str(item).strip() for item in workers if str(item).strip()}, ""


def _safe_workspace_file(rel_path: str) -> Path | None:
    cleaned = (rel_path or "").strip().replace("\\", "/")
    if cleaned.startswith("workspace/"):
        cleaned = cleaned[len("workspace/") :]
    if not cleaned:
        return None

    candidate = (WORKSPACE_DIR / cleaned).resolve()
    try:
        relative = candidate.relative_to(WORKSPACE_DIR.resolve())
    except ValueError:
        return None

    if not relative.parts:
        return None
    if relative.parts[0] not in ALLOWED_WORKSPACE_DIRS:
        return None
    if not candidate.is_file():
        return None
    return candidate


def _collect_workspace_files() -> list[dict]:
    files: list[dict] = []
    for dirname in sorted(ALLOWED_WORKSPACE_DIRS):
        root = WORKSPACE_DIR / dirname
        if not root.exists():
            continue
        for path in root.rglob("*"):
            if not path.is_file():
                continue
            stat = path.stat()
            rel_path = path.relative_to(WORKSPACE_DIR).as_posix()
            files.append(
                {
                    "filename": path.name,
                    "relative_path": rel_path,
                    "size_bytes": stat.st_size,
                    "modified_time": datetime.fromtimestamp(stat.st_mtime).strftime(
                        "%Y-%m-%d %H:%M:%S"
                    ),
                    "modified_ts": stat.st_mtime,
                    "view_url": f"/files/view?path={quote_plus(rel_path)}",
                }
            )
    files.sort(key=lambda item: item["modified_ts"], reverse=True)
    return files


def _build_artifact_links(artifacts: list[str]) -> list[dict]:
    links: list[dict] = []
    for artifact in artifacts:
        artifact_path = str(artifact)
        safe_file = _safe_workspace_file(artifact_path)
        if safe_file is None:
            links.append({"path": artifact_path, "download_url": "", "available": False})
            continue
        rel_path = safe_file.relative_to(WORKSPACE_DIR).as_posix()
        links.append(
            {
                "path": artifact_path,
                "download_url": f"/download?path={quote_plus(rel_path)}",
                "available": True,
            }
        )
    return links


def _get_interactions() -> list[dict]:
    with INTERACTION_LOCK:
        return list(INTERACTION_HISTORY)


def _session_user_id(request: Request) -> int | None:
    raw = request.session.get(SESSION_USER_ID_KEY)
    if raw is None:
        return None
    try:
        user_id = int(raw)
    except (TypeError, ValueError):
        return None
    return user_id if user_id > 0 else None


def _current_user(request: Request):
    user_id = _session_user_id(request)
    if user_id is None:
        return None
    user = get_user_by_id(user_id)
    if user is None:
        request.session.clear()
        return None
    return user


def _login_user(request: Request, user_id: int, email: str) -> None:
    request.session[SESSION_USER_ID_KEY] = int(user_id)
    request.session[SESSION_USER_EMAIL_KEY] = (email or "").strip().lower()


def _logout_user(request: Request) -> None:
    request.session.clear()


def _login_redirect() -> RedirectResponse:
    return RedirectResponse(url="/login", status_code=303)


def _set_flash(request: Request, message: str) -> None:
    request.session[SESSION_FLASH_KEY] = (message or "").strip()


def _pop_flash(request: Request) -> str:
    message = str(request.session.get(SESSION_FLASH_KEY, "")).strip()
    if SESSION_FLASH_KEY in request.session:
        request.session.pop(SESSION_FLASH_KEY)
    return message


def _add_interaction(
    command_text: str,
    steps: list[dict],
    interpretation: str,
    capability_status: str,
    capability_status_key: str,
    capability_hint: str,
    chosen_workers: list[str],
    will_run: list[str],
    will_not_run: list[str],
    result_summary: str,
    suggested_phrasing: list[str],
    expected_outputs: list[str],
    sender_profile_key: str,
    campaign_prompt_path: str,
) -> None:
    interaction = {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "command_text": command_text,
        "interpretation": interpretation,
        "capability_status": capability_status,
        "capability_status_key": capability_status_key,
        "capability_hint": capability_hint,
        "chosen_workers": chosen_workers,
        "will_run": will_run,
        "will_not_run": will_not_run,
        "result_summary": result_summary,
        "suggested_phrasing": suggested_phrasing,
        "expected_outputs": expected_outputs,
        "sender_profile_key": sender_profile_key,
        "campaign_prompt_path": campaign_prompt_path,
        "steps": steps,
    }
    with INTERACTION_LOCK:
        INTERACTION_HISTORY.append(interaction)


def _render_index(
    request: Request,
    command_text: str,
    error_data: dict | None,
    sender_profile_key: str,
    campaign_prompt_path: str,
) -> HTMLResponse:
    profiles = load_sender_profiles()
    profile_options = sorted(profiles.keys())
    selected_profile = sender_profile_key.strip() or DEFAULT_SENDER_PROFILE_KEY
    if selected_profile not in profiles and profile_options:
        selected_profile = profile_options[0]
    selected_profile_obj = profiles.get(selected_profile, {})
    sender_validation = validate_sender_profile(selected_profile_obj)
    prompt_value = campaign_prompt_path.strip() or campaign_path_display(
        DEFAULT_CAMPAIGN_PROMPT_PATH
    )
    return templates.TemplateResponse(
        request,
        "index.html",
        {
            "current_user": _current_user(request),
            "flash_message": _pop_flash(request),
            "backend_url": BACKEND_URL,
            "command_text": command_text,
            "interactions": _get_interactions(),
            "error_data": error_data,
            "what_i_can_do_examples": HOME_EXAMPLES,
            "sender_profile_options": profile_options,
            "selected_sender_profile_key": selected_profile,
            "selected_sender_profile": selected_profile_obj,
            "sender_profile_valid": bool(sender_validation.get("valid")),
            "sender_profile_errors": [
                str(item) for item in sender_validation.get("errors", [])
            ],
            "campaign_prompt_path": prompt_value,
        },
    )


def _build_leads_overview(limit: int = 250, user_id: int | None = None) -> list[dict]:
    leads = get_leads(limit=limit)
    drafts = get_drafts(limit=max(500, limit * 4), user_id=user_id)
    latest_draft_by_lead: dict[int, object] = {}
    for draft in drafts:
        if draft.lead_id not in latest_draft_by_lead:
            latest_draft_by_lead[draft.lead_id] = draft

    rows: list[dict] = []
    for lead in leads:
        contacts = get_contacts_by_lead(lead.id)
        primary_contact = contacts[0] if contacts else None
        latest_draft = latest_draft_by_lead.get(lead.id)
        rows.append(
            {
                "lead_id": lead.id,
                "name": lead.name,
                "domain": lead.domain,
                "website": lead.website,
                "source": lead.source,
                "query": lead.query,
                "source_type": lead.source_type,
                "entity_type": lead.entity_type,
                "extraction_source": lead.extraction_source,
                "parent_source_url": lead.parent_source_url,
                "quality_score": f"{lead.quality_score:.1f}",
                "validation_reason": lead.validation_reason,
                "status": lead.status,
                "discovered_at": lead.discovered_at,
                "contact_email": (primary_contact.email if primary_contact else ""),
                "contact_phone": (primary_contact.phone if primary_contact else ""),
                "contact_confidence": (
                    f"{primary_contact.confidence:.2f}" if primary_contact else ""
                ),
                "contact_count": len(contacts),
                "draft_id": (latest_draft.id if latest_draft else 0),
                "draft_status": (latest_draft.status if latest_draft else ""),
                "draft_email": (latest_draft.email if latest_draft else ""),
            }
        )
    return rows


def _default_campaign_form_data() -> dict[str, str]:
    profiles = load_sender_profiles()
    selected = profiles.get(DEFAULT_SENDER_PROFILE_KEY, {})
    services = selected.get("services", [])
    services_text = ""
    if isinstance(services, list):
        services_text = ", ".join(str(item).strip() for item in services if str(item).strip())

    return {
        "campaign_name": "",
        "audience": "",
        "location": "",
        "max_leads": "",
        "extra_notes": "",
        "sender_name": str(selected.get("name", "")).strip(),
        "business_name": str(selected.get("business", "")).strip(),
        "phone": str(selected.get("phone", "")).strip(),
        "email": "",
        "website": "",
        "city": str(selected.get("city", "")).strip() or "Victoria BC",
        "services_offered": services_text,
        "unique_angle": str(selected.get("angle", "")).strip(),
        "offer": "Quick quote for driveway or walkway cleaning before showings or listing photos.",
        "tone": "Friendly, short, local, respectful.",
        "call_to_action": "Reply if you want a quick quote this week.",
    }


def _render_campaign_new(
    request: Request,
    form_data: dict[str, str],
    errors: list[str],
    summary_message: str = "",
) -> HTMLResponse:
    user = _current_user(request)
    user_campaigns = list_campaigns_for_user(user.id, limit=10) if user else []
    return templates.TemplateResponse(
        request,
        "campaigns_new.html",
        {
            "current_user": user,
            "recent_campaigns": user_campaigns,
            "form_data": form_data,
            "errors": errors,
            "summary_message": summary_message,
            "flash_message": _pop_flash(request),
        },
    )


def _build_campaign_discovery_query(form_data: dict[str, str]) -> str:
    audience = form_data.get("audience", "").strip()
    location = form_data.get("location", "").strip()
    extra_notes = form_data.get("extra_notes", "").strip()
    max_leads = form_data.get("max_leads", "").strip()

    query = f"find {audience} businesses in {location}"
    if extra_notes:
        query += f"; focus: {extra_notes}"
    if max_leads:
        query += f"; target around {max_leads} leads"
    return query


def _generate_campaign_id(campaign_name: str) -> str:
    base = re.sub(r"[^a-z0-9]+", "-", (campaign_name or "").lower()).strip("-")
    if not base:
        base = "campaign"
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    token = uuid4().hex[:6]
    return f"{base}-{timestamp}-{token}"


def _build_campaign_prompt_text(form_data: dict[str, str]) -> str:
    audience = form_data.get("audience", "").strip()
    location = form_data.get("location", "").strip()
    city = form_data.get("city", "").strip()
    services = form_data.get("services_offered", "").strip()
    angle = form_data.get("unique_angle", "").strip()
    offer = form_data.get("offer", "").strip()
    tone = form_data.get("tone", "").strip()
    cta = form_data.get("call_to_action", "").strip()

    return (
        f"Goal:\n{offer or 'Offer relevant local services with clear value.'}\n\n"
        f"Audience:\n{audience} in {location}\n\n"
        f"Tone:\n{tone or 'Friendly, concise, local.'}\n\n"
        f"Offer:\n{offer or 'Provide a quick quote.'}\n\n"
        f"CallToAction:\n{cta or 'Reply to get a quick quote.'}\n\n"
        f"SenderContext:\n"
        f"City: {city}\n"
        f"Services: {services}\n"
        f"Angle: {angle}\n"
    )


def _build_campaign_artifact_entries(artifact_paths: list[str]) -> list[dict]:
    entries: list[dict] = []
    for artifact_path in artifact_paths:
        safe_file = _safe_workspace_file(artifact_path)
        if safe_file is None:
            entries.append(
                {
                    "path": artifact_path,
                    "available": False,
                    "download_url": "",
                    "modified_time": "",
                    "size_bytes": 0,
                }
            )
            continue
        rel_path = safe_file.relative_to(WORKSPACE_DIR).as_posix()
        stat = safe_file.stat()
        entries.append(
            {
                "path": artifact_path,
                "available": True,
                "download_url": f"/download?path={quote_plus(rel_path)}",
                "modified_time": datetime.fromtimestamp(stat.st_mtime).strftime(
                    "%Y-%m-%d %H:%M:%S"
                ),
                "size_bytes": stat.st_size,
            }
        )
    return entries


def _render_auth_page(
    request: Request,
    *,
    template_name: str,
    email: str = "",
    error: str = "",
) -> HTMLResponse:
    return templates.TemplateResponse(
        request,
        template_name,
        {
            "current_user": _current_user(request),
            "email": email,
            "error": error,
            "flash_message": _pop_flash(request),
        },
    )


@app.get("/", response_class=HTMLResponse)
async def index(request: Request) -> HTMLResponse:
    return _render_index(
        request,
        "",
        None,
        DEFAULT_SENDER_PROFILE_KEY,
        campaign_path_display(DEFAULT_CAMPAIGN_PROMPT_PATH),
    )


@app.get("/signup", response_class=HTMLResponse)
async def signup_page(request: Request) -> HTMLResponse:
    if _current_user(request) is not None:
        return RedirectResponse(url="/", status_code=303)
    return _render_auth_page(request, template_name="signup.html")


@app.post("/signup", response_class=HTMLResponse)
async def signup_submit(request: Request):
    form = await request.form()
    email = str(form.get("email", "")).strip().lower()
    password = str(form.get("password", ""))
    if not email or "@" not in email:
        return _render_auth_page(
            request,
            template_name="signup.html",
            email=email,
            error="Enter a valid email address.",
        )
    if len(password) < 8:
        return _render_auth_page(
            request,
            template_name="signup.html",
            email=email,
            error="Password must be at least 8 characters.",
        )
    if get_user_by_email(email) is not None:
        return _render_auth_page(
            request,
            template_name="signup.html",
            email=email,
            error="That email is already registered. Try logging in.",
        )
    try:
        user = create_user(email=email, password=password)
    except Exception as exc:
        return _render_auth_page(
            request,
            template_name="signup.html",
            email=email,
            error=f"Could not create account: {exc}",
        )
    _login_user(request, user.id, user.email)
    return RedirectResponse(url="/campaigns/new", status_code=303)


@app.get("/login", response_class=HTMLResponse)
async def login_page(request: Request) -> HTMLResponse:
    if _current_user(request) is not None:
        return RedirectResponse(url="/", status_code=303)
    return _render_auth_page(request, template_name="login.html")


@app.post("/login", response_class=HTMLResponse)
async def login_submit(request: Request):
    form = await request.form()
    email = str(form.get("email", "")).strip().lower()
    password = str(form.get("password", ""))
    user = get_user_by_email(email)
    if user is None or not verify_password(password, user.password_hash):
        return _render_auth_page(
            request,
            template_name="login.html",
            email=email,
            error="Invalid email or password.",
        )
    _login_user(request, user.id, user.email)
    return RedirectResponse(url="/", status_code=303)


@app.post("/logout")
async def logout(request: Request) -> RedirectResponse:
    _logout_user(request)
    return RedirectResponse(url="/", status_code=303)


@app.get("/capabilities", response_class=HTMLResponse)
async def capabilities(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(
        request,
        "capabilities.html",
        {
            **build_capabilities_page_context(),
            "current_user": _current_user(request),
        },
    )


@app.get("/campaigns/new", response_class=HTMLResponse)
async def campaigns_new(request: Request) -> HTMLResponse:
    if _current_user(request) is None:
        return _login_redirect()
    return _render_campaign_new(request, _default_campaign_form_data(), [])


@app.post("/campaigns/new")
async def campaigns_create(request: Request):
    user = _current_user(request)
    if user is None:
        return _login_redirect()
    form = await request.form()
    fields = [
        "campaign_name",
        "audience",
        "location",
        "max_leads",
        "extra_notes",
        "sender_name",
        "business_name",
        "phone",
        "email",
        "website",
        "city",
        "services_offered",
        "unique_angle",
        "offer",
        "tone",
        "call_to_action",
    ]
    form_data = {field: str(form.get(field, "")).strip() for field in fields}
    errors: list[str] = []

    if errors:
        return _render_campaign_new(request, form_data, errors)

    try:
        campaign_data = dict(form_data)
        campaign_data["user_email"] = user.email
        campaign_id = CAMPAIGN_SERVICE.create_campaign(user.id, campaign_data)
        CAMPAIGN_SERVICE.run_campaign(campaign_id)
    except ValueError as exc:
        errors.append(str(exc))
        return _render_campaign_new(request, form_data, errors)
    except Exception as exc:
        errors.append(f"Campaign execution failed: {exc}")
        return _render_campaign_new(request, form_data, errors)

    return RedirectResponse(url=f"/campaigns/{campaign_id}", status_code=303)


@app.get("/campaigns/{campaign_id}", response_class=HTMLResponse)
async def campaigns_view(request: Request, campaign_id: str) -> HTMLResponse:
    user = _current_user(request)
    if user is None:
        return _login_redirect()
    campaign = get_campaign_record_for_user(campaign_id, user.id)
    if campaign is None:
        raise HTTPException(status_code=404, detail="Campaign not found.")
    campaign_artifacts = campaign.get("artifacts", CAMPAIGN_ARTIFACT_PATHS)
    if not isinstance(campaign_artifacts, list):
        campaign_artifacts = CAMPAIGN_ARTIFACT_PATHS
    return templates.TemplateResponse(
        request,
        "campaigns_view.html",
        {
            "current_user": user,
            "flash_message": _pop_flash(request),
            "campaign": campaign,
            "artifact_entries": _build_campaign_artifact_entries(
                [str(item) for item in campaign_artifacts]
            ),
        },
    )


@app.get("/leads", response_class=HTMLResponse)
async def leads(request: Request, limit: int = 200) -> HTMLResponse:
    user = _current_user(request)
    bounded_limit = max(1, min(limit, 1000))
    return templates.TemplateResponse(
        request,
        "leads.html",
        {
            "current_user": user,
            "leads": _build_leads_overview(
                limit=bounded_limit,
                user_id=(user.id if user else None),
            ),
            "lead_limit": bounded_limit,
            "db_path": DB_PATH,
        },
    )


@app.get("/drafts", response_class=HTMLResponse)
async def drafts(
    request: Request,
    status: str = "",
    limit: int = 250,
) -> HTMLResponse:
    user = _current_user(request)
    if user is None:
        return _login_redirect()
    bounded_limit = max(1, min(limit, 2000))
    status_filter = status.strip()
    rows = get_drafts(status=status_filter or None, limit=bounded_limit, user_id=user.id)
    return templates.TemplateResponse(
        request,
        "drafts.html",
        {
            "current_user": user,
            "drafts": rows,
            "status_filter": status_filter,
            "draft_limit": bounded_limit,
        },
    )


@app.get("/drafts/{draft_id}", response_class=HTMLResponse)
async def draft_view(request: Request, draft_id: int) -> HTMLResponse:
    user = _current_user(request)
    if user is None:
        return _login_redirect()
    draft = get_draft_by_id(draft_id, user_id=user.id)
    if draft is None:
        raise HTTPException(status_code=404, detail="Draft not found.")

    contacts = get_contacts_by_lead(draft.lead_id)
    gmail_connection = get_gmail_connection_by_user(user.id)
    return templates.TemplateResponse(
        request,
        "draft_view.html",
        {
            "current_user": user,
            "flash_message": _pop_flash(request),
            "draft": draft,
            "contacts": contacts,
            "gmail_connected": bool(gmail_connection and gmail_connection.access_token),
        },
    )


@app.post("/drafts/{draft_id}/approve")
async def approve_draft(request: Request, draft_id: int) -> RedirectResponse:
    user = _current_user(request)
    if user is None:
        return _login_redirect()
    draft = update_draft_status(draft_id, "approved", user_id=user.id)
    if draft is None:
        raise HTTPException(status_code=404, detail="Draft not found.")
    return RedirectResponse(url=f"/drafts/{draft_id}", status_code=303)


@app.post("/drafts/{draft_id}/reject")
async def reject_draft(request: Request, draft_id: int) -> RedirectResponse:
    user = _current_user(request)
    if user is None:
        return _login_redirect()
    draft = update_draft_status(draft_id, "rejected", user_id=user.id)
    if draft is None:
        raise HTTPException(status_code=404, detail="Draft not found.")
    return RedirectResponse(url=f"/drafts/{draft_id}", status_code=303)


@app.post("/drafts/{draft_id}/edit")
async def edit_draft(request: Request, draft_id: int) -> RedirectResponse:
    user = _current_user(request)
    if user is None:
        return _login_redirect()
    existing = get_draft_by_id(draft_id, user_id=user.id)
    if existing is None:
        raise HTTPException(status_code=404, detail="Draft not found.")

    form = await request.form()
    email = str(form.get("email", existing.email)).strip()
    subject = str(form.get("subject", existing.subject)).strip()
    body = str(form.get("body", existing.body)).strip()
    status_value = str(form.get("status", existing.status)).strip() or existing.status

    updated = update_draft_status(
        draft_id,
        status_value,
        email=email,
        subject=subject,
        body=body,
        user_id=user.id,
    )
    if updated is None:
        raise HTTPException(status_code=404, detail="Draft not found.")
    return RedirectResponse(url=f"/drafts/{draft_id}", status_code=303)


@app.post("/drafts/{draft_id}/send")
async def send_draft(request: Request, draft_id: int) -> RedirectResponse:
    user = _current_user(request)
    if user is None:
        return _login_redirect()

    draft = get_draft_by_id(draft_id, user_id=user.id)
    if draft is None:
        raise HTTPException(status_code=404, detail="Draft not found.")
    if draft.status != "approved":
        _set_flash(request, "Only approved drafts can be sent.")
        return RedirectResponse(url=f"/drafts/{draft_id}", status_code=303)

    connection = get_gmail_connection_by_user(user.id)
    if connection is None or not connection.access_token:
        _set_flash(request, "Connect Gmail before sending drafts.")
        return RedirectResponse(url=f"/drafts/{draft_id}", status_code=303)

    create_result = create_gmail_draft(
        connection=connection,
        to_email=draft.email,
        subject=draft.subject,
        body=draft.body,
    )
    if not create_result.get("ok"):
        _set_flash(
            request,
            "Gmail draft creation failed: " + str(create_result.get("error", "unknown error")),
        )
        return RedirectResponse(url=f"/drafts/{draft_id}", status_code=303)

    send_result = send_gmail_message(
        connection=connection,
        draft_id=str(create_result.get("draft_id", "")),
    )
    if not send_result.get("ok"):
        _set_flash(
            request,
            "Gmail send failed: " + str(send_result.get("error", "unknown error")),
        )
        return RedirectResponse(url=f"/drafts/{draft_id}", status_code=303)

    update_draft_status(draft_id, "sent", user_id=user.id)
    _set_flash(request, "Draft sent via Gmail.")
    return RedirectResponse(url=f"/drafts/{draft_id}", status_code=303)


@app.get("/gmail/status", response_class=HTMLResponse)
async def gmail_status(request: Request) -> HTMLResponse:
    user = _current_user(request)
    if user is None:
        return _login_redirect()
    connection = get_gmail_connection_by_user(user.id)
    return templates.TemplateResponse(
        request,
        "gmail_status.html",
        {
            "current_user": user,
            "flash_message": _pop_flash(request),
            "oauth_configured": is_oauth_configured(),
            "gmail_connection": connection,
        },
    )


@app.get("/gmail/connect")
async def gmail_connect(request: Request) -> RedirectResponse:
    user = _current_user(request)
    if user is None:
        return _login_redirect()
    if not is_oauth_configured():
        _set_flash(
            request,
            "Gmail OAuth is not configured yet. Set GMAIL_CLIENT_ID, GMAIL_CLIENT_SECRET, and GMAIL_REDIRECT_URI.",
        )
        return RedirectResponse(url="/gmail/status", status_code=303)

    state = sha256(f"{user.id}-{uuid4().hex}".encode("utf-8")).hexdigest()
    request.session[SESSION_GMAIL_STATE_KEY] = state
    return RedirectResponse(url=build_gmail_auth_url(state), status_code=303)


@app.get("/gmail/callback")
async def gmail_callback(request: Request) -> RedirectResponse:
    user = _current_user(request)
    if user is None:
        return _login_redirect()

    returned_state = str(request.query_params.get("state", "")).strip()
    expected_state = str(request.session.get(SESSION_GMAIL_STATE_KEY, "")).strip()
    if not returned_state or returned_state != expected_state:
        _set_flash(request, "Gmail OAuth state check failed. Please try connect again.")
        return RedirectResponse(url="/gmail/status", status_code=303)

    request.session.pop(SESSION_GMAIL_STATE_KEY, None)
    oauth_error = str(request.query_params.get("error", "")).strip()
    if oauth_error:
        _set_flash(request, f"Gmail connect cancelled or failed: {oauth_error}")
        return RedirectResponse(url="/gmail/status", status_code=303)

    code = str(request.query_params.get("code", "")).strip()
    if not code:
        _set_flash(request, "Missing authorization code from Gmail OAuth callback.")
        return RedirectResponse(url="/gmail/status", status_code=303)

    token_result = exchange_code_for_tokens(code)
    if not token_result.get("ok"):
        _set_flash(request, "Gmail token exchange failed: " + str(token_result.get("error", "")))
        return RedirectResponse(url="/gmail/status", status_code=303)

    existing = get_gmail_connection_by_user(user.id)
    refresh_token = str(token_result.get("refresh_token") or "").strip()
    if not refresh_token and existing is not None:
        refresh_token = existing.refresh_token

    connection = upsert_gmail_connection(
        user_id=user.id,
        email=user.email,
        access_token=str(token_result.get("access_token") or "").strip(),
        refresh_token=refresh_token,
        token_expiry=str(token_result.get("token_expiry") or "").strip(),
        scope=str(token_result.get("scope") or "").strip(),
    )
    profile_result = get_gmail_profile_email(connection)
    if profile_result.get("ok"):
        upsert_gmail_connection(
            user_id=user.id,
            email=str(profile_result.get("email") or user.email),
            access_token=connection.access_token,
            refresh_token=connection.refresh_token,
            token_expiry=connection.token_expiry,
            scope=connection.scope,
        )

    _set_flash(request, "Gmail connected. You can now send approved drafts.")
    return RedirectResponse(url="/gmail/status", status_code=303)


@app.post("/command", response_class=HTMLResponse)
async def command(request: Request) -> HTMLResponse:
    user = _current_user(request)
    form = await request.form()
    command_text = str(form.get("command", "")).strip()
    sender_profile_key = str(form.get("sender_profile_key", "")).strip() or DEFAULT_SENDER_PROFILE_KEY
    campaign_prompt_path = (
        str(form.get("campaign_prompt_path", "")).strip()
        or campaign_path_display(DEFAULT_CAMPAIGN_PROMPT_PATH)
    )
    if not command_text:
        result_data = {"ok": False, "error": "Please enter a command."}
        return _render_index(
            request,
            command_text,
            result_data,
            sender_profile_key,
            campaign_prompt_path,
        )

    backend_workers, backend_workers_error = _get_backend_workers()
    command_plan = classify_command(command_text, backend_workers)
    workflow_steps = command_plan.get("workflow", [])
    if not isinstance(workflow_steps, list):
        workflow_steps = []

    if backend_workers_error and workflow_steps:
        will_not_run = command_plan.get("will_not_run", [])
        if isinstance(will_not_run, list):
            will_not_run.append(
                f"Backend worker list unavailable: {backend_workers_error}."
            )
        command_plan["will_not_run"] = will_not_run
        command_plan["runnable"] = False
        command_plan["status_key"] = "partial"
        command_plan["status_label"] = "Partially supported"
        command_plan["result_hint"] = "Command mapping succeeded, but backend is unavailable."

    step_results: list[dict] = []
    before_draft_id = get_latest_draft_id()
    if command_plan.get("runnable"):
        for step in workflow_steps:
            requested_worker = str(step.get("requested_worker", "")).strip()
            args = step.get("args", [])
            if not isinstance(args, list):
                args = []
            args = [str(item) for item in args]
            if requested_worker == "business_outreach":
                args.extend(
                    [
                        "--sender-profile-key",
                        sender_profile_key,
                        "--campaign-prompt-path",
                        campaign_prompt_path,
                    ]
                )

            result_data = run_worker_via_backend(requested_worker, args)

            artifact_payload = fetch_json(f"/artifacts?worker={quote_plus(requested_worker)}")
            artifacts = artifact_payload.get("artifacts", [])
            if not isinstance(artifacts, list):
                artifacts = []

            step_results.append(
                {
                    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "requested_worker": requested_worker,
                    "mapped_worker": requested_worker,
                    "result_data": result_data,
                    "artifacts": [str(item) for item in artifacts],
                    "artifact_links": _build_artifact_links([str(item) for item in artifacts]),
                }
            )

            if not result_data.get("ok"):
                break
    if user is not None:
        assign_draft_owner_for_new_records(before_draft_id, user.id)

    chosen_workers = command_plan.get("chosen_workers", [])
    if not isinstance(chosen_workers, list):
        chosen_workers = []

    result_summary = str(command_plan.get("result_hint", "")).strip()
    if step_results:
        step_ok = [bool(step["result_data"].get("ok")) for step in step_results]
        if all(step_ok):
            result_summary = (
                f"Executed {len(step_results)} step(s) successfully."
            )
        else:
            first_failed = next(
                (
                    str(step["result_data"].get("error", "")).strip()
                    for step in step_results
                    if not step["result_data"].get("ok")
                ),
                "",
            )
            result_summary = (
                f"Execution stopped after {len(step_results)} step(s). "
                f"{first_failed or 'A worker reported an error.'}"
            )
    elif not result_summary:
        result_summary = "No workers were executed."

    _add_interaction(
        command_text=command_text,
        steps=step_results,
        interpretation=str(command_plan.get("interpretation", "")).strip(),
        capability_status=str(command_plan.get("status_label", "Unsupported")).strip(),
        capability_status_key=str(command_plan.get("status_key", "unsupported")).strip(),
        capability_hint=str(command_plan.get("result_hint", "")).strip(),
        chosen_workers=[str(worker) for worker in chosen_workers],
        will_run=[str(item) for item in command_plan.get("will_run", [])],
        will_not_run=[str(item) for item in command_plan.get("will_not_run", [])],
        result_summary=result_summary,
        suggested_phrasing=[str(item) for item in command_plan.get("suggested_phrasing", [])],
        expected_outputs=get_output_paths_for_workers([str(worker) for worker in chosen_workers]),
        sender_profile_key=sender_profile_key,
        campaign_prompt_path=campaign_prompt_path,
    )
    return _render_index(
        request,
        "",
        None,
        sender_profile_key,
        campaign_prompt_path,
    )


@app.get("/runs", response_class=HTMLResponse)
async def runs(request: Request, n: int = 20) -> HTMLResponse:
    n = max(1, min(n, 200))
    history_data = fetch_json(f"/history?n={n}")
    latest_data = fetch_json("/latest")
    return templates.TemplateResponse(
        request,
        "runs.html",
        {
            "current_user": _current_user(request),
            "history_data": history_data,
            "latest_data": latest_data,
            "history_limit": n,
        },
    )


@app.get("/artifacts", response_class=HTMLResponse)
async def artifacts(request: Request, worker: str | None = None) -> HTMLResponse:
    if worker and worker.strip():
        encoded_worker = quote_plus(worker.strip())
        artifacts_data = fetch_json(f"/artifacts?worker={encoded_worker}")
    else:
        artifacts_data = fetch_json("/artifacts")

    return templates.TemplateResponse(
        request,
        "artifacts.html",
        {
            "current_user": _current_user(request),
            "artifacts_data": artifacts_data,
            "selected_worker": (worker or "").strip(),
        },
    )


@app.get("/files", response_class=HTMLResponse)
async def files(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(
        request,
        "files.html",
        {"current_user": _current_user(request), "files": _collect_workspace_files()},
    )


@app.get("/files/view", response_class=HTMLResponse)
async def file_view(request: Request, path: str = "") -> HTMLResponse:
    file_path = _safe_workspace_file(path)
    if file_path is None:
        return templates.TemplateResponse(
            request,
            "file_view.html",
            {
                "current_user": _current_user(request),
                "error": "Invalid or missing file path.",
                "relative_path": path,
                "file_kind": "unknown",
                "csv_header": [],
                "csv_rows": [],
                "csv_truncated": False,
                "text_preview": "",
                "text_truncated": False,
            },
        )

    rel_path = file_path.relative_to(WORKSPACE_DIR).as_posix()
    suffix = file_path.suffix.lower()
    if suffix == ".csv":
        csv_header: list[str] = []
        csv_rows: list[list[str]] = []
        csv_truncated = False
        with file_path.open("r", encoding="utf-8", newline="") as f:
            reader = csv.reader(f)
            csv_header = next(reader, [])
            for idx, row in enumerate(reader):
                if idx >= MAX_CSV_PREVIEW_ROWS:
                    csv_truncated = True
                    break
                csv_rows.append(row)

        return templates.TemplateResponse(
            request,
            "file_view.html",
            {
                "current_user": _current_user(request),
                "error": "",
                "relative_path": rel_path,
                "file_kind": "csv",
                "csv_header": csv_header,
                "csv_rows": csv_rows,
                "csv_truncated": csv_truncated,
                "text_preview": "",
                "text_truncated": False,
            },
        )

    text_preview = ""
    text_truncated = False
    if suffix in TEXT_PREVIEW_SUFFIXES:
        text_preview = file_path.read_text(encoding="utf-8", errors="ignore")
        if suffix == ".json":
            try:
                text_preview = json.dumps(json.loads(text_preview), ensure_ascii=False, indent=2)
            except json.JSONDecodeError:
                pass
        if len(text_preview) > MAX_TEXT_PREVIEW_CHARS:
            text_preview = text_preview[:MAX_TEXT_PREVIEW_CHARS]
            text_truncated = True

    return templates.TemplateResponse(
        request,
        "file_view.html",
        {
            "current_user": _current_user(request),
            "error": "",
            "relative_path": rel_path,
            "file_kind": "text" if suffix in TEXT_PREVIEW_SUFFIXES else "other",
            "csv_header": [],
            "csv_rows": [],
            "csv_truncated": False,
            "text_preview": text_preview,
            "text_truncated": text_truncated,
        },
    )


@app.get("/download")
async def download(path: str = "") -> FileResponse:
    file_path = _safe_workspace_file(path)
    if file_path is None:
        raise HTTPException(status_code=400, detail="Invalid or missing file path.")
    return FileResponse(
        path=str(file_path),
        filename=file_path.name,
        media_type="application/octet-stream",
    )
