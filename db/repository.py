from __future__ import annotations

import base64
import hashlib
import hmac
import json
import secrets
from datetime import datetime, timezone

from db.database import get_connection
from db.models import Contact, Draft, GmailConnection, Lead, Run, User


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def hash_password(password: str) -> str:
    normalized_password = (password or "").encode("utf-8")
    salt = secrets.token_bytes(16)
    digest = hashlib.pbkdf2_hmac("sha256", normalized_password, salt, 600_000)
    return "pbkdf2_sha256$600000$" + base64.b64encode(salt).decode("ascii") + "$" + base64.b64encode(digest).decode("ascii")


def verify_password(password: str, encoded_hash: str) -> bool:
    parts = (encoded_hash or "").split("$")
    if len(parts) != 4 or parts[0] != "pbkdf2_sha256":
        return False
    try:
        iterations = int(parts[1])
        salt = base64.b64decode(parts[2].encode("ascii"))
        expected = base64.b64decode(parts[3].encode("ascii"))
    except Exception:
        return False
    computed = hashlib.pbkdf2_hmac(
        "sha256",
        (password or "").encode("utf-8"),
        salt,
        max(1, iterations),
    )
    return hmac.compare_digest(computed, expected)


def create_user(email: str, password: str) -> User:
    normalized_email = (email or "").strip().lower()
    if not normalized_email:
        raise ValueError("Email is required.")
    if not (password or ""):
        raise ValueError("Password is required.")
    created_at = _utc_now()
    password_hash = hash_password(password)
    with get_connection() as conn:
        cur = conn.execute(
            """
            INSERT INTO users (email, password_hash, created_at)
            VALUES (?, ?, ?)
            """,
            (normalized_email, password_hash, created_at),
        )
        row = conn.execute("SELECT * FROM users WHERE id = ?", (cur.lastrowid,)).fetchone()
    if row is None:
        raise RuntimeError("Failed to create user")
    return User.from_row(row)


def get_user_by_email(email: str) -> User | None:
    normalized_email = (email or "").strip().lower()
    if not normalized_email:
        return None
    with get_connection() as conn:
        row = conn.execute("SELECT * FROM users WHERE email = ?", (normalized_email,)).fetchone()
    if row is None:
        return None
    return User.from_row(row)


def get_user_by_id(user_id: int) -> User | None:
    with get_connection() as conn:
        row = conn.execute("SELECT * FROM users WHERE id = ?", (int(user_id),)).fetchone()
    if row is None:
        return None
    return User.from_row(row)


def create_lead(
    name: str,
    domain: str,
    website: str,
    source: str,
    query: str,
    discovered_at: str | None = None,
    status: str = "discovered",
    raw_title: str = "",
    normalized_name: str = "",
    source_type: str = "",
    entity_type: str = "",
    extraction_source: str = "",
    parent_source_url: str = "",
    parent_entity_type: str = "",
    child_profile_extracted: int = 0,
    quality_score: float = 0.0,
    realtor_confidence: float = 0.0,
    role_validation_reason: str = "",
    validation_reason: str = "",
) -> Lead:
    timestamp = (discovered_at or "").strip() or _utc_now()
    with get_connection() as conn:
        cur = conn.execute(
            """
            INSERT INTO leads (
                name,
                raw_title,
                normalized_name,
                domain,
                website,
                source,
                query,
                source_type,
                entity_type,
                extraction_source,
                parent_source_url,
                parent_entity_type,
                child_profile_extracted,
                quality_score,
                realtor_confidence,
                role_validation_reason,
                validation_reason,
                discovered_at,
                status
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                (name or "").strip(),
                (raw_title or "").strip(),
                (normalized_name or "").strip(),
                (domain or "").strip(),
                (website or "").strip(),
                (source or "").strip(),
                (query or "").strip(),
                (source_type or "").strip(),
                (entity_type or "").strip(),
                (extraction_source or "").strip(),
                (parent_source_url or "").strip(),
                (parent_entity_type or "").strip(),
                int(child_profile_extracted or 0),
                float(quality_score or 0.0),
                float(realtor_confidence or 0.0),
                (role_validation_reason or "").strip(),
                (validation_reason or "").strip(),
                timestamp,
                (status or "").strip() or "discovered",
            ),
        )
        row = conn.execute("SELECT * FROM leads WHERE id = ?", (cur.lastrowid,)).fetchone()
    if row is None:
        raise RuntimeError("Failed to create lead record")
    return Lead.from_row(row)


def get_lead_by_id(lead_id: int) -> Lead | None:
    with get_connection() as conn:
        row = conn.execute("SELECT * FROM leads WHERE id = ?", (lead_id,)).fetchone()
    if row is None:
        return None
    return Lead.from_row(row)


def get_leads(
    status: str | None = None,
    query: str | None = None,
    limit: int = 500,
) -> list[Lead]:
    limit = max(1, min(int(limit), 5000))
    sql = "SELECT * FROM leads"
    clauses: list[str] = []
    params: list[str | int] = []

    normalized_status = (status or "").strip()
    if normalized_status:
        clauses.append("status = ?")
        params.append(normalized_status)

    normalized_query = (query or "").strip()
    if normalized_query:
        clauses.append("query = ?")
        params.append(normalized_query)

    if clauses:
        sql += " WHERE " + " AND ".join(clauses)

    sql += " ORDER BY id DESC LIMIT ?"
    params.append(limit)

    with get_connection() as conn:
        rows = conn.execute(sql, params).fetchall()
    return [Lead.from_row(row) for row in rows]


def update_lead_status(lead_id: int, status: str) -> None:
    normalized_status = (status or "").strip()
    if not normalized_status:
        return
    with get_connection() as conn:
        conn.execute("UPDATE leads SET status = ? WHERE id = ?", (normalized_status, lead_id))


def create_contact(
    lead_id: int,
    email: str,
    phone: str,
    source_page: str,
    confidence: float = 0.0,
) -> Contact:
    normalized_email = (email or "").strip()
    normalized_phone = (phone or "").strip()
    normalized_source_page = (source_page or "").strip()
    normalized_confidence = float(confidence or 0.0)

    with get_connection() as conn:
        existing = conn.execute(
            """
            SELECT * FROM contacts
            WHERE lead_id = ? AND email = ? AND phone = ? AND source_page = ?
            ORDER BY id DESC LIMIT 1
            """,
            (lead_id, normalized_email, normalized_phone, normalized_source_page),
        ).fetchone()
        if existing is not None:
            return Contact.from_row(existing)

        cur = conn.execute(
            """
            INSERT INTO contacts (lead_id, email, phone, source_page, confidence)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                int(lead_id),
                normalized_email,
                normalized_phone,
                normalized_source_page,
                normalized_confidence,
            ),
        )
        row = conn.execute("SELECT * FROM contacts WHERE id = ?", (cur.lastrowid,)).fetchone()
    if row is None:
        raise RuntimeError("Failed to create contact record")
    return Contact.from_row(row)


def get_contacts_by_lead(lead_id: int) -> list[Contact]:
    with get_connection() as conn:
        rows = conn.execute(
            "SELECT * FROM contacts WHERE lead_id = ? ORDER BY id DESC",
            (int(lead_id),),
        ).fetchall()
    return [Contact.from_row(row) for row in rows]


def create_draft(
    lead_id: int,
    email: str,
    subject: str,
    body: str,
    status: str = "draft",
    created_at: str | None = None,
    sender_profile: str = "",
    campaign_prompt: str = "",
    user_id: int | None = None,
) -> Draft:
    timestamp = (created_at or "").strip() or _utc_now()
    normalized_status = (status or "").strip() or "draft"
    with get_connection() as conn:
        cur = conn.execute(
            """
            INSERT INTO drafts (
                lead_id,
                user_id,
                email,
                subject,
                body,
                status,
                created_at,
                approved_at,
                sent_at,
                sender_profile,
                campaign_prompt
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, '', '', ?, ?)
            """,
            (
                int(lead_id),
                int(user_id) if user_id else None,
                (email or "").strip(),
                (subject or "").strip(),
                (body or "").strip(),
                normalized_status,
                timestamp,
                (sender_profile or "").strip(),
                (campaign_prompt or "").strip(),
            ),
        )
        row = conn.execute(
            """
            SELECT
                d.*,
                l.name AS lead_name,
                l.domain AS lead_domain,
                l.website AS lead_website
            FROM drafts d
            JOIN leads l ON l.id = d.lead_id
            WHERE d.id = ?
            """,
            (cur.lastrowid,),
        ).fetchone()
    if row is None:
        raise RuntimeError("Failed to create draft record")
    return Draft.from_row(row)


def get_draft_by_id(draft_id: int, user_id: int | None = None) -> Draft | None:
    with get_connection() as conn:
        if user_id is None:
            row = conn.execute(
                """
                SELECT
                    d.*,
                    l.name AS lead_name,
                    l.domain AS lead_domain,
                    l.website AS lead_website
                FROM drafts d
                JOIN leads l ON l.id = d.lead_id
                WHERE d.id = ?
                """,
                (int(draft_id),),
            ).fetchone()
        else:
            row = conn.execute(
                """
                SELECT
                    d.*,
                    l.name AS lead_name,
                    l.domain AS lead_domain,
                    l.website AS lead_website
                FROM drafts d
                JOIN leads l ON l.id = d.lead_id
                WHERE d.id = ? AND d.user_id = ?
                """,
                (int(draft_id), int(user_id)),
            ).fetchone()
    if row is None:
        return None
    return Draft.from_row(row)


def get_drafts(
    status: str | None = None,
    limit: int = 500,
    user_id: int | None = None,
) -> list[Draft]:
    limit = max(1, min(int(limit), 5000))
    sql = """
        SELECT
            d.*,
            l.name AS lead_name,
            l.domain AS lead_domain,
            l.website AS lead_website
        FROM drafts d
        JOIN leads l ON l.id = d.lead_id
    """
    params: list[str | int] = []
    normalized_status = (status or "").strip()
    clauses: list[str] = []
    if normalized_status:
        clauses.append("d.status = ?")
        params.append(normalized_status)
    if user_id is not None:
        clauses.append("d.user_id = ?")
        params.append(int(user_id))
    if clauses:
        sql += " WHERE " + " AND ".join(clauses)
    sql += " ORDER BY d.id DESC LIMIT ?"
    params.append(limit)

    with get_connection() as conn:
        rows = conn.execute(sql, params).fetchall()
    return [Draft.from_row(row) for row in rows]


def update_draft_status(
    draft_id: int,
    status: str,
    *,
    email: str | None = None,
    subject: str | None = None,
    body: str | None = None,
    user_id: int | None = None,
) -> Draft | None:
    draft = get_draft_by_id(draft_id, user_id=user_id)
    if draft is None:
        return None

    normalized_status = (status or "").strip() or draft.status
    normalized_email = draft.email if email is None else (email or "").strip()
    normalized_subject = draft.subject if subject is None else (subject or "").strip()
    normalized_body = draft.body if body is None else (body or "").strip()
    approved_at = draft.approved_at
    sent_at = draft.sent_at

    if normalized_status == "approved" and not approved_at:
        approved_at = _utc_now()
    if normalized_status == "sent" and not sent_at:
        sent_at = _utc_now()
    if normalized_status in {"draft", "rejected"}:
        sent_at = ""
        if normalized_status == "draft":
            approved_at = ""

    with get_connection() as conn:
        conn.execute(
            """
            UPDATE drafts
            SET email = ?, subject = ?, body = ?, status = ?, approved_at = ?, sent_at = ?
            WHERE id = ?
            """,
            (
                normalized_email,
                normalized_subject,
                normalized_body,
                normalized_status,
                approved_at,
                sent_at,
                int(draft_id),
            ),
        )

    return get_draft_by_id(draft_id, user_id=user_id)


def assign_draft_owner_for_new_records(min_id_exclusive: int, user_id: int) -> int:
    with get_connection() as conn:
        cur = conn.execute(
            """
            UPDATE drafts
            SET user_id = ?
            WHERE id > ? AND (user_id IS NULL OR user_id = 0)
            """,
            (int(user_id), int(min_id_exclusive)),
        )
    return int(cur.rowcount or 0)


def get_latest_draft_id() -> int:
    with get_connection() as conn:
        row = conn.execute("SELECT COALESCE(MAX(id), 0) AS max_id FROM drafts").fetchone()
    if row is None:
        return 0
    return int(row["max_id"] or 0)


def record_run(
    worker: str,
    args: list[str],
    status: str,
    started_at: str,
    finished_at: str,
) -> Run:
    normalized_args = json.dumps([str(item) for item in args], ensure_ascii=False)
    with get_connection() as conn:
        cur = conn.execute(
            """
            INSERT INTO runs (worker, args, status, started_at, finished_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                (worker or "").strip(),
                normalized_args,
                (status or "").strip(),
                (started_at or "").strip(),
                (finished_at or "").strip(),
            ),
        )
        row = conn.execute("SELECT * FROM runs WHERE id = ?", (cur.lastrowid,)).fetchone()
    if row is None:
        raise RuntimeError("Failed to record run")
    return Run.from_row(row)


def upsert_gmail_connection(
    *,
    user_id: int,
    email: str,
    access_token: str,
    refresh_token: str,
    token_expiry: str,
    scope: str,
) -> GmailConnection:
    now = _utc_now()
    with get_connection() as conn:
        existing = conn.execute(
            "SELECT id FROM gmail_connections WHERE user_id = ?",
            (int(user_id),),
        ).fetchone()
        if existing is None:
            cur = conn.execute(
                """
                INSERT INTO gmail_connections (
                    user_id, email, access_token, refresh_token, token_expiry, scope, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    int(user_id),
                    (email or "").strip(),
                    (access_token or "").strip(),
                    (refresh_token or "").strip(),
                    (token_expiry or "").strip(),
                    (scope or "").strip(),
                    now,
                    now,
                ),
            )
            row_id = int(cur.lastrowid)
        else:
            row_id = int(existing["id"])
            conn.execute(
                """
                UPDATE gmail_connections
                SET email = ?, access_token = ?, refresh_token = ?, token_expiry = ?, scope = ?, updated_at = ?
                WHERE id = ?
                """,
                (
                    (email or "").strip(),
                    (access_token or "").strip(),
                    (refresh_token or "").strip(),
                    (token_expiry or "").strip(),
                    (scope or "").strip(),
                    now,
                    row_id,
                ),
            )
        row = conn.execute("SELECT * FROM gmail_connections WHERE id = ?", (row_id,)).fetchone()
    if row is None:
        raise RuntimeError("Failed to upsert Gmail connection")
    return GmailConnection.from_row(row)


def get_gmail_connection_by_user(user_id: int) -> GmailConnection | None:
    with get_connection() as conn:
        row = conn.execute(
            "SELECT * FROM gmail_connections WHERE user_id = ?",
            (int(user_id),),
        ).fetchone()
    if row is None:
        return None
    return GmailConnection.from_row(row)
