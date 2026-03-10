from __future__ import annotations

import sqlite3
from pathlib import Path
from threading import Lock


BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "workspace" / "data"
DB_PATH = DATA_DIR / "assistant.db"

_INIT_LOCK = Lock()
_INITIALIZED = False


def _create_tables(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email TEXT NOT NULL UNIQUE,
            password_hash TEXT NOT NULL,
            created_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS leads (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            raw_title TEXT NOT NULL DEFAULT '',
            normalized_name TEXT NOT NULL DEFAULT '',
            domain TEXT NOT NULL DEFAULT '',
            website TEXT NOT NULL DEFAULT '',
            source TEXT NOT NULL DEFAULT '',
            query TEXT NOT NULL DEFAULT '',
            source_type TEXT NOT NULL DEFAULT '',
            entity_type TEXT NOT NULL DEFAULT '',
            extraction_source TEXT NOT NULL DEFAULT '',
            parent_source_url TEXT NOT NULL DEFAULT '',
            parent_entity_type TEXT NOT NULL DEFAULT '',
            child_profile_extracted INTEGER NOT NULL DEFAULT 0,
            quality_score REAL NOT NULL DEFAULT 0,
            realtor_confidence REAL NOT NULL DEFAULT 0,
            role_validation_reason TEXT NOT NULL DEFAULT '',
            validation_reason TEXT NOT NULL DEFAULT '',
            discovered_at TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'discovered'
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS contacts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            lead_id INTEGER NOT NULL,
            email TEXT NOT NULL DEFAULT '',
            phone TEXT NOT NULL DEFAULT '',
            source_page TEXT NOT NULL DEFAULT '',
            confidence REAL NOT NULL DEFAULT 0,
            FOREIGN KEY(lead_id) REFERENCES leads(id) ON DELETE CASCADE
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS drafts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            lead_id INTEGER NOT NULL,
            user_id INTEGER,
            email TEXT NOT NULL DEFAULT '',
            subject TEXT NOT NULL DEFAULT '',
            body TEXT NOT NULL DEFAULT '',
            status TEXT NOT NULL DEFAULT 'draft',
            created_at TEXT NOT NULL,
            approved_at TEXT NOT NULL DEFAULT '',
            sent_at TEXT NOT NULL DEFAULT '',
            sender_profile TEXT NOT NULL DEFAULT '',
            campaign_prompt TEXT NOT NULL DEFAULT '',
            FOREIGN KEY(lead_id) REFERENCES leads(id) ON DELETE CASCADE,
            FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE SET NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            worker TEXT NOT NULL,
            args TEXT NOT NULL DEFAULT '[]',
            status TEXT NOT NULL,
            started_at TEXT NOT NULL,
            finished_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS gmail_connections (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL UNIQUE,
            email TEXT NOT NULL DEFAULT '',
            access_token TEXT NOT NULL DEFAULT '',
            refresh_token TEXT NOT NULL DEFAULT '',
            token_expiry TEXT NOT NULL DEFAULT '',
            scope TEXT NOT NULL DEFAULT '',
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_contacts_lead_id ON contacts(lead_id)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_drafts_lead_id ON drafts(lead_id)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_drafts_status ON drafts(status)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_leads_status ON leads(status)"
    )


def _ensure_column(
    conn: sqlite3.Connection,
    table_name: str,
    column_name: str,
    column_definition: str,
) -> None:
    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    existing_columns = {str(row[1]) for row in rows}
    if column_name in existing_columns:
        return
    conn.execute(
        f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_definition}"
    )


def init_db() -> None:
    global _INITIALIZED
    if _INITIALIZED:
        return

    with _INIT_LOCK:
        if _INITIALIZED:
            return

        DATA_DIR.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(DB_PATH, timeout=30, check_same_thread=False)
        try:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA foreign_keys=ON")
            _create_tables(conn)
            _ensure_column(conn, "leads", "raw_title", "TEXT NOT NULL DEFAULT ''")
            _ensure_column(conn, "leads", "normalized_name", "TEXT NOT NULL DEFAULT ''")
            _ensure_column(conn, "leads", "source_type", "TEXT NOT NULL DEFAULT ''")
            _ensure_column(conn, "leads", "entity_type", "TEXT NOT NULL DEFAULT ''")
            _ensure_column(conn, "leads", "extraction_source", "TEXT NOT NULL DEFAULT ''")
            _ensure_column(conn, "leads", "parent_source_url", "TEXT NOT NULL DEFAULT ''")
            _ensure_column(conn, "leads", "parent_entity_type", "TEXT NOT NULL DEFAULT ''")
            _ensure_column(conn, "leads", "child_profile_extracted", "INTEGER NOT NULL DEFAULT 0")
            _ensure_column(conn, "leads", "quality_score", "REAL NOT NULL DEFAULT 0")
            _ensure_column(conn, "leads", "realtor_confidence", "REAL NOT NULL DEFAULT 0")
            _ensure_column(conn, "leads", "role_validation_reason", "TEXT NOT NULL DEFAULT ''")
            _ensure_column(conn, "leads", "validation_reason", "TEXT NOT NULL DEFAULT ''")
            _ensure_column(conn, "drafts", "sender_profile", "TEXT NOT NULL DEFAULT ''")
            _ensure_column(conn, "drafts", "campaign_prompt", "TEXT NOT NULL DEFAULT ''")
            _ensure_column(conn, "drafts", "user_id", "INTEGER")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_drafts_user_id ON drafts(user_id)")
            conn.commit()
            _INITIALIZED = True
        finally:
            conn.close()


def get_connection() -> sqlite3.Connection:
    init_db()
    conn = sqlite3.connect(DB_PATH, timeout=30, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    return conn
