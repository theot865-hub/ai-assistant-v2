from pathlib import Path
import subprocess
import sqlite3
import json
from ddgs import DDGS

BASE_DIR = Path(__file__).resolve().parent / "workspace"
BASE_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH = BASE_DIR / "memory.sqlite3"

def _db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""CREATE TABLE IF NOT EXISTS kv (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL
    )""")
    conn.execute("""CREATE TABLE IF NOT EXISTS chatlog (
        ts DATETIME DEFAULT CURRENT_TIMESTAMP,
        role TEXT NOT NULL,
        content TEXT NOT NULL
    )""")
    conn.commit()
    return conn

def write_file(rel_path: str, content: str) -> str:
    p = (BASE_DIR / rel_path).resolve()
    if not str(p).startswith(str(BASE_DIR.resolve())):
        return "Blocked: invalid path."
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(content, encoding="utf-8")
    return f"Wrote {p}"

def read_file(rel_path: str) -> str:
    p = (BASE_DIR / rel_path).resolve()
    if not str(p).startswith(str(BASE_DIR.resolve())):
        return "Blocked: invalid path."
    if not p.exists():
        return "File not found"
    return p.read_text(encoding="utf-8")

def run_safe_cmd(cmd: list[str]) -> str:
    ALLOW = {"ls", "pwd", "whoami", "date"}
    if not cmd or cmd[0] not in ALLOW:
        return "Blocked: command not allowed."
    out = subprocess.run(cmd, capture_output=True, text=True)
    return (out.stdout + out.stderr).strip()

def memory_set(key: str, value: str) -> str:
    conn = _db()
    conn.execute(
        "INSERT INTO kv(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (key, value),
    )
    conn.commit()
    conn.close()
    return f"Saved memory: {key}"

def memory_get(key: str) -> str:
    conn = _db()
    cur = conn.execute("SELECT value FROM kv WHERE key=?", (key,))
    row = cur.fetchone()
    conn.close()
    return row[0] if row else "(no value)"

def memory_list() -> str:
    conn = _db()
    cur = conn.execute("SELECT key, value FROM kv ORDER BY key")
    rows = cur.fetchall()
    conn.close()
    if not rows:
        return "(empty)"
    return "\n".join([f"{k} = {v}" for k, v in rows])

def log(role: str, content: str) -> str:
    conn = _db()
    conn.execute("INSERT INTO chatlog(role, content) VALUES(?,?)", (role, content))
    conn.commit()
    conn.close()
    return "logged"

def search_web(query: str, max_results: int = 5) -> str:
    max_results = max(1, min(int(max_results), 8))
    results = []
    with DDGS() as ddgs:
        for r in ddgs.text(query, max_results=max_results):
            results.append({
                "title": r.get("title"),
                "url": r.get("href") or r.get("url"),
                "snippet": r.get("body") or r.get("snippet"),
            })
    return json.dumps(results, ensure_ascii=False, indent=2)