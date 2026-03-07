from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


BASE_DIR = Path(__file__).resolve().parent
HISTORY_PATH = BASE_DIR / "workspace" / "logs" / "run_history.jsonl"


def append_run(
    worker: str,
    args: list[str],
    ok: bool,
    result: str,
    error: str,
    duration_seconds: float,
) -> None:
    HISTORY_PATH.parent.mkdir(parents=True, exist_ok=True)
    entry = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "worker": worker,
        "args": args,
        "ok": ok,
        "result": result,
        "error": error,
        "duration_seconds": round(float(duration_seconds), 3),
    }
    with HISTORY_PATH.open("a", encoding="utf-8") as f:
        f.write(json.dumps(entry, ensure_ascii=False) + "\n")


def read_recent(n: int = 20) -> list[dict[str, Any]]:
    limit = max(1, int(n))
    if not HISTORY_PATH.exists():
        return []

    lines = HISTORY_PATH.read_text(encoding="utf-8").splitlines()
    entries: list[dict[str, Any]] = []
    for line in lines[-limit:]:
        try:
            parsed = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(parsed, dict):
            entries.append(parsed)
    return entries

