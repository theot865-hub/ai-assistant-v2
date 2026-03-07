from __future__ import annotations

import csv
import json
import os
from collections import deque
from datetime import datetime
from pathlib import Path
from threading import Lock
from urllib.error import URLError
from urllib.parse import quote_plus
from urllib.request import Request as URLRequest, urlopen

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates


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

app = FastAPI(title="Assistant v2 Web")
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))
INTERACTION_HISTORY: deque[dict] = deque(maxlen=MAX_CHAT_INTERACTIONS)
INTERACTION_LOCK = Lock()


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


def _get_backend_workers() -> set[str]:
    workers_data = fetch_json("/workers")
    if not workers_data.get("ok"):
        return set()
    workers = workers_data.get("workers", [])
    if not isinstance(workers, list):
        return set()
    return {str(item).strip() for item in workers if str(item).strip()}


def _resolve_worker(requested_worker: str, backend_workers: set[str]) -> str:
    if requested_worker in backend_workers:
        return requested_worker
    return requested_worker


def map_command_to_workflow(command: str) -> list[dict]:
    text = (command or "").strip()
    lowered = text.lower()

    has_realtor_pipeline = ("pipeline" in lowered) or ("realtor" in lowered)
    has_find_businesses = (
        ("find businesses" in lowered)
        or ("find companies" in lowered)
        or ("business discovery" in lowered)
    )
    has_contact = any(
        phrase in lowered
        for phrase in ["contact", "emails", "phones", "get contact info"]
    )
    has_outreach = any(
        phrase in lowered
        for phrase in ["outreach", "message", "email them"]
    )

    if has_realtor_pipeline:
        return [{"requested_worker": "pipeline", "args": []}]

    if has_find_businesses:
        steps: list[dict] = [{"requested_worker": "business_discovery", "args": [text]}]
        if has_contact or has_outreach:
            steps.append({"requested_worker": "business_enrichment", "args": []})
        if has_outreach:
            steps.append({"requested_worker": "business_outreach", "args": []})
        return steps

    if "leads" in lowered:
        return [{"requested_worker": "leads", "args": []}]
    if "outreach" in lowered:
        return [{"requested_worker": "outreach", "args": []}]
    return [{"requested_worker": "research", "args": [text]}]


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


def _add_interaction(command_text: str, steps: list[dict]) -> None:
    interaction = {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "command_text": command_text,
        "steps": steps,
    }
    with INTERACTION_LOCK:
        INTERACTION_HISTORY.append(interaction)


@app.get("/", response_class=HTMLResponse)
async def index(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(
        request,
        "index.html",
        {
            "backend_url": BACKEND_URL,
            "command_text": "",
            "interactions": _get_interactions(),
            "error_data": None,
        },
    )


@app.post("/command", response_class=HTMLResponse)
async def command(request: Request) -> HTMLResponse:
    form = await request.form()
    command_text = str(form.get("command", "")).strip()
    if not command_text:
        result_data = {"ok": False, "error": "Please enter a command."}
        return templates.TemplateResponse(
            request,
            "index.html",
            {
                "backend_url": BACKEND_URL,
                "command_text": command_text,
                "interactions": _get_interactions(),
                "error_data": result_data,
            },
        )

    backend_workers = _get_backend_workers()
    workflow_steps = map_command_to_workflow(command_text)
    step_results: list[dict] = []

    for step in workflow_steps:
        requested_worker = str(step.get("requested_worker", "")).strip()
        args = step.get("args", [])
        if not isinstance(args, list):
            args = []
        args = [str(item) for item in args]

        mapped_worker = _resolve_worker(requested_worker, backend_workers)
        result_data = run_worker_via_backend(mapped_worker, args)

        artifact_payload = fetch_json(f"/artifacts?worker={quote_plus(mapped_worker)}")
        artifacts = artifact_payload.get("artifacts", [])
        if not isinstance(artifacts, list):
            artifacts = []

        step_results.append(
            {
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "requested_worker": requested_worker,
                "mapped_worker": mapped_worker,
                "result_data": result_data,
                "artifacts": [str(item) for item in artifacts],
                "artifact_links": _build_artifact_links([str(item) for item in artifacts]),
            }
        )

        if not result_data.get("ok"):
            break

    _add_interaction(command_text, step_results)
    return templates.TemplateResponse(
        request,
        "index.html",
        {
            "backend_url": BACKEND_URL,
            "command_text": "",
            "interactions": _get_interactions(),
            "error_data": None,
        },
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
        {"artifacts_data": artifacts_data, "selected_worker": (worker or "").strip()},
    )


@app.get("/files", response_class=HTMLResponse)
async def files(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(
        request,
        "files.html",
        {"files": _collect_workspace_files()},
    )


@app.get("/files/view", response_class=HTMLResponse)
async def file_view(request: Request, path: str = "") -> HTMLResponse:
    file_path = _safe_workspace_file(path)
    if file_path is None:
        return templates.TemplateResponse(
            request,
            "file_view.html",
            {
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
