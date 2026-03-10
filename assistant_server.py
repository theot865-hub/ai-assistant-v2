from __future__ import annotations

import argparse
import json
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

import run_worker
from run_history import append_run, read_recent
from run_state import (
    get_artifacts_for_worker,
    get_latest_artifacts_all_workers,
    get_latest_success_by_worker,
    update_run_state,
)
from scheduler import JOBS
from workers.registry import WORKER_REGISTRY


HOST = "127.0.0.1"
DEFAULT_PORT = 8787


class WorkerRequestHandler(BaseHTTPRequestHandler):
    def _send_json(self, status: int, payload: dict) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self) -> None:
        parsed_url = urlparse(self.path)
        if parsed_url.path != "/run":
            self._send_json(
                404,
                {"ok": False, "worker": "", "result": "", "error": "Not found"},
            )
            return

        started_at = time.time()
        worker_for_log = ""
        args_for_log: list[str] = []

        try:
            content_length = int(self.headers.get("Content-Length", "0"))
        except ValueError:
            error = "Invalid Content-Length header"
            append_run(worker_for_log, args_for_log, False, "", error, time.time() - started_at)
            update_run_state(worker_for_log, False, "", error)
            self._send_json(
                400,
                {
                    "ok": False,
                    "worker": "",
                    "result": "",
                    "error": error,
                },
            )
            return

        raw_body = self.rfile.read(content_length) if content_length > 0 else b"{}"
        try:
            payload = json.loads(raw_body.decode("utf-8"))
        except Exception:
            error = "Invalid JSON body"
            append_run(worker_for_log, args_for_log, False, "", error, time.time() - started_at)
            update_run_state(worker_for_log, False, "", error)
            self._send_json(
                400,
                {
                    "ok": False,
                    "worker": "",
                    "result": "",
                    "error": error,
                },
            )
            return

        worker = payload.get("worker")
        args = payload.get("args", [])
        if not isinstance(worker, str) or not worker.strip():
            error = "Field 'worker' must be a non-empty string"
            append_run(worker_for_log, args_for_log, False, "", error, time.time() - started_at)
            update_run_state(worker_for_log, False, "", error)
            self._send_json(
                400,
                {
                    "ok": False,
                    "worker": "",
                    "result": "",
                    "error": error,
                },
            )
            return
        worker_for_log = worker.strip()
        if not isinstance(args, list) or any(not isinstance(item, str) for item in args):
            error = "Field 'args' must be an array of strings"
            append_run(worker_for_log, args_for_log, False, "", error, time.time() - started_at)
            update_run_state(worker_for_log, False, "", error)
            self._send_json(
                400,
                {
                    "ok": False,
                    "worker": worker_for_log,
                    "result": "",
                    "error": error,
                },
            )
            return
        args_for_log = args

        status_code = 200
        try:
            ok, dispatch_result = run_worker.dispatch_worker(worker_for_log, args_for_log)
            result = dispatch_result if ok else ""
            error = "" if ok else dispatch_result
        except Exception as exc:
            ok = False
            result = ""
            error = str(exc)
            status_code = 500

        append_run(worker_for_log, args_for_log, ok, result, error, time.time() - started_at)
        update_run_state(worker_for_log, ok, result, error)
        self._send_json(
            status_code,
            {
                "ok": ok,
                "worker": worker_for_log,
                "result": result,
                "error": error,
            },
        )

    def do_GET(self) -> None:
        parsed_url = urlparse(self.path)

        if parsed_url.path == "/health":
            self._send_json(200, {"ok": True, "status": "running"})
            return

        if parsed_url.path == "/workers":
            self._send_json(200, {"ok": True, "workers": list(WORKER_REGISTRY.keys())})
            return

        if parsed_url.path == "/jobs":
            self._send_json(200, {"ok": True, "jobs": JOBS})
            return

        if parsed_url.path == "/history":
            raw_n = parse_qs(parsed_url.query).get("n", ["20"])[0]
            try:
                n = int(raw_n)
            except ValueError:
                n = 20
            self._send_json(200, {"ok": True, "history": read_recent(n)})
            return

        if parsed_url.path == "/latest":
            self._send_json(200, {"ok": True, "latest": get_latest_success_by_worker()})
            return

        if parsed_url.path == "/artifacts":
            worker = parse_qs(parsed_url.query).get("worker", [""])[0].strip()
            if worker:
                self._send_json(
                    200,
                    {"ok": True, "worker": worker, "artifacts": get_artifacts_for_worker(worker)},
                )
                return
            self._send_json(200, {"ok": True, "artifacts": get_latest_artifacts_all_workers()})
            return

        self._send_json(404, {"ok": False, "error": "Not found"})

    def log_message(self, format: str, *args: object) -> None:
        # Keep server output minimal.
        return


def main() -> int:
    parser = argparse.ArgumentParser(description="Local worker HTTP server")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    cli_args = parser.parse_args()

    server = ThreadingHTTPServer((HOST, cli_args.port), WorkerRequestHandler)
    print(f"Worker server listening on http://{HOST}:{cli_args.port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
