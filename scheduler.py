from __future__ import annotations

import argparse
import sys
import time
from typing import Any

from run_history import append_run
from run_worker import dispatch_worker


JOBS: list[dict[str, Any]] = [
    {
        "name": "daily_pipeline",
        "worker": "pipeline",
        "args": ["victoria pressure washing businesses"],
        "interval_seconds": 86400,
    }
]


def _now_str() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())


def _log(message: str) -> None:
    print(f"[{_now_str()}] {message}", flush=True)


def run_scheduler(test_seconds: int | None = None, poll_seconds: float = 1.0) -> int:
    now = time.time()
    next_run_at = {job["name"]: now for job in JOBS}
    end_at = (now + test_seconds) if test_seconds is not None else None

    if test_seconds is None:
        _log("Scheduler started.")
    else:
        _log(f"Scheduler started in test mode for {test_seconds} seconds.")

    while True:
        current = time.time()
        if end_at is not None and current >= end_at:
            _log("Scheduler stopped (test mode complete).")
            return 0

        for job in JOBS:
            name = str(job.get("name", "")).strip()
            worker = str(job.get("worker", "")).strip()
            args = job.get("args", [])
            interval = int(job.get("interval_seconds", 60))

            if not isinstance(args, list):
                args = []
            args = [str(item) for item in args]

            due_at = next_run_at.get(name, current)
            if current < due_at:
                continue

            _log(f"Job start: {name} worker={worker} args={args}")
            started = time.time()
            try:
                original_argv = sys.argv[:]
                try:
                    # Prevent scheduler CLI args from leaking into worker internals.
                    sys.argv = [original_argv[0]]
                    ok, result = dispatch_worker(worker, args)
                finally:
                    sys.argv = original_argv
                status = "ok" if ok else "error"
                elapsed = time.time() - started
                result_text = result if ok else ""
                error_text = "" if ok else result
                append_run(worker, args, ok, result_text, error_text, elapsed)
                _log(f"Job end: {name} status={status} duration={elapsed:.1f}s result={result}")
            except Exception as exc:
                elapsed = time.time() - started
                append_run(worker, args, False, "", str(exc), elapsed)
                _log(f"Job end: {name} status=exception duration={elapsed:.1f}s error={exc}")

            next_run_at[name] = time.time() + max(1, interval)

        time.sleep(max(0.1, poll_seconds))


def main() -> int:
    parser = argparse.ArgumentParser(description="In-process worker scheduler")
    parser.add_argument(
        "--test-seconds",
        type=int,
        default=None,
        help="Run scheduler in test mode for a fixed number of seconds.",
    )
    parser.add_argument(
        "--poll-seconds",
        type=float,
        default=1.0,
        help="Loop sleep interval in seconds.",
    )
    args = parser.parse_args()
    return run_scheduler(test_seconds=args.test_seconds, poll_seconds=args.poll_seconds)


if __name__ == "__main__":
    raise SystemExit(main())
