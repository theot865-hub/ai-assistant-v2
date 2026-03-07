from __future__ import annotations

import sys

from workers.registry import WORKER_REGISTRY


def dispatch_worker(worker: str, args: list[str] | None = None) -> tuple[bool, str]:
    worker_name = (worker or "").strip()
    worker_args = args or []
    runner = WORKER_REGISTRY.get(worker_name)
    if runner is None:
        return False, f"Unknown worker: {worker_name}"

    original_argv = sys.argv[:]
    try:
        # Prevent outer CLIs (server/scheduler) from leaking args into worker internals.
        sys.argv = [original_argv[0]]
        if worker_name in {
            "research",
            "outreach",
            "pipeline",
            "enrichment",
            "business_discovery",
            "business_enrichment",
            "business_outreach",
        }:
            value = " ".join(worker_args).strip() or None
            return True, runner(value)

        return True, runner()
    finally:
        sys.argv = original_argv


def main() -> int:
    worker = sys.argv[1] if len(sys.argv) > 1 else ""
    ok, result = dispatch_worker(worker, sys.argv[2:])
    print(result)
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
