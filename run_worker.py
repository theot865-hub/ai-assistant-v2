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
        if worker_name == "business_outreach":
            input_path: str | None = None
            sender_profile_key: str | None = None
            campaign_prompt_path: str | None = None
            idx = 0
            while idx < len(worker_args):
                token = worker_args[idx].strip()
                if token == "--input-path" and idx + 1 < len(worker_args):
                    input_path = worker_args[idx + 1].strip() or None
                    idx += 2
                    continue
                if token == "--sender-profile-key" and idx + 1 < len(worker_args):
                    sender_profile_key = worker_args[idx + 1].strip() or None
                    idx += 2
                    continue
                if token == "--campaign-prompt-path" and idx + 1 < len(worker_args):
                    campaign_prompt_path = worker_args[idx + 1].strip() or None
                    idx += 2
                    continue
                if "=" in token:
                    key, value = token.split("=", 1)
                    normalized_key = key.strip().lower()
                    normalized_value = value.strip()
                    if normalized_key == "input_path":
                        input_path = normalized_value or None
                    elif normalized_key == "sender_profile_key":
                        sender_profile_key = normalized_value or None
                    elif normalized_key == "campaign_prompt_path":
                        campaign_prompt_path = normalized_value or None
                    idx += 1
                    continue
                if input_path is None and token:
                    input_path = token
                idx += 1

            return True, runner(
                input_path=input_path,
                sender_profile_key=sender_profile_key,
                campaign_prompt_path=campaign_prompt_path,
            )

        if worker_name in {
            "research",
            "outreach",
            "pipeline",
            "enrichment",
            "business_discovery",
            "business_enrichment",
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
