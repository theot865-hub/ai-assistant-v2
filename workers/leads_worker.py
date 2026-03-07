from __future__ import annotations

import run_lead_pipeline_cli


def run() -> str:
    code = run_lead_pipeline_cli.main()
    if code == 0:
        return "leads worker completed successfully"
    return f"leads worker failed (exit code {code})"

