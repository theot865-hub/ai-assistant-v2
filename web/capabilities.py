from __future__ import annotations

from typing import Any


STATUS_SUPPORTED = "supported"
STATUS_PARTIAL = "partial"
STATUS_UNSUPPORTED = "unsupported"

STATUS_LABELS = {
    STATUS_SUPPORTED: "Supported and runnable",
    STATUS_PARTIAL: "Partially supported",
    STATUS_UNSUPPORTED: "Unsupported",
}

WORKER_ORDER = [
    "leads",
    "research",
    "outreach",
    "enrichment",
    "pipeline",
    "business_discovery",
    "business_enrichment",
    "business_outreach",
]

CAPABILITY_MANIFEST: dict[str, dict[str, Any]] = {
    "leads": {
        "what_it_does": "Builds the core realtor lead CSV using local pipeline sources.",
        "required_inputs": ["No command args required."],
        "outputs": ["workspace/leads/all_realtor_leads.csv"],
        "limitations": ["Focused on realtor lead flow, not arbitrary industries."],
        "examples": ["run leads", "generate realtor leads"],
    },
    "research": {
        "what_it_does": "Runs web research for a query and writes a short result summary.",
        "required_inputs": ["A text query."],
        "outputs": ["workspace/jobs/latest_research.txt"],
        "limitations": ["Uses top web results only; not deep crawling."],
        "examples": ["research victoria roofing market", "research local competitors"],
    },
    "outreach": {
        "what_it_does": "Drafts realtor outreach messages from existing realtor leads.",
        "required_inputs": ["Optional input CSV path; defaults to realtor leads CSV."],
        "outputs": ["workspace/leads/all_realtor_outreach.csv"],
        "limitations": ["Drafts messages only; does not send emails."],
        "examples": ["run outreach", "draft outreach for realtor leads"],
    },
    "enrichment": {
        "what_it_does": "Extracts website/email/phone from existing realtor lead profile pages.",
        "required_inputs": ["Optional input CSV path; defaults to realtor leads CSV."],
        "outputs": ["workspace/leads/all_realtor_leads_enriched.csv"],
        "limitations": ["Only parses publicly reachable pages; fields may remain blank."],
        "examples": ["run enrichment", "enrich realtor leads"],
    },
    "pipeline": {
        "what_it_does": "Runs business discovery, enrichment, and outreach drafting in sequence.",
        "required_inputs": ["Optional business query (used by discovery stage)."],
        "outputs": [
            "workspace/leads/business_discovery.csv",
            "workspace/leads/business_discovery_enriched.csv",
            "workspace/leads/business_discovery_outreach.csv",
        ],
        "limitations": ["Uses the current local business worker sequence and defaults."],
        "examples": ["run pipeline", "run pipeline for victoria businesses"],
    },
    "business_discovery": {
        "what_it_does": "Finds businesses from web search and stores title/url/snippet rows.",
        "required_inputs": ["A business discovery query (niche + location)."],
        "outputs": ["workspace/leads/business_discovery.csv"],
        "limitations": ["Discovery quality depends on search result quality."],
        "examples": ["find roofing companies in victoria", "find local plumbing businesses"],
    },
    "business_enrichment": {
        "what_it_does": "Visits discovered business URLs and extracts domain/email/phone when available.",
        "required_inputs": ["Optional input CSV path; defaults to business discovery CSV."],
        "outputs": ["workspace/leads/business_discovery_enriched.csv"],
        "limitations": [
            "No login/paywall bypass.",
            "No guarantee every site exposes contact details.",
        ],
        "examples": [
            "find roofing companies and get contact info",
            "run business enrichment",
        ],
    },
    "business_outreach": {
        "what_it_does": "Generates outreach draft messages for enriched business leads.",
        "required_inputs": ["Optional input CSV path; defaults to enriched business CSV."],
        "outputs": ["workspace/leads/business_discovery_outreach.csv"],
        "limitations": ["Creates drafts only; does not send email or book meetings."],
        "examples": ["draft outreach for discovered businesses", "run business outreach"],
    },
}

SYSTEM_CAN_DO = [
    "Discover local businesses by query and save lead tables.",
    "Enrich discovered leads with basic public contact fields.",
    "Draft outreach messages from enriched leads.",
    "Run a realtor-oriented full pipeline.",
    "Run research summaries for explicit queries.",
]

SYSTEM_CANNOT_DO_YET = [
    "Send emails or messages through ESPs/inboxes.",
    "Book meetings or schedule calendar events.",
    "Guarantee contact data for every discovered business.",
    "Perform unrestricted scraping of any website.",
]

HOME_EXAMPLES = [
    "find roofing companies in victoria",
    "find roofing companies and get contact info",
    "find roofing companies and draft outreach",
    "run pipeline for victoria local businesses",
    "research local pressure washing competitors in victoria",
]

OUTPUT_LOCATIONS = [
    "workspace/data/assistant.db",
    "workspace/leads/",
    "workspace/jobs/",
    "workspace/logs/",
]


def get_ordered_manifest() -> list[dict[str, Any]]:
    manifest_items: list[dict[str, Any]] = []
    for worker in WORKER_ORDER:
        details = CAPABILITY_MANIFEST.get(worker)
        if not details:
            continue
        manifest_items.append({"worker": worker, **details})
    return manifest_items


def get_output_paths_for_workers(workers: list[str]) -> list[str]:
    paths: list[str] = []
    for worker in workers:
        details = CAPABILITY_MANIFEST.get(worker, {})
        for output in details.get("outputs", []):
            if output not in paths:
                paths.append(output)
    return paths


def build_capabilities_page_context() -> dict[str, Any]:
    return {
        "manifest_items": get_ordered_manifest(),
        "system_can_do": SYSTEM_CAN_DO,
        "system_cannot_do_yet": SYSTEM_CANNOT_DO_YET,
        "example_commands": HOME_EXAMPLES,
        "output_locations": OUTPUT_LOCATIONS,
    }


def _contains_any(text: str, phrases: tuple[str, ...]) -> bool:
    return any(phrase in text for phrase in phrases)


def _append_unique(items: list[str], value: str) -> None:
    if value not in items:
        items.append(value)


def _is_business_discovery_intent(text: str) -> bool:
    if _contains_any(
        text,
        (
            "find businesses",
            "find business",
            "find companies",
            "find company",
            "business discovery",
        ),
    ):
        return True

    business_nouns = (
        "business",
        "businesses",
        "company",
        "companies",
        "roofing",
        "plumbing",
        "hvac",
        "contractor",
        "restaurant",
        "shop",
        "agency",
    )
    return "find " in text and any(noun in text for noun in business_nouns)


def _extract_suffix(full_text: str, keyword: str) -> str:
    lowered = full_text.lower()
    idx = lowered.find(keyword)
    if idx < 0:
        return ""
    suffix = full_text[idx + len(keyword) :].strip()
    return suffix


def classify_command(command_text: str, available_workers: set[str] | None = None) -> dict[str, Any]:
    raw = (command_text or "").strip()
    lowered = raw.lower()
    workflow: list[dict[str, Any]] = []
    will_run: list[str] = []
    will_not_run: list[str] = []
    suggested_phrasing: list[str] = []

    knows_available_workers = bool(available_workers)
    available = set(available_workers or set())

    def add_step(worker: str, args: list[str], summary: str) -> None:
        for existing in workflow:
            if existing["requested_worker"] == worker:
                return
        workflow.append({"requested_worker": worker, "args": args})
        will_run.append(summary)

    has_contact_request = _contains_any(
        lowered,
        (
            "contact info",
            "contact details",
            "get contact",
            "emails",
            "email addresses",
            "phone",
            "phone numbers",
        ),
    )
    has_outreach_request = _contains_any(
        lowered,
        (
            "outreach",
            "draft outreach",
            "draft email",
            "reach out",
            "message them",
            "email them",
            "contact them",
        ),
    )
    has_send_email_request = _contains_any(
        lowered,
        (
            "send email",
            "send emails",
            "send outreach",
            "actually send",
        ),
    )
    has_meeting_request = _contains_any(
        lowered,
        (
            "book meeting",
            "book meetings",
            "schedule meeting",
            "schedule meetings",
            "set appointment",
            "book appointment",
        ),
    )
    has_scrape_request = "scrape" in lowered
    has_scrape_any_website = _contains_any(
        lowered,
        (
            "scrape any website",
            "scrape all websites",
            "any website",
        ),
    )

    interpretation = "Command did not map to a known runnable workflow."

    if "pipeline" in lowered:
        pipeline_query = _extract_suffix(raw, "pipeline for")
        args = [pipeline_query] if pipeline_query else []
        add_step("pipeline", args, "Run the full pipeline workflow.")
        interpretation = "Run the full pipeline."
    elif _is_business_discovery_intent(lowered):
        add_step(
            "business_discovery",
            [raw],
            f'Run business discovery for query: "{raw}".',
        )
        if has_contact_request or has_outreach_request or has_send_email_request:
            add_step(
                "business_enrichment",
                [],
                "Run business enrichment to extract public contact details.",
            )
        if has_outreach_request or has_send_email_request:
            add_step(
                "business_outreach",
                [],
                "Run business outreach to draft messages.",
            )
        if has_contact_request:
            interpretation = "Find businesses and enrich contact details."
        elif has_outreach_request or has_send_email_request:
            interpretation = "Find businesses, enrich contacts, and draft outreach."
        else:
            interpretation = "Find businesses with the requested query."
    else:
        if _contains_any(lowered, ("run leads", "generate leads")) or lowered == "leads":
            add_step("leads", [], "Run leads worker.")
            interpretation = "Generate realtor leads."
        elif "business enrichment" in lowered:
            add_step("business_enrichment", [], "Run business enrichment.")
            interpretation = "Enrich previously discovered businesses."
        elif "business outreach" in lowered:
            add_step("business_outreach", [], "Run business outreach message drafting.")
            interpretation = "Draft outreach for enriched businesses."
        elif _contains_any(lowered, ("run enrichment", "enrich leads")):
            add_step("enrichment", [], "Run realtor enrichment.")
            interpretation = "Enrich realtor leads with contact details."
        elif _contains_any(lowered, ("run outreach", "draft outreach")):
            add_step("outreach", [], "Run realtor outreach drafting.")
            interpretation = "Draft outreach for realtor leads."
        elif _contains_any(lowered, ("research", "look up", "investigate")):
            add_step("research", [raw], f'Run research for query: "{raw}".')
            interpretation = "Run web research summary."
        elif has_send_email_request:
            add_step("outreach", [], "Draft outreach messages from existing leads.")
            interpretation = "Draft outreach only (no delivery)."
            _append_unique(
                suggested_phrasing,
                "draft outreach messages for existing leads",
            )

    if has_send_email_request:
        _append_unique(
            will_not_run,
            "Actual email delivery is not supported yet; outreach workers only draft messages.",
        )
        _append_unique(
            suggested_phrasing,
            "find roofing companies and draft outreach",
        )

    if has_meeting_request:
        _append_unique(
            will_not_run,
            "Booking or scheduling meetings is not supported yet.",
        )
        _append_unique(
            suggested_phrasing,
            "find roofing companies and get contact info",
        )

    if has_scrape_request:
        _append_unique(
            will_not_run,
            "Arbitrary scraping of any website is not supported (no auth/paywall bypass, no unrestricted crawling).",
        )
        _append_unique(
            suggested_phrasing,
            "find roofing companies in victoria and get contact info",
        )
        if has_scrape_any_website:
            interpretation = "Attempt broad website scraping."

    requested_workers = [step["requested_worker"] for step in workflow]
    runnable_workflow = list(workflow)
    if knows_available_workers:
        missing_workers = [worker for worker in requested_workers if worker not in available]
        for worker in missing_workers:
            _append_unique(
                will_not_run,
                f'Worker "{worker}" is not available on the backend.',
            )
        runnable_workflow = [
            step for step in workflow if step["requested_worker"] in available
        ]

    chosen_workers = [step["requested_worker"] for step in runnable_workflow]
    if workflow and not chosen_workers:
        _append_unique(will_run, "No workers will run until backend worker availability is fixed.")

    if not workflow:
        if has_scrape_request:
            status_key = STATUS_PARTIAL
            _append_unique(
                will_run,
                "No worker runs for this exact command; provide a niche/location query to run business discovery.",
            )
        else:
            status_key = STATUS_UNSUPPORTED
    else:
        status_key = STATUS_PARTIAL if will_not_run else STATUS_SUPPORTED

    if status_key == STATUS_UNSUPPORTED and not suggested_phrasing:
        suggested_phrasing.extend(HOME_EXAMPLES[:3])

    runnable = bool(chosen_workers) and status_key in {STATUS_SUPPORTED, STATUS_PARTIAL}
    result_hint = ""
    if status_key == STATUS_SUPPORTED:
        result_hint = "Command is supported and will run now."
    elif status_key == STATUS_PARTIAL:
        result_hint = "Command is partially supported. Supported parts can run."
    else:
        result_hint = "Command is unsupported. No workflow will run."

    return {
        "interpretation": interpretation,
        "status_key": status_key,
        "status_label": STATUS_LABELS[status_key],
        "workflow": runnable_workflow,
        "requested_workers": requested_workers,
        "chosen_workers": chosen_workers,
        "will_run": will_run,
        "will_not_run": will_not_run,
        "suggested_phrasing": suggested_phrasing,
        "result_hint": result_hint,
        "runnable": runnable,
    }
