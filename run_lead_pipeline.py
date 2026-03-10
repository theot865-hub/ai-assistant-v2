from __future__ import annotations

from collections import Counter

from merge_leads import OUTPUT_PATH as MERGED_OUTPUT_PATH
from merge_leads import main as merge_main
from rankmyagent_scraper import main as rankmyagent_main
from rew_scraper import main as rew_main
from validate_leads import has_blocking_errors, print_report, validate_rows, load_rows


def source_counts(rows: list[dict[str, str]]) -> Counter[str]:
    counts: Counter[str] = Counter()
    for row in rows:
        counts[row.get("Source", "").strip() or "(unknown)"] += 1
    return counts


def main() -> int:
    rankmyagent_main()
    rew_main()
    merge_main()

    merged_rows = load_rows(MERGED_OUTPUT_PATH)
    report = validate_rows(merged_rows)
    print_report(report, MERGED_OUTPUT_PATH)

    counts = source_counts(merged_rows)
    print("Final counts:")
    print(f"- Total leads: {len(merged_rows)}")
    for source, count in sorted(counts.items()):
        print(f"- {source}: {count}")

    if has_blocking_errors(report):
        print("Pipeline failed validation.")
        return 1

    print("Pipeline completed successfully.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
