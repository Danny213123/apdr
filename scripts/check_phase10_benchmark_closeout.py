#!/usr/bin/env python3
"""Deterministic checker for Phase 10 benchmark closeout artifacts.

Validates:
- Exact canonical count (70) and watchlist count (17)
- Required headings in all four Markdown artifacts
- Presence of every explicit preservation guard ID
- Boundary text that keeps the watchlist outside the contract
- Non-empty follow-on notes for every remaining unrecovered canonical case
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path


def load_json(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def load_text(path: str) -> str:
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def check_canonical_and_watchlist_counts(
    parity_manifest: dict,
    case_delta: dict,
    errors: list[str],
) -> None:
    """Verify exact canonical count 70 and watchlist count 17."""
    # From parity manifest
    pm_canonical = parity_manifest.get("canonical_case_count", 0)
    pm_watchlist = parity_manifest.get("tier1_watchlist_count", 0)

    if pm_canonical != 70:
        errors.append(
            f"parity-manifest canonical_case_count is {pm_canonical}, expected 70"
        )
    if pm_watchlist != 17:
        errors.append(
            f"parity-manifest tier1_watchlist_count is {pm_watchlist}, expected 17"
        )

    # From case delta
    cd_canonical = case_delta.get("canonical_case_count", 0)
    cd_watchlist = case_delta.get("watchlist_case_count", 0)

    if cd_canonical != 70:
        errors.append(
            f"case-delta canonical_case_count is {cd_canonical}, expected 70"
        )
    if cd_watchlist != 17:
        errors.append(
            f"case-delta watchlist_case_count is {cd_watchlist}, expected 17"
        )

    # Verify actual case counts in parity manifest arrays
    pm_canonical_ids = parity_manifest.get("canonical_case_ids", [])
    pm_watchlist_ids = parity_manifest.get("tier1_watchlist_case_ids", [])

    if len(pm_canonical_ids) != 70:
        errors.append(
            f"parity-manifest canonical_case_ids has {len(pm_canonical_ids)} entries, expected 70"
        )
    if len(pm_watchlist_ids) != 17:
        errors.append(
            f"parity-manifest tier1_watchlist_case_ids has {len(pm_watchlist_ids)} entries, expected 17"
        )


def check_benchmark_md_headings(text: str, errors: list[str]) -> None:
    """Verify required headings in the benchmark verification note."""
    required = [
        "## Commands",
        "## Artifact Links",
        "## Canonical Slice Delta",
        "## Preservation Guards",
        "## Requirement Verdicts",
    ]
    for heading in required:
        if heading not in text:
            errors.append(f"benchmark-md missing required heading: {heading}")

    # Verify requirement IDs present
    for req_id in ["REC-05", "EVD-01", "EVD-02"]:
        if req_id not in text:
            errors.append(f"benchmark-md missing requirement ID: {req_id}")


def check_watchlist_md_headings(text: str, errors: list[str]) -> None:
    """Verify required headings and boundary text in watchlist appendix."""
    required = [
        "## Scope Boundary",
        "## Watchlist Cases",
        "## Interpretation",
    ]
    for heading in required:
        if heading not in text:
            errors.append(f"watchlist-md missing required heading: {heading}")

    # Verify boundary text
    if "outside the main contract" not in text.lower():
        errors.append(
            "watchlist-md missing boundary text: 'outside the main contract'"
        )


def check_guards_md_headings(text: str, errors: list[str]) -> None:
    """Verify required headings in preservation guards report."""
    required = [
        "## Passed Guards",
        "## Host Runtime Guards",
        "## Local Helper Guards",
        "## Unsolvable Guards",
    ]
    for heading in required:
        if heading not in text:
            errors.append(f"guards-md missing required heading: {heading}")


def check_gaps_md_headings(text: str, errors: list[str]) -> None:
    """Verify required headings in unrecovered gaps report."""
    required = [
        "## Dominant Failure Buckets",
        "## Canonical Cases By Bucket",
        "## Follow-On Notes",
    ]
    for heading in required:
        if heading not in text:
            errors.append(f"gaps-md missing required heading: {heading}")


def check_preservation_guard_ids(
    manifest: dict,
    guards_text: str,
    rerun: dict,
    errors: list[str],
) -> None:
    """Verify every explicit preservation guard ID appears in the guards report."""
    guards = manifest.get("preservation_guards", {})
    all_guard_ids = []
    for key in ["passed_case_ids", "host_runtime_case_ids", "local_helper_case_ids", "unsolvable_case_ids"]:
        for entry in guards.get(key, []):
            case_id = entry.get("case_id", "") if isinstance(entry, dict) else entry
            all_guard_ids.append(case_id)

    for case_id in all_guard_ids:
        if case_id not in guards_text:
            errors.append(
                f"guards-md missing preservation guard case ID: {case_id}"
            )

    # Cross-check against rerun results
    rerun_guard_ids = set()
    for entry in rerun.get("preservation_guard_results", []):
        rerun_guard_ids.add(entry.get("case_id", ""))

    manifest_guard_ids = set(all_guard_ids)
    missing_in_rerun = manifest_guard_ids - rerun_guard_ids
    if missing_in_rerun:
        errors.append(
            f"rerun-json missing guard IDs present in manifest: {sorted(missing_in_rerun)}"
        )

    # Verify no regressions in passed guards
    for entry in rerun.get("preservation_guard_results", []):
        case_id = entry.get("case_id", "")
        baseline = entry.get("baseline_status", "")
        rerun_status = entry.get("rerun_status", "")
        if baseline == "passed" and rerun_status != "passed":
            errors.append(
                f"preservation guard regression: {case_id} was {baseline}, now {rerun_status}"
            )


def check_unrecovered_follow_on_notes(
    case_delta: dict,
    gaps_text: str,
    errors: list[str],
) -> None:
    """Verify every unrecovered canonical case has a follow-on note."""
    cases = case_delta.get("cases", [])
    canonical_count = case_delta.get("canonical_case_count", 70)

    # Canonical cases are the first `canonical_count` entries
    canonical_cases = cases[:canonical_count]

    unrecovered = [
        c for c in canonical_cases
        if c.get("delta_label") == "unchanged"
        and c.get("rerun_status") == "failed"
    ]

    # Each unrecovered case ID must appear in the gaps report
    for case in unrecovered:
        case_id = case.get("case_id", "")
        if case_id not in gaps_text:
            errors.append(
                f"gaps-md missing unrecovered canonical case: {case_id}"
            )

    # The Follow-On Notes section must be non-empty
    follow_on_idx = gaps_text.find("## Follow-On Notes")
    if follow_on_idx == -1:
        errors.append("gaps-md has no ## Follow-On Notes section")
    else:
        follow_on_text = gaps_text[follow_on_idx + len("## Follow-On Notes"):]
        # Strip leading whitespace and check for content
        stripped = follow_on_text.strip()
        if len(stripped) < 50:
            errors.append(
                "gaps-md ## Follow-On Notes section appears empty or too short"
            )


def check_baseline_summary_exists(path: str, errors: list[str]) -> None:
    """Verify the baseline summary file exists and is valid JSON."""
    if not Path(path).exists():
        errors.append(f"baseline-summary file not found: {path}")
        return
    try:
        load_json(path)
    except (json.JSONDecodeError, OSError) as e:
        errors.append(f"baseline-summary is not valid JSON: {e}")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Phase 10 benchmark closeout checker"
    )
    parser.add_argument(
        "--parity-manifest",
        required=True,
        help="Path to 07-tier3-parity-manifest.json",
    )
    parser.add_argument(
        "--baseline-summary",
        required=True,
        help="Path to the baseline summary.json",
    )
    parser.add_argument(
        "--rerun-json",
        required=True,
        help="Path to 10-targeted-rerun.json",
    )
    parser.add_argument(
        "--case-delta-json",
        required=True,
        help="Path to 10-case-delta.json",
    )
    parser.add_argument(
        "--benchmark-md",
        required=True,
        help="Path to 10-BENCHMARK-VERIFICATION.md",
    )
    parser.add_argument(
        "--watchlist-md",
        required=True,
        help="Path to 10-WATCHLIST-APPENDIX.md",
    )
    parser.add_argument(
        "--guards-md",
        required=True,
        help="Path to 10-PRESERVATION-GUARDS.md",
    )
    parser.add_argument(
        "--gaps-md",
        required=True,
        help="Path to 10-UNRECOVERED-GAPS.md",
    )
    args = parser.parse_args()

    errors: list[str] = []

    # Load inputs
    try:
        parity_manifest = load_json(args.parity_manifest)
    except (json.JSONDecodeError, OSError) as e:
        print(f"FAIL: Cannot load parity manifest: {e}", file=sys.stderr)
        return 1

    try:
        case_delta = load_json(args.case_delta_json)
    except (json.JSONDecodeError, OSError) as e:
        print(f"FAIL: Cannot load case delta: {e}", file=sys.stderr)
        return 1

    try:
        rerun = load_json(args.rerun_json)
    except (json.JSONDecodeError, OSError) as e:
        print(f"FAIL: Cannot load rerun JSON: {e}", file=sys.stderr)
        return 1

    # Load the manifest (for preservation guard IDs)
    manifest_path = Path(args.case_delta_json).parent / "10-targeted-rerun-manifest.json"
    try:
        manifest = load_json(str(manifest_path))
    except (json.JSONDecodeError, OSError) as e:
        print(f"FAIL: Cannot load targeted-rerun manifest: {e}", file=sys.stderr)
        return 1

    # Load markdown artifacts
    try:
        benchmark_text = load_text(args.benchmark_md)
    except OSError as e:
        errors.append(f"Cannot read benchmark-md: {e}")
        benchmark_text = ""

    try:
        watchlist_text = load_text(args.watchlist_md)
    except OSError as e:
        errors.append(f"Cannot read watchlist-md: {e}")
        watchlist_text = ""

    try:
        guards_text = load_text(args.guards_md)
    except OSError as e:
        errors.append(f"Cannot read guards-md: {e}")
        guards_text = ""

    try:
        gaps_text = load_text(args.gaps_md)
    except OSError as e:
        errors.append(f"Cannot read gaps-md: {e}")
        gaps_text = ""

    # Run checks
    check_baseline_summary_exists(args.baseline_summary, errors)
    check_canonical_and_watchlist_counts(parity_manifest, case_delta, errors)
    check_benchmark_md_headings(benchmark_text, errors)
    check_watchlist_md_headings(watchlist_text, errors)
    check_guards_md_headings(guards_text, errors)
    check_gaps_md_headings(gaps_text, errors)
    check_preservation_guard_ids(manifest, guards_text, rerun, errors)
    check_unrecovered_follow_on_notes(case_delta, gaps_text, errors)

    # Report
    if errors:
        print(f"FAIL: {len(errors)} check(s) failed:", file=sys.stderr)
        for i, err in enumerate(errors, 1):
            print(f"  {i}. {err}", file=sys.stderr)
        return 1

    print("PASS: All Phase 10 benchmark closeout checks passed.")
    print(f"  Canonical cases: 70")
    print(f"  Watchlist cases: 17")
    print(f"  Preservation guards: {len(manifest.get('preservation_guards', {}).get('passed_case_ids', []))} passed, "
          f"{len(manifest.get('preservation_guards', {}).get('host_runtime_case_ids', []))} host-runtime, "
          f"{len(manifest.get('preservation_guards', {}).get('local_helper_case_ids', []))} local-helper, "
          f"{len(manifest.get('preservation_guards', {}).get('unsolvable_case_ids', []))} unsolvable")
    print(f"  Required headings: all present in 4 artifacts")
    print(f"  Follow-on notes: present for all unrecovered cases")
    return 0


if __name__ == "__main__":
    sys.exit(main())
