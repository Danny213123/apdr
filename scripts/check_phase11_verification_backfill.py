#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path


class CheckError(RuntimeError):
    pass


def load_text(path_text: str) -> str:
    path = Path(path_text)
    try:
        return path.read_text(encoding="utf-8")
    except OSError as exc:
        raise CheckError(f"cannot read {path}: {exc}") from exc


def require_contains(text: str, needle: str, label: str, errors: list[str]) -> None:
    if needle not in text:
        errors.append(f"{label} missing required text: {needle}")


def require_not_contains(text: str, needle: str, label: str, errors: list[str]) -> None:
    if needle in text:
        errors.append(f"{label} still contains forbidden text: {needle}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate the Phase 11 verification backfill and state-repair artifacts."
    )
    parser.add_argument("--phase7-verification", required=True)
    parser.add_argument("--phase7-uat", required=True)
    parser.add_argument("--phase8-verification", required=True)
    parser.add_argument("--project-md", required=True)
    parser.add_argument("--state-md", required=True)
    parser.add_argument("--closeout-md", required=True)
    parser.add_argument("--audit-md", required=True)
    parser.add_argument("--repair-md", required=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    errors: list[str] = []

    try:
        phase7_verification = load_text(args.phase7_verification)
        phase7_uat = load_text(args.phase7_uat)
        phase8_verification = load_text(args.phase8_verification)
        project_md = load_text(args.project_md)
        state_md = load_text(args.state_md)
        closeout_md = load_text(args.closeout_md)
        audit_md = load_text(args.audit_md)
        repair_md = load_text(args.repair_md)
    except CheckError as exc:
        print(f"FAIL: {exc}", file=sys.stderr)
        return 1

    for needle in ("status: passed", "FAM-01", "FAM-02", "FAM-03", "## Gaps Summary"):
        require_contains(phase8_verification, needle, "phase8-verification", errors)

    require_contains(phase7_verification, "status: passed", "phase7-verification", errors)
    require_contains(
        phase7_verification,
        "no remaining human verification blockers",
        "phase7-verification",
        errors,
    )
    require_not_contains(phase7_verification, "status: human_needed", "phase7-verification", errors)

    for needle in ("status: passed", "passed: 2", "pending: 0", "approved on 2026-03-28"):
        require_contains(phase7_uat, needle, "phase7-uat", errors)

    for needle in ("Phase 11", "Phase 12", "live benchmark proof"):
        require_contains(project_md, needle, "project-md", errors)

    for needle in (
        "current_phase: 11",
        "current_phase_name: verification-backfill-and-state-repair",
        "completed_phases: 4",
        "total_phases: 6",
    ):
        require_contains(state_md, needle, "state-md", errors)

    for needle in ("not ready for milestone completion", "Phase 12"):
        require_contains(closeout_md, needle, "closeout-md", errors)

    for needle in ("status: gaps_found", "REC-02", "REC-03", "REC-04", "Phase 12"):
        require_contains(audit_md, needle, "audit-md", errors)

    for needle in (
        "FAM-01 | 08 | `[x]` | listed | missing | Orphaned",
        "Phase 8 lacks any verification report",
        "still shows Phase 9 incomplete",
        "07-VERIFICATION.md still has status human_needed",
    ):
        require_not_contains(audit_md, needle, "audit-md", errors)

    for needle in (
        "## Fixed Audit Gaps",
        "## Repaired Artifacts",
        "## Remaining Audit Gaps",
        "## Phase 12 Handoff",
        "REC-02",
        "REC-03",
        "REC-04",
    ):
        require_contains(repair_md, needle, "repair-md", errors)

    if errors:
        print(f"FAIL: {len(errors)} check(s) failed.", file=sys.stderr)
        for index, error in enumerate(errors, start=1):
            print(f"  {index}. {error}", file=sys.stderr)
        return 1

    print("PASS: Phase 11 verification backfill checks passed.")
    print("  Phase 7 manual-review artifacts are passed.")
    print("  Phase 8 verification artifact exists and covers FAM-01/FAM-02/FAM-03.")
    print("  PROJECT.md, STATE.md, and the Phase 10 closeout all reflect the Phase 11/12 gap-closure state.")
    print("  The refreshed audit keeps only the REC-02/REC-03/REC-04 live-proof gaps open.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
