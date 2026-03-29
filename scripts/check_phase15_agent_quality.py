#!/usr/bin/env python3
"""Validate Phase 15 baseline-versus-candidate tier3 quality artifacts."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


REQUIRED_COMPARISON_KEYS = (
    "slice_id",
    "sample_count",
    "resolved",
    "abstained",
    "failed",
    "skipped",
    "success_rate",
    "agent_mode",
    "tool_profile",
    "retrieval_profile",
    "thinking_mode",
    "llm_context_window",
    "inference_policy",
    "policy_label",
    "model_name",
)

ATTRIBUTION_KEYS = (
    "agent_mode",
    "tool_profile",
    "retrieval_profile",
    "thinking_mode",
    "llm_context_window",
    "inference_policy",
    "policy_label",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Check Phase 15 baseline versus candidate agent-quality artifacts."
    )
    parser.add_argument("--baseline", required=True, help="Path to the baseline JSON artifact.")
    parser.add_argument("--candidate", required=True, help="Path to the candidate JSON artifact.")
    parser.add_argument("--output-md", default="", help="Optional Markdown summary output path.")
    return parser.parse_args()


def load_artifact(path_text: str) -> dict[str, Any]:
    path = Path(path_text).expanduser().resolve()
    if not path.exists():
        raise SystemExit(f"Artifact not found: {path}")
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise SystemExit(f"Invalid JSON in {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise SystemExit(f"Artifact must be a JSON object: {path}")
    return payload


def missing_keys(payload: dict[str, Any]) -> list[str]:
    missing: list[str] = []
    for key in REQUIRED_COMPARISON_KEYS:
        value = payload.get(key)
        if value is None:
            missing.append(key)
            continue
        if isinstance(value, str) and not value.strip():
            missing.append(key)
    return missing


def compare_artifacts(
    baseline: dict[str, Any],
    candidate: dict[str, Any],
) -> tuple[list[str], list[str], dict[str, Any]]:
    errors: list[str] = []
    notes: list[str] = []

    for label, payload in (("baseline", baseline), ("candidate", candidate)):
        missing = missing_keys(payload)
        if missing:
            errors.append(f"{label} artifact is missing required keys: {', '.join(missing)}")

    for key in ("slice_id", "sample_count", "validation_backend", "build_profile"):
        if baseline.get(key) != candidate.get(key):
            errors.append(
                f"Artifacts must match on {key}: baseline={baseline.get(key)!r}, candidate={candidate.get(key)!r}"
            )

    if candidate.get("skipped") != baseline.get("skipped"):
        errors.append(
            f"Candidate must preserve skipped count: baseline={baseline.get('skipped')}, candidate={candidate.get('skipped')}"
        )

    changed_attribution = [
        key for key in ATTRIBUTION_KEYS if baseline.get(key) != candidate.get(key)
    ]
    if not changed_attribution:
        errors.append("Candidate must change at least one attributable agent or policy field.")
    else:
        notes.append("Changed attribution fields: " + ", ".join(changed_attribution))

    baseline_resolved = int(baseline.get("resolved", 0))
    candidate_resolved = int(candidate.get("resolved", 0))
    baseline_failed = int(baseline.get("failed", 0))
    candidate_failed = int(candidate.get("failed", 0))
    baseline_abstained = int(baseline.get("abstained", 0))
    candidate_abstained = int(candidate.get("abstained", 0))
    baseline_success = float(baseline.get("success_rate", 0.0))
    candidate_success = float(candidate.get("success_rate", 0.0))

    resolved_gain = candidate_resolved - baseline_resolved
    success_gain = candidate_success - baseline_success
    failed_delta = candidate_failed - baseline_failed

    improvement = (
        resolved_gain > 0
        or success_gain > 0
        or (candidate_resolved >= baseline_resolved and candidate_failed < baseline_failed)
        or (candidate_resolved == baseline_resolved and candidate_abstained > baseline_abstained)
    )
    if not improvement:
        errors.append(
            "Candidate must improve resolved count, success rate, failed count, or abstain quality."
        )

    if candidate_failed > baseline_failed:
        errors.append(
            f"Candidate increased failed count: baseline={baseline_failed}, candidate={candidate_failed}"
        )

    verdict = {
        "baseline_resolved": baseline_resolved,
        "candidate_resolved": candidate_resolved,
        "baseline_failed": baseline_failed,
        "candidate_failed": candidate_failed,
        "baseline_abstained": baseline_abstained,
        "candidate_abstained": candidate_abstained,
        "baseline_success_rate": baseline_success,
        "candidate_success_rate": candidate_success,
        "resolved_gain": resolved_gain,
        "success_rate_gain": round(success_gain, 4),
        "changed_attribution_fields": changed_attribution,
    }
    return errors, notes, verdict


def render_markdown(
    baseline_path: str,
    candidate_path: str,
    verdict: dict[str, Any],
    notes: list[str],
    passed: bool,
) -> str:
    lines = [
        "# Phase 15 Agent Quality Check",
        "",
        f"- Baseline: `{baseline_path}`",
        f"- Candidate: `{candidate_path}`",
        f"- Verdict: `{'PASS' if passed else 'FAIL'}`",
        f"- Resolved gain: `{verdict['resolved_gain']}`",
        f"- Success-rate gain: `{verdict['success_rate_gain']}`",
        "",
        "## Notes",
        "",
    ]
    if notes:
        for note in notes:
            lines.append(f"- {note}")
    else:
        lines.append("- none")
    return "\n".join(lines) + "\n"


def main() -> int:
    args = parse_args()
    baseline = load_artifact(args.baseline)
    candidate = load_artifact(args.candidate)
    errors, notes, verdict = compare_artifacts(baseline, candidate)
    passed = not errors

    if args.output_md:
        output_path = Path(args.output_md).expanduser().resolve()
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(
            render_markdown(args.baseline, args.candidate, verdict, notes + errors, passed),
            encoding="utf-8",
        )

    if errors:
        for error in errors:
            print(f"ERROR: {error}")
        return 1

    print("Phase 15 agent quality check passed.")
    for note in notes:
        print(f"NOTE: {note}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
