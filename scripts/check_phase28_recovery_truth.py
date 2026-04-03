#!/usr/bin/env python3
"""Check the Phase 28 recovery-attempt and failure-truth contract."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


ALLOWED_FAILURE_TRUTH_CLASSES = {
    "llm-no-output",
    "provider-tooling-failure",
    "docker-infrastructure-failure",
    "dependency-runtime-failure",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Check the Phase 28 recovery-attempt and failure-truth contract."
    )
    parser.add_argument("--applied-json", required=True, help="Path to the recovery-applied sample.")
    parser.add_argument("--truth-json", required=True, help="Path to the failure-truth sample.")
    parser.add_argument("--status-json", required=True, help="Path to write checker status JSON.")
    parser.add_argument(
        "--probe-only",
        action="store_true",
        help="Validate the frozen Phase 28 contract without requiring a live replay.",
    )
    return parser.parse_args()


def load_json_object(path_text: str, label: str) -> dict[str, Any]:
    path = Path(path_text).expanduser().resolve()
    if not path.exists():
        raise ValueError(f"{label} not found: {path}")
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"{label} is not valid JSON: {path} ({exc})") from exc
    if not isinstance(payload, dict):
        raise ValueError(f"{label} must be a JSON object: {path}")
    return payload


def normalize_text(value: Any) -> str:
    return str(value or "").strip()


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(message)


def write_status(path_text: str, payload: dict[str, Any]) -> None:
    path = Path(path_text).expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def validate_recovery_attempt(entry: Any, label: str) -> dict[str, str]:
    require(isinstance(entry, dict), f"{label} must be a JSON object")
    attempt_index = int(entry.get("attempt_index") or 0)
    recovery_outcome = normalize_text(entry.get("recovery_outcome"))
    failure_class = normalize_text(entry.get("failure_class"))
    diagnostic_preview = normalize_text(entry.get("diagnostic_preview"))
    combined_log_path = normalize_text(entry.get("combined_log_path"))
    authored_plan_path = normalize_text(entry.get("authored_plan_path"))
    docker_plan_path = normalize_text(entry.get("docker_plan_path"))

    require(attempt_index > 0, f"{label} must keep a positive attempt_index")
    require(recovery_outcome != "", f"{label} must keep recovery_outcome")
    require(diagnostic_preview != "", f"{label} must keep diagnostic_preview")
    require(combined_log_path != "", f"{label} must keep combined_log_path")
    require(authored_plan_path != "", f"{label} must keep authored_plan_path")
    require(docker_plan_path != "", f"{label} must keep docker_plan_path")

    return {
        "recovery_outcome": recovery_outcome,
        "failure_class": failure_class,
        "diagnostic_preview": diagnostic_preview,
    }


def validate_applied_sample(payload: dict[str, Any]) -> dict[str, str]:
    require(normalize_text(payload.get("phase")) == "28", "Applied sample must keep phase=28")
    require(normalize_text(payload.get("mode")) == "recovery-applied", "Applied sample must keep mode=recovery-applied")
    require(normalize_text(payload.get("validation_backend")) == "docker", "Applied sample must keep validation_backend=docker")
    require(normalize_text(payload.get("recovery_outcome")) == "applied", "Applied sample must keep recovery_outcome=applied")
    require(
        normalize_text(payload.get("recovery_attempts_path")).endswith("recovery-attempts.json"),
        "Applied sample must preserve recovery_attempts_path",
    )
    attempts = payload.get("recovery_attempts")
    require(isinstance(attempts, list) and bool(attempts), "Applied sample must include recovery_attempts")
    first = validate_recovery_attempt(attempts[0], "Applied sample first recovery attempt")
    require(first["recovery_outcome"] == "applied", "Applied sample attempt must keep recovery_outcome=applied")
    require(
        any(
            normalize_text(attempts[0].get(field)) != ""
            for field in ("correct_package", "add_package", "remove_package")
        ),
        "Applied sample attempt must preserve at least one applied package change",
    )
    return {
        "sample_id": normalize_text(payload.get("sample_id")),
        "applied_failure_class": first["failure_class"],
    }


def validate_truth_sample(payload: dict[str, Any]) -> dict[str, str]:
    require(normalize_text(payload.get("phase")) == "28", "Truth sample must keep phase=28")
    require(normalize_text(payload.get("mode")) == "failure-truth", "Truth sample must keep mode=failure-truth")
    failure_truth_class = normalize_text(payload.get("failure_truth_class"))
    require(
        failure_truth_class in ALLOWED_FAILURE_TRUTH_CLASSES,
        f"Truth sample drifted to unsupported failure_truth_class={failure_truth_class!r}",
    )
    require(normalize_text(payload.get("failure_truth_detail")) != "", "Truth sample must keep failure_truth_detail")
    require(
        normalize_text(payload.get("recovery_attempts_path")).endswith("recovery-attempts.json"),
        "Truth sample must preserve recovery_attempts_path",
    )
    attempts = payload.get("recovery_attempts")
    require(isinstance(attempts, list) and bool(attempts), "Truth sample must include recovery_attempts")
    last = validate_recovery_attempt(attempts[-1], "Truth sample final recovery attempt")
    top_level_outcome = normalize_text(payload.get("recovery_outcome"))
    require(top_level_outcome != "", "Truth sample must keep top-level recovery_outcome")
    require(
        top_level_outcome == last["recovery_outcome"],
        "Truth sample top-level recovery_outcome must match the final recovery attempt",
    )
    if failure_truth_class == "provider-tooling-failure":
        require(
            last["failure_class"] in {"timeout", "transport-failure", "provider-tooling-failure"},
            "Provider-tooling truth sample must keep a provider/tooling recovery failure class",
        )
    return {
        "sample_id": normalize_text(payload.get("sample_id")),
        "failure_truth_class": failure_truth_class,
        "recovery_outcome": top_level_outcome,
    }


def main() -> int:
    args = parse_args()
    status: dict[str, Any] = {
        "phase": "28",
        "probe_only": bool(args.probe_only),
        "mode": "probe" if args.probe_only else "contract",
        "passed": False,
        "errors": [],
    }
    try:
        applied = load_json_object(args.applied_json, "Phase 28 recovery-applied sample")
        truth = load_json_object(args.truth_json, "Phase 28 failure-truth sample")
        applied_summary = validate_applied_sample(applied)
        truth_summary = validate_truth_sample(truth)
        require(
            applied_summary["sample_id"] == truth_summary["sample_id"],
            "Applied and truth samples must preserve the same sample_id",
        )
        status.update(
            {
                "passed": True,
                "sample_id": applied_summary["sample_id"],
                "recovery_outcome": truth_summary["recovery_outcome"],
                "failure_truth_class": truth_summary["failure_truth_class"],
                "applied_failure_class": applied_summary["applied_failure_class"],
            }
        )
    except Exception as exc:  # noqa: BLE001 - deterministic CLI gate
        status["errors"].append(str(exc))

    write_status(args.status_json, status)
    return 0 if status["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
