#!/usr/bin/env python3
"""Check the Phase 23 policy-truth and failure-family proof contract."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


EXPECTED_SLICE_ID = "phase23-policy-truth-proof-v1"
EXPECTED_PROOF_SCOPE = (
    "Phase 23 inspectability and failure-family truth only; "
    "not the Phase 24 comparison harness."
)
EXPECTED_UI_TRUTH_KEYS = (
    "requestedLlmValidationPolicy",
    "validationPath",
    "llmValidationRoute",
    "dockerStatus",
    "dockerBypassReason",
    "failureFamily",
    "debugDir",
    "dockerBypassNote",
)
EXPECTED_CASES = (
    {
        "case_id": "docker-first-attempted-dependency-resolution",
        "relative_path": "contracts/docker-first-attempted-dependency-resolution/snippet.py",
        "requested_policy": "docker-first",
        "validation_path": "docker->llm-agent",
        "llm_validation_route": "docker-first",
        "docker_status": "attempted",
        "docker_bypass_reason": None,
        "failure_family": "dependency-resolution",
        "failure_truth_basis": "package-resolution miss",
        "required_debug_artifacts": [
            "Dockerfile",
            "docker-build.command.txt",
            "docker-run.command.txt",
            "build.log",
            "run.log",
            "combined.log",
        ],
    },
    {
        "case_id": "env-first-control",
        "relative_path": "contracts/env-first-control/snippet.py",
        "requested_policy": "env-first",
        "validation_path": "env->llm-agent",
        "llm_validation_route": "env-first-control",
        "docker_status": "env-first control",
        "docker_bypass_reason": "explicit env-first control policy",
        "failure_family": "dependency-resolution",
        "failure_truth_basis": "control route remains dependency-resolution",
        "required_debug_artifacts": ["docker-bypass.txt"],
    },
    {
        "case_id": "docker-cli-unavailable-bypass",
        "relative_path": "contracts/docker-cli-unavailable-bypass/snippet.py",
        "requested_policy": "docker-first",
        "validation_path": "env",
        "llm_validation_route": "env-first-docker-bypass",
        "docker_status": "bypassed",
        "docker_bypass_reason": "docker cli unavailable",
        "failure_family": "environment-specific",
        "failure_truth_basis": "docker cli unavailable bypass",
        "required_debug_artifacts": ["docker-bypass.txt"],
    },
    {
        "case_id": "docker-daemon-unavailable-bypass",
        "relative_path": "contracts/docker-daemon-unavailable-bypass/snippet.py",
        "requested_policy": "docker-first",
        "validation_path": "env",
        "llm_validation_route": "env-first-docker-bypass",
        "docker_status": "bypassed",
        "docker_bypass_reason": "docker daemon unavailable",
        "failure_family": "environment-specific",
        "failure_truth_basis": "docker daemon unavailable bypass",
        "required_debug_artifacts": ["docker-bypass.txt"],
    },
    {
        "case_id": "host-runtime-pre-skip",
        "relative_path": "contracts/host-runtime-pre-skip/snippet.py",
        "requested_policy": "docker-first",
        "validation_path": "env",
        "llm_validation_route": "env-first-host-runtime",
        "docker_status": "host-runtime pre-skip",
        "docker_bypass_reason": "host-runtime pre-skip",
        "failure_family": "environment-specific",
        "failure_truth_basis": "host-runtime pre-skip route",
        "required_debug_artifacts": ["docker-bypass.txt"],
    },
    {
        "case_id": "framework-runtime-environment-specific",
        "relative_path": "contracts/framework-runtime-environment-specific/snippet.py",
        "requested_policy": "docker-first",
        "validation_path": "docker->llm-agent",
        "llm_validation_route": "docker-first",
        "docker_status": "attempted",
        "docker_bypass_reason": None,
        "failure_family": "environment-specific",
        "failure_truth_basis": "framework-runtime marker",
        "required_debug_artifacts": [
            "Dockerfile",
            "docker-build.command.txt",
            "docker-run.command.txt",
            "build.log",
            "run.log",
            "combined.log",
        ],
    },
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Check the Phase 23 policy-truth and failure-family proof contract."
    )
    parser.add_argument(
        "--slice-json",
        required=True,
        help="Path to the fixed Phase 23 policy-truth slice manifest.",
    )
    parser.add_argument(
        "--status-json",
        required=True,
        help="Path to write the machine-readable checker status.",
    )
    parser.add_argument(
        "--probe-only",
        action="store_true",
        help="Validate the frozen contract without requiring a benchmark replay.",
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


def normalize_optional_string(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def normalize_required_list(
    value: Any,
    *,
    label: str,
    source_path: str,
) -> list[str]:
    if not isinstance(value, list):
        raise ValueError(f"{label} must be a list: {source_path}")
    normalized = [normalize_optional_string(item) for item in value]
    if any(item is None for item in normalized):
        raise ValueError(f"{label} cannot contain blanks: {source_path}")
    return [item for item in normalized if item is not None]


def validate_root_fields(
    payload: dict[str, Any], source_path: str
) -> tuple[str, str, list[str]]:
    slice_id = normalize_optional_string(payload.get("slice_id"))
    if slice_id != EXPECTED_SLICE_ID:
        raise ValueError(
            f"Slice manifest must keep slice_id {EXPECTED_SLICE_ID!r}: {source_path}"
        )

    proof_scope = normalize_optional_string(payload.get("proof_scope"))
    if proof_scope != EXPECTED_PROOF_SCOPE:
        raise ValueError(
            "proof_scope drifted from the Phase 23 contract: "
            f"{EXPECTED_PROOF_SCOPE!r}"
        )

    truth_keys = normalize_required_list(
        payload.get("required_ui_truth_keys"),
        label="required_ui_truth_keys",
        source_path=source_path,
    )
    if tuple(truth_keys) != EXPECTED_UI_TRUTH_KEYS:
        raise ValueError(
            "required_ui_truth_keys drifted from the Phase 23 contract: "
            + ", ".join(EXPECTED_UI_TRUTH_KEYS)
        )

    return slice_id, proof_scope, truth_keys


def validate_case(
    entry: dict[str, Any],
    expected: dict[str, Any],
    *,
    index: int,
    source_path: str,
) -> dict[str, Any]:
    normalized: dict[str, Any] = {}
    for field in (
        "case_id",
        "relative_path",
        "requested_policy",
        "validation_path",
        "llm_validation_route",
        "docker_status",
        "failure_family",
        "failure_truth_basis",
    ):
        value = normalize_optional_string(entry.get(field))
        expected_value = expected[field]
        if value != expected_value:
            raise ValueError(
                f"Slice case {index} {field} mismatch: {value!r} != {expected_value!r}"
            )
        normalized[field] = value

    bypass_reason = normalize_optional_string(entry.get("docker_bypass_reason"))
    if bypass_reason != expected["docker_bypass_reason"]:
        raise ValueError(
            f"{normalized['case_id']} docker_bypass_reason mismatch: "
            f"{bypass_reason!r} != {expected['docker_bypass_reason']!r}"
        )
    normalized["docker_bypass_reason"] = bypass_reason

    required_debug_artifacts = normalize_required_list(
        entry.get("required_debug_artifacts"),
        label=f"{normalized['case_id']} required_debug_artifacts",
        source_path=source_path,
    )
    if required_debug_artifacts != expected["required_debug_artifacts"]:
        raise ValueError(
            f"{normalized['case_id']} required_debug_artifacts mismatch: "
            f"{required_debug_artifacts!r} != {expected['required_debug_artifacts']!r}"
        )
    normalized["required_debug_artifacts"] = required_debug_artifacts

    path_hops = [part.strip() for part in normalized["validation_path"].split("->") if part.strip()]
    if normalized["docker_status"] == "attempted" and "docker" not in path_hops:
        raise ValueError(
            f"{normalized['case_id']} must include docker in validation_path when "
            "docker_status is attempted"
        )
    if normalized["docker_status"] != "attempted" and "docker-bypass.txt" not in required_debug_artifacts:
        raise ValueError(
            f"{normalized['case_id']} must require docker-bypass.txt when docker was not attempted"
        )
    if bypass_reason is None and normalized["docker_status"] != "attempted":
        raise ValueError(
            f"{normalized['case_id']} must define docker_bypass_reason when docker_status is not attempted"
        )
    if bypass_reason is not None and normalized["docker_status"] == "attempted":
        raise ValueError(
            f"{normalized['case_id']} cannot define docker_bypass_reason when docker_status is attempted"
        )

    return normalized


def validate_slice_contract(payload: dict[str, Any], source_path: str) -> dict[str, Any]:
    slice_id, proof_scope, required_ui_truth_keys = validate_root_fields(payload, source_path)
    cases = payload.get("cases")
    if not isinstance(cases, list) or not cases:
        raise ValueError(f"Slice manifest must contain a non-empty cases array: {source_path}")
    if len(cases) != len(EXPECTED_CASES):
        raise ValueError(
            f"Slice manifest must contain exactly {len(EXPECTED_CASES)} cases: {source_path}"
        )

    normalized_cases: list[dict[str, Any]] = []
    for index, expected in enumerate(EXPECTED_CASES):
        entry = cases[index]
        if not isinstance(entry, dict):
            raise ValueError(f"Slice case {index} must be an object: {source_path}")
        normalized_cases.append(
            validate_case(entry, expected, index=index, source_path=source_path)
        )

    return {
        "path": str(Path(source_path).expanduser().resolve()),
        "slice_id": slice_id,
        "proof_scope": proof_scope,
        "required_ui_truth_keys": required_ui_truth_keys,
        "case_count": len(normalized_cases),
        "cases": normalized_cases,
    }


def write_status(path_text: str, payload: dict[str, Any]) -> None:
    path = Path(path_text).expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def main() -> int:
    args = parse_args()
    status: dict[str, Any] = {
        "phase": "23",
        "plan": "03",
        "probe_only": bool(args.probe_only),
        "mode": "probe" if args.probe_only else "contract",
        "passed": False,
        "errors": [],
    }
    try:
        slice_payload = load_json_object(args.slice_json, "Phase 23 policy-truth slice")
        slice_contract = validate_slice_contract(slice_payload, args.slice_json)
        route_counts: dict[str, int] = {}
        failure_family_counts: dict[str, int] = {}
        docker_status_counts: dict[str, int] = {}
        for case in slice_contract["cases"]:
            route = case["llm_validation_route"]
            route_counts[route] = route_counts.get(route, 0) + 1
            failure_family = case["failure_family"]
            failure_family_counts[failure_family] = (
                failure_family_counts.get(failure_family, 0) + 1
            )
            docker_status = case["docker_status"]
            docker_status_counts[docker_status] = docker_status_counts.get(docker_status, 0) + 1
        status.update(
            {
                "passed": True,
                "slice_contract": slice_contract,
                "route_counts": route_counts,
                "failure_family_counts": failure_family_counts,
                "docker_status_counts": docker_status_counts,
                "environment_specific_cases": [
                    case["case_id"]
                    for case in slice_contract["cases"]
                    if case["failure_family"] == "environment-specific"
                ],
                "dependency_resolution_cases": [
                    case["case_id"]
                    for case in slice_contract["cases"]
                    if case["failure_family"] == "dependency-resolution"
                ],
            }
        )
    except ValueError as exc:
        status["errors"].append(str(exc))

    write_status(args.status_json, status)
    return 0 if status["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
