#!/usr/bin/env python3
"""Check the Phase 22 docker-first policy proof contract."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


EXPECTED_SLICE_ID = "phase22-docker-policy-proof-v1"
EXPECTED_TOP_LEVEL_FIELDS = (
    "requested_llm_validation_policy",
    "llm_validation_route",
    "validation_path",
    "docker_bypass_reason",
    "docker_bypass_note",
)
EXPECTED_CASES = (
    {
        "case_id": "docker-first-default",
        "relative_path": "contracts/docker-first-default/snippet.py",
        "requested_llm_validation_policy": "docker-first",
        "llm_validation_route": "docker-first",
        "expected_first_hop": "docker",
        "docker_bypass_reason": None,
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
        "requested_llm_validation_policy": "env-first",
        "llm_validation_route": "env-first-control",
        "expected_first_hop": "env",
        "docker_bypass_reason": "explicit env-first control policy",
        "required_debug_artifacts": ["docker-bypass.txt"],
    },
    {
        "case_id": "docker-bypass-fallback",
        "relative_path": "contracts/docker-bypass-fallback/snippet.py",
        "requested_llm_validation_policy": "docker-first",
        "llm_validation_route": "env-first-docker-bypass",
        "expected_first_hop": "env",
        "docker_bypass_reason": "docker cli unavailable",
        "required_debug_artifacts": ["docker-bypass.txt"],
    },
    {
        "case_id": "docker-daemon-unavailable",
        "relative_path": "contracts/docker-daemon-unavailable/snippet.py",
        "requested_llm_validation_policy": "docker-first",
        "llm_validation_route": "env-first-docker-bypass",
        "expected_first_hop": "env",
        "docker_bypass_reason": "docker daemon unavailable",
        "required_debug_artifacts": ["docker-bypass.txt"],
    },
    {
        "case_id": "host-runtime-pre-skip",
        "relative_path": "contracts/host-runtime-pre-skip/snippet.py",
        "requested_llm_validation_policy": "docker-first",
        "llm_validation_route": "env-first-host-runtime",
        "expected_first_hop": "env",
        "docker_bypass_reason": "host-runtime pre-skip",
        "required_debug_artifacts": ["docker-bypass.txt"],
    },
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Check the Phase 22 docker-first policy proof contract."
    )
    parser.add_argument(
        "--slice-json",
        required=True,
        help="Path to the fixed Phase 22 policy slice manifest.",
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


def validate_root_fields(payload: dict[str, Any], source_path: str) -> tuple[str, list[str]]:
    slice_id = normalize_optional_string(payload.get("slice_id"))
    if slice_id != EXPECTED_SLICE_ID:
        raise ValueError(
            f"Slice manifest must keep slice_id {EXPECTED_SLICE_ID!r}: {source_path}"
        )

    fields = payload.get("required_top_level_fields")
    if not isinstance(fields, list):
        raise ValueError(
            f"Slice manifest must contain required_top_level_fields as a list: {source_path}"
        )
    normalized_fields = [normalize_optional_string(value) for value in fields]
    if any(value is None for value in normalized_fields):
        raise ValueError(f"required_top_level_fields cannot contain blanks: {source_path}")
    if tuple(normalized_fields) != EXPECTED_TOP_LEVEL_FIELDS:
        raise ValueError(
            "required_top_level_fields drifted from the Phase 22 contract: "
            + ", ".join(EXPECTED_TOP_LEVEL_FIELDS)
        )

    return slice_id, [value for value in normalized_fields if value is not None]


def validate_case(
    entry: dict[str, Any],
    expected: dict[str, Any],
    *,
    index: int,
    source_path: str,
) -> dict[str, Any]:
    case_id = normalize_optional_string(entry.get("case_id"))
    if case_id != expected["case_id"]:
        raise ValueError(
            f"Slice case {index} must keep case_id {expected['case_id']!r}: {source_path}"
        )

    relative_path = normalize_optional_string(entry.get("relative_path"))
    if relative_path != expected["relative_path"]:
        raise ValueError(
            f"{case_id} relative_path mismatch: {relative_path!r} != {expected['relative_path']!r}"
        )

    requested_policy = normalize_optional_string(entry.get("requested_llm_validation_policy"))
    if requested_policy != expected["requested_llm_validation_policy"]:
        raise ValueError(
            f"{case_id} requested_llm_validation_policy mismatch: "
            f"{requested_policy!r} != {expected['requested_llm_validation_policy']!r}"
        )

    route = normalize_optional_string(entry.get("llm_validation_route"))
    if route != expected["llm_validation_route"]:
        raise ValueError(
            f"{case_id} llm_validation_route mismatch: {route!r} != {expected['llm_validation_route']!r}"
        )

    first_hop = normalize_optional_string(entry.get("expected_first_hop"))
    if first_hop != expected["expected_first_hop"]:
        raise ValueError(
            f"{case_id} expected_first_hop mismatch: {first_hop!r} != {expected['expected_first_hop']!r}"
        )

    bypass_reason = normalize_optional_string(entry.get("docker_bypass_reason"))
    if bypass_reason != expected["docker_bypass_reason"]:
        raise ValueError(
            f"{case_id} docker_bypass_reason mismatch: "
            f"{bypass_reason!r} != {expected['docker_bypass_reason']!r}"
        )

    required_debug_artifacts = entry.get("required_debug_artifacts")
    if not isinstance(required_debug_artifacts, list) or not required_debug_artifacts:
        raise ValueError(f"{case_id} required_debug_artifacts must be a non-empty list")
    normalized_artifacts = [normalize_optional_string(value) for value in required_debug_artifacts]
    if any(value is None for value in normalized_artifacts):
        raise ValueError(f"{case_id} required_debug_artifacts cannot contain blanks")
    if normalized_artifacts != expected["required_debug_artifacts"]:
        raise ValueError(
            f"{case_id} required_debug_artifacts mismatch: "
            f"{normalized_artifacts!r} != {expected['required_debug_artifacts']!r}"
        )

    if first_hop == "docker" and bypass_reason is not None:
        raise ValueError(f"{case_id} cannot define docker_bypass_reason when first hop is docker")
    if first_hop == "env" and "docker-bypass.txt" not in normalized_artifacts:
        raise ValueError(
            f"{case_id} must require docker-bypass.txt when Docker is not the first hop"
        )

    return {
        "case_id": case_id,
        "relative_path": relative_path,
        "requested_llm_validation_policy": requested_policy,
        "llm_validation_route": route,
        "expected_first_hop": first_hop,
        "docker_bypass_reason": bypass_reason,
        "required_debug_artifacts": normalized_artifacts,
    }


def validate_slice_contract(payload: dict[str, Any], source_path: str) -> dict[str, Any]:
    slice_id, required_top_level_fields = validate_root_fields(payload, source_path)
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
        "required_top_level_fields": required_top_level_fields,
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
        "phase": "22",
        "plan": "04",
        "probe_only": bool(args.probe_only),
        "mode": "probe" if args.probe_only else "contract",
        "passed": False,
        "errors": [],
    }
    try:
        slice_payload = load_json_object(args.slice_json, "Phase 22 policy slice")
        slice_contract = validate_slice_contract(slice_payload, args.slice_json)
        route_counts: dict[str, int] = {}
        for case in slice_contract["cases"]:
            route = case["llm_validation_route"]
            route_counts[route] = route_counts.get(route, 0) + 1
        status.update(
            {
                "passed": True,
                "slice_contract": slice_contract,
                "route_counts": route_counts,
                "docker_attempt_cases": [
                    case["case_id"]
                    for case in slice_contract["cases"]
                    if case["expected_first_hop"] == "docker"
                ],
                "docker_bypass_cases": [
                    case["case_id"]
                    for case in slice_contract["cases"]
                    if case["expected_first_hop"] == "env"
                ],
            }
        )
    except ValueError as exc:
        status["errors"].append(str(exc))

    write_status(args.status_json, status)
    return 0 if status["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
