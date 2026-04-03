#!/usr/bin/env python3
"""Check the Phase 27 authored-versus-executed Docker artifact contract."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Check the Phase 27 authored-versus-executed Docker artifact contract."
    )
    parser.add_argument("--authored-json", required=True, help="Path to the authored Docker sample.")
    parser.add_argument("--executed-json", required=True, help="Path to the executed Docker sample.")
    parser.add_argument("--status-json", required=True, help="Path to write checker status JSON.")
    parser.add_argument(
        "--probe-only",
        action="store_true",
        help="Validate the frozen Phase 27 contract without requiring a live replay.",
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


def validate_authored_sample(payload: dict[str, Any]) -> dict[str, Any]:
    require(normalize_text(payload.get("phase")) == "27", "Authored sample must keep phase=27")
    require(normalize_text(payload.get("mode")) == "authored", "Authored sample must keep mode=authored")
    require(
        normalize_text(payload.get("docker_plan_status")) == "available",
        "Authored sample must keep docker_plan_status=available",
    )
    require(normalize_text(payload.get("docker_plan_path")) != "", "Authored sample must preserve docker_plan_path")
    require(
        normalize_text(payload.get("authored_dockerfile_path")) != "",
        "Authored sample must preserve authored_dockerfile_path",
    )
    docker_plan = payload.get("docker_plan")
    require(isinstance(docker_plan, dict), "Authored sample must include docker_plan")
    require(normalize_text(docker_plan.get("base_image")) != "", "Authored docker_plan must include base_image")
    require(isinstance(docker_plan.get("system_packages"), list), "Authored docker_plan must include system_packages")
    require(
        isinstance(docker_plan.get("smoke_strategy"), dict),
        "Authored docker_plan must include smoke_strategy",
    )
    require(
        isinstance(docker_plan.get("deterministic_fallback_sections"), list),
        "Authored docker_plan must include deterministic_fallback_sections",
    )
    require(normalize_text(docker_plan.get("authorship")) != "", "Authored docker_plan must include authorship")
    require(
        normalize_text(payload.get("docker_plan_authorship")) == normalize_text(docker_plan.get("authorship")),
        "Top-level docker_plan_authorship must match docker_plan.authorship",
    )
    top_level_sections = payload.get("docker_plan_fallback_sections")
    require(isinstance(top_level_sections, list), "Top-level docker_plan_fallback_sections must be a list")
    require(
        top_level_sections == docker_plan.get("deterministic_fallback_sections"),
        "Top-level docker_plan_fallback_sections must match docker_plan.deterministic_fallback_sections",
    )
    return {
        "sample_id": normalize_text(payload.get("sample_id")),
        "docker_plan_path": normalize_text(payload.get("docker_plan_path")),
        "authored_dockerfile_path": normalize_text(payload.get("authored_dockerfile_path")),
        "authorship": normalize_text(docker_plan.get("authorship")),
    }


def validate_executed_sample(payload: dict[str, Any]) -> dict[str, Any]:
    require(normalize_text(payload.get("phase")) == "27", "Executed sample must keep phase=27")
    require(normalize_text(payload.get("mode")) == "executed", "Executed sample must keep mode=executed")
    require(
        normalize_text(payload.get("validation_backend")) == "docker",
        "Executed sample must keep validation_backend=docker",
    )
    require(
        normalize_text(payload.get("executed_dockerfile_path")).endswith("Dockerfile.executed"),
        "Executed sample must preserve executed_dockerfile_path",
    )
    require(
        "docker-build.command" in normalize_text(payload.get("docker_build_command_path")),
        "Executed sample must preserve docker_build_command_path",
    )
    require(
        "docker-run.command" in normalize_text(payload.get("docker_run_command_path")),
        "Executed sample must preserve docker_run_command_path",
    )
    require(
        normalize_text(payload.get("executed_image_ref")) != "",
        "Executed sample must preserve executed_image_ref",
    )
    require(bool(payload.get("image_handoff_verified")) is True, "Executed sample must keep image_handoff_verified=true")
    require(
        normalize_text(payload.get("image_inspect_path")) != "",
        "Executed sample must preserve image_inspect_path",
    )
    require(
        normalize_text(payload.get("build_image_id")) != "",
        "Executed sample must preserve build_image_id",
    )
    return {
        "sample_id": normalize_text(payload.get("sample_id")),
        "docker_plan_path": normalize_text(payload.get("docker_plan_path")),
        "authored_dockerfile_path": normalize_text(payload.get("authored_dockerfile_path")),
        "executed_image_ref": normalize_text(payload.get("executed_image_ref")),
    }


def main() -> int:
    args = parse_args()
    status: dict[str, Any] = {
        "phase": "27",
        "probe_only": bool(args.probe_only),
        "mode": "probe" if args.probe_only else "contract",
        "passed": False,
        "errors": [],
    }
    try:
        authored = load_json_object(args.authored_json, "Phase 27 authored Docker sample")
        executed = load_json_object(args.executed_json, "Phase 27 executed Docker sample")
        authored_summary = validate_authored_sample(authored)
        executed_summary = validate_executed_sample(executed)
        require(
            authored_summary["sample_id"] == executed_summary["sample_id"],
            "Authored and executed samples must preserve the same sample_id",
        )
        require(
            authored_summary["docker_plan_path"] == executed_summary["docker_plan_path"],
            "Executed sample must preserve the authored docker_plan_path",
        )
        require(
            authored_summary["authored_dockerfile_path"] == executed_summary["authored_dockerfile_path"],
            "Executed sample must preserve the authored_dockerfile_path",
        )
        status.update(
            {
                "passed": True,
                "sample_id": authored_summary["sample_id"],
                "docker_plan_path": authored_summary["docker_plan_path"],
                "authored_dockerfile_path": authored_summary["authored_dockerfile_path"],
                "executed_image_ref": executed_summary["executed_image_ref"],
                "authorship": authored_summary["authorship"],
            }
        )
    except Exception as exc:  # noqa: BLE001 - deterministic CLI gate
        status["errors"].append(str(exc))

    write_status(args.status_json, status)
    return 0 if status["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
