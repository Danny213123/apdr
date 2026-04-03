#!/usr/bin/env python3
"""Check the Phase 26 authored case-plan and intake-failure contract."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


REQUIRED_PLAN_CONFIDENCE_KEYS = (
    "imports",
    "package_mappings",
    "runtime_assumptions",
    "smoke_strategy",
)

REQUIRED_FAILURE_CLASSES = {
    "empty-output",
    "timeout",
    "schema-validation-failure",
    "invalid-json",
    "transport-failure",
    "provider-tooling-failure",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Check the Phase 26 authored case-plan and intake-failure contract."
    )
    parser.add_argument("--plan-json", required=True, help="Path to the successful authored-plan sample.")
    parser.add_argument("--failure-json", required=True, help="Path to the intake-failure sample.")
    parser.add_argument("--status-json", required=True, help="Path to write checker status.")
    parser.add_argument(
        "--probe-only",
        action="store_true",
        help="Validate the frozen Phase 26 contract without requiring a live benchmark replay.",
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


def write_status(path_text: str, payload: dict[str, Any]) -> None:
    path = Path(path_text).expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(message)


def validate_plan(payload: dict[str, Any]) -> dict[str, Any]:
    require(normalize_text(payload.get("phase")) == "26", "Plan sample must keep phase=26")
    require(normalize_text(payload.get("authored_plan_status")) == "available", "Plan sample must keep authored_plan_status=available")
    require(normalize_text(payload.get("authored_plan_path")) != "", "Plan sample must preserve authored_plan_path")
    authored_plan = payload.get("authored_plan")
    require(isinstance(authored_plan, dict), "Plan sample must include an authored_plan object")

    extracted_imports = authored_plan.get("extracted_imports")
    package_mappings = authored_plan.get("package_mappings")
    runtime_assumptions = authored_plan.get("runtime_assumptions")
    system_dependency_hints = authored_plan.get("system_dependency_hints")
    smoke_strategy = authored_plan.get("smoke_strategy")
    section_confidence = authored_plan.get("section_confidence")
    authorship = normalize_text(authored_plan.get("authorship"))
    fallback_sections = authored_plan.get("deterministic_fallback_sections")

    require(isinstance(extracted_imports, list) and bool(extracted_imports), "Plan sample must include extracted_imports")
    require(isinstance(package_mappings, list) and bool(package_mappings), "Plan sample must include package_mappings")
    require(isinstance(runtime_assumptions, list) and bool(runtime_assumptions), "Plan sample must include runtime_assumptions")
    require(isinstance(system_dependency_hints, list), "Plan sample must include system_dependency_hints")
    require(isinstance(smoke_strategy, dict), "Plan sample must include smoke_strategy")
    require(isinstance(section_confidence, dict), "Plan sample must include section_confidence")
    require(authorship != "", "Plan sample must include authorship truth")
    require(isinstance(fallback_sections, list), "Plan sample must include deterministic_fallback_sections")

    for key in REQUIRED_PLAN_CONFIDENCE_KEYS:
        require(key in section_confidence, f"Plan sample is missing section_confidence[{key!r}]")

    require(normalize_text(smoke_strategy.get("mode")) != "", "Plan smoke_strategy must include mode")
    import_targets = smoke_strategy.get("import_targets")
    require(isinstance(import_targets, list) and bool(import_targets), "Plan smoke_strategy must include import_targets")
    require(
        set(import_targets).issubset(set(extracted_imports)),
        "Plan smoke_strategy import_targets must be a subset of extracted_imports",
    )

    top_level_authorship = normalize_text(payload.get("authored_plan_authorship"))
    require(top_level_authorship == authorship, "Top-level authored_plan_authorship must match authored_plan.authorship")
    top_level_fallback_sections = payload.get("authored_plan_fallback_sections")
    require(
        isinstance(top_level_fallback_sections, list),
        "Top-level authored_plan_fallback_sections must be a list",
    )
    require(
        top_level_fallback_sections == fallback_sections,
        "Top-level authored_plan_fallback_sections must match authored_plan.deterministic_fallback_sections",
    )

    package_sources = {
        normalize_text(item.get("source"))
        for item in package_mappings
        if isinstance(item, dict)
    }
    if fallback_sections or any(source != "llm" for source in package_sources if source):
        require(
            "deterministic" in authorship,
            "Plan sample must declare deterministic fallback in authorship when fallback sections exist",
        )

    return {
        "authorship": authorship,
        "fallback_sections": fallback_sections,
        "import_count": len(extracted_imports),
        "mapping_count": len(package_mappings),
    }


def validate_failure(payload: dict[str, Any]) -> dict[str, Any]:
    require(normalize_text(payload.get("phase")) == "26", "Failure sample must keep phase=26")
    require(normalize_text(payload.get("mode")) == "llm-only", "Failure sample must preserve llm-only mode")
    require(normalize_text(payload.get("authored_plan_status")) == "unusable", "Failure sample must keep authored_plan_status=unusable")
    require(normalize_text(payload.get("intake_failure_path")) != "", "Failure sample must preserve intake_failure_path")
    require(normalize_text(payload.get("llm_only_behavior")) == "fail", "Failure sample must preserve strict llm-only behavior")

    intake_failure = payload.get("intake_failure")
    require(isinstance(intake_failure, dict), "Failure sample must include intake_failure")
    failure_class = normalize_text(intake_failure.get("failure_class"))
    require(failure_class in REQUIRED_FAILURE_CLASSES, f"Failure sample drifted to unsupported failure_class={failure_class!r}")
    require(normalize_text(intake_failure.get("reason")) != "", "Failure sample must include reason")
    require(normalize_text(intake_failure.get("diagnostic_preview")) != "", "Failure sample must include diagnostic_preview")
    require(
        normalize_text(intake_failure.get("authored_plan_status")) == "unusable",
        "Failure sample intake_failure.authored_plan_status must stay unusable",
    )
    require(
        normalize_text(intake_failure.get("llm_only_behavior")) == "fail",
        "Failure sample intake_failure.llm_only_behavior must stay fail",
    )

    return {
        "failure_class": failure_class,
        "diagnostic_preview": normalize_text(intake_failure.get("diagnostic_preview")),
        "llm_only_behavior": normalize_text(intake_failure.get("llm_only_behavior")),
    }


def main() -> int:
    args = parse_args()
    status: dict[str, Any] = {
        "phase": "26",
        "probe_only": bool(args.probe_only),
        "mode": "probe" if args.probe_only else "contract",
        "passed": False,
        "errors": [],
    }
    try:
        plan_payload = load_json_object(args.plan_json, "Phase 26 authored-plan sample")
        failure_payload = load_json_object(args.failure_json, "Phase 26 intake-failure sample")
        plan_summary = validate_plan(plan_payload)
        failure_summary = validate_failure(failure_payload)
        status.update(
            {
                "passed": True,
                "plan_authorship": plan_summary["authorship"],
                "fallback_sections": plan_summary["fallback_sections"],
                "import_count": plan_summary["import_count"],
                "mapping_count": plan_summary["mapping_count"],
                "failure_class": failure_summary["failure_class"],
                "llm_only_behavior": failure_summary["llm_only_behavior"],
            }
        )
    except Exception as exc:  # noqa: BLE001 - deterministic CLI gate
        status["errors"].append(str(exc))

    write_status(args.status_json, status)
    return 0 if status["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
