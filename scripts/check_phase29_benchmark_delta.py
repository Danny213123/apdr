#!/usr/bin/env python3
"""Check the Phase 29 LLM benchmark delta contract."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


FAILURE_TRUTH_CLASSES = (
    "llm-no-output",
    "provider-tooling-failure",
    "docker-infrastructure-failure",
    "dependency-runtime-failure",
)

TIMING_FIELDS = (
    "duration_seconds",
    "solve_duration_seconds",
    "validation_duration_seconds",
    "install_duration_seconds",
    "docker_startup_duration_seconds",
    "smoke_duration_seconds",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Check the Phase 29 baseline-versus-candidate benchmark contract."
    )
    parser.add_argument("--baseline-artifact", required=True, help="Path to the baseline artifact JSON.")
    parser.add_argument("--candidate-artifact", required=True, help="Path to the candidate artifact JSON.")
    parser.add_argument("--status-json", required=True, help="Path to write the checker status.")
    parser.add_argument("--mode", choices=("llm", "llm-only"), required=True, help="Mode contract to validate.")
    parser.add_argument(
        "--probe-only",
        action="store_true",
        help="Validate the frozen contract without requiring a live replay.",
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


def normalize_float(value: Any) -> float:
    text = normalize_text(value)
    if not text:
        return 0.0
    try:
        return round(float(text), 2)
    except ValueError as exc:
        raise ValueError(f"Expected numeric value, got {value!r}") from exc


def expected_validation_backend(mode: str) -> str:
    return "docker" if mode == "llm-only" else "llm"


def write_status(path_text: str, payload: dict[str, Any]) -> None:
    path = Path(path_text).expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def validate_contract(artifact: dict[str, Any], *, label: str, mode: str, variant: str) -> list[dict[str, Any]]:
    if normalize_text(artifact.get("mode")) != mode:
        raise ValueError(f"{label} artifact must keep mode={mode}")
    if normalize_text(artifact.get("variant")) != variant:
        raise ValueError(f"{label} artifact must keep variant={variant}")
    if normalize_text(artifact.get("validation_backend")) != expected_validation_backend(mode):
        raise ValueError(
            f"{label} artifact must keep validation_backend={expected_validation_backend(mode)}"
        )
    if normalize_text(artifact.get("model_name")) == "":
        raise ValueError(f"{label} artifact is missing model_name")
    if normalize_text(artifact.get("base_url")) == "":
        raise ValueError(f"{label} artifact is missing base_url")

    run_contract = artifact.get("run_contract")
    if not isinstance(run_contract, dict):
        raise ValueError(f"{label} artifact must include run_contract")
    if normalize_text(run_contract.get("validation_backend")) != expected_validation_backend(mode):
        raise ValueError(
            f"{label} run_contract must keep validation_backend={expected_validation_backend(mode)}"
        )

    results = artifact.get("results")
    if not isinstance(results, list) or not results:
        raise ValueError(f"{label} artifact must contain a non-empty results array")

    normalized: list[dict[str, Any]] = []
    for index, row in enumerate(results):
        if not isinstance(row, dict):
            raise ValueError(f"{label} result row {index} must be an object")
        relative_path = normalize_text(row.get("relative_path") or row.get("snippet"))
        if not relative_path:
            raise ValueError(f"{label} result row {index} is missing relative_path")
        result_origin = normalize_text(row.get("resultOrigin"))
        if not result_origin:
            raise ValueError(f"{label} result row {relative_path} is missing resultOrigin")
        normalized.append(
            {
                "relative_path": relative_path,
                "display_status": normalize_text(row.get("display_status")),
                "validation_backend": normalize_text(row.get("validation_backend")),
                "validation_path": normalize_text(row.get("validation_path")),
                "failure_truth_class": normalize_text(row.get("failure_truth_class")),
                "failure_truth_detail": normalize_text(row.get("failure_truth_detail")),
                "recovery_outcome": normalize_text(row.get("recovery_outcome")),
                "resultOrigin": result_origin,
                **{field: normalize_float(row.get(field)) for field in TIMING_FIELDS},
            }
        )
    return normalized


def summarize_results(results: list[dict[str, Any]]) -> dict[str, Any]:
    passes = 0
    skips = 0
    failures = 0
    truth_counts = {truth: 0 for truth in FAILURE_TRUTH_CLASSES}
    timing_totals = {field: 0.0 for field in TIMING_FIELDS}
    origins: dict[str, int] = {}

    for row in results:
        status = row["display_status"].lower()
        if status == "pass":
            passes += 1
        elif status == "skip":
            skips += 1
        else:
            failures += 1
        truth = row["failure_truth_class"]
        if truth in truth_counts:
            truth_counts[truth] += 1
        origins[row["resultOrigin"]] = origins.get(row["resultOrigin"], 0) + 1
        for field in TIMING_FIELDS:
            timing_totals[field] += row[field]

    return {
        "passes": passes,
        "skips": skips,
        "failures": failures,
        "failure_truth_counts": truth_counts,
        "timing_totals": {field: round(value, 2) for field, value in timing_totals.items()},
        "origins": origins,
    }


def validate_parity(
    baseline_artifact: dict[str, Any],
    candidate_artifact: dict[str, Any],
    baseline_results: list[dict[str, Any]],
    candidate_results: list[dict[str, Any]],
) -> None:
    if normalize_text(baseline_artifact.get("slice_id")) != normalize_text(candidate_artifact.get("slice_id")):
        raise ValueError("Artifacts must keep the same slice_id")
    if normalize_text(baseline_artifact.get("mode")) != normalize_text(candidate_artifact.get("mode")):
        raise ValueError("Artifacts must keep the same mode")
    if normalize_text(baseline_artifact.get("validation_backend")) != normalize_text(
        candidate_artifact.get("validation_backend")
    ):
        raise ValueError("Artifacts must keep the same validation_backend")
    if normalize_text(baseline_artifact.get("model_name")) != normalize_text(candidate_artifact.get("model_name")):
        raise ValueError("Artifacts must keep the same model_name")
    if normalize_text(baseline_artifact.get("base_url")) != normalize_text(candidate_artifact.get("base_url")):
        raise ValueError("Artifacts must keep the same base_url")

    baseline_contract = baseline_artifact["run_contract"]
    candidate_contract = candidate_artifact["run_contract"]
    for field in ("validation_backend", "build_profile", "cache_state"):
        if normalize_text(baseline_contract.get(field)) != normalize_text(candidate_contract.get(field)):
            raise ValueError(f"Artifacts drifted on run_contract.{field}")

    baseline_paths = [row["relative_path"] for row in baseline_results]
    candidate_paths = [row["relative_path"] for row in candidate_results]
    if baseline_paths != candidate_paths:
        raise ValueError("Artifacts must keep the exact same ordered case set")

    for baseline_row, candidate_row in zip(baseline_results, candidate_results):
        if baseline_row["resultOrigin"] != candidate_row["resultOrigin"]:
            raise ValueError(
                f"Artifacts drifted on resultOrigin for {baseline_row['relative_path']}: "
                f"{baseline_row['resultOrigin']} != {candidate_row['resultOrigin']}"
            )


def compute_deltas(baseline_summary: dict[str, Any], candidate_summary: dict[str, Any]) -> dict[str, Any]:
    timing_deltas = {
        field: round(
            candidate_summary["timing_totals"][field] - baseline_summary["timing_totals"][field],
            2,
        )
        for field in TIMING_FIELDS
    }
    deltas = {
        "pass_delta": candidate_summary["passes"] - baseline_summary["passes"],
        "skip_delta": candidate_summary["skips"] - baseline_summary["skips"],
        "failure_delta": candidate_summary["failures"] - baseline_summary["failures"],
        "llm_no_output_delta": (
            candidate_summary["failure_truth_counts"]["llm-no-output"]
            - baseline_summary["failure_truth_counts"]["llm-no-output"]
        ),
        "provider_tooling_failure_delta": (
            candidate_summary["failure_truth_counts"]["provider-tooling-failure"]
            - baseline_summary["failure_truth_counts"]["provider-tooling-failure"]
        ),
        "docker_infrastructure_failure_delta": (
            candidate_summary["failure_truth_counts"]["docker-infrastructure-failure"]
            - baseline_summary["failure_truth_counts"]["docker-infrastructure-failure"]
        ),
        "dependency_runtime_failure_delta": (
            candidate_summary["failure_truth_counts"]["dependency-runtime-failure"]
            - baseline_summary["failure_truth_counts"]["dependency-runtime-failure"]
        ),
        "timing_deltas": timing_deltas,
    }
    if not any(
        value != 0
        for value in (
            deltas["pass_delta"],
            deltas["skip_delta"],
            deltas["failure_delta"],
            deltas["llm_no_output_delta"],
            deltas["provider_tooling_failure_delta"],
            deltas["docker_infrastructure_failure_delta"],
            deltas["dependency_runtime_failure_delta"],
        )
    ) and not any(value != 0 for value in timing_deltas.values()):
        raise ValueError("Artifacts must demonstrate at least one benchmark delta")
    return deltas


def main() -> int:
    args = parse_args()
    status: dict[str, Any] = {
        "phase": "29",
        "probe_only": bool(args.probe_only),
        "mode": args.mode,
        "passed": False,
        "errors": [],
    }
    try:
        baseline_artifact = load_json_object(args.baseline_artifact, "Phase 29 baseline artifact")
        candidate_artifact = load_json_object(args.candidate_artifact, "Phase 29 candidate artifact")
        baseline_results = validate_contract(
            baseline_artifact, label="baseline", mode=args.mode, variant="baseline"
        )
        candidate_results = validate_contract(
            candidate_artifact, label="candidate", mode=args.mode, variant="candidate"
        )
        validate_parity(baseline_artifact, candidate_artifact, baseline_results, candidate_results)
        baseline_summary = summarize_results(baseline_results)
        candidate_summary = summarize_results(candidate_results)
        deltas = compute_deltas(baseline_summary, candidate_summary)
        status.update(
            {
                "passed": True,
                "slice_id": normalize_text(baseline_artifact.get("slice_id")),
                "case_count": len(baseline_results),
                "baseline_artifact": str(Path(args.baseline_artifact).expanduser().resolve()),
                "candidate_artifact": str(Path(args.candidate_artifact).expanduser().resolve()),
                "baseline_summary": baseline_summary,
                "candidate_summary": candidate_summary,
                "deltas": deltas,
            }
        )
    except ValueError as exc:
        status["errors"].append(str(exc))

    write_status(args.status_json, status)
    return 0 if status["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
