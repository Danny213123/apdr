#!/usr/bin/env python3
"""Check the Phase 24 env-first versus docker-first comparison contract."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


DOMINANT_BUCKETS = (
    "module-not-found",
    "version-not-found",
    "environment-build-failed",
)

TIMING_FIELDS = (
    "duration_seconds",
    "solve_duration_seconds",
    "validation_duration_seconds",
    "env_create_duration_seconds",
    "install_duration_seconds",
    "docker_startup_duration_seconds",
    "smoke_duration_seconds",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Check the Phase 24 env-first versus docker-first comparison contract."
    )
    parser.add_argument("--env-artifact", required=True, help="Path to the env-first artifact JSON.")
    parser.add_argument("--docker-artifact", required=True, help="Path to the docker-first artifact JSON.")
    parser.add_argument("--status-json", required=True, help="Path to write the checker status.")
    parser.add_argument(
        "--probe-only",
        action="store_true",
        help="Validate the frozen contract without requiring a live paired replay.",
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


def write_status(path_text: str, payload: dict[str, Any]) -> None:
    path = Path(path_text).expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def validate_contract(artifact: dict[str, Any], *, expected_policy: str, label: str) -> list[dict[str, Any]]:
    if normalize_text(artifact.get("validation_backend")) != "llm":
        raise ValueError(f"{label} artifact must keep validation_backend=llm")
    if normalize_text(artifact.get("llm_validation_policy")) != expected_policy:
        raise ValueError(f"{label} artifact must keep llm_validation_policy={expected_policy}")
    if normalize_text(artifact.get("mode")) != expected_policy:
        raise ValueError(f"{label} artifact mode must match {expected_policy}")
    if normalize_text(artifact.get("model_name")) == "":
        raise ValueError(f"{label} artifact is missing model_name")
    if normalize_text(artifact.get("base_url")) == "":
        raise ValueError(f"{label} artifact is missing base_url")

    run_contract = artifact.get("run_contract")
    if not isinstance(run_contract, dict):
        raise ValueError(f"{label} artifact must include run_contract")
    if normalize_text(run_contract.get("validation_backend")) != "llm":
        raise ValueError(f"{label} run_contract must keep validation_backend=llm")
    if normalize_text(run_contract.get("llm_validation_policy")) != expected_policy:
        raise ValueError(f"{label} run_contract must keep llm_validation_policy={expected_policy}")

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
        requested_policy = normalize_text(row.get("requested_llm_validation_policy"))
        if requested_policy != expected_policy:
            raise ValueError(
                f"{label} result row {relative_path} drifted from requested policy {expected_policy}"
            )
        validation_backend = normalize_text(row.get("validation_backend"))
        if validation_backend != "llm":
            raise ValueError(f"{label} result row {relative_path} drifted from validation_backend=llm")
        validation_path = normalize_text(row.get("validation_path"))
        if not validation_path:
            raise ValueError(f"{label} result row {relative_path} is missing validation_path")
        result_origin = normalize_text(row.get("resultOrigin"))
        if not result_origin:
            raise ValueError(f"{label} result row {relative_path} is missing resultOrigin")
        normalized.append(
            {
                "relative_path": relative_path,
                "display_status": normalize_text(row.get("display_status")),
                "requested_llm_validation_policy": requested_policy,
                "validation_backend": validation_backend,
                "validation_path": validation_path,
                "llm_validation_route": normalize_text(row.get("llm_validation_route")),
                "failure_bucket": normalize_text(row.get("failure_bucket")),
                "failure_family": normalize_text(row.get("failure_family")),
                "resultOrigin": result_origin,
                **{field: normalize_float(row.get(field)) for field in TIMING_FIELDS},
            }
        )
    return normalized


def summarize_results(results: list[dict[str, Any]]) -> dict[str, Any]:
    passes = 0
    skips = 0
    failures = 0
    failure_buckets = {bucket: 0 for bucket in DOMINANT_BUCKETS}
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
            bucket = row["failure_bucket"]
            if bucket in failure_buckets:
                failure_buckets[bucket] += 1
        origins[row["resultOrigin"]] = origins.get(row["resultOrigin"], 0) + 1
        for field in TIMING_FIELDS:
            timing_totals[field] += row[field]

    return {
        "passes": passes,
        "skips": skips,
        "failures": failures,
        "failure_buckets": failure_buckets,
        "timing_totals": {field: round(value, 2) for field, value in timing_totals.items()},
        "origins": origins,
    }


def validate_parity(
    env_artifact: dict[str, Any],
    docker_artifact: dict[str, Any],
    env_results: list[dict[str, Any]],
    docker_results: list[dict[str, Any]],
) -> None:
    if normalize_text(env_artifact.get("slice_id")) != normalize_text(docker_artifact.get("slice_id")):
        raise ValueError("Artifacts must keep the same slice_id")
    if normalize_text(env_artifact.get("model_name")) != normalize_text(docker_artifact.get("model_name")):
        raise ValueError("Artifacts must keep the same model_name")
    if normalize_text(env_artifact.get("base_url")) != normalize_text(docker_artifact.get("base_url")):
        raise ValueError("Artifacts must keep the same base_url")

    env_contract = env_artifact["run_contract"]
    docker_contract = docker_artifact["run_contract"]
    for field in ("validation_backend", "execution_mode", "build_profile", "cache_state"):
        if normalize_text(env_contract.get(field)) != normalize_text(docker_contract.get(field)):
            raise ValueError(f"Artifacts drifted on run_contract.{field}")

    env_paths = [row["relative_path"] for row in env_results]
    docker_paths = [row["relative_path"] for row in docker_results]
    if env_paths != docker_paths:
        raise ValueError("Artifacts must keep the exact same ordered case set")

    for env_row, docker_row in zip(env_results, docker_results):
        if env_row["relative_path"] != docker_row["relative_path"]:
            raise ValueError("Artifacts drifted on paired case order")
        if env_row["resultOrigin"] != docker_row["resultOrigin"]:
            raise ValueError(
                f"Artifacts drifted on resultOrigin for {env_row['relative_path']}: "
                f"{env_row['resultOrigin']} != {docker_row['resultOrigin']}"
            )


def compute_deltas(env_summary: dict[str, Any], docker_summary: dict[str, Any]) -> dict[str, Any]:
    bucket_deltas = {
        bucket: docker_summary["failure_buckets"][bucket] - env_summary["failure_buckets"][bucket]
        for bucket in DOMINANT_BUCKETS
    }
    timing_deltas = {
        field: round(
            docker_summary["timing_totals"][field] - env_summary["timing_totals"][field],
            2,
        )
        for field in TIMING_FIELDS
    }
    deltas = {
        "pass_delta": docker_summary["passes"] - env_summary["passes"],
        "skip_delta": docker_summary["skips"] - env_summary["skips"],
        "failure_delta": docker_summary["failures"] - env_summary["failures"],
        "dominant_bucket_deltas": bucket_deltas,
        "timing_deltas": timing_deltas,
    }
    if deltas["pass_delta"] == 0:
        raise ValueError("Comparison artifacts must demonstrate a non-zero pass_delta")
    if not any(value != 0 for value in bucket_deltas.values()):
        raise ValueError("Comparison artifacts must demonstrate at least one dominant-bucket delta")
    if not any(value != 0 for value in timing_deltas.values()):
        raise ValueError("Comparison artifacts must demonstrate at least one timing delta")
    return deltas


def main() -> int:
    args = parse_args()
    status: dict[str, Any] = {
        "phase": "24",
        "probe_only": bool(args.probe_only),
        "mode": "probe" if args.probe_only else "contract",
        "passed": False,
        "errors": [],
    }
    try:
        env_artifact = load_json_object(args.env_artifact, "Phase 24 env-first artifact")
        docker_artifact = load_json_object(args.docker_artifact, "Phase 24 docker-first artifact")
        env_results = validate_contract(env_artifact, expected_policy="env-first", label="env-first")
        docker_results = validate_contract(docker_artifact, expected_policy="docker-first", label="docker-first")
        validate_parity(env_artifact, docker_artifact, env_results, docker_results)
        env_summary = summarize_results(env_results)
        docker_summary = summarize_results(docker_results)
        deltas = compute_deltas(env_summary, docker_summary)
        status.update(
            {
                "passed": True,
                "slice_id": normalize_text(env_artifact.get("slice_id")),
                "case_count": len(env_results),
                "env_artifact": str(Path(args.env_artifact).expanduser().resolve()),
                "docker_artifact": str(Path(args.docker_artifact).expanduser().resolve()),
                "env_summary": env_summary,
                "docker_summary": docker_summary,
                "deltas": deltas,
            }
        )
    except ValueError as exc:
        status["errors"].append(str(exc))

    write_status(args.status_json, status)
    return 0 if status["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
