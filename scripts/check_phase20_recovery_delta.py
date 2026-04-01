#!/usr/bin/env python3
"""Check the Phase 20 dominant-bucket recovery proof artifacts."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


EXPECTED_SLICE_PATHS = (
    "hard-gists/04ef258fa29e4e685287a30cf60462d0/snippet.py",
    "hard-gists/09648344984565f9477a/snippet.py",
    "hard-gists/101323115e70bb6671d3/snippet.py",
    "hard-gists/10295174/snippet.py",
    "hard-gists/1096373/snippet.py",
    "hard-gists/1239373/snippet.py",
    "hard-gists/00e9638c0efad1adac878522cf172484/snippet.py",
    "hard-gists/056626de3fbdc7cf7b59de1d9f6279d1/snippet.py",
    "hard-gists/03de5c4c21138da5c29d/snippet.py",
)
DOMINANT_BUCKETS = (
    "module-not-found",
    "version-not-found",
    "environment-build-failed",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Check the Phase 20 dominant-bucket recovery proof artifacts."
    )
    parser.add_argument("--slice-json", required=True, help="Path to the fixed Phase 20 slice manifest.")
    parser.add_argument("--baseline-json", required=True, help="Path to the baseline sample artifact.")
    parser.add_argument("--candidate-json", required=True, help="Path to the candidate sample artifact.")
    parser.add_argument("--status-json", required=True, help="Path to write the checker status JSON.")
    parser.add_argument(
        "--probe-only",
        action="store_true",
        help="Validate the proof contract without requiring a live replay.",
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


def validate_slice_contract(payload: dict[str, Any], source_path: str) -> dict[str, Any]:
    slice_id = str(payload.get("slice_id") or "").strip()
    if not slice_id:
        raise ValueError(f"Slice manifest is missing slice_id: {source_path}")
    cases = payload.get("cases")
    if not isinstance(cases, list) or not cases:
        raise ValueError(f"Slice manifest must contain a non-empty cases array: {source_path}")
    actual_paths = []
    for index, entry in enumerate(cases):
        if not isinstance(entry, dict):
            raise ValueError(f"Slice case {index} must be an object: {source_path}")
        relative_path = str(entry.get("relative_path") or "").strip()
        if not relative_path:
            raise ValueError(f"Slice case {index} is missing relative_path: {source_path}")
        actual_paths.append(relative_path)
    if tuple(actual_paths) != EXPECTED_SLICE_PATHS:
        raise ValueError(
            "Slice manifest must keep the fixed dominant-bucket ordering: "
            + ", ".join(EXPECTED_SLICE_PATHS)
        )
    return {
        "path": str(Path(source_path).expanduser().resolve()),
        "slice_id": slice_id,
        "relative_paths": actual_paths,
        "case_count": len(actual_paths),
    }


def computed_counts(results: list[dict[str, Any]]) -> dict[str, Any]:
    passes = 0
    skips = 0
    failures = 0
    buckets = {bucket: 0 for bucket in DOMINANT_BUCKETS}
    for row in results:
        display_status = str(row.get("display_status") or "").strip().lower()
        if display_status == "pass":
            passes += 1
            continue
        if display_status == "skip":
            skips += 1
            continue
        failures += 1
        bucket = str(row.get("failure_bucket") or row.get("validation_status") or "").strip()
        if bucket in buckets:
            buckets[bucket] += 1
    return {
        "passes": passes,
        "skips": skips,
        "failures": failures,
        "failure_buckets": buckets,
    }


def validate_sample(
    payload: dict[str, Any],
    label: str,
    *,
    expected_slice_id: str,
    expected_backend: str | None,
    expected_model: str | None,
) -> dict[str, Any]:
    slice_id = str(payload.get("slice_id") or "").strip()
    if slice_id != expected_slice_id:
        raise ValueError(f"{label} slice_id mismatch: {slice_id!r} != {expected_slice_id!r}")

    backend = str(payload.get("validation_backend") or "").strip().lower()
    model_name = str(payload.get("model_name") or "").strip()
    if expected_backend is not None and backend != expected_backend:
        raise ValueError(f"{label} validation_backend mismatch: {backend!r} != {expected_backend!r}")
    if expected_model is not None and model_name != expected_model:
        raise ValueError(f"{label} model_name mismatch: {model_name!r} != {expected_model!r}")

    historical_results = payload.get("historical_results")
    if not isinstance(historical_results, list):
        raise ValueError(f"{label} historical_results must be a list")
    results = payload.get("results")
    if not isinstance(results, list) or not results:
        raise ValueError(f"{label} results must be a non-empty list")

    actual_paths = [str(row.get("relative_path") or "").strip() for row in results]
    if tuple(actual_paths) != EXPECTED_SLICE_PATHS:
        raise ValueError(f"{label} results do not preserve the locked slice ordering")

    for row in results:
        if not str(row.get("resultOrigin") or "").strip():
            raise ValueError(f"{label} rows must preserve resultOrigin for Phase 19 provenance truth")
        if not str(row.get("validation_path") or "").strip():
            raise ValueError(f"{label} rows must preserve validation_path for Phase 18 route truth")

    counts = computed_counts(results)
    declared_counts = payload.get("counts")
    if isinstance(declared_counts, dict):
        declared_buckets = declared_counts.get("failure_buckets")
        if not isinstance(declared_buckets, dict):
            raise ValueError(f"{label} counts.failure_buckets must be an object")
        for key in ("passes", "skips", "failures"):
            if int(declared_counts.get(key, -1)) != counts[key]:
                raise ValueError(f"{label} declared counts do not match computed {key}")
        for bucket in DOMINANT_BUCKETS:
            if int(declared_buckets.get(bucket, -1)) != counts["failure_buckets"][bucket]:
                raise ValueError(f"{label} declared counts do not match computed {bucket}")

    return {
        "path": str(Path(payload.get("source_summary") or "").expanduser())
        if payload.get("source_summary")
        else label,
        "slice_id": slice_id,
        "validation_backend": backend,
        "model_name": model_name,
        "counts": counts,
        "results": results,
    }


def validate_baseline_against_slice(
    slice_payload: dict[str, Any],
    baseline_results: list[dict[str, Any]],
) -> list[dict[str, str]]:
    baseline_by_path = {
        str(row.get("relative_path") or "").strip(): row for row in baseline_results
    }
    checked_cases = []
    for entry in slice_payload["cases"]:
        relative_path = str(entry.get("relative_path") or "").strip()
        baseline = baseline_by_path.get(relative_path)
        if baseline is None:
            raise ValueError(f"Baseline sample is missing slice case: {relative_path}")
        expected_status = str(entry.get("observed_validation_status") or "").strip()
        observed_status = str(baseline.get("validation_status") or "").strip()
        if observed_status != expected_status:
            raise ValueError(
                f"{relative_path} baseline validation_status mismatch: "
                f"{observed_status!r} != {expected_status!r}"
            )
        checked_cases.append(
            {
                "relative_path": relative_path,
                "validation_status": observed_status,
                "failure_bucket": str(baseline.get("failure_bucket") or "").strip(),
            }
        )
    return checked_cases


def main() -> int:
    args = parse_args()
    errors: list[str] = []
    status_payload: dict[str, Any] = {
        "phase": "20",
        "plan": "03",
        "mode": "probe" if args.probe_only else "live",
        "probe_only": bool(args.probe_only),
        "passed": False,
        "errors": errors,
    }
    try:
        slice_payload = load_json_object(args.slice_json, "Phase 20 slice manifest")
        baseline_payload = load_json_object(args.baseline_json, "Phase 20 baseline sample")
        candidate_payload = load_json_object(args.candidate_json, "Phase 20 candidate sample")

        slice_contract = validate_slice_contract(slice_payload, args.slice_json)
        baseline_contract = validate_sample(
            baseline_payload,
            "baseline sample",
            expected_slice_id=slice_contract["slice_id"],
            expected_backend=None,
            expected_model=None,
        )
        candidate_contract = validate_sample(
            candidate_payload,
            "candidate sample",
            expected_slice_id=slice_contract["slice_id"],
            expected_backend=baseline_contract["validation_backend"],
            expected_model=baseline_contract["model_name"],
        )
        checked_cases = validate_baseline_against_slice(slice_payload, baseline_contract["results"])

        baseline_counts = baseline_contract["counts"]
        candidate_counts = candidate_contract["counts"]
        delta_passes = candidate_counts["passes"] - baseline_counts["passes"]
        bucket_deltas = {
            bucket: candidate_counts["failure_buckets"][bucket] - baseline_counts["failure_buckets"][bucket]
            for bucket in DOMINANT_BUCKETS
        }
        if delta_passes <= 0:
            raise ValueError("Candidate sample must show a strictly positive pass delta")
        unchanged_or_worse = {
            bucket: delta for bucket, delta in bucket_deltas.items() if delta >= 0
        }
        if unchanged_or_worse:
            raise ValueError(
                "Candidate sample must reduce every dominant bucket: "
                + ", ".join(f"{bucket}={delta}" for bucket, delta in unchanged_or_worse.items())
            )

        status_payload.update(
            {
                "passed": True,
                "slice_contract": slice_contract,
                "baseline_contract": {
                    "path": str(Path(args.baseline_json).expanduser().resolve()),
                    "counts": baseline_counts,
                    "checked_cases": checked_cases,
                },
                "candidate_contract": {
                    "path": str(Path(args.candidate_json).expanduser().resolve()),
                    "counts": candidate_counts,
                },
                "delta_passes": delta_passes,
                "bucket_deltas": bucket_deltas,
            }
        )
    except Exception as exc:  # noqa: BLE001
        errors.append(str(exc))

    status_path = Path(args.status_json).expanduser().resolve()
    status_path.write_text(json.dumps(status_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    if errors:
        raise SystemExit(1)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
