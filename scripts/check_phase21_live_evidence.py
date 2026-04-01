#!/usr/bin/env python3
"""Validate the Phase 21 live evidence pack."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
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
REQUIRED_CASE_CATEGORIES = (
    "recovered-delta",
    "backend-path-truth",
    "failure-family-truth",
    "fallback-truth",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate the Phase 21 live evidence pack.")
    parser.add_argument("--baseline-json", required=True, help="Path to the Phase 21 live baseline artifact.")
    parser.add_argument("--candidate-json", required=True, help="Path to the Phase 21 live candidate artifact.")
    parser.add_argument("--case-index", required=True, help="Path to the representative case index JSON.")
    parser.add_argument("--evidence-md", required=True, help="Path to the live evidence note.")
    parser.add_argument("--cases-md", required=True, help="Path to the representative cases guide.")
    parser.add_argument("--closeout-md", required=True, help="Path to the milestone closeout note.")
    parser.add_argument("--status-json", required=True, help="Path to write the checker status JSON.")
    parser.add_argument(
        "--probe-only",
        action="store_true",
        help="Validate the pack without requiring the final closeout note to exist yet.",
    )
    return parser.parse_args()


def load_json_object(path_text: str, label: str) -> dict[str, Any]:
    path = resolve_repo_path(path_text)
    if not path.exists():
        raise ValueError(f"{label} not found: {path}")
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"{label} is not valid JSON: {path} ({exc})") from exc
    if not isinstance(payload, dict):
        raise ValueError(f"{label} must be a JSON object: {path}")
    return payload


def resolve_repo_path(path_text: str | Path) -> Path:
    path = Path(path_text).expanduser()
    if path.is_absolute():
        return path.resolve()
    return (REPO_ROOT / path).resolve()


def first_text(*values: Any) -> str:
    for value in values:
        text = str(value or "").strip()
        if text:
            return text
    return ""


def compute_counts(results: list[dict[str, Any]]) -> dict[str, Any]:
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


def normalize_display_status(row: dict[str, Any]) -> str:
    display_status = str(row.get("display_status") or "").strip().lower()
    if display_status in {"pass", "fail", "skip"}:
        return display_status
    if bool(row.get("skipped")):
        return "skip"
    if bool(row.get("succeeded")):
        return "pass"
    return "fail"


def normalize_row(row: dict[str, Any]) -> dict[str, Any]:
    normalized = {
        "relative_path": first_text(row.get("relative_path"), row.get("snippet")),
        "display_status": normalize_display_status(row),
        "artifact_dir": first_text(row.get("artifact_dir")),
        "validation_path": first_text(row.get("validation_path"), row.get("validationPath")),
        "fallback_outcome": first_text(row.get("fallback_outcome"), row.get("fallbackOutcome")),
        "failure_family": first_text(row.get("failure_family"), row.get("failureFamily")),
        "failure_bucket": first_text(
            row.get("failure_bucket"),
            row.get("failureBucket"),
            row.get("validation_status"),
            row.get("validationStatus"),
        ),
        "validation_status": first_text(row.get("validation_status"), row.get("validationStatus")),
        "resultOrigin": first_text(row.get("resultOrigin")),
        "escalated_backend": first_text(row.get("escalated_backend"), row.get("escalatedBackend")),
    }
    normalized["raw"] = row
    return normalized


def validate_sample(
    payload: dict[str, Any],
    label: str,
    *,
    expected_slice_id: str | None,
    expected_backend: str | None,
    expected_model: str | None,
) -> dict[str, Any]:
    slice_id = str(payload.get("slice_id") or "").strip()
    if not slice_id:
        raise ValueError(f"{label} is missing slice_id")
    if expected_slice_id is not None and slice_id != expected_slice_id:
        raise ValueError(f"{label} slice_id mismatch: {slice_id!r} != {expected_slice_id!r}")

    backend = str(payload.get("validation_backend") or "").strip().lower()
    model_name = str(payload.get("model_name") or "").strip()
    if expected_backend is not None and backend != expected_backend:
        raise ValueError(f"{label} validation_backend mismatch: {backend!r} != {expected_backend!r}")
    if expected_model is not None and model_name != expected_model:
        raise ValueError(f"{label} model_name mismatch: {model_name!r} != {expected_model!r}")

    results = payload.get("results")
    historical_results = payload.get("historical_results")
    if not isinstance(results, list) or not results:
        raise ValueError(f"{label} results must be a non-empty list")
    if not isinstance(historical_results, list):
        raise ValueError(f"{label} historical_results must be a list")

    normalized_results = [normalize_row(row) for row in results if isinstance(row, dict)]
    actual_paths = [row["relative_path"] for row in normalized_results]
    if tuple(actual_paths) != EXPECTED_SLICE_PATHS:
        raise ValueError(f"{label} results do not preserve the locked slice ordering")

    for row in normalized_results:
        if not row["resultOrigin"]:
            raise ValueError(f"{label} rows must preserve resultOrigin")
        if not row["validation_path"]:
            raise ValueError(f"{label} rows must preserve validation_path")
        if not row["artifact_dir"]:
            raise ValueError(f"{label} rows must preserve artifact_dir")

    computed = compute_counts(normalized_results)
    declared = payload.get("counts")
    if isinstance(declared, dict):
        declared_buckets = declared.get("failure_buckets")
        if not isinstance(declared_buckets, dict):
            raise ValueError(f"{label} counts.failure_buckets must be an object")
        for key in ("passes", "skips", "failures"):
            if int(declared.get(key, -1)) != computed[key]:
                raise ValueError(f"{label} declared counts do not match computed {key}")
        for bucket in DOMINANT_BUCKETS:
            if int(declared_buckets.get(bucket, -1)) != computed["failure_buckets"][bucket]:
                raise ValueError(f"{label} declared counts do not match computed {bucket}")

    source_run = str(payload.get("source_run") or "").strip()
    if label == "candidate artifact":
        if not source_run.startswith("runs/"):
            raise ValueError("candidate artifact must point to a real runs/... source_run")
        if "probe" in source_run:
            raise ValueError("candidate artifact must not point to a probe-only source_run")

    return {
        "slice_id": slice_id,
        "validation_backend": backend,
        "model_name": model_name,
        "source_run": source_run,
        "counts": computed,
        "results": normalized_results,
        "row_by_path": {row["relative_path"]: row for row in normalized_results},
    }


def validate_markdown(path_text: str, label: str, headings: tuple[str, ...], phrases: tuple[str, ...]) -> dict[str, Any]:
    path = resolve_repo_path(path_text)
    if not path.exists():
        raise ValueError(f"{label} not found: {path}")
    content = path.read_text(encoding="utf-8")
    missing_headings = [heading for heading in headings if heading not in content]
    missing_phrases = [phrase for phrase in phrases if phrase not in content]
    if missing_headings:
        raise ValueError(f"{label} is missing headings: {', '.join(missing_headings)}")
    if missing_phrases:
        raise ValueError(f"{label} is missing phrases: {', '.join(missing_phrases)}")
    return {
        "path": str(path),
        "headings": list(headings),
        "phrases": list(phrases),
    }


def load_case_entries(payload: dict[str, Any]) -> list[dict[str, Any]]:
    entries = payload.get("entries")
    if isinstance(entries, list):
        return [entry for entry in entries if isinstance(entry, dict)]
    if isinstance(payload.get("cases"), list):
        return [entry for entry in payload["cases"] if isinstance(entry, dict)]
    raise ValueError("case index must contain an entries array")


def field_value(row: dict[str, Any], field_name: str) -> str:
    mapping = {
        "validation_path": "validation_path",
        "fallback_outcome": "fallback_outcome",
        "failure_family": "failure_family",
        "resultOrigin": "resultOrigin",
        "display_status": "display_status",
        "failure_bucket": "failure_bucket",
        "validation_status": "validation_status",
        "artifact_dir": "artifact_dir",
        "escalated_backend": "escalated_backend",
    }
    key = mapping.get(field_name, field_name)
    value = row.get(key)
    return str(value or "").strip()


def validate_case_index(
    payload: dict[str, Any],
    baseline_contract: dict[str, Any],
    candidate_contract: dict[str, Any],
) -> dict[str, Any]:
    entries = load_case_entries(payload)
    if not entries:
        raise ValueError("case index must contain at least one entry")

    category_map: dict[str, list[str]] = {category: [] for category in REQUIRED_CASE_CATEGORIES}
    union_fields: set[str] = set()
    recovered_delta_paths: list[str] = []
    truthful_non_pass_paths: list[str] = []
    backend_truth_paths: list[str] = []
    fallback_truth_paths: list[str] = []

    for index, entry in enumerate(entries):
        category = str(entry.get("category") or "").strip()
        relative_path = str(entry.get("relative_path") or "").strip()
        proof_claim = str(entry.get("proof_claim") or "").strip()
        reviewer_fields = entry.get("reviewer_fields")
        baseline_artifact = str(entry.get("baseline_artifact") or "").strip()
        candidate_artifact = str(entry.get("candidate_artifact") or "").strip()

        if not category:
            raise ValueError(f"case index entry {index} is missing category")
        if not relative_path:
            raise ValueError(f"case index entry {index} is missing relative_path")
        if not proof_claim:
            raise ValueError(f"case index entry {index} is missing proof_claim")
        if not isinstance(reviewer_fields, list) or not reviewer_fields:
            raise ValueError(f"case index entry {index} must provide reviewer_fields")
        if not baseline_artifact or not candidate_artifact:
            raise ValueError(f"case index entry {index} must provide baseline_artifact and candidate_artifact")

        baseline_path = resolve_repo_path(baseline_artifact)
        candidate_path = resolve_repo_path(candidate_artifact)
        if not baseline_path.exists():
            raise ValueError(f"case index baseline_artifact does not exist: {baseline_path}")
        if not candidate_path.exists():
            raise ValueError(f"case index candidate_artifact does not exist: {candidate_path}")

        baseline_row = baseline_contract["row_by_path"].get(relative_path)
        candidate_row = candidate_contract["row_by_path"].get(relative_path)
        if baseline_row is None:
            raise ValueError(f"baseline artifact is missing case index path: {relative_path}")
        if candidate_row is None:
            raise ValueError(f"candidate artifact is missing case index path: {relative_path}")

        normalized_fields = [str(field).strip() for field in reviewer_fields if str(field).strip()]
        if not normalized_fields:
            raise ValueError(f"case index entry {index} reviewer_fields cannot be empty")
        union_fields.update(normalized_fields)
        for field_name in normalized_fields:
            if not field_value(candidate_row, field_name) and not field_value(baseline_row, field_name):
                raise ValueError(
                    f"case index field {field_name!r} is empty in both baseline and candidate for {relative_path}"
                )

        if category in category_map:
            category_map[category].append(relative_path)

        if category == "recovered-delta":
            if baseline_row["display_status"] != "pass" and candidate_row["display_status"] == "pass":
                recovered_delta_paths.append(relative_path)
        if category == "backend-path-truth":
            candidate_path_truth = candidate_row["validation_path"]
            if candidate_path_truth and ("->" in candidate_path_truth or candidate_path_truth != baseline_row["validation_path"]):
                backend_truth_paths.append(relative_path)
        if category == "failure-family-truth":
            if candidate_row["display_status"] != "pass" and candidate_row["failure_family"] and candidate_row["resultOrigin"]:
                truthful_non_pass_paths.append(relative_path)
        if category == "fallback-truth":
            if candidate_row["fallback_outcome"] and candidate_row["validation_path"]:
                fallback_truth_paths.append(relative_path)

    missing_categories = [category for category, paths in category_map.items() if not paths]
    if missing_categories:
        raise ValueError(f"case index is missing required categories: {', '.join(missing_categories)}")
    missing_truth_fields = [field for field in ("fallback_outcome", "resultOrigin", "validation_path", "failure_family") if field not in union_fields]
    if missing_truth_fields:
        raise ValueError(f"case index reviewer_fields must surface: {', '.join(missing_truth_fields)}")
    if not recovered_delta_paths:
        raise ValueError("case index must include at least one real recovered-delta case")
    if not truthful_non_pass_paths:
        raise ValueError("case index must include at least one truthful non-pass case")
    if not backend_truth_paths:
        raise ValueError("case index must include at least one backend-path-truth case")
    if not fallback_truth_paths:
        raise ValueError("case index must include at least one fallback-truth case")

    return {
        "entry_count": len(entries),
        "required_categories": list(REQUIRED_CASE_CATEGORIES),
        "category_paths": category_map,
        "recovered_delta_paths": recovered_delta_paths,
        "truthful_non_pass_paths": truthful_non_pass_paths,
        "backend_truth_paths": backend_truth_paths,
        "fallback_truth_paths": fallback_truth_paths,
        "reviewer_fields": sorted(union_fields),
    }


def main() -> int:
    args = parse_args()
    errors: list[str] = []
    status_payload: dict[str, Any] = {
        "phase": "21",
        "plan": "03",
        "mode": "probe" if args.probe_only else "live",
        "probe_only": bool(args.probe_only),
        "passed": False,
        "errors": errors,
    }

    try:
        baseline_payload = load_json_object(args.baseline_json, "Phase 21 baseline artifact")
        candidate_payload = load_json_object(args.candidate_json, "Phase 21 candidate artifact")
        case_index_payload = load_json_object(args.case_index, "Phase 21 case index")

        baseline_contract = validate_sample(
            baseline_payload,
            "baseline artifact",
            expected_slice_id=None,
            expected_backend=None,
            expected_model=None,
        )
        candidate_contract = validate_sample(
            candidate_payload,
            "candidate artifact",
            expected_slice_id=baseline_contract["slice_id"],
            expected_backend=baseline_contract["validation_backend"],
            expected_model=baseline_contract["model_name"],
        )

        baseline_counts = baseline_contract["counts"]
        candidate_counts = candidate_contract["counts"]
        delta_passes = candidate_counts["passes"] - baseline_counts["passes"]
        bucket_deltas = {
            bucket: candidate_counts["failure_buckets"][bucket] - baseline_counts["failure_buckets"][bucket]
            for bucket in DOMINANT_BUCKETS
        }
        if delta_passes <= 0:
            raise ValueError("candidate artifact must show a strictly positive pass delta")
        unchanged_or_worse = {
            bucket: delta for bucket, delta in bucket_deltas.items() if delta >= 0
        }
        if unchanged_or_worse:
            raise ValueError(
                "candidate artifact must reduce every dominant bucket: "
                + ", ".join(f"{bucket}={delta}" for bucket, delta in unchanged_or_worse.items())
            )

        evidence_contract = validate_markdown(
            args.evidence_md,
            "live evidence note",
            headings=(
                "## Locked Slice",
                "## Before/After Bucket Counts",
                "## Run Contract",
                "## Review Notes",
            ),
            phrases=("March 30, 2026 baseline", "v2.3 candidate"),
        )
        cases_contract = validate_markdown(
            args.cases_md,
            "representative cases guide",
            headings=(
                "## Representative Cases",
                "## Recovered Cases",
                "## Truth-Surface Cases",
                "## Remaining Limits",
            ),
            phrases=("validation_path", "failure_family", "resultOrigin"),
        )
        case_contract = validate_case_index(case_index_payload, baseline_contract, candidate_contract)

        closeout_contract: dict[str, Any] | None = None
        closeout_path = resolve_repo_path(args.closeout_md)
        if not args.probe_only or closeout_path.exists():
            closeout_contract = validate_markdown(
                args.closeout_md,
                "milestone closeout note",
                headings=(
                    "## Evidence Mode",
                    "## Before/After Counts",
                    "## Representative Cases",
                    "## Requirement Verdicts",
                    "## Final Signoff",
                ),
                phrases=("EVD-08",),
            )

        status_payload.update(
            {
                "passed": True,
                "baseline_contract": {
                    "path": str(resolve_repo_path(args.baseline_json)),
                    "slice_id": baseline_contract["slice_id"],
                    "validation_backend": baseline_contract["validation_backend"],
                    "model_name": baseline_contract["model_name"],
                    "source_run": baseline_contract["source_run"],
                    "counts": baseline_counts,
                },
                "candidate_contract": {
                    "path": str(resolve_repo_path(args.candidate_json)),
                    "slice_id": candidate_contract["slice_id"],
                    "validation_backend": candidate_contract["validation_backend"],
                    "model_name": candidate_contract["model_name"],
                    "source_run": candidate_contract["source_run"],
                    "counts": candidate_counts,
                },
                "delta_contract": {
                    "passed": True,
                    "delta_passes": delta_passes,
                    "bucket_deltas": bucket_deltas,
                },
                "case_contract": {
                    "passed": True,
                    **case_contract,
                },
                "markdown_contract": {
                    "passed": True,
                    "evidence": evidence_contract,
                    "cases": cases_contract,
                    "closeout": closeout_contract,
                },
            }
        )
    except Exception as exc:  # noqa: BLE001
        errors.append(str(exc))

    status_path = resolve_repo_path(args.status_json)
    status_path.write_text(json.dumps(status_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    if errors:
        raise SystemExit(1)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
