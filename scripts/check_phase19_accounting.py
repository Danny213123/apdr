#!/usr/bin/env python3
"""Check the Phase 19 classification and resume-accounting proof artifacts."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
from typing import Any


EXPECTED_SLICE_CASES = (
    (
        "hard-gists/0115e0ce312f26ff59f4fbf4f5821ca2/snippet.py",
        "skip",
        "environment-specific",
    ),
    (
        "hard-gists/00135b0dfee0ae165ad2/snippet.py",
        "skip",
        "environment-specific",
    ),
    (
        "hard-gists/04ef258fa29e4e685287a30cf60462d0/snippet.py",
        "fail",
        "dependency-resolution",
    ),
    (
        "hard-gists/00e9638c0efad1adac878522cf172484/snippet.py",
        "fail",
        "dependency-resolution",
    ),
)
ENVIRONMENT_SPECIFIC_STATUSES = {
    "host-runtime-required",
    "skipped-framework-runtime",
    "skipped-host-runtime",
}
DEPENDENCY_RESOLUTION_STATUSES = {
    "environment-build-failed",
    "module-not-found",
    "version-not-found",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Check Phase 19 accounting and classification proof artifacts."
    )
    parser.add_argument("--slice-json", required=True, help="Path to the fixed live slice manifest.")
    parser.add_argument(
        "--fixture-json",
        required=True,
        help="Path to the mixed historical/live fixture used for provenance checks.",
    )
    parser.add_argument(
        "--status-json",
        required=True,
        help="Path to write the machine-readable checker status.",
    )
    parser.add_argument(
        "--probe-only",
        action="store_true",
        help="Validate the fixed slice and fixture contracts without any replay step.",
    )
    return parser.parse_args()


def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


sys.path.insert(0, str(repo_root()))

from benchmark_ui.service import BenchmarkService


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


def resolve_repo_path(path_text: str) -> Path:
    path = Path(path_text).expanduser()
    if path.is_absolute():
        return path.resolve()
    return (repo_root() / path).resolve()


def validate_slice_contract(payload: dict[str, Any], source_path: str) -> dict[str, Any]:
    slice_id = str(payload.get("slice_id") or "").strip()
    if not slice_id:
        raise ValueError(f"Slice manifest is missing slice_id: {source_path}")

    cases = payload.get("cases")
    if not isinstance(cases, list) or not cases:
        raise ValueError(f"Slice manifest must contain a non-empty cases array: {source_path}")

    expected_paths = [row[0] for row in EXPECTED_SLICE_CASES]
    actual_paths: list[str] = []
    for index, entry in enumerate(cases):
        if not isinstance(entry, dict):
            raise ValueError(f"Slice case {index} must be an object: {source_path}")
        relative_path = str(entry.get("relative_path") or "").strip()
        if not relative_path:
            raise ValueError(f"Slice case {index} is missing relative_path: {source_path}")
        actual_paths.append(relative_path)
    if actual_paths != expected_paths:
        raise ValueError(
            "Slice manifest must keep the fixed March 30 relative_path contract in order: "
            + ", ".join(expected_paths)
        )

    summary_path_text = str(payload.get("source_summary") or "").strip()
    if not summary_path_text:
        raise ValueError(f"Slice manifest is missing source_summary: {source_path}")
    summary_path = resolve_repo_path(summary_path_text)
    summary = load_json_object(str(summary_path), "Phase 19 source summary")
    results = summary.get("results")
    if not isinstance(results, list):
        raise ValueError(f"Phase 19 source summary is missing results array: {summary_path}")

    result_map = {
        str(result.get("snippet") or "").strip(): result
        for result in results
        if isinstance(result, dict)
    }
    service = BenchmarkService()
    checked_cases: list[dict[str, Any]] = []
    for entry, expected in zip(cases, EXPECTED_SLICE_CASES):
        relative_path, expected_display_status, expected_failure_family = expected
        result = result_map.get(relative_path)
        if not isinstance(result, dict):
            raise ValueError(f"Source summary is missing locked slice case: {relative_path}")

        observed_status = str(entry.get("observed_validation_status") or "").strip().lower()
        actual_validation_status = service._result_validation_status(result)
        if observed_status != actual_validation_status:
            raise ValueError(
                f"{relative_path} observed_validation_status does not match source summary: "
                f"{observed_status!r} != {actual_validation_status!r}"
            )

        observed_reason = str(entry.get("observed_validation_reason") or "").strip()
        actual_reason = service._result_validation_reason(result)
        if observed_reason != actual_reason:
            raise ValueError(
                f"{relative_path} observed_validation_reason does not match source summary."
            )

        actual_display_status = service._display_status(result).lower()
        if actual_display_status != expected_display_status:
            raise ValueError(
                f"{relative_path} expected_display_status mismatch: "
                f"{expected_display_status!r} != {actual_display_status!r}"
            )

        declared_failure_family = str(entry.get("expected_failure_family") or "").strip().lower()
        if declared_failure_family != expected_failure_family:
            raise ValueError(
                f"{relative_path} expected_failure_family must stay locked to "
                f"{expected_failure_family!r}, got {declared_failure_family!r}"
            )

        if expected_failure_family == "environment-specific":
            allowed_statuses = ENVIRONMENT_SPECIFIC_STATUSES
        else:
            allowed_statuses = DEPENDENCY_RESOLUTION_STATUSES
        if actual_validation_status not in allowed_statuses:
            raise ValueError(
                f"{relative_path} source validation_status {actual_validation_status!r} is incompatible "
                f"with expected_failure_family {expected_failure_family!r}"
            )

        checked_cases.append(
            {
                "relative_path": relative_path,
                "validation_status": actual_validation_status,
                "validation_reason": actual_reason,
                "display_status": actual_display_status,
                "expected_failure_family": expected_failure_family,
                "artifact_dir": str(entry.get("artifact_dir") or "").strip(),
            }
        )

    return {
        "path": str(Path(source_path).expanduser().resolve()),
        "slice_id": slice_id,
        "source_summary": str(summary_path),
        "case_count": len(checked_cases),
        "relative_paths": actual_paths,
        "checked_cases": checked_cases,
    }


def _expected_count_block(raw: Any, label: str, *, required: bool = True) -> dict[str, int]:
    if raw is None and not required:
        return {}
    if not isinstance(raw, dict):
        raise ValueError(f"{label} must be an object")
    normalized: dict[str, int] = {}
    for key in ("completed", "successes", "failures", "skipped"):
        if key not in raw:
            raise ValueError(f"{label} is missing {key}")
        normalized[key] = int(raw[key])
    return normalized


def validate_fixture_contract(payload: dict[str, Any], source_path: str) -> dict[str, Any]:
    fixture_id = str(payload.get("fixture_id") or "").strip()
    if not fixture_id:
        raise ValueError(f"Fixture is missing fixture_id: {source_path}")

    summary = payload.get("summary")
    if not isinstance(summary, dict):
        raise ValueError(f"Fixture is missing summary object: {source_path}")
    historical_results = summary.get("historical_results")
    live_results = summary.get("results")
    if not isinstance(historical_results, list) or not historical_results:
        raise ValueError(f"Fixture must contain non-empty historical_results: {source_path}")
    if not isinstance(live_results, list) or not live_results:
        raise ValueError(f"Fixture must contain non-empty results: {source_path}")

    expected = payload.get("expected")
    if not isinstance(expected, dict):
        raise ValueError(f"Fixture is missing expected object: {source_path}")
    combined_expected = _expected_count_block(expected.get("combined"), "expected.combined")
    live_expected = _expected_count_block(expected.get("live_only"), "expected.live_only")
    historical_expected = _expected_count_block(
        expected.get("historical_only"),
        "expected.historical_only",
    )
    live_only_available = bool(expected.get("live_only_available"))
    expected_origins = expected.get("completed_case_origins")
    if not isinstance(expected_origins, list) or not expected_origins:
        raise ValueError(f"expected.completed_case_origins must be a non-empty array: {source_path}")

    if combined_expected["completed"] == live_expected["completed"]:
        raise ValueError("Fixture must encode a live-only completed count that differs from the combined count")

    service = BenchmarkService()
    snapshot = service._historical_run_snapshot(
        "phase19-proof-fixture",
        summary,
        repo_root(),
    )

    actual_combined = {
        "completed": int(snapshot.get("completed") or 0),
        "successes": int(snapshot.get("successes") or 0),
        "failures": int(snapshot.get("failures") or 0),
        "skipped": int(snapshot.get("skipped") or 0),
    }
    actual_live = {
        "completed": int(snapshot.get("liveCompleted") or 0),
        "successes": int(snapshot.get("liveSuccesses") or 0),
        "failures": int(snapshot.get("liveFailures") or 0),
        "skipped": int(snapshot.get("liveSkipped") or 0),
    }
    actual_historical = {
        "completed": int(snapshot.get("historicalCompleted") or 0),
        "successes": actual_combined["successes"] - actual_live["successes"],
        "failures": actual_combined["failures"] - actual_live["failures"],
        "skipped": actual_combined["skipped"] - actual_live["skipped"],
    }
    actual_origins = [
        str(row.get("resultOrigin") or "").strip()
        for row in snapshot.get("completedCases", [])
        if isinstance(row, dict)
    ]

    if actual_combined != combined_expected:
        raise ValueError(
            f"Combined accounting mismatch: expected {combined_expected}, got {actual_combined}"
        )
    if actual_live != live_expected:
        raise ValueError(
            f"Live-only accounting mismatch: expected {live_expected}, got {actual_live}"
        )
    if actual_historical != historical_expected:
        raise ValueError(
            f"Historical-only accounting mismatch: expected {historical_expected}, got {actual_historical}"
        )
    if bool(snapshot.get("liveOnlyAvailable")) != live_only_available:
        raise ValueError(
            "Fixture liveOnlyAvailable mismatch: "
            f"expected {live_only_available!r}, got {bool(snapshot.get('liveOnlyAvailable'))!r}"
        )
    if actual_origins != [str(item) for item in expected_origins]:
        raise ValueError(
            f"Completed-case origin order mismatch: expected {expected_origins}, got {actual_origins}"
        )

    return {
        "path": str(Path(source_path).expanduser().resolve()),
        "fixture_id": fixture_id,
        "combined": actual_combined,
        "live_only": actual_live,
        "historical_only": actual_historical,
        "live_only_available": bool(snapshot.get("liveOnlyAvailable")),
        "completed_case_origins": actual_origins,
    }


def write_status(path_text: str, payload: dict[str, Any]) -> None:
    path = Path(path_text).expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def main() -> int:
    args = parse_args()
    errors: list[str] = []
    slice_contract: dict[str, Any] | None = None
    provenance_contract: dict[str, Any] | None = None
    try:
        slice_payload = load_json_object(args.slice_json, "Slice manifest")
        slice_contract = validate_slice_contract(slice_payload, args.slice_json)
    except ValueError as exc:
        errors.append(str(exc))

    try:
        fixture_payload = load_json_object(args.fixture_json, "Mixed provenance fixture")
        provenance_contract = validate_fixture_contract(fixture_payload, args.fixture_json)
    except ValueError as exc:
        errors.append(str(exc))

    status = {
        "errors": errors,
        "mode": "probe" if args.probe_only else "check",
        "passed": not errors,
        "phase": "19",
        "plan": "03",
        "probe_only": bool(args.probe_only),
        "slice_contract": slice_contract,
        "provenance_contract": provenance_contract,
    }
    write_status(args.status_json, status)
    if errors:
        for message in errors:
            print(message, flush=True)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
