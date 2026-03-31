#!/usr/bin/env python3
"""Check the Phase 17 fixed slice and fallback-outcome proof artifacts."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


EXPECTED_SLICE_PATHS = (
    "hard-gists/00e9638c0efad1adac878522cf172484/snippet.py",
    "hard-gists/01c99322cf985e771827/snippet.py",
    "hard-gists/01b8b8e1909ae0f601c85e142f2bd15b/snippet.py",
    "hard-gists/026a4d6400b1efac9a13a3296f16e655/snippet.py",
    "hard-gists/1233846/snippet.py",
)
REQUIRED_SAMPLE_KEYS = (
    "fallback_invoked",
    "fallback_outcome",
    "fallback_reason",
    "validation_status",
)
REQUIRED_SAMPLE_OUTCOMES = ("passed", "abstained", "failed")
REQUIRED_LIVE_METADATA_KEYS = (
    "fallback_invoked",
    "fallback_outcome",
    "fallback_reason",
)
CRASH_SIGNATURE = "ValueError: 'confidence' is already being used as a state key"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Check Phase 17 fallback proof artifacts and live replay outputs."
    )
    parser.add_argument("--run-dir", default="", help="Benchmark run directory to validate in live mode.")
    parser.add_argument("--slice-json", required=True, help="Path to the fixed live slice manifest.")
    parser.add_argument("--sample-json", required=True, help="Path to the fallback outcome sample contract.")
    parser.add_argument("--status-json", required=True, help="Path to write the machine-readable checker status.")
    parser.add_argument(
        "--proof-md",
        default="",
        help="Optional reviewer-facing proof note path. Accepted for the live replay contract.",
    )
    parser.add_argument(
        "--probe-only",
        action="store_true",
        help="Only validate the slice and sample contract and emit the status JSON.",
    )
    return parser.parse_args()


def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


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


def resolve_path(path_text: str, base_dir: Path) -> Path:
    path = Path(path_text).expanduser()
    if path.is_absolute():
        return path.resolve()
    repo_candidate = (repo_root() / path).resolve()
    if repo_candidate.exists():
        return repo_candidate
    return (base_dir / path).resolve()


def parse_bool(value: Any, *, key_name: str) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "1", "yes"}:
            return True
        if normalized in {"false", "0", "no"}:
            return False
    raise ValueError(f"{key_name} must be a boolean or boolean-like string, got {value!r}")


def load_output_metadata(output_path: Path) -> dict[str, str]:
    metadata: dict[str, str] = {}
    if not output_path.exists():
        return metadata
    for raw_line in output_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.rstrip()
        if not line or line == "---" or ":" not in line:
            continue
        key, value = line.split(":", 1)
        metadata[key.strip()] = value.lstrip()
    return metadata


def validate_slice_contract(payload: dict[str, Any], source_path: str) -> dict[str, Any]:
    slice_id = str(payload.get("slice_id") or "").strip()
    if not slice_id:
        raise ValueError(f"Slice manifest is missing slice_id: {source_path}")
    cases = payload.get("cases")
    if not isinstance(cases, list) or not cases:
        raise ValueError(f"Slice manifest must contain a non-empty cases array: {source_path}")
    relative_paths = []
    for index, entry in enumerate(cases):
        if not isinstance(entry, dict):
            raise ValueError(f"Slice case {index} must be an object: {source_path}")
        relative_path = str(entry.get("relative_path") or "").strip()
        if not relative_path:
            raise ValueError(f"Slice case {index} is missing relative_path: {source_path}")
        relative_paths.append(relative_path)
    if tuple(relative_paths) != EXPECTED_SLICE_PATHS:
        raise ValueError(
            "Slice manifest must keep the fixed March 30 relative_path contract in order: "
            + ", ".join(EXPECTED_SLICE_PATHS)
        )
    return {
        "path": str(Path(source_path).expanduser().resolve()),
        "slice_id": slice_id,
        "case_count": len(relative_paths),
        "relative_paths": relative_paths,
    }


def validate_sample_contract(payload: dict[str, Any], source_path: str) -> dict[str, Any]:
    sample_id = str(payload.get("sample_id") or "").strip()
    if not sample_id:
        raise ValueError(f"Sample contract is missing sample_id: {source_path}")
    sample_rows = payload.get("sample_rows")
    if not isinstance(sample_rows, list) or len(sample_rows) != 3:
        raise ValueError(f"Sample contract must contain exactly 3 sample_rows: {source_path}")
    seen_outcomes: list[str] = []
    normalized_rows: list[dict[str, Any]] = []
    expected_keys = set(REQUIRED_SAMPLE_KEYS)
    for index, row in enumerate(sample_rows):
        if not isinstance(row, dict):
            raise ValueError(f"Sample row {index} must be an object: {source_path}")
        if set(row.keys()) != expected_keys:
            raise ValueError(
                f"Sample row {index} must use exactly these keys: {', '.join(REQUIRED_SAMPLE_KEYS)}"
            )
        fallback_invoked = parse_bool(row["fallback_invoked"], key_name=f"sample_rows[{index}].fallback_invoked")
        if not fallback_invoked:
            raise ValueError(f"Sample row {index} must set fallback_invoked=true")
        fallback_outcome = str(row["fallback_outcome"] or "").strip().lower()
        if fallback_outcome not in REQUIRED_SAMPLE_OUTCOMES:
            raise ValueError(
                f"Sample row {index} has invalid fallback_outcome={fallback_outcome!r}; "
                f"expected one of {', '.join(REQUIRED_SAMPLE_OUTCOMES)}"
            )
        fallback_reason = str(row["fallback_reason"] or "").strip()
        if not fallback_reason:
            raise ValueError(f"Sample row {index} must include a non-empty fallback_reason")
        validation_status = str(row["validation_status"] or "").strip()
        if not validation_status:
            raise ValueError(f"Sample row {index} must include a non-empty validation_status")
        seen_outcomes.append(fallback_outcome)
        normalized_rows.append(
            {
                "fallback_invoked": True,
                "fallback_outcome": fallback_outcome,
                "fallback_reason": fallback_reason,
                "validation_status": validation_status,
            }
        )
    if sorted(seen_outcomes) != sorted(REQUIRED_SAMPLE_OUTCOMES):
        raise ValueError(
            "Sample contract must include exactly one row each for passed, abstained, and failed"
        )
    return {
        "path": str(Path(source_path).expanduser().resolve()),
        "sample_id": sample_id,
        "row_count": len(normalized_rows),
        "required_keys": list(REQUIRED_SAMPLE_KEYS),
        "required_outcomes": list(REQUIRED_SAMPLE_OUTCOMES),
        "sample_rows": normalized_rows,
    }


def validate_live_run(run_dir_text: str, slice_paths: list[str], proof_md: str) -> dict[str, Any]:
    run_dir = Path(run_dir_text).expanduser().resolve()
    if not run_dir.exists():
        raise ValueError(f"Run directory not found: {run_dir}")
    summary_path = run_dir / "summary.json"
    summary = load_json_object(str(summary_path), "Run summary")
    results = summary.get("results")
    if not isinstance(results, list):
        raise ValueError(f"Run summary is missing results array: {summary_path}")

    result_map = {
        str(result.get("snippet") or "").strip(): result
        for result in results
        if isinstance(result, dict)
    }

    benchmark_context_log = str(summary.get("benchmark_context_log") or "").strip()
    context_log_path = resolve_path(benchmark_context_log, run_dir) if benchmark_context_log else (run_dir / "benchmark-context.log")
    if not context_log_path.exists():
        raise ValueError(f"Benchmark context log not found: {context_log_path}")
    context_text = context_log_path.read_text(encoding="utf-8")
    crash_signature_present = CRASH_SIGNATURE in context_text

    case_checks: list[dict[str, Any]] = []
    errors: list[str] = []
    for relative_path in slice_paths:
        result = result_map.get(relative_path)
        if not isinstance(result, dict):
            errors.append(f"Run summary is missing slice case result: {relative_path}")
            continue

        output_metadata = result.get("output_metadata")
        if not isinstance(output_metadata, dict):
            output_metadata = {}
        output_files = result.get("output_files")
        if not isinstance(output_files, list):
            output_files = []
        resolved_output_paths = [
            str(resolve_path(str(path_text), run_dir))
            for path_text in output_files
            if str(path_text).strip()
        ]
        file_metadata: dict[str, str] = {}
        for output_path_text in resolved_output_paths:
            output_path = Path(output_path_text)
            file_metadata = load_output_metadata(output_path)
            if file_metadata:
                break

        metadata_source = "summary"
        effective_metadata: dict[str, Any] = dict(output_metadata)
        if not all(key in effective_metadata for key in REQUIRED_LIVE_METADATA_KEYS):
            if all(key in file_metadata for key in REQUIRED_LIVE_METADATA_KEYS):
                effective_metadata = file_metadata
                metadata_source = "output-file"

        missing_keys = [
            key for key in REQUIRED_LIVE_METADATA_KEYS if key not in effective_metadata
        ]
        case_error_messages: list[str] = []
        fallback_invoked: bool | None = None
        fallback_outcome = str(effective_metadata.get("fallback_outcome") or "").strip().lower()
        fallback_reason = str(effective_metadata.get("fallback_reason") or "").strip()
        if missing_keys:
            case_error_messages.append(
                f"{relative_path} is missing fallback metadata keys: {', '.join(missing_keys)}"
            )
        else:
            try:
                fallback_invoked = parse_bool(
                    effective_metadata.get("fallback_invoked"),
                    key_name=f"{relative_path}.fallback_invoked",
                )
            except ValueError as exc:
                case_error_messages.append(str(exc))
            if fallback_outcome and fallback_outcome not in REQUIRED_SAMPLE_OUTCOMES:
                case_error_messages.append(
                    f"{relative_path} has invalid fallback_outcome={fallback_outcome!r}"
                )
            if fallback_invoked and not fallback_outcome:
                case_error_messages.append(
                    f"{relative_path} must set fallback_outcome when fallback_invoked=true"
                )
            if fallback_invoked and not fallback_reason:
                case_error_messages.append(
                    f"{relative_path} must set fallback_reason when fallback_invoked=true"
                )

        case_checks.append(
            {
                "relative_path": relative_path,
                "artifact_dir": result.get("artifact_dir", ""),
                "metadata_source": metadata_source,
                "output_files": resolved_output_paths,
                "validation_status": str(effective_metadata.get("validation_status") or result.get("validation_status") or "").strip(),
                "fallback_invoked": fallback_invoked,
                "fallback_outcome": fallback_outcome,
                "fallback_reason": fallback_reason,
                "errors": case_error_messages,
            }
        )
        errors.extend(case_error_messages)

    if crash_signature_present:
        errors.append(
            "Benchmark context log still contains the removed crash signature: "
            f"{CRASH_SIGNATURE}"
        )

    live_status = {
        "run_dir": str(run_dir),
        "summary_json": str(summary_path),
        "benchmark_context_log": str(context_log_path),
        "proof_md": str(Path(proof_md).expanduser().resolve()) if proof_md else "",
        "required_metadata_keys": list(REQUIRED_LIVE_METADATA_KEYS),
        "crash_signature": CRASH_SIGNATURE,
        "crash_signature_present": crash_signature_present,
        "case_checks": case_checks,
        "errors": errors,
    }
    return live_status


def write_status(path_text: str, status: dict[str, Any]) -> None:
    path = Path(path_text).expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(status, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def main() -> int:
    args = parse_args()
    errors: list[str] = []
    status: dict[str, Any] = {
        "phase": "17",
        "plan": "03",
        "mode": "probe" if args.probe_only else "live",
        "probe_only": args.probe_only,
        "required_live_metadata_keys": list(REQUIRED_LIVE_METADATA_KEYS),
    }

    try:
        slice_payload = load_json_object(args.slice_json, "Slice manifest")
        status["slice_contract"] = validate_slice_contract(slice_payload, args.slice_json)
    except ValueError as exc:
        errors.append(str(exc))
        status["slice_contract"] = {}

    try:
        sample_payload = load_json_object(args.sample_json, "Sample contract")
        status["sample_contract"] = validate_sample_contract(sample_payload, args.sample_json)
    except ValueError as exc:
        errors.append(str(exc))
        status["sample_contract"] = {}

    if not args.probe_only:
        if not args.run_dir.strip():
            errors.append("--run-dir is required unless --probe-only is set")
            status["live_run"] = {}
        elif status["slice_contract"]:
            try:
                status["live_run"] = validate_live_run(
                    args.run_dir,
                    list(status["slice_contract"]["relative_paths"]),
                    args.proof_md,
                )
                errors.extend(status["live_run"]["errors"])
            except ValueError as exc:
                errors.append(str(exc))
                status["live_run"] = {}
        else:
            status["live_run"] = {}

    status["passed"] = not errors
    status["errors"] = errors
    write_status(args.status_json, status)

    if errors:
        for error in errors:
            print(f"ERROR: {error}")
        return 1

    if args.probe_only:
        print("Phase 17 fallback artifact probe passed.")
    else:
        print("Phase 17 live fallback artifact check passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
