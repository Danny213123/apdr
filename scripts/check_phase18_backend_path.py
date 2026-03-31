#!/usr/bin/env python3
"""Check the Phase 18 fixed slice and backend-path proof artifacts."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


EXPECTED_SLICE_PATHS = (
    "hard-gists/00e9638c0efad1adac878522cf172484/snippet.py",
    "hard-gists/056626de3fbdc7cf7b59de1d9f6279d1/snippet.py",
    "hard-gists/01c99322cf985e771827/snippet.py",
    "hard-gists/10295174/snippet.py",
    "hard-gists/1231964e784ab9acb65d/snippet.py",
)
REQUIRED_LIVE_KEYS = ("validation_backend", "validation_path", "escalated_backend")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Check Phase 18 backend-path proof artifacts and live replay outputs."
    )
    parser.add_argument("--run-dir", default="", help="Benchmark run directory to validate in live mode.")
    parser.add_argument("--slice-json", required=True, help="Path to the fixed live slice manifest.")
    parser.add_argument("--status-json", required=True, help="Path to write the machine-readable checker status.")
    parser.add_argument("--proof-md", default="", help="Optional proof note output path.")
    parser.add_argument(
        "--probe-only",
        action="store_true",
        help="Only validate the fixed slice contract and emit the status JSON.",
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
    relative_paths: list[str] = []
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


def validate_live_run(run_dir_text: str, slice_paths: list[str]) -> dict[str, Any]:
    run_dir = Path(run_dir_text).expanduser().resolve()
    if not run_dir.exists():
        raise ValueError(f"Run directory not found: {run_dir}")

    summary = load_json_object(str(run_dir / "summary.json"), "Run summary")
    results = summary.get("results")
    if not isinstance(results, list):
        raise ValueError(f"Run summary is missing results array: {run_dir / 'summary.json'}")

    result_map = {
        str(result.get("snippet") or "").strip(): result
        for result in results
        if isinstance(result, dict)
    }

    checks: list[dict[str, Any]] = []
    errors: list[str] = []
    for relative_path in slice_paths:
        result = result_map.get(relative_path)
        if not isinstance(result, dict):
            errors.append(f"Run summary is missing slice case result: {relative_path}")
            continue

        output_metadata = result.get("output_metadata")
        if not isinstance(output_metadata, dict):
            output_metadata = {}
        effective_metadata: dict[str, Any] = dict(output_metadata)
        for output_path_text in result.get("output_files", []) or []:
            file_metadata = load_output_metadata(resolve_path(str(output_path_text), run_dir))
            if file_metadata:
                effective_metadata = {**file_metadata, **effective_metadata}
                break

        validation_backend = str(effective_metadata.get("validation_backend") or "").strip()
        validation_path = str(effective_metadata.get("validation_path") or "").strip()
        escalated_backend = str(effective_metadata.get("escalated_backend") or "").strip()
        validation_status = str(effective_metadata.get("validation_status") or "").strip()

        case_errors: list[str] = []
        missing = [key for key in REQUIRED_LIVE_KEYS if not str(effective_metadata.get(key) or "").strip()]
        if missing:
            case_errors.append(f"missing keys: {', '.join(missing)}")
        if validation_backend and validation_backend != "llm":
            case_errors.append(f"validation_backend must remain llm, got {validation_backend}")
        if validation_path and not validation_path.startswith("env->docker"):
            case_errors.append(f"validation_path must start with env->docker, got {validation_path}")
        if escalated_backend and escalated_backend != "docker":
            case_errors.append(f"escalated_backend must be docker, got {escalated_backend}")
        if case_errors:
            errors.append(f"{relative_path}: {'; '.join(case_errors)}")

        checks.append(
            {
                "relative_path": relative_path,
                "validation_status": validation_status,
                "validation_backend": validation_backend,
                "validation_path": validation_path,
                "escalated_backend": escalated_backend,
                "passed": not case_errors,
            }
        )

    return {
        "run_dir": str(run_dir),
        "checked_cases": checks,
        "passed": not errors,
        "errors": errors,
    }


def write_status(path_text: str, payload: dict[str, Any]) -> None:
    path = Path(path_text).expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def write_proof_md(path_text: str, slice_contract: dict[str, Any], live_validation: dict[str, Any]) -> None:
    path = Path(path_text).expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "# Phase 18 Backend Path Proof",
        "",
        "## Slice Contract",
        "",
        "This proof stays anchored to the fixed Phase 18 replay slice:",
        "",
    ]
    for relative_path in slice_contract["relative_paths"]:
        lines.append(f"- `{relative_path}`")
    lines.extend(
        [
            "",
            "## Live Validation",
            "",
            f"- Run directory: `{live_validation['run_dir']}`",
            f"- Passed: `{live_validation['passed']}`",
            "",
            "## Before/After Review",
            "",
            "Before the Phase 18 routing changes, the March 30 baseline artifacts keep `validation_backend: env` and do not expose `validation_path` or `escalated_backend` for this fixed slice.",
            "",
            "After replaying the fixed slice with the Phase 18 changes, reviewers should require all of these conditions:",
            "",
            "- `validation_backend` remains `llm` to preserve requested-mode truth",
            "- `validation_path` begins with `env->docker`",
            "- `escalated_backend` is `docker`",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    args = parse_args()
    try:
        slice_payload = load_json_object(args.slice_json, "Slice manifest")
        slice_contract = validate_slice_contract(slice_payload, args.slice_json)
    except ValueError as exc:
        write_status(
            args.status_json,
            {
                "passed": False,
                "probe_only": bool(args.probe_only),
                "error": str(exc),
            },
        )
        print(str(exc), flush=True)
        return 1

    status: dict[str, Any] = {
        "passed": True,
        "probe_only": bool(args.probe_only),
        "slice_contract": slice_contract,
    }
    if args.probe_only:
        write_status(args.status_json, status)
        return 0

    if not args.run_dir.strip():
        write_status(
            args.status_json,
            {
                **status,
                "passed": False,
                "error": "--run-dir is required unless --probe-only is set.",
            },
        )
        print("--run-dir is required unless --probe-only is set.", flush=True)
        return 1

    try:
        live_validation = validate_live_run(args.run_dir, slice_contract["relative_paths"])
    except ValueError as exc:
        write_status(
            args.status_json,
            {
                **status,
                "passed": False,
                "error": str(exc),
            },
        )
        print(str(exc), flush=True)
        return 1

    status["live_validation"] = live_validation
    status["passed"] = bool(live_validation.get("passed"))
    write_status(args.status_json, status)

    if args.proof_md.strip():
        write_proof_md(args.proof_md, slice_contract, live_validation)

    if not status["passed"]:
        for error in live_validation.get("errors", []):
            print(error, flush=True)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
