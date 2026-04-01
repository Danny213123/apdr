#!/usr/bin/env python3
"""Extract Phase 20 dominant-bucket benchmark artifacts from an existing run."""

from __future__ import annotations

import argparse
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


DOMINANT_BUCKETS = (
    "module-not-found",
    "version-not-found",
    "environment-build-failed",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Create a Phase 20 dominant-bucket artifact from an existing benchmark "
            "summary and frozen slice manifest. Probe mode extracts metadata only."
        )
    )
    parser.add_argument("--slice-json", required=True, help="Path to the locked Phase 20 slice manifest.")
    parser.add_argument("--summary-json", required=True, help="Benchmark summary to extract rows from.")
    parser.add_argument("--output-json", required=True, help="Path to write the extracted artifact JSON.")
    parser.add_argument("--output-md", default="", help="Optional Markdown summary output.")
    parser.add_argument("--mode", choices=("baseline", "candidate"), required=True, help="Artifact mode label.")
    parser.add_argument(
        "--validation-backend",
        choices=("env", "docker", "llm"),
        default="llm",
        help="Configured validation backend label recorded in the artifact.",
    )
    parser.add_argument("--model-name", default="qwen3.5:9b", help="Model name recorded in the artifact.")
    parser.add_argument("--base-url", default="http://localhost:11434", help="Base URL recorded in the artifact.")
    parser.add_argument(
        "--probe-only",
        action="store_true",
        help="Extract an artifact from an existing run without launching APDR.",
    )
    args = parser.parse_args()
    if not args.probe_only:
        parser.error("Phase 20 currently supports extraction-only mode. Pass --probe-only.")
    return args


def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def load_json_object(path_text: str, label: str) -> dict[str, Any]:
    path = Path(path_text).expanduser().resolve()
    if not path.exists():
        raise SystemExit(f"{label} not found: {path}")
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise SystemExit(f"{label} is not valid JSON: {path} ({exc})") from exc
    if not isinstance(payload, dict):
        raise SystemExit(f"{label} must be a JSON object: {path}")
    return payload


def parse_report(report_path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if not report_path.exists():
        raise SystemExit(f"Resolution report missing: {report_path}")
    for line in report_path.read_text(encoding="utf-8").splitlines():
        if ": " not in line:
            continue
        key, value = line.split(": ", 1)
        values[key.strip()] = value.strip()
    return values


def display_status(result: dict[str, Any]) -> str:
    if bool(result.get("skipped")):
        return "skip"
    if bool(result.get("succeeded")):
        return "pass"
    return "fail"


def infer_failure_family(status: str) -> str | None:
    if not status:
        return None
    if status == "skipped-host-runtime":
        return "environment-specific"
    if status == "passed":
        return None
    return "dependency-resolution"


def count_results(results: list[dict[str, Any]]) -> dict[str, Any]:
    passes = 0
    skips = 0
    failures = 0
    buckets: Counter[str] = Counter()
    for row in results:
        status = str(row.get("display_status") or "").strip().lower()
        if status == "pass":
            passes += 1
            continue
        if status == "skip":
            skips += 1
            continue
        failures += 1
        bucket = str(row.get("failure_bucket") or row.get("validation_status") or "").strip()
        if bucket:
            buckets[bucket] += 1
    return {
        "passes": passes,
        "skips": skips,
        "failures": failures,
        "failure_buckets": {key: buckets.get(key, 0) for key in DOMINANT_BUCKETS},
    }


def build_artifact(args: argparse.Namespace) -> dict[str, Any]:
    slice_payload = load_json_object(args.slice_json, "Phase 20 slice manifest")
    summary = load_json_object(args.summary_json, "Phase 20 source summary")
    cases = slice_payload.get("cases")
    if not isinstance(cases, list) or not cases:
        raise SystemExit("Slice manifest must contain a non-empty cases array.")
    summary_results = summary.get("results")
    if not isinstance(summary_results, list):
        raise SystemExit("Benchmark summary must contain a results array.")

    row_by_snippet = {
        str(row.get("snippet") or "").strip(): row
        for row in summary_results
        if isinstance(row, dict)
    }

    extracted_results: list[dict[str, Any]] = []
    for entry in cases:
        if not isinstance(entry, dict):
            raise SystemExit("Each slice case must be a JSON object.")
        relative_path = str(entry.get("relative_path") or "").strip()
        if not relative_path:
            raise SystemExit("Each slice case must provide relative_path.")
        summary_row = row_by_snippet.get(relative_path)
        if not isinstance(summary_row, dict):
            raise SystemExit(f"Summary does not contain locked slice case: {relative_path}")
        artifact_dir = repo_root() / str(summary_row.get("artifact_dir") or "").strip()
        report_values = parse_report(artifact_dir / "resolution-report.txt")
        validation_status = str(report_values.get("validation_status") or "").strip()
        validation_reason = str(report_values.get("validation_reason") or "").strip()
        failure_bucket = str(report_values.get("failure_bucket") or validation_status).strip()
        failure_family = str(report_values.get("failure_family") or "").strip() or infer_failure_family(
            validation_status
        )
        extracted_results.append(
            {
                "relative_path": relative_path,
                "snippet": relative_path,
                "artifact_dir": str(summary_row.get("artifact_dir") or "").strip(),
                "resultOrigin": "live",
                "display_status": display_status(summary_row),
                "succeeded": bool(summary_row.get("succeeded")),
                "skipped": bool(summary_row.get("skipped")),
                "validation_status": validation_status,
                "validation_reason": validation_reason,
                "failure_bucket": failure_bucket,
                "failure_family": failure_family,
                "validation_backend": args.validation_backend,
                "validation_path": str(report_values.get("validation_path") or "env").strip(),
                "escalated_backend": report_values.get("escalated_backend"),
            }
        )

    run_contract = summary.get("run_contract")
    if not isinstance(run_contract, dict):
        run_contract = {
            "run_contract_version": "1",
            "tool": "apdr",
            "model_name": args.model_name,
            "base_url": args.base_url,
            "validation_backend": args.validation_backend,
            "run_intent": args.mode,
            "execution_mode": "llm-hybrid" if args.validation_backend == "llm" else "env-fast",
            "cache_state": "unknown",
            "host_architecture": "unknown",
            "apdr_binary_architecture": "unknown",
            "python_architecture": "unknown",
            "llm_context_window": "16384",
            "inference_policy": "temperature=inherited",
            "build_profile": "standard",
        }

    artifact = {
        "sample_id": f"phase20-{args.mode}-artifact",
        "slice_id": str(slice_payload.get("slice_id") or "").strip(),
        "mode": args.mode,
        "source_run": str(slice_payload.get("source_run") or "").strip(),
        "source_summary": str(Path(args.summary_json).expanduser().resolve()),
        "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "validation_backend": args.validation_backend,
        "model_name": args.model_name,
        "base_url": args.base_url,
        "run_contract": run_contract,
        "historical_results": [],
        "results": extracted_results,
    }
    artifact["counts"] = count_results(extracted_results)
    return artifact


def write_markdown(output_path: Path, artifact: dict[str, Any]) -> None:
    counts = artifact["counts"]
    buckets = counts["failure_buckets"]
    lines = [
        "# Phase 20 Dominant-Bucket Artifact",
        "",
        f"- Mode: `{artifact['mode']}`",
        f"- Slice: `{artifact['slice_id']}`",
        f"- Validation backend: `{artifact['validation_backend']}`",
        f"- Model: `{artifact['model_name']}`",
        f"- Passes: `{counts['passes']}`",
        f"- Failures: `{counts['failures']}`",
        f"- Module-not-found: `{buckets['module-not-found']}`",
        f"- Version-not-found: `{buckets['version-not-found']}`",
        f"- Environment-build-failed: `{buckets['environment-build-failed']}`",
    ]
    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    args = parse_args()
    artifact = build_artifact(args)
    output_json = Path(args.output_json).expanduser().resolve()
    output_json.write_text(json.dumps(artifact, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    if args.output_md:
        write_markdown(Path(args.output_md).expanduser().resolve(), artifact)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
