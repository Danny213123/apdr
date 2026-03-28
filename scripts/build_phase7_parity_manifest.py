#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import re
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
REPORT_SECTIONS = {
    "resolved_dependencies",
    "config_dependencies",
    "unresolved",
    "notes",
    "validation_attempts",
}
KNOWN_BUCKETS = (
    "module-not-found",
    "version-not-found",
    "dependency-conflict",
    "import-error",
    "syntax-error",
    "environment-build-failed",
)
CANONICAL_CASE_COUNT = 70
TIER1_WATCHLIST_COUNT = 17


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build the canonical Phase 7 tier3 parity manifest and summary."
    )
    parser.add_argument("--summary-json", required=True, help="Path to APDR summary.json.")
    parser.add_argument("--pllm-csv", required=True, help="Path to the pllm comparison CSV.")
    parser.add_argument("--output-json", required=True, help="Destination for the JSON manifest.")
    parser.add_argument("--output-md", required=True, help="Destination for the Markdown summary.")
    return parser.parse_args()


def clean_text(value: Any) -> str:
    text = str(value or "").strip()
    if text.lower() in {"", "--", "none", "null"}:
        return ""
    return text


def basename_from_artifact_dir(value: Any) -> str:
    text = clean_text(value)
    if not text:
        return ""
    parts = [part for part in re.split(r"[\\/]+", text) if part]
    return parts[-1] if parts else ""


def parse_int(value: Any) -> int:
    text = clean_text(value)
    if not text:
        return 0
    try:
        return int(float(text))
    except ValueError:
        raise ValueError(f"expected numeric value, got {value!r}") from None


def repo_relative_text(value: Any) -> str:
    text = clean_text(value)
    if not text:
        return ""
    normalized = text.replace("\\", "/")
    root = str(REPO_ROOT).replace("\\", "/")
    if normalized.lower().startswith(root.lower() + "/"):
        return normalized[len(root) + 1 :]
    return normalized.lstrip("./")


def load_summary(summary_path: Path) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    payload = json.loads(summary_path.read_text(encoding="utf-8"))
    results = payload.get("results")
    if not isinstance(results, list):
        raise ValueError(f"{summary_path} does not contain a top-level 'results' list")
    return payload, results


def load_pllm_pass_counts(csv_path: Path) -> dict[str, int]:
    with csv_path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames is None or "name" not in reader.fieldnames or "passed" not in reader.fieldnames:
            raise ValueError(f"{csv_path} is missing required 'name'/'passed' columns")
        counts: dict[str, int] = {}
        for row in reader:
            case_id = clean_text(row.get("name"))
            if not case_id:
                continue
            if case_id in counts:
                raise ValueError(f"duplicate pllm case id {case_id!r} in {csv_path}")
            counts[case_id] = parse_int(row.get("passed"))
    return counts


def parse_resolution_report(report_path: Path) -> dict[str, str]:
    metadata: dict[str, str] = {}
    if not report_path.exists():
        return metadata
    section = ""
    for raw_line in report_path.read_text(encoding="utf-8", errors="replace").splitlines():
        stripped = raw_line.strip()
        if not stripped:
            continue
        if stripped.endswith(":") and stripped[:-1] in REPORT_SECTIONS:
            section = stripped[:-1]
            continue
        if section:
            continue
        if ":" not in stripped:
            continue
        key, value = stripped.split(":", 1)
        metadata[key.strip()] = value.strip()
    return metadata


def canonicalize_bucket(value: Any) -> str:
    cleaned = clean_text(value).lower()
    if not cleaned:
        return ""
    if cleaned == "build-failed":
        return "environment-build-failed"
    return cleaned


def bucket_from_log_tail(log_tail: list[str]) -> str:
    joined = "\n".join(str(line or "").lower() for line in log_tail)
    for bucket in KNOWN_BUCKETS:
        if bucket in joined:
            return bucket
    if "build-failed" in joined:
        return "environment-build-failed"
    return ""


def determine_normalized_bucket(
    output_metadata: dict[str, Any],
    report_metadata: dict[str, str],
    log_tail: list[str],
) -> tuple[str, str]:
    checks = (
        ("summary.failure_bucket", output_metadata.get("failure_bucket")),
        ("summary.validation_status", output_metadata.get("validation_status")),
        ("report.failure_bucket", report_metadata.get("failure_bucket")),
        ("report.validation_status", report_metadata.get("validation_status")),
    )
    for source, value in checks:
        bucket = canonicalize_bucket(value)
        if bucket:
            return bucket, source
    bucket = bucket_from_log_tail(log_tail)
    if bucket:
        return bucket, "log_tail"
    return "unclassified", "default"


def derive_report_path(case_id: str, output_metadata: dict[str, Any]) -> Path:
    reported = clean_text(output_metadata.get("report_path"))
    if reported:
        candidate = Path(reported)
        if candidate.exists():
            return candidate
    return REPO_ROOT / "runs" / "20260327-150339-apdr" / "cases" / case_id / "resolution-report.txt"


def build_case_entry(case: dict[str, Any], pllm_pass_count: int) -> dict[str, Any]:
    output_metadata = case.get("output_metadata") or {}
    if not isinstance(output_metadata, dict):
        output_metadata = {}
    case_id = basename_from_artifact_dir(case.get("artifact_dir"))
    report_path = derive_report_path(case_id, output_metadata)
    report_metadata = parse_resolution_report(report_path)
    log_tail = case.get("log_tail") or []
    if not isinstance(log_tail, list):
        log_tail = [str(log_tail)]
    normalized_bucket, normalized_bucket_source = determine_normalized_bucket(
        output_metadata=output_metadata,
        report_metadata=report_metadata,
        log_tail=[str(item) for item in log_tail],
    )
    validation_reason = clean_text(output_metadata.get("validation_reason")) or clean_text(
        report_metadata.get("validation_reason")
    )
    return {
        "case_id": case_id,
        "tier": clean_text(case.get("tier")),
        "artifact_dir": repo_relative_text(case.get("artifact_dir")),
        "snippet": clean_text(case.get("snippet")),
        "requirements": case.get("requirements") or [],
        "pllm_pass_count": pllm_pass_count,
        "raw_validation_status": clean_text(output_metadata.get("validation_status")),
        "raw_failure_bucket": clean_text(output_metadata.get("failure_bucket")),
        "validation_reason": validation_reason,
        "report_path": repo_relative_text(str(report_path)),
        "log_tail": [str(item) for item in log_tail],
        "normalized_bucket": normalized_bucket,
        "normalized_bucket_source": normalized_bucket_source,
    }


def select_overlap_cases(
    results: list[dict[str, Any]],
    pllm_pass_counts: dict[str, int],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    canonical_cases: list[dict[str, Any]] = []
    tier1_watchlist: list[dict[str, Any]] = []
    for case in results:
        succeeded = bool(case.get("succeeded"))
        skipped = bool(case.get("skipped"))
        if succeeded or skipped:
            continue
        case_id = basename_from_artifact_dir(case.get("artifact_dir"))
        if not case_id:
            continue
        pllm_pass_count = pllm_pass_counts.get(case_id, 0)
        if pllm_pass_count < 1:
            continue
        entry = build_case_entry(case, pllm_pass_count)
        if entry["tier"] == "tier3":
            canonical_cases.append(entry)
        elif entry["tier"] == "tier1":
            tier1_watchlist.append(entry)
    canonical_cases.sort(key=lambda item: item["case_id"])
    tier1_watchlist.sort(key=lambda item: item["case_id"])
    return canonical_cases, tier1_watchlist


def markdown_table_for_buckets(bucket_totals: dict[str, int]) -> str:
    lines = ["| Bucket | Cases |", "| --- | ---: |"]
    for bucket, total in sorted(bucket_totals.items(), key=lambda item: (-item[1], item[0])):
        lines.append(f"| `{bucket}` | {total} |")
    return "\n".join(lines)


def choose_representative_cases(cases: list[dict[str, Any]]) -> list[dict[str, Any]]:
    representatives: list[dict[str, Any]] = []
    seen_buckets: set[str] = set()
    for case in sorted(cases, key=lambda item: (item["normalized_bucket"], item["case_id"])):
        bucket = case["normalized_bucket"]
        if bucket in seen_buckets:
            continue
        seen_buckets.add(bucket)
        representatives.append(case)
    return representatives


def build_markdown(
    manifest: dict[str, Any],
    summary_path: str,
    pllm_csv_path: str,
) -> str:
    canonical_cases = manifest["cases"]
    representatives = choose_representative_cases(canonical_cases)
    lines = [
        "# Phase 7 Tier3 Parity Manifest",
        "",
        "## Source Inputs",
        f"- `summary.json`: `{summary_path}`",
        f"- `pllm` CSV: `{pllm_csv_path}`",
        f"- Generated at: `{manifest['generated_at']}`",
        "- Normalization precedence: `summary.failure_bucket`, `summary.validation_status`, `report.failure_bucket`, `report.validation_status`, `log_tail`, `unclassified`",
        "",
        "## Canonical Slice",
        f"- Canonical tier3 cases: `{manifest['canonical_case_count']}`",
        f"- Overlap cases with APDR failure and `pllm` pass >= 1: `{manifest['pllm_overlap_case_count']}`",
        f"- Excluded tier1 watchlist cases: `{manifest['tier1_watchlist_count']}`",
        "- Inclusion rule: APDR failed, APDR did not skip, `pllm` passed at least once, and the stored summary `tier` equals `tier3`.",
        "",
        "## Normalized Buckets",
        markdown_table_for_buckets(manifest["normalized_bucket_totals"]),
        "",
        "## Representative Cases",
    ]
    for case in representatives:
        lines.extend(
            [
                f"- `{case['case_id']}`: `{case['normalized_bucket']}` via `{case['normalized_bucket_source']}`; `pllm_pass_count={case['pllm_pass_count']}`; snippet `{case['snippet']}`; reason: {case['validation_reason'] or '--'}",
            ]
        )
    lines.extend(
        [
            "",
            "## Tier1 Watchlist",
            f"The `{manifest['tier1_watchlist_count']}` tier1 overlap cases are outside the Phase 7 contract. They remain a watchlist for later milestone work and are not part of the canonical tier3 baseline.",
            "",
            f"Watchlist case IDs: {', '.join(manifest['tier1_watchlist_case_ids'])}",
            "",
        ]
    )
    return "\n".join(lines)


def build_manifest(
    summary_path: Path,
    pllm_csv_path: Path,
) -> dict[str, Any]:
    _, results = load_summary(summary_path)
    pllm_pass_counts = load_pllm_pass_counts(pllm_csv_path)
    canonical_cases, tier1_watchlist = select_overlap_cases(results, pllm_pass_counts)

    if len(canonical_cases) != CANONICAL_CASE_COUNT:
        raise ValueError(
            f"expected {CANONICAL_CASE_COUNT} canonical tier3 cases, found {len(canonical_cases)}"
        )
    if len(tier1_watchlist) != TIER1_WATCHLIST_COUNT:
        raise ValueError(
            f"expected {TIER1_WATCHLIST_COUNT} tier1 watchlist cases, found {len(tier1_watchlist)}"
        )

    bucket_totals = Counter(case["normalized_bucket"] for case in canonical_cases)
    manifest = {
        "generated_at": datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z"),
        "summary_json": repo_relative_text(str(summary_path)),
        "pllm_csv": repo_relative_text(str(pllm_csv_path)),
        "pllm_overlap_case_count": len(canonical_cases) + len(tier1_watchlist),
        "canonical_case_count": len(canonical_cases),
        "tier1_watchlist_count": len(tier1_watchlist),
        "normalization_precedence": [
            "summary.failure_bucket",
            "summary.validation_status",
            "report.failure_bucket",
            "report.validation_status",
            "log_tail",
            "unclassified",
        ],
        "normalized_bucket_totals": dict(sorted(bucket_totals.items())),
        "canonical_case_ids": [case["case_id"] for case in canonical_cases],
        "tier1_watchlist_case_ids": [case["case_id"] for case in tier1_watchlist],
        "cases": canonical_cases,
    }
    return manifest


def write_outputs(manifest: dict[str, Any], output_json: Path, output_md: Path) -> None:
    output_json.parent.mkdir(parents=True, exist_ok=True)
    output_md.parent.mkdir(parents=True, exist_ok=True)
    output_json.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    markdown = build_markdown(
        manifest=manifest,
        summary_path=manifest["summary_json"],
        pllm_csv_path=manifest["pllm_csv"],
    )
    output_md.write_text(markdown, encoding="utf-8")


def main() -> int:
    args = parse_args()
    summary_path = Path(args.summary_json).resolve()
    pllm_csv_path = Path(args.pllm_csv).resolve()
    output_json = Path(args.output_json).resolve()
    output_md = Path(args.output_md).resolve()
    manifest = build_manifest(summary_path=summary_path, pllm_csv_path=pllm_csv_path)
    write_outputs(manifest=manifest, output_json=output_json, output_md=output_md)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
