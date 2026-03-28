#!/usr/bin/env python3
"""Phase 10 manifest-driven targeted benchmark rerun wrapper.

Reads the Phase 10 rerun manifest and either reruns the locked case set,
builds an explicit dry-run comparison package, or probes live-proof readiness.

Outputs:
  - targeted-rerun.json  (separate canonical, watchlist, and guard results)
  - case-delta.json      (per-case APDR vs baseline vs pllm delta)
  - TARGETED-RERUN.md    (reviewer-facing markdown with required sections)
  - status-json          (machine-readable live-proof readiness / blocker state)
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import shutil
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]

DEFAULT_APDR_FLAGS = [
    "--range", "5",
    "--max-retries", "5",
    "--docker-timeout", "900",
    "--validation-backend", "llm",
    "--allow-llm",
    "--no-execute-snippet",
    "--force-validate",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Phase 10 manifest-driven targeted benchmark rerun."
    )
    parser.add_argument("--manifest-json", required=True,
                        help="Path to 10-targeted-rerun-manifest.json")
    parser.add_argument("--baseline-summary", required=True,
                        help="Path to the locked APDR baseline summary.json")
    parser.add_argument("--pllm-csv", required=True,
                        help="Path to pllm_results/csv/summary-all-runs.csv")
    parser.add_argument("--output-json", default="",
                        help="Path for targeted-rerun.json output "
                             "(required unless --probe-only)")
    parser.add_argument("--case-delta-json", default="",
                        help="Path for case-delta.json output "
                             "(required unless --probe-only)")
    parser.add_argument("--output-md", default="",
                        help="Path for TARGETED-RERUN.md output "
                             "(required unless --probe-only)")
    parser.add_argument("--context-log", default="",
                        help="Optional benchmark context log path")
    parser.add_argument("--llm-model", default="qwen3.5:9b",
                        help="LLM model for rerun (default: qwen3.5:9b)")
    parser.add_argument("--llm-base-url", default="http://localhost:11434",
                        help="LLM base URL (default: http://localhost:11434)")
    parser.add_argument("--apdr-command", default="",
                        help="Path to the APDR binary (default: auto-detect)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Build delta artifacts from existing baseline data "
                             "without executing APDR")
    parser.add_argument("--require-live", action="store_true",
                        help="Require explicit live execution semantics. If live "
                             "inputs are missing, report a blocker instead of "
                             "falling back to dry-run.")
    parser.add_argument("--probe-only", action="store_true",
                        help="Do not rerun cases. Write readiness / blocker status "
                             "for a requested live proof.")
    parser.add_argument("--status-json", default="",
                        help="Machine-readable readiness / blocker output for "
                             "Phase 12 live-proof flow.")
    return parser.parse_args()


def load_manifest(path: str) -> dict[str, Any]:
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def resolve_repo_path(path_text: str) -> Path:
    path = Path(path_text)
    if path.is_absolute():
        return path
    return REPO_ROOT / path


def locate_command_path(command_text: str) -> Path | None:
    raw = command_text.strip()
    if not raw:
        return None
    candidate = Path(raw)
    if candidate.is_absolute() and candidate.exists():
        return candidate
    repo_candidate = (REPO_ROOT / candidate).resolve()
    if repo_candidate.exists():
        return repo_candidate
    which_candidate = shutil.which(raw)
    if which_candidate:
        return Path(which_candidate)
    return None


def actual_mode(args: argparse.Namespace) -> str:
    if args.probe_only:
        return "probe-only"
    if args.dry_run:
        return "dry-run"
    return "live-rerun"


def requested_mode(args: argparse.Namespace) -> str:
    if args.dry_run:
        return "dry-run"
    if args.require_live or args.probe_only:
        return "live-proof"
    return "live-rerun"


def validate_args(args: argparse.Namespace) -> None:
    if args.dry_run and args.require_live:
        raise SystemExit("--dry-run and --require-live are mutually exclusive.")
    if args.probe_only and args.dry_run:
        raise SystemExit("--probe-only cannot be combined with --dry-run.")
    if args.probe_only and not args.status_json:
        raise SystemExit("--probe-only requires --status-json.")
    if not args.probe_only:
        missing_outputs = [
            flag for flag, value in (
                ("--output-json", args.output_json),
                ("--case-delta-json", args.case_delta_json),
                ("--output-md", args.output_md),
            )
            if not value
        ]
        if missing_outputs:
            raise SystemExit(
                "Non-probe runs require output paths: "
                + ", ".join(missing_outputs)
            )


def probe_command(command_text: str) -> dict[str, Any]:
    info: dict[str, Any] = {
        "raw": command_text,
        "resolved": "",
        "exists": False,
        "invocable": False,
        "returncode": None,
        "error": "",
    }
    command_path = locate_command_path(command_text)
    if command_path is None:
        return info
    info["resolved"] = str(command_path)
    info["exists"] = command_path.exists()
    if not command_path.exists():
        return info
    try:
        completed = subprocess.run(
            [str(command_path), "--help"],
            cwd=REPO_ROOT,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
            timeout=10,
        )
        info["invocable"] = True
        info["returncode"] = completed.returncode
    except (subprocess.TimeoutExpired, FileNotFoundError, OSError) as exc:
        info["error"] = str(exc)
    return info


def build_live_status(args: argparse.Namespace) -> dict[str, Any]:
    manifest_path = resolve_repo_path(args.manifest_json)
    baseline_path = resolve_repo_path(args.baseline_summary)
    pllm_path = resolve_repo_path(args.pllm_csv)
    command_info = probe_command(args.apdr_command)

    blockers: list[str] = []
    canonical_case_count = 0
    watchlist_case_count = 0
    preservation_guard_count = 0
    manifest_error = ""

    if not manifest_path.exists():
        blockers.append(f"Manifest not found: {manifest_path}")
    else:
        try:
            manifest = load_manifest(str(manifest_path))
            canonical_case_count = len(manifest.get("canonical_case_ids", []))
            watchlist_case_count = len(manifest.get("tier1_watchlist_case_ids", []))
            guards = manifest.get("preservation_guards", {})
            preservation_guard_count = sum(
                len(guards.get(key, []))
                for key in (
                    "passed_case_ids",
                    "host_runtime_case_ids",
                    "local_helper_case_ids",
                    "unsolvable_case_ids",
                )
            )
        except (OSError, json.JSONDecodeError, TypeError, ValueError) as exc:
            manifest_error = str(exc)
            blockers.append(f"Manifest could not be parsed: {manifest_path} ({exc})")

    if not baseline_path.exists():
        blockers.append(f"Baseline summary not found: {baseline_path}")
    if not pllm_path.exists():
        blockers.append(f"pllm CSV not found: {pllm_path}")

    live_requested = requested_mode(args) != "dry-run"
    if live_requested:
        if not args.apdr_command.strip():
            blockers.append("Live proof requires --apdr-command.")
        elif not command_info["exists"]:
            blockers.append(f"APDR command not found: {args.apdr_command}")
        elif not command_info["invocable"]:
            detail = command_info["error"] or "command could not be started"
            blockers.append(f"APDR command is not invocable: {detail}")

    live_ready = live_requested and not blockers
    if args.probe_only:
        terminal_state = "ready-for-live-rerun" if live_ready else "hard-blocker"
    elif live_ready:
        terminal_state = "live-rerun-requested"
    elif requested_mode(args) == "dry-run":
        terminal_state = "dry-run"
    else:
        terminal_state = "hard-blocker"

    return {
        "generated_at": utc_now(),
        "requested_mode": requested_mode(args),
        "actual_mode": actual_mode(args),
        "live_ready": live_ready,
        "terminal_state": terminal_state,
        "blocker_reason": blockers[0] if blockers else "",
        "blockers": blockers,
        "probe_only": args.probe_only,
        "require_live": args.require_live,
        "dry_run": args.dry_run,
        "manifest": args.manifest_json,
        "baseline_summary": args.baseline_summary,
        "pllm_csv": args.pllm_csv,
        "apdr_command": command_info["resolved"] or args.apdr_command,
        "canonical_case_count": canonical_case_count,
        "watchlist_case_count": watchlist_case_count,
        "preservation_guard_count": preservation_guard_count,
        "prerequisites": {
            "manifest_exists": manifest_path.exists(),
            "manifest_error": manifest_error,
            "baseline_summary_exists": baseline_path.exists(),
            "pllm_csv_exists": pllm_path.exists(),
            "apdr_command_provided": bool(args.apdr_command.strip()),
            "apdr_command_exists": bool(command_info["exists"]),
            "apdr_command_invocable": bool(command_info["invocable"]),
            "apdr_command_returncode": command_info["returncode"],
            "apdr_command_error": command_info["error"],
        },
    }


def output_metadata_skipped(metadata: dict[str, str]) -> bool:
    status = str(metadata.get("validation_status") or "").strip().lower()
    return status.startswith("skipped") or status == "host-runtime-required"


def read_requirements_if_updated(
    requirements_path: Path,
    existing_mtime: float | None,
    started_at: float,
) -> list[str]:
    if not requirements_path.exists():
        return []
    current_mtime = requirements_path.stat().st_mtime
    if existing_mtime is not None and current_mtime < started_at - 1:
        return []
    try:
        return [
            line.strip()
            for line in requirements_path.read_text(encoding="utf-8").splitlines()
            if line.strip() and not line.lstrip().startswith("#")
        ]
    except OSError:
        return []


def read_output_metadata(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    metadata: dict[str, str] = {}
    try:
        for raw_line in path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line == "---":
                continue
            if ":" in line:
                key, value = line.split(":", 1)
            elif "=" in line:
                key, value = line.split("=", 1)
            else:
                continue
            metadata[key.strip().lower()] = value.strip()
    except OSError:
        return {}
    return metadata


def load_baseline_summary(path: str) -> dict[str, Any]:
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def load_pllm_csv(path: str) -> dict[str, dict[str, Any]]:
    """Load pllm CSV into a dict keyed by case name."""
    pllm: dict[str, dict[str, Any]] = {}
    with open(path, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            name = row.get("name", "").strip()
            if name:
                pllm[name] = {
                    "name": name,
                    "file": row.get("file", ""),
                    "result": row.get("result", ""),
                    "python_modules": row.get("python_modules", ""),
                    "duration": float(row.get("duration", 0) or 0),
                    "passed": int(row.get("passed", 0) or 0),
                }
    return pllm


def build_case_map(summary: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """Build a map from case_id to result from the baseline summary."""
    case_map: dict[str, dict[str, Any]] = {}
    for result in summary.get("results", []):
        artifact_dir = result.get("artifact_dir", "")
        case_id = os.path.basename(os.path.normpath(artifact_dir))
        if case_id:
            case_map[case_id] = result
    return case_map


def result_validation_status(result: dict[str, Any]) -> str:
    """Extract validation_status from a result's output_metadata."""
    metadata = result.get("output_metadata")
    if not isinstance(metadata, dict):
        return ""
    return str(metadata.get("validation_status") or "").strip().lower()


def result_requirements(result: dict[str, Any]) -> list[str]:
    """Extract requirements from a result."""
    reqs = result.get("requirements", [])
    if isinstance(reqs, list):
        normalized = [r for r in reqs if r and isinstance(r, str)]
        if normalized:
            return normalized
    artifact_dir = result.get("artifact_dir")
    if artifact_dir:
        req_path = resolve_repo_path(str(artifact_dir)) / "requirements.txt"
        if req_path.exists():
            try:
                return [
                    line.strip()
                    for line in req_path.read_text(encoding="utf-8").splitlines()
                    if line.strip() and not line.lstrip().startswith("#")
                ]
            except OSError:
                return []
    return []


def classify_baseline_status(result: dict[str, Any]) -> str:
    """Classify a baseline result as passed, skipped, or failed.

    Uses the same host-runtime reclassification rule from service.py:
    host-runtime skips with valid requirements and returncode 0 are passes.
    """
    validation_status = result_validation_status(result)
    is_host_skip = (
        validation_status.startswith("skipped")
        or validation_status == "host-runtime-required"
    )

    if is_host_skip:
        has_requirements = bool(result_requirements(result))
        if has_requirements and int(result.get("returncode", 1)) == 0:
            return "passed"
        return "skipped"

    if result.get("skipped"):
        return "skipped"

    if result.get("succeeded") and int(result.get("returncode", 1)) == 0:
        return "passed"

    return "failed"


def classify_pllm_status(pllm_entry: dict[str, Any] | None) -> str:
    """Classify a pllm result as PASS, FAIL, or MISSING."""
    if pllm_entry is None:
        return "MISSING"
    passed = int(pllm_entry.get("passed", 0) or 0)
    return "PASS" if passed > 0 else "FAIL"


def normalize_failure_bucket(result: dict[str, Any]) -> str:
    """Extract normalized failure bucket from a result."""
    metadata = result.get("output_metadata")
    if isinstance(metadata, dict):
        bucket = str(metadata.get("failure_bucket") or "").strip()
        if bucket:
            return bucket
        vs = str(metadata.get("validation_status") or "").strip()
        if vs and vs not in ("passed", "skipped", "host-runtime-required"):
            return vs

    log_tail = result.get("log_tail", [])
    if isinstance(log_tail, list):
        for line in reversed(log_tail):
            lowered = str(line).lower()
            if "modulenotfounderror" in lowered or "no module named" in lowered:
                return "module-not-found"
            if "could not find a version" in lowered or "no matching distribution" in lowered:
                return "version-not-found"
            if "importerror" in lowered:
                return "import-error"
            if "syntaxerror" in lowered:
                return "syntax-error"
    return "unclassified"


def compute_delta_label(baseline_status: str, rerun_status: str) -> str:
    """Compute the delta label between baseline and rerun status."""
    if baseline_status == rerun_status:
        return "unchanged"
    if baseline_status == "failed" and rerun_status == "passed":
        return "recovered"
    if baseline_status == "passed" and rerun_status == "failed":
        return "regressed"
    if baseline_status == "skipped" and rerun_status == "passed":
        return "newly-passed"
    if baseline_status == "passed" and rerun_status == "skipped":
        return "newly-skipped"
    return f"{baseline_status}->{rerun_status}"


def build_case_delta(
    case_id: str,
    snippet: str,
    baseline_result: dict[str, Any] | None,
    pllm_entry: dict[str, Any] | None,
    rerun_result: dict[str, Any] | None,
    is_dry_run: bool,
) -> dict[str, Any]:
    """Build a per-case delta entry."""
    if baseline_result is not None:
        baseline_status = classify_baseline_status(baseline_result)
        baseline_bucket = normalize_failure_bucket(baseline_result) if baseline_status == "failed" else ""
        validation_status = result_validation_status(baseline_result)
        validation_reason = ""
        metadata = baseline_result.get("output_metadata")
        if isinstance(metadata, dict):
            validation_reason = str(metadata.get("validation_reason") or "").strip()
    else:
        baseline_status = "missing"
        baseline_bucket = ""
        validation_status = ""
        validation_reason = ""

    pllm_status = classify_pllm_status(pllm_entry)

    if is_dry_run:
        rerun_status = baseline_status
        rerun_bucket = baseline_bucket
    elif rerun_result is not None:
        rerun_status = classify_baseline_status(rerun_result)
        rerun_bucket = normalize_failure_bucket(rerun_result) if rerun_status == "failed" else ""
    else:
        rerun_status = "not-run"
        rerun_bucket = ""

    delta_label = compute_delta_label(baseline_status, rerun_status)

    return {
        "case_id": case_id,
        "snippet": snippet,
        "baseline_status": baseline_status,
        "rerun_status": rerun_status,
        "pllm_status": pllm_status,
        "delta_label": delta_label,
        "baseline_bucket": baseline_bucket,
        "rerun_bucket": rerun_bucket,
        "validation_status": validation_status,
        "validation_reason": validation_reason,
    }


def run_apdr_for_case(
    case_id: str,
    snippet: str,
    output_dir: str,
    llm_model: str,
    llm_base_url: str,
    apdr_command: str,
    context_log: str,
) -> dict[str, Any] | None:
    """Run APDR for a single case and return the result dict."""
    snippet_path = REPO_ROOT / snippet
    if not snippet_path.exists():
        return None

    case_output = Path(output_dir) / case_id
    case_output.mkdir(parents=True, exist_ok=True)
    existing_outputs = {path.resolve() for path in case_output.glob("output_data_*.yml")}
    requirements_path = case_output / "requirements.txt"
    existing_requirements_mtime = (
        requirements_path.stat().st_mtime if requirements_path.exists() else None
    )
    started_at = time.time()

    cmd = [apdr_command, "resolve", str(snippet_path), "--output", str(case_output)]
    cmd.extend(DEFAULT_APDR_FLAGS)
    cmd.extend(["--llm-provider", "ollama"])
    cmd.extend(["--llm-model", llm_model])
    cmd.extend(["--llm-base-url", llm_base_url])
    if context_log:
        cmd.extend(["--benchmark-context-log", context_log])

    try:
        completed = subprocess.run(
            cmd,
            cwd=REPO_ROOT,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            check=False,
            timeout=1200,
        )
        output_paths: list[Path] = []
        for path in sorted(
            case_output.glob("output_data_*.yml"),
            key=lambda item: item.stat().st_mtime,
            reverse=True,
        ):
            resolved = path.resolve()
            if resolved not in existing_outputs or path.stat().st_mtime >= started_at - 1:
                output_paths.append(path)
        output_files = [str(path.relative_to(REPO_ROOT)) for path in output_paths]
        output_metadata = read_output_metadata(output_paths[0]) if output_paths else {}
        requirements = read_requirements_if_updated(
            requirements_path,
            existing_requirements_mtime,
            started_at,
        )
        skipped = output_metadata_skipped(output_metadata)
        if skipped and requirements and completed.returncode == 0:
            skipped = False
        succeeded = (
            not skipped
            and completed.returncode == 0
            and (bool(requirements) or bool(output_files))
        )
        return {
            "returncode": completed.returncode,
            "succeeded": succeeded,
            "skipped": skipped,
            "snippet": snippet,
            "artifact_dir": str(case_output),
            "output_files": output_files[:5],
            "output_metadata": output_metadata,
            "requirements": requirements,
            "log_tail": completed.stdout.splitlines()[-25:] if completed.stdout else [],
        }
    except (subprocess.TimeoutExpired, FileNotFoundError, OSError) as exc:
        return {
            "returncode": 1,
            "succeeded": False,
            "skipped": False,
            "snippet": snippet,
            "artifact_dir": str(case_output),
            "output_metadata": {},
            "requirements": [],
            "log_tail": [f"Error running APDR: {exc}"],
        }


def process_case_set(
    case_ids: list[str],
    case_map: dict[str, dict[str, Any]],
    pllm: dict[str, dict[str, Any]],
    manifest: dict[str, Any],
    args: argparse.Namespace,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Process a set of case IDs and return results + deltas."""
    results = []
    deltas = []

    for case_id in case_ids:
        baseline_result = case_map.get(case_id)
        pllm_entry = pllm.get(case_id)
        snippet = f"hard-gists/{case_id}/snippet.py"

        if baseline_result:
            snippet = baseline_result.get("snippet", snippet)

        rerun_result = None
        if not args.dry_run and not args.probe_only and args.apdr_command:
            rerun_result = run_apdr_for_case(
                case_id, snippet, str(Path(args.output_json).parent / "rerun-cases"),
                args.llm_model, args.llm_base_url, args.apdr_command, args.context_log,
            )

        delta = build_case_delta(case_id, snippet, baseline_result, pllm_entry, rerun_result, args.dry_run)
        results.append({
            "case_id": case_id,
            "snippet": snippet,
            "baseline_status": delta["baseline_status"],
            "rerun_status": delta["rerun_status"],
            "pllm_status": delta["pllm_status"],
            "delta_label": delta["delta_label"],
        })
        deltas.append(delta)

    return results, deltas


def process_guard_set(
    guard_entries: list[dict[str, Any]],
    case_map: dict[str, dict[str, Any]],
    pllm: dict[str, dict[str, Any]],
    args: argparse.Namespace,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Process preservation guard entries."""
    results = []
    deltas = []

    for entry in guard_entries:
        case_id = entry["case_id"]
        snippet = entry.get("snippet", f"hard-gists/{case_id}/snippet.py")
        baseline_result = case_map.get(case_id)
        pllm_entry = pllm.get(case_id)

        delta = build_case_delta(case_id, snippet, baseline_result, pllm_entry, None, True)
        results.append({
            "case_id": case_id,
            "snippet": snippet,
            "baseline_status": delta["baseline_status"],
            "rerun_status": delta["rerun_status"],
            "pllm_status": delta["pllm_status"],
            "guard_type": "preservation",
        })
        deltas.append(delta)

    return results, deltas


def write_json(path: str, data: Any) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")


def render_markdown(
    args: argparse.Namespace,
    manifest: dict[str, Any],
    canonical_results: list[dict[str, Any]],
    watchlist_results: list[dict[str, Any]],
    guard_results: list[dict[str, Any]],
    canonical_deltas: list[dict[str, Any]],
) -> str:
    """Render the TARGETED-RERUN.md markdown."""
    lines = [
        "# Phase 10: Targeted Benchmark Rerun",
        "",
        f"Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')}",
        f"Mode: {'dry-run (delta from baseline)' if args.dry_run else 'live rerun'}",
        f"Baseline: `{args.baseline_summary}`",
        f"pllm source: `{args.pllm_csv}`",
        "",
    ]

    # Command Contract
    lines.extend([
        "## Command Contract",
        "",
        "The baseline March 27, 2026 command shape used for reruns:",
        "",
        "```",
        f"apdr resolve <snippet> --output <case-dir> --range 5 --max-retries 5 "
        f"--docker-timeout 900 --validation-backend llm --llm-provider ollama "
        f"--llm-model {args.llm_model} --llm-base-url {args.llm_base_url} "
        f"--allow-llm --no-execute-snippet --force-validate "
        f"--benchmark-context-log <context-log>",
        "```",
        "",
        "Manifest: `{}`".format(args.manifest_json),
        "",
    ])

    # Canonical Slice
    canonical_passed = sum(1 for r in canonical_results if r["baseline_status"] == "passed")
    canonical_failed = sum(1 for r in canonical_results if r["baseline_status"] == "failed")
    canonical_skipped = sum(1 for r in canonical_results if r["baseline_status"] == "skipped")

    # Bucket breakdown for failed cases
    bucket_counts: dict[str, int] = {}
    for d in canonical_deltas:
        if d["baseline_status"] == "failed" and d["baseline_bucket"]:
            bucket = d["baseline_bucket"]
            bucket_counts[bucket] = bucket_counts.get(bucket, 0) + 1

    pllm_pass_count = sum(1 for r in canonical_results if r["pllm_status"] == "PASS")

    lines.extend([
        "## Canonical Slice",
        "",
        f"**{len(canonical_results)} cases** (locked Phase 7 tier3 parity slice)",
        "",
        f"| Metric | Count |",
        f"|--------|-------|",
        f"| Passed | {canonical_passed} |",
        f"| Failed | {canonical_failed} |",
        f"| Skipped | {canonical_skipped} |",
        f"| pllm PASS | {pllm_pass_count} |",
        "",
    ])

    if bucket_counts:
        lines.extend([
            "### Failure Buckets",
            "",
            "| Bucket | Count |",
            "|--------|-------|",
        ])
        for bucket in sorted(bucket_counts, key=lambda b: -bucket_counts[b]):
            lines.append(f"| {bucket} | {bucket_counts[bucket]} |")
        lines.append("")

    # Per-case table
    lines.extend([
        "### Per-Case Delta",
        "",
        "| Case ID | Baseline | Rerun | pllm | Delta |",
        "|---------|----------|-------|------|-------|",
    ])
    for r in canonical_results:
        lines.append(
            f"| `{r['case_id']}` | {r['baseline_status']} | {r['rerun_status']} "
            f"| {r['pllm_status']} | {r['delta_label']} |"
        )
    lines.append("")

    # Watchlist
    watchlist_passed = sum(1 for r in watchlist_results if r["baseline_status"] == "passed")
    watchlist_failed = sum(1 for r in watchlist_results if r["baseline_status"] == "failed")
    watchlist_skipped = sum(1 for r in watchlist_results if r["baseline_status"] == "skipped")

    lines.extend([
        "## Watchlist",
        "",
        f"**{len(watchlist_results)} cases** (separate from canonical contract)",
        "",
        f"| Metric | Count |",
        f"|--------|-------|",
        f"| Passed | {watchlist_passed} |",
        f"| Failed | {watchlist_failed} |",
        f"| Skipped | {watchlist_skipped} |",
        "",
        "| Case ID | Baseline | Rerun | pllm | Delta |",
        "|---------|----------|-------|------|-------|",
    ])
    for r in watchlist_results:
        lines.append(
            f"| `{r['case_id']}` | {r['baseline_status']} | {r['rerun_status']} "
            f"| {r['pllm_status']} | {r['delta_label']} |"
        )
    lines.append("")

    # Preservation Guards
    guard_groups = {
        "Passed (must stay passed)": [],
        "Host-runtime (must stay skipped or passed)": [],
        "Local-helper (expected skip)": [],
        "Unsolvable (expected skip or fail)": [],
    }
    guards = manifest.get("preservation_guards", {})
    passed_ids = {e["case_id"] for e in guards.get("passed_case_ids", [])}
    host_ids = {e["case_id"] for e in guards.get("host_runtime_case_ids", [])}
    local_ids = {e["case_id"] for e in guards.get("local_helper_case_ids", [])}
    unsolvable_ids = {e["case_id"] for e in guards.get("unsolvable_case_ids", [])}

    for r in guard_results:
        cid = r["case_id"]
        if cid in passed_ids:
            guard_groups["Passed (must stay passed)"].append(r)
        elif cid in host_ids:
            guard_groups["Host-runtime (must stay skipped or passed)"].append(r)
        elif cid in local_ids:
            guard_groups["Local-helper (expected skip)"].append(r)
        elif cid in unsolvable_ids:
            guard_groups["Unsolvable (expected skip or fail)"].append(r)

    lines.extend([
        "## Preservation Guards",
        "",
        "Verification-only cases outside the canonical 70-case delta math.",
        "",
    ])
    for group_name, group_results in guard_groups.items():
        if not group_results:
            continue
        lines.extend([
            f"### {group_name}",
            "",
            "| Case ID | Baseline | pllm |",
            "|---------|----------|------|",
        ])
        for r in group_results:
            lines.append(f"| `{r['case_id']}` | {r['baseline_status']} | {r['pllm_status']} |")
        lines.append("")

    return "\n".join(lines).rstrip() + "\n"


def main() -> int:
    args = parse_args()
    validate_args(args)

    status = build_live_status(args)
    if args.status_json:
        write_json(args.status_json, status)
    if status["apdr_command"]:
        args.apdr_command = str(status["apdr_command"])
    if args.probe_only:
        print(f"Wrote live-proof status to {args.status_json}")
        if status["live_ready"]:
            print("Live proof preflight passed.")
        else:
            print(f"Live proof blocked: {status['blocker_reason']}")
        return 0
    if requested_mode(args) != "dry-run" and not status["live_ready"]:
        blocker = status["blocker_reason"] or "live rerun prerequisites are not satisfied"
        print(f"Live rerun blocked: {blocker}", file=sys.stderr)
        return 1

    manifest = load_manifest(args.manifest_json)
    summary = load_baseline_summary(args.baseline_summary)
    pllm = load_pllm_csv(args.pllm_csv)
    case_map = build_case_map(summary)

    canonical_ids = manifest.get("canonical_case_ids", [])
    watchlist_ids = manifest.get("tier1_watchlist_case_ids", [])
    guards = manifest.get("preservation_guards", {})

    # Process canonical cases
    canonical_results, canonical_deltas = process_case_set(
        canonical_ids, case_map, pllm, manifest, args
    )

    # Process watchlist cases
    watchlist_results, watchlist_deltas = process_case_set(
        watchlist_ids, case_map, pllm, manifest, args
    )

    # Process preservation guards
    all_guard_entries = []
    for key in ("passed_case_ids", "host_runtime_case_ids", "local_helper_case_ids", "unsolvable_case_ids"):
        all_guard_entries.extend(guards.get(key, []))

    guard_results, guard_deltas = process_guard_set(
        all_guard_entries, case_map, pllm, args
    )

    # Write targeted-rerun.json
    rerun_output = {
        "generated_at": utc_now(),
        "mode": "dry-run" if args.dry_run else "live",
        "requested_mode": requested_mode(args),
        "actual_mode": actual_mode(args),
        "manifest": args.manifest_json,
        "baseline_summary": args.baseline_summary,
        "pllm_csv": args.pllm_csv,
        "llm_model": args.llm_model,
        "llm_base_url": args.llm_base_url,
        "canonical_case_count": len(canonical_ids),
        "watchlist_case_count": len(watchlist_ids),
        "preservation_guard_count": len(all_guard_entries),
        "canonical_results": canonical_results,
        "watchlist_results": watchlist_results,
        "preservation_guard_results": guard_results,
    }
    write_json(args.output_json, rerun_output)

    # Write case-delta.json
    all_deltas = canonical_deltas + watchlist_deltas + guard_deltas
    delta_summary_buckets: dict[str, int] = {}
    for d in canonical_deltas:
        if d["baseline_status"] == "failed" and d["baseline_bucket"]:
            b = d["baseline_bucket"]
            delta_summary_buckets[b] = delta_summary_buckets.get(b, 0) + 1

    delta_output = {
        "generated_at": utc_now(),
        "baseline_summary": args.baseline_summary,
        "pllm_csv": args.pllm_csv,
        "benchmark_context_log": args.context_log,
        "canonical_case_count": len(canonical_ids),
        "watchlist_case_count": len(watchlist_ids),
        "preservation_guard_count": len(all_guard_entries),
        "canonical_bucket_totals": delta_summary_buckets,
        "cases": all_deltas,
    }
    write_json(args.case_delta_json, delta_output)

    # Write TARGETED-RERUN.md
    md = render_markdown(args, manifest, canonical_results, watchlist_results, guard_results, canonical_deltas)
    md_path = Path(args.output_md)
    md_path.parent.mkdir(parents=True, exist_ok=True)
    md_path.write_text(md, encoding="utf-8")

    print(f"Wrote targeted rerun to {args.output_json}")
    print(f"Wrote case delta to {args.case_delta_json}")
    print(f"Wrote markdown to {args.output_md}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
