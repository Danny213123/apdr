#!/usr/bin/env python3
"""Extract or replay Phase 20 dominant-bucket benchmark artifacts."""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from queue import Empty, Queue
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from benchmark_ui.runner import BenchmarkWorker
from benchmark_ui.service import BenchmarkService
from benchmark_ui.state import AppState


DOMINANT_BUCKETS = (
    "module-not-found",
    "version-not-found",
    "environment-build-failed",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Create a Phase 20 dominant-bucket artifact either by extracting rows "
            "from an existing benchmark summary or by replaying the locked slice "
            "through the benchmark worker."
        )
    )
    parser.add_argument("--slice-json", required=True, help="Path to the locked Phase 20 slice manifest.")
    parser.add_argument(
        "--summary-json",
        default="",
        help="Benchmark summary to extract rows from when using --probe-only.",
    )
    parser.add_argument(
        "--dataset-root",
        default="",
        help="Optional extracted dataset root for --execute-live sanity checks.",
    )
    parser.add_argument(
        "--dataset-tar",
        default="",
        help="Optional dataset archive for --execute-live. Defaults to hard-gists.tar.gz in the repo root.",
    )
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
        "--build-profile",
        default="standard",
        help="Build profile recorded in the replay run config.",
    )
    parser.add_argument(
        "--cache-state",
        default="unknown",
        help="Cache-state label recorded in the replay run config.",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=1,
        help="Worker count for --execute-live. Defaults to 1 for the fixed slice.",
    )
    parser.add_argument(
        "--resume-run-id",
        default="",
        help="Optional saved benchmark run id to resume for --execute-live.",
    )
    parser.add_argument(
        "--loop-count",
        type=int,
        default=5,
        help="Loop count forwarded to the benchmark worker for --execute-live.",
    )
    parser.add_argument(
        "--search-range",
        type=int,
        default=5,
        help="Search range forwarded to the benchmark worker for --execute-live.",
    )
    parser.add_argument(
        "--temperature",
        type=float,
        default=None,
        help="Optional explicit temperature for --execute-live.",
    )
    parser.add_argument(
        "--probe-only",
        action="store_true",
        help="Extract an artifact from an existing run without launching APDR.",
    )
    parser.add_argument(
        "--execute-live",
        action="store_true",
        help="Replay the locked slice through the benchmark worker and extract the resulting artifact.",
    )
    args = parser.parse_args()
    if args.probe_only == args.execute_live:
        parser.error("Choose exactly one of --probe-only or --execute-live.")
    if args.probe_only and not args.summary_json.strip():
        parser.error("--summary-json is required for --probe-only.")
    return args


def repo_relative(path_text: str | Path) -> str:
    path = Path(path_text).expanduser().resolve()
    try:
        return str(path.relative_to(REPO_ROOT))
    except ValueError:
        return str(path)


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
        return values
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
    if status.startswith("skipped") or status == "host-runtime-required":
        return "environment-specific"
    if status == "passed":
        return None
    return "dependency-resolution"


def first_text(*values: Any) -> str:
    for value in values:
        text = str(value or "").strip()
        if text:
            return text
    return ""


def optional_text(*values: Any) -> str | None:
    text = first_text(*values)
    return text or None


def optional_bool(*values: Any) -> bool | None:
    for value in values:
        if value is None:
            continue
        if isinstance(value, bool):
            return value
        text = str(value).strip().lower()
        if text in {"1", "true", "yes", "on"}:
            return True
        if text in {"0", "false", "no", "off"}:
            return False
    return None


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


def build_artifact_from_summary(
    *,
    slice_payload: dict[str, Any],
    summary: dict[str, Any],
    output_json: str,
    validation_backend: str,
    model_name: str,
    base_url: str,
    source_run: str,
    source_summary: str,
    mode: str,
) -> dict[str, Any]:
    cases = slice_payload.get("cases")
    if not isinstance(cases, list) or not cases:
        raise SystemExit("Slice manifest must contain a non-empty cases array.")
    live_results = summary.get("results")
    historical_results = summary.get("historical_results")
    if not isinstance(live_results, list):
        raise SystemExit("Benchmark summary must contain a results array.")
    if not isinstance(historical_results, list):
        historical_results = []

    summary_results = [*historical_results, *live_results]

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

        artifact_dir = REPO_ROOT / str(summary_row.get("artifact_dir") or entry.get("artifact_dir") or "").strip()
        report_values = parse_report(artifact_dir / "resolution-report.txt")
        interrupted_reason = ""
        if not report_values:
            interrupted_reason = (
                f"Missing resolution-report.txt in {repo_relative(artifact_dir)}; "
                f"benchmark case ended with returncode {summary_row.get('returncode')}."
            )
        validation_status = first_text(
            report_values.get("validation_status"),
            summary_row.get("validation_status"),
            summary_row.get("validationStatus"),
            "interrupted" if interrupted_reason else "",
        )
        validation_reason = first_text(
            report_values.get("validation_reason"),
            summary_row.get("validation_reason"),
            summary_row.get("validationReason"),
            interrupted_reason,
        )
        failure_bucket = first_text(
            report_values.get("failure_bucket"),
            summary_row.get("failure_bucket"),
            summary_row.get("failureBucket"),
            validation_status,
        )
        failure_family = optional_text(
            report_values.get("failure_family"),
            summary_row.get("failure_family"),
            summary_row.get("failureFamily"),
        ) or ("environment-specific" if interrupted_reason else infer_failure_family(validation_status))
        validation_path = first_text(
            report_values.get("validation_path"),
            summary_row.get("validation_path"),
            summary_row.get("validationPath"),
            "interrupted" if interrupted_reason else "env",
        )
        escalated_backend = optional_text(
            report_values.get("escalated_backend"),
            summary_row.get("escalated_backend"),
            summary_row.get("escalatedBackend"),
        )
        fallback_invoked = optional_bool(
            report_values.get("fallback_invoked"),
            summary_row.get("fallback_invoked"),
            summary_row.get("fallbackInvoked"),
        )
        fallback_outcome = optional_text(
            report_values.get("fallback_outcome"),
            summary_row.get("fallback_outcome"),
            summary_row.get("fallbackOutcome"),
        )
        fallback_reason = optional_text(
            report_values.get("fallback_reason"),
            summary_row.get("fallback_reason"),
            summary_row.get("fallbackReason"),
        )
        extracted_results.append(
            {
                "relative_path": relative_path,
                "snippet": relative_path,
                "artifact_dir": repo_relative(artifact_dir),
                "resultOrigin": first_text(summary_row.get("resultOrigin"), "live"),
                "display_status": display_status(summary_row),
                "succeeded": bool(summary_row.get("succeeded")),
                "skipped": bool(summary_row.get("skipped")),
                "validation_status": validation_status,
                "validation_reason": validation_reason,
                "failure_bucket": failure_bucket,
                "failure_family": failure_family,
                "validation_backend": validation_backend,
                "validation_path": validation_path,
                "escalated_backend": escalated_backend,
                "fallback_invoked": fallback_invoked,
                "fallback_outcome": fallback_outcome,
                "fallback_reason": fallback_reason,
            }
        )

    run_contract = summary.get("run_contract")
    if not isinstance(run_contract, dict):
        run_contract = {
            "run_contract_version": "1",
            "tool": "apdr",
            "model_name": model_name,
            "base_url": base_url,
            "validation_backend": validation_backend,
            "run_intent": mode,
            "execution_mode": "llm-hybrid" if validation_backend == "llm" else "env-fast",
            "cache_state": "unknown",
            "host_architecture": "unknown",
            "apdr_binary_architecture": "unknown",
            "python_architecture": "unknown",
            "llm_context_window": "16384",
            "inference_policy": "temperature=inherited",
            "build_profile": "standard",
        }

    artifact = {
        "sample_id": Path(output_json).expanduser().resolve().stem,
        "slice_id": str(slice_payload.get("slice_id") or "").strip(),
        "mode": mode,
        "source_run": source_run,
        "source_summary": source_summary,
        "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "validation_backend": validation_backend,
        "model_name": model_name,
        "base_url": base_url,
        "run_contract": run_contract,
        "historical_results": [],
        "results": extracted_results,
    }
    artifact["counts"] = count_results(extracted_results)
    return artifact


def print_worker_message(message: dict[str, Any]) -> None:
    event_type = str(message.get("type") or message.get("kind") or "").strip()
    if event_type == "plan":
        print(
            f"[plan] total={message.get('total')} run_dir={message.get('run_dir')} "
            f"workers={message.get('effective_workers')}"
        )
        warnings = list(message.get("preflight_warnings") or [])
        for warning in warnings:
            print(f"[warn] {warning}")
        return
    if event_type == "status":
        print(f"[status] {message.get('text')}")
        return
    if event_type == "progress":
        result = message.get("result") or {}
        print(
            f"[progress] {message.get('completed')}/{message.get('total')} "
            f"{message.get('snippet')} rc={message.get('returncode')} "
            f"duration={message.get('duration')}"
        )
        validation_path = first_text(result.get("validationPath"), result.get("validation_path"))
        fallback_outcome = first_text(result.get("fallbackOutcome"), result.get("fallback_outcome"))
        failure_bucket = first_text(result.get("failureBucket"), result.get("failure_bucket"))
        if validation_path or fallback_outcome or failure_bucket:
            parts = []
            if validation_path:
                parts.append(f"path={validation_path}")
            if fallback_outcome:
                parts.append(f"fallback={fallback_outcome}")
            if failure_bucket:
                parts.append(f"bucket={failure_bucket}")
            print(f"[detail] {' '.join(parts)}")
        return
    if event_type == "done":
        print(
            f"[done] status={message.get('status')} run_dir={message.get('run_dir')} "
            f"total={message.get('total')}"
        )
        return
    if event_type == "error":
        print(f"[error] {message.get('message')}")
        trace = str(message.get("trace") or "").strip()
        if trace:
            print(trace)
        return


def run_live_summary(args: argparse.Namespace, slice_payload: dict[str, Any]) -> tuple[dict[str, Any], Path]:
    state = AppState()
    service = BenchmarkService(state)
    runtime_ok, runtime_detail, _runner = state.validate_tool_runtime(
        "apdr",
        "",
        args.validation_backend,
    )
    if not runtime_ok:
        raise SystemExit(runtime_detail)

    dataset_tar = (
        Path(args.dataset_tar).expanduser().resolve()
        if args.dataset_tar.strip()
        else state.default_dataset_tar.resolve()
    )
    if not dataset_tar.exists():
        raise SystemExit(f"Dataset archive not found: {dataset_tar}")

    if args.dataset_root.strip():
        dataset_root = Path(args.dataset_root).expanduser().resolve()
        if not dataset_root.exists():
            raise SystemExit(f"Dataset root not found: {dataset_root}")
    if args.resume_run_id.strip():
        summary = state.load_run_summary(args.resume_run_id.strip())
        if not summary:
            raise SystemExit(f"Saved run not found for resume: {args.resume_run_id.strip()}")
        config = service._run_config_from_summary(summary)
        config["_resume_from_run_id"] = args.resume_run_id.strip()
        config["_resume_results"] = service._summary_results(summary)
        config["workers"] = int(args.workers)
        config["replay_manifest"] = str(Path(args.slice_json).expanduser().resolve())
        config["model"] = args.model_name
        config["base_url"] = args.base_url
        config["validation_backend"] = args.validation_backend
        config["run_intent"] = args.mode
        config["cache_state"] = args.cache_state
        config["build_profile"] = args.build_profile
        if args.temperature is not None:
            config["temperature"] = float(args.temperature)
    else:
        payload = {
            "tool": "apdr",
            "dataset_tar": str(dataset_tar),
            "loop_count": int(args.loop_count),
            "search_range": int(args.search_range),
            "rag": True,
            "verbose": False,
            "snippet_limit": "",
            "python_command": "",
            "validation_backend": args.validation_backend,
            "run_intent": args.mode,
            "cache_state": args.cache_state,
            "build_profile": args.build_profile,
            "workers": int(args.workers),
            "replay_manifest": str(Path(args.slice_json).expanduser().resolve()),
            "model": args.model_name,
            "base_url": args.base_url,
        }
        if args.temperature is not None:
            payload["temperature"] = float(args.temperature)
        config = service._hydrate_run_config(service._normalize_run_config(payload, validate=True))

    queue: Queue[dict[str, Any]] = Queue()
    worker = BenchmarkWorker(state, config, queue)
    print(
        f"[start] replaying {slice_payload.get('slice_id')} with backend={args.validation_backend} "
        f"model={args.model_name} workers={config.get('workers') or 1}"
    )
    worker.start()
    error_messages: list[str] = []
    while worker.is_alive():
        try:
            message = queue.get(timeout=1)
        except Empty:
            continue
        print_worker_message(message)
        if str(message.get("type") or message.get("kind") or "") == "error":
            error_messages.append(str(message.get("message") or "Unknown benchmark worker error."))
    worker.join()
    while True:
        try:
            message = queue.get_nowait()
        except Empty:
            break
        print_worker_message(message)
        if str(message.get("type") or message.get("kind") or "") == "error":
            error_messages.append(str(message.get("message") or "Unknown benchmark worker error."))

    if error_messages:
        raise SystemExit("\n".join(error_messages))
    if worker.run_dir is None:
        raise SystemExit("Benchmark worker did not produce a run directory.")

    summary_path = worker.run_dir / "summary.json"
    summary = load_json_object(str(summary_path), "Phase 21 candidate summary")
    status = str(summary.get("status") or "").strip().lower()
    if status not in {"completed", "stopped"}:
        raise SystemExit(f"Replay run did not complete cleanly: {status or 'unknown'}")
    return summary, worker.run_dir


def write_markdown(output_path: Path, artifact: dict[str, Any]) -> None:
    counts = artifact["counts"]
    buckets = counts["failure_buckets"]
    lines = [
        "# Phase 20 Dominant-Bucket Artifact",
        "",
        f"- Mode: `{artifact['mode']}`",
        f"- Slice: `{artifact['slice_id']}`",
        f"- Source run: `{artifact['source_run']}`",
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
    slice_payload = load_json_object(args.slice_json, "Phase 20 slice manifest")

    if args.probe_only:
        summary_path = Path(args.summary_json).expanduser().resolve()
        summary = load_json_object(str(summary_path), "Phase 20 source summary")
        source_run = (
            first_text(slice_payload.get("source_run"), repo_relative(summary_path.parent))
            if args.mode == "baseline"
            else repo_relative(summary_path.parent)
        )
        source_summary = repo_relative(summary_path)
    else:
        summary, run_dir = run_live_summary(args, slice_payload)
        summary_path = run_dir / "summary.json"
        source_run = repo_relative(run_dir)
        source_summary = repo_relative(summary_path)

    artifact = build_artifact_from_summary(
        slice_payload=slice_payload,
        summary=summary,
        output_json=args.output_json,
        validation_backend=args.validation_backend,
        model_name=args.model_name,
        base_url=args.base_url,
        source_run=source_run,
        source_summary=source_summary,
        mode=args.mode,
    )
    output_json = Path(args.output_json).expanduser().resolve()
    output_json.write_text(json.dumps(artifact, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    if args.output_md:
        write_markdown(Path(args.output_md).expanduser().resolve(), artifact)
    print(f"[artifact] wrote {repo_relative(output_json)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
