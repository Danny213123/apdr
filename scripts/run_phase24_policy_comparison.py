#!/usr/bin/env python3
"""Create Phase 24 env-first versus docker-first comparison artifacts."""

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

POLICIES = ("env-first", "docker-first")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Create a Phase 24 env-first or docker-first llm comparison artifact "
            "either by extracting a saved summary or by replaying the locked slice."
        )
    )
    parser.add_argument("--slice-json", required=True, help="Path to the locked Phase 24 slice manifest.")
    parser.add_argument(
        "--summary-json",
        default="",
        help="Saved benchmark summary to extract when using --probe-only.",
    )
    parser.add_argument("--output-json", required=True, help="Path to write the artifact JSON.")
    parser.add_argument("--output-md", default="", help="Optional Markdown summary output.")
    parser.add_argument("--mode", choices=POLICIES, required=True, help="Comparison label for the artifact.")
    parser.add_argument(
        "--llm-validation-policy",
        choices=POLICIES,
        required=True,
        help="Explicit llm first-hop policy for the artifact.",
    )
    parser.add_argument("--model-name", default="qwen3.5:9b", help="Model name for execute-live runs.")
    parser.add_argument("--base-url", default="http://localhost:11434", help="Base URL for execute-live runs.")
    parser.add_argument(
        "--build-profile",
        default="standard",
        help="Build profile recorded in execute-live artifacts.",
    )
    parser.add_argument(
        "--cache-state",
        default="unknown",
        help="Cache-state label recorded in execute-live artifacts.",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=1,
        help="Worker count for execute-live. Defaults to 1 for fixed-slice parity.",
    )
    parser.add_argument(
        "--dataset-root",
        default="",
        help="Optional extracted dataset root for execute-live sanity checks.",
    )
    parser.add_argument(
        "--dataset-tar",
        default="",
        help="Optional dataset archive for execute-live. Defaults to hard-gists.tar.gz in the repo root.",
    )
    parser.add_argument(
        "--resume-run-id",
        default="",
        help="Optional saved run id to resume for execute-live.",
    )
    parser.add_argument(
        "--loop-count",
        type=int,
        default=5,
        help="Loop count forwarded to the benchmark worker for execute-live.",
    )
    parser.add_argument(
        "--search-range",
        type=int,
        default=5,
        help="Search range forwarded to the benchmark worker for execute-live.",
    )
    parser.add_argument(
        "--temperature",
        type=float,
        default=None,
        help="Optional explicit temperature for execute-live.",
    )
    parser.add_argument(
        "--probe-only",
        action="store_true",
        help="Extract a deterministic artifact from a saved summary fixture.",
    )
    parser.add_argument(
        "--execute-live",
        action="store_true",
        help="Replay the locked slice through the benchmark worker.",
    )
    args = parser.parse_args()
    if args.mode != args.llm_validation_policy:
        parser.error("--mode must match --llm-validation-policy for Phase 24 artifacts.")
    if args.probe_only == args.execute_live:
        parser.error("Choose exactly one of --probe-only or --execute-live.")
    if args.probe_only and not args.summary_json.strip():
        parser.error("--summary-json is required for --probe-only.")
    return args


def repo_relative(path_text: str | Path) -> str:
    path = Path(path_text).expanduser()
    if not path.is_absolute():
        path = (REPO_ROOT / path).resolve()
    else:
        path = path.resolve()
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


def first_text(*values: Any) -> str:
    for value in values:
        text = str(value or "").strip()
        if text:
            return text
    return ""


def optional_text(*values: Any) -> str | None:
    text = first_text(*values)
    return text or None


def safe_float(value: Any) -> float | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def millis_to_seconds(value: Any) -> float | None:
    parsed = safe_float(value)
    if parsed is None:
        return None
    return round(parsed / 1000.0, 2)


def first_seconds(*values: Any) -> float:
    for value in values:
        parsed = safe_float(value)
        if parsed is not None:
            return round(parsed, 2)
    return 0.0


def first_millis_as_seconds(*values: Any) -> float:
    for value in values:
        parsed = millis_to_seconds(value)
        if parsed is not None:
            return parsed
    return 0.0


def normalize_policy(value: Any) -> str:
    text = str(value or "").strip().lower()
    if text == "env-first":
        return "env-first"
    return "docker-first"


def infer_display_status(result: dict[str, Any]) -> str:
    if bool(result.get("skipped")):
        return "skip"
    if bool(result.get("succeeded")):
        return "pass"
    return "fail"


def summary_results(summary: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for origin_key, default_origin in (("historical_results", "historical"), ("results", "live")):
        items = summary.get(origin_key)
        if not isinstance(items, list):
            continue
        for item in items:
            if not isinstance(item, dict):
                continue
            row = dict(item)
            row.setdefault("resultOrigin", default_origin)
            rows.append(row)
    return rows


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
        bucket = str(row.get("failure_bucket") or "").strip()
        if bucket:
            buckets[bucket] += 1
    return {
        "passes": passes,
        "skips": skips,
        "failures": failures,
        "failure_buckets": {key: buckets.get(key, 0) for key in DOMINANT_BUCKETS},
    }


def timing_totals(results: list[dict[str, Any]]) -> dict[str, float]:
    totals = {field: 0.0 for field in TIMING_FIELDS}
    for row in results:
        for field in TIMING_FIELDS:
            totals[field] += float(row.get(field) or 0.0)
    return {field: round(value, 2) for field, value in totals.items()}


def build_run_contract(summary: dict[str, Any], *, llm_validation_policy: str, model_name: str, base_url: str, mode: str) -> dict[str, Any]:
    run_contract = summary.get("run_contract")
    if not isinstance(run_contract, dict):
        run_contract = {}
    contract = {
        "run_contract_version": str(run_contract.get("run_contract_version") or "1"),
        "model_name": str(run_contract.get("model_name") or summary.get("model_name") or model_name).strip(),
        "base_url": str(run_contract.get("base_url") or summary.get("base_url") or base_url).strip(),
        "run_intent": str(run_contract.get("run_intent") or f"phase24-{mode}").strip(),
        "execution_mode": str(run_contract.get("execution_mode") or "llm-hybrid").strip(),
        "cache_state": str(run_contract.get("cache_state") or summary.get("cache_state") or "unknown").strip(),
        "host_architecture": str(run_contract.get("host_architecture") or "unknown").strip(),
        "apdr_binary_architecture": str(run_contract.get("apdr_binary_architecture") or "unknown").strip(),
        "python_architecture": str(run_contract.get("python_architecture") or "unknown").strip(),
        "llm_context_window": str(run_contract.get("llm_context_window") or "16384").strip(),
        "inference_policy": str(run_contract.get("inference_policy") or "temperature=inherited").strip(),
        "build_profile": str(run_contract.get("build_profile") or summary.get("build_profile") or "standard").strip(),
        "validation_backend": "llm",
        "llm_validation_policy": llm_validation_policy,
    }
    if contract["validation_backend"] != "llm":
        raise SystemExit("Phase 24 artifacts require validation_backend=llm.")
    return contract


def build_artifact_from_summary(
    *,
    slice_payload: dict[str, Any],
    summary: dict[str, Any],
    output_json: str,
    llm_validation_policy: str,
    model_name: str,
    base_url: str,
    source_run: str,
    source_summary: str,
    mode: str,
) -> dict[str, Any]:
    cases = slice_payload.get("cases")
    if not isinstance(cases, list) or not cases:
        raise SystemExit("Slice manifest must contain a non-empty cases array.")

    run_contract = build_run_contract(
        summary,
        llm_validation_policy=llm_validation_policy,
        model_name=model_name,
        base_url=base_url,
        mode=mode,
    )
    row_by_snippet = {
        str(row.get("snippet") or "").strip(): row
        for row in summary_results(summary)
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

        metadata = summary_row.get("output_metadata")
        if not isinstance(metadata, dict):
            metadata = {}
        artifact_dir_text = str(summary_row.get("artifact_dir") or entry.get("artifact_dir") or "").strip()
        artifact_dir_path = (REPO_ROOT / artifact_dir_text).resolve() if artifact_dir_text else None
        report_values = parse_report(artifact_dir_path / "resolution-report.txt") if artifact_dir_path and artifact_dir_path.exists() else {}

        requested_policy = normalize_policy(
            optional_text(
                summary_row.get("requested_llm_validation_policy"),
                summary_row.get("requestedLlmValidationPolicy"),
                metadata.get("requested_llm_validation_policy"),
                report_values.get("requested_llm_validation_policy"),
                summary.get("llm_validation_policy"),
                llm_validation_policy,
            )
        )
        if requested_policy != llm_validation_policy:
            raise SystemExit(
                f"Case {relative_path} drifted from requested policy {llm_validation_policy}: {requested_policy}"
            )

        validation_backend = first_text(
            summary_row.get("validation_backend"),
            summary_row.get("validationBackend"),
            metadata.get("validation_backend"),
            report_values.get("validation_backend"),
            summary.get("validation_backend"),
            "llm",
        )
        if validation_backend != "llm":
            raise SystemExit(f"Case {relative_path} drifted from validation_backend=llm: {validation_backend}")

        validation_path = first_text(
            summary_row.get("validation_path"),
            summary_row.get("validationPath"),
            metadata.get("validation_path"),
            report_values.get("validation_path"),
        )
        if not validation_path:
            raise SystemExit(f"Case {relative_path} is missing validation_path.")

        display_status = first_text(summary_row.get("display_status")) or infer_display_status(summary_row)
        failure_bucket = first_text(
            summary_row.get("failure_bucket"),
            summary_row.get("failureBucket"),
            metadata.get("failure_bucket"),
            report_values.get("failure_bucket"),
        )
        failure_family = first_text(
            summary_row.get("failure_family"),
            summary_row.get("failureFamily"),
            metadata.get("failure_family"),
            report_values.get("failure_family"),
        )

        result = {
            "case_id": str(entry.get("case_id") or "").strip(),
            "relative_path": relative_path,
            "snippet": relative_path,
            "artifact_dir": repo_relative(artifact_dir_text) if artifact_dir_text else "",
            "resultOrigin": first_text(summary_row.get("resultOrigin"), "live"),
            "display_status": display_status,
            "succeeded": bool(summary_row.get("succeeded")),
            "skipped": bool(summary_row.get("skipped")),
            "validation_backend": validation_backend,
            "validation_path": validation_path,
            "requested_llm_validation_policy": requested_policy,
            "llm_validation_route": first_text(
                summary_row.get("llm_validation_route"),
                summary_row.get("llmValidationRoute"),
                metadata.get("llm_validation_route"),
                report_values.get("llm_validation_route"),
            ),
            "docker_bypass_reason": optional_text(
                summary_row.get("docker_bypass_reason"),
                summary_row.get("dockerBypassReason"),
                metadata.get("docker_bypass_reason"),
                report_values.get("docker_bypass_reason"),
            ),
            "failure_bucket": failure_bucket,
            "failure_family": failure_family,
            "duration_seconds": first_seconds(
                summary_row.get("duration_seconds"),
                summary_row.get("durationSeconds"),
            ),
            "solve_duration_seconds": first_seconds(
                summary_row.get("solve_duration_seconds"),
                summary_row.get("solveDurationSeconds"),
                first_millis_as_seconds(
                    metadata.get("solve_duration_ms"),
                    report_values.get("solve_duration_ms"),
                ),
            ),
            "validation_duration_seconds": first_seconds(
                summary_row.get("validation_duration_seconds"),
                summary_row.get("validationDurationSeconds"),
                first_millis_as_seconds(
                    metadata.get("validation_duration_ms"),
                    report_values.get("validation_duration_ms"),
                ),
            ),
            "env_create_duration_seconds": first_seconds(
                summary_row.get("env_create_duration_seconds"),
                summary_row.get("envCreateDurationSeconds"),
                first_millis_as_seconds(
                    metadata.get("env_create_duration_ms"),
                    report_values.get("env_create_duration_ms"),
                ),
            ),
            "install_duration_seconds": first_seconds(
                summary_row.get("install_duration_seconds"),
                summary_row.get("installDurationSeconds"),
                first_millis_as_seconds(
                    metadata.get("install_duration_ms"),
                    report_values.get("install_duration_ms"),
                ),
            ),
            "docker_startup_duration_seconds": first_seconds(
                summary_row.get("docker_startup_duration_seconds"),
                summary_row.get("dockerStartupDurationSeconds"),
                first_millis_as_seconds(
                    metadata.get("docker_startup_duration_ms"),
                    report_values.get("docker_startup_duration_ms"),
                ),
            ),
            "smoke_duration_seconds": first_seconds(
                summary_row.get("smoke_duration_seconds"),
                summary_row.get("smokeDurationSeconds"),
                first_millis_as_seconds(
                    metadata.get("smoke_duration_ms"),
                    report_values.get("smoke_duration_ms"),
                ),
            ),
        }
        extracted_results.append(result)

    artifact = {
        "phase": "24",
        "sample_id": Path(output_json).expanduser().resolve().stem,
        "slice_id": str(slice_payload.get("slice_id") or "").strip(),
        "mode": mode,
        "validation_backend": "llm",
        "llm_validation_policy": llm_validation_policy,
        "model_name": run_contract["model_name"],
        "base_url": run_contract["base_url"],
        "source_run": source_run,
        "source_summary": source_summary,
        "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "run_contract": run_contract,
        "historical_results": [],
        "results": extracted_results,
    }
    artifact["counts"] = count_results(extracted_results)
    artifact["timing_totals"] = timing_totals(extracted_results)
    return artifact


def print_worker_message(message: dict[str, Any]) -> None:
    event_type = str(message.get("type") or message.get("kind") or "").strip()
    if event_type == "plan":
        print(
            f"[plan] total={message.get('total')} run_dir={message.get('run_dir')} "
            f"workers={message.get('effective_workers')}"
        )
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
        if validation_path:
            print(f"[detail] path={validation_path}")
        return
    if event_type == "done":
        print(
            f"[done] status={message.get('status')} run_dir={message.get('run_dir')} "
            f"total={message.get('total')}"
        )
        return
    if event_type == "error":
        print(f"[error] {message.get('message')}")
        return


def run_live_summary(args: argparse.Namespace, slice_payload: dict[str, Any]) -> tuple[dict[str, Any], Path]:
    from benchmark_ui.runner import BenchmarkWorker
    from benchmark_ui.service import BenchmarkService
    from benchmark_ui.state import AppState

    state = AppState()
    service = BenchmarkService(state)
    runtime_ok, runtime_detail, _runner = state.validate_tool_runtime("apdr", "", "llm")
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
        config["validation_backend"] = "llm"
        config["llm_validation_policy"] = args.llm_validation_policy
        config["run_intent"] = f"phase24-{args.mode}"
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
            "validation_backend": "llm",
            "llm_validation_policy": args.llm_validation_policy,
            "run_intent": f"phase24-{args.mode}",
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
        f"[start] replaying {slice_payload.get('slice_id')} with "
        f"policy={args.llm_validation_policy} model={args.model_name} workers={config.get('workers') or 1}"
    )
    worker.start()
    errors: list[str] = []
    while worker.is_alive():
        try:
            message = queue.get(timeout=1)
        except Empty:
            continue
        print_worker_message(message)
        if str(message.get("type") or message.get("kind") or "") == "error":
            errors.append(str(message.get("message") or "Unknown benchmark worker error."))
    worker.join()
    while True:
        try:
            message = queue.get_nowait()
        except Empty:
            break
        print_worker_message(message)
        if str(message.get("type") or message.get("kind") or "") == "error":
            errors.append(str(message.get("message") or "Unknown benchmark worker error."))

    if errors:
        raise SystemExit("\n".join(errors))
    if worker.run_dir is None:
        raise SystemExit("Benchmark worker did not produce a run directory.")

    summary_path = worker.run_dir / "summary.json"
    summary = load_json_object(str(summary_path), "Phase 24 live summary")
    status = str(summary.get("status") or "").strip().lower()
    if status not in {"completed", "stopped"}:
        raise SystemExit(f"Replay run did not complete cleanly: {status or 'unknown'}")
    return summary, worker.run_dir


def write_markdown(output_path: Path, artifact: dict[str, Any]) -> None:
    counts = artifact["counts"]
    timings = artifact["timing_totals"]
    buckets = counts["failure_buckets"]
    lines = [
        "# Phase 24 Policy Comparison Artifact",
        "",
        f"- Mode: `{artifact['mode']}`",
        f"- Policy: `{artifact['llm_validation_policy']}`",
        f"- Slice: `{artifact['slice_id']}`",
        f"- Source run: `{artifact['source_run']}`",
        f"- Passes: `{counts['passes']}`",
        f"- Failures: `{counts['failures']}`",
        f"- Module-not-found: `{buckets['module-not-found']}`",
        f"- Version-not-found: `{buckets['version-not-found']}`",
        f"- Environment-build-failed: `{buckets['environment-build-failed']}`",
        f"- Total duration seconds: `{timings['duration_seconds']}`",
        f"- Docker startup seconds: `{timings['docker_startup_duration_seconds']}`",
    ]
    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    args = parse_args()
    slice_payload = load_json_object(args.slice_json, "Phase 24 slice manifest")

    if args.probe_only:
        summary_path = Path(args.summary_json).expanduser().resolve()
        summary = load_json_object(str(summary_path), "Phase 24 source summary")
        source_run = repo_relative(summary_path.parent)
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
        llm_validation_policy=args.llm_validation_policy,
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
