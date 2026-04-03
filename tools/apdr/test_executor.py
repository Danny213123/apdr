#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path


def append_context_log(log_path: str, kind: str, message: str) -> None:
    if not log_path:
        return
    path = Path(log_path).expanduser()
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now(timezone.utc).isoformat()
        block = f"===== {timestamp} kind={kind} =====\n{message.rstrip()}\n\n"
        with path.open("a", encoding="utf-8") as handle:
            handle.write(block)
    except OSError:
        return


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compatibility wrapper for the APDR Rust CLI")
    parser.add_argument("-f", "--file", required=True, help="Snippet file to resolve")
    parser.add_argument("--output-dir", default="", help="Directory for APDR benchmark artifacts")
    parser.add_argument("-b", "--base", default="http://localhost:11434", help="LLM base URL for optional Ollama fallback")
    parser.add_argument("-m", "--model", default="qwen3.5:9b", help="LLM model used when APDR's LLM fallback is enabled")
    parser.add_argument("-t", "--temp", default="0.7", help="Compatibility flag retained for benchmark parity")
    parser.add_argument("-l", "--loop", type=int, default=5, help="Maximum APDR recovery retries")
    parser.add_argument("-r", "--range", type=int, default=1, help="Python version search range")
    parser.add_argument("-ra", "--rag", default="true", help="Enable APDR's optional LLM-assisted resolution tier")
    parser.add_argument("--docker-timeout", type=int, default=900, help="Validation install/import timeout in seconds")
    parser.add_argument(
        "--validation-backend",
        choices=("env", "docker", "llm"),
        default="env",
        help="Validation backend: env (local venvs), docker, or llm (docker-first required)",
    )
    parser.add_argument(
        "--llm-validation-policy",
        choices=("docker-first", "env-first"),
        default="docker-first",
        help="First validation hop inside llm mode: docker-first (legacy env-first inputs are normalized)",
    )
    parser.add_argument("--no-validate", action="store_true", help="Skip APDR validation")
    parser.add_argument("--no-execute-snippet", action="store_true", help="Only import resolved packages in smoke tests")
    parser.add_argument("--no-parallel-versions", action="store_true", help="Validate only the selected Python version")
    parser.add_argument("--benchmark-context-log", default="", help="Append benchmark build/run/LLM trace to this file")
    parser.add_argument("--run-contract-json", default="", help="Path to the benchmark run contract JSON")
    parser.add_argument("--llm-only", action="store_true", help="Use LLM-only mode (skip heuristic tiers, keep Docker validation)")
    parser.add_argument("--force-validate", action="store_true", help="Force venv validation even for cached/pre-solved results")
    parser.add_argument("--validation-timeout", type=int, default=0, help="Per-attempt validation timeout in seconds")
    parser.add_argument(
        "--build-profile", default="",
        help="Requested build profile (release, debug, pgo, standard). "
             "When set, choose_command prefers the matching binary and warns "
             "instead of silently falling back to cargo run.",
    )
    parser.add_argument("-v", "--verbose", action="store_true", help="Print the raw CLI output")
    return parser.parse_args()


def choose_command(tool_dir: Path, build_profile: str = "") -> tuple[list[str], list[str]]:
    release_binary = tool_dir / "target" / "release" / "apdr"
    debug_binary = tool_dir / "target" / "debug" / "apdr"
    release_binary_windows = tool_dir / "target" / "release" / "apdr.exe"
    debug_binary_windows = tool_dir / "target" / "debug" / "apdr.exe"
    source_mtime = newest_source_mtime(tool_dir)
    profile = str(build_profile or "").strip().lower().replace("_", "-")

    if profile in {"release", "pgo"}:
        preferred = [release_binary_windows, release_binary]
        fallback = [debug_binary_windows, debug_binary]
    elif profile == "debug":
        preferred = [debug_binary_windows, debug_binary]
        fallback = [release_binary_windows, release_binary]
    else:
        candidates = [
            candidate
            for candidate in (debug_binary_windows, release_binary_windows, debug_binary, release_binary)
            if candidate.exists() and candidate.stat().st_mtime >= source_mtime
        ]
        if candidates:
            freshest = max(candidates, key=lambda path: path.stat().st_mtime)
            return [str(freshest)], []
        preferred = [debug_binary_windows, release_binary_windows, debug_binary, release_binary]
        fallback = []

    warnings: list[str] = []
    stale_preferred: list[Path] = []
    for candidate in preferred:
        if not candidate.exists():
            continue
        if candidate.stat().st_mtime >= source_mtime:
            return [str(candidate)], warnings
        stale_preferred.append(candidate)

    if stale_preferred:
        warnings.append(
            "Requested build_profile="
            f"{profile or 'standard'} but the matching binary is older than the Rust sources: "
            + ", ".join(str(path.relative_to(tool_dir)) for path in stale_preferred)
        )

    fresh_fallbacks = [
        candidate
        for candidate in fallback
        if candidate.exists() and candidate.stat().st_mtime >= source_mtime
    ]
    if fresh_fallbacks:
        selected = fresh_fallbacks[0]
        warnings.append(
            f"Requested build_profile={profile or 'standard'} but using "
            f"{selected.relative_to(tool_dir)} instead."
        )
        return [str(selected)], warnings

    if fallback:
        existing_fallbacks = [candidate for candidate in fallback if candidate.exists()]
        if existing_fallbacks:
            warnings.append(
                f"Requested build_profile={profile or 'standard'} but only found stale "
                f"fallback binaries: {', '.join(str(path.relative_to(tool_dir)) for path in existing_fallbacks)}."
            )

    warnings.append(
        f"No fresh prebuilt APDR binary found for build_profile={profile or 'standard'}; "
        "falling back to cargo run, which includes build overhead."
    )
    return ["cargo", "run", "--quiet", "--"], warnings


def newest_source_mtime(tool_dir: Path) -> float:
    paths = [tool_dir / "Cargo.toml"]
    paths.extend(path for path in (tool_dir / "src").rglob("*.rs"))
    paths.extend(path for path in (tool_dir / "data").rglob("*") if path.is_file())
    mtimes = [path.stat().st_mtime for path in paths if path.exists()]
    return max(mtimes, default=0.0)


def parse_summary(stdout: str) -> dict[str, str]:
    summary: dict[str, str] = {}
    for line in stdout.splitlines():
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        summary[key.strip()] = value.strip()
    return summary


def _load_required_run_contract_keys() -> tuple[str, ...]:
    repo_root = Path(__file__).resolve().parents[2]
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))
    from benchmark_ui.run_contract import REQUIRED_RUN_CONTRACT_KEYS

    return tuple(str(key) for key in REQUIRED_RUN_CONTRACT_KEYS)


def load_run_contract(path_text: str) -> dict[str, str]:
    if not path_text.strip():
        return {}
    path = Path(path_text).expanduser().resolve()
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError(f"Run contract {path} must be a JSON object")
    required = _load_required_run_contract_keys()
    contract = {key: str(raw.get(key, "")).strip() for key in required}
    missing = [key for key, value in contract.items() if not value]
    if missing:
        raise ValueError(f"Run contract {path} is missing required keys: {', '.join(missing)}")
    return contract


def _summary_or_contract(
    summary: dict[str, str],
    upper_key: str,
    contract: dict[str, str],
    contract_key: str,
    fallback: str = "",
) -> str:
    return str(summary.get(upper_key) or contract.get(contract_key) or fallback).strip()


def write_output_file(
    snippet_dir: Path,
    python_version: str,
    summary: dict[str, str],
    args: argparse.Namespace,
    run_contract: dict[str, str],
) -> Path:
    output_path = snippet_dir / f"output_data_{python_version}.yml"
    model_name = _summary_or_contract(summary, "MODEL_NAME", run_contract, "model_name", args.model)
    base_url = _summary_or_contract(summary, "BASE_URL", run_contract, "base_url", args.base)
    content = [
        "---",
        f"python_version: {python_version}",
        "tool: apdr",
        f"model: {args.model}",
        f"model_name: {model_name}",
        f"base_url: {base_url}",
        f"temperature: {args.temp}",
        f"loop_count: {args.loop}",
        f"search_range: {args.range}",
        f"rag_enabled: {args.rag}",
        f"requirements_path: {summary.get('REQUIREMENTS_PATH', '')}",
        f"report_path: {summary.get('REPORT_PATH', '')}",
        f"resolved_count: {summary.get('RESOLVED_COUNT', '0')}",
        f"unresolved_count: {summary.get('UNRESOLVED_COUNT', '0')}",
        f"solvability_decision: {summary.get('SOLVABILITY_DECISION', '')}",
        f"solvability_confidence: {summary.get('SOLVABILITY_CONFIDENCE', '0.00')}",
        f"solvability_reason: {summary.get('SOLVABILITY_REASON', '')}",
        f"solvability_source: {summary.get('SOLVABILITY_SOURCE', '')}",
        f"llm_calls: {summary.get('LLM_CALLS', '0')}",
        f"env_builds: {summary.get('ENV_BUILDS', '0')}",
        f"retries: {summary.get('RETRIES', '0')}",
        f"solve_duration_ms: {summary.get('SOLVE_DURATION_MS', '0')}",
        f"validation_duration_ms: {summary.get('VALIDATION_DURATION_MS', '0')}",
        f"llm_duration_ms: {summary.get('LLM_DURATION_MS', '0')}",
        f"env_create_duration_ms: {summary.get('ENV_CREATE_DURATION_MS', '0')}",
        f"install_duration_ms: {summary.get('INSTALL_DURATION_MS', '0')}",
        f"docker_startup_duration_ms: {summary.get('DOCKER_STARTUP_DURATION_MS', '0')}",
        f"smoke_duration_ms: {summary.get('SMOKE_DURATION_MS', '0')}",
        f"validation_backend: {summary.get('VALIDATION_BACKEND', '')}",
        f"validation_path: {summary.get('VALIDATION_PATH', '')}",
        f"run_contract_version: {_summary_or_contract(summary, 'RUN_CONTRACT_VERSION', run_contract, 'run_contract_version')}",
        f"run_intent: {_summary_or_contract(summary, 'RUN_INTENT', run_contract, 'run_intent')}",
        f"execution_mode: {_summary_or_contract(summary, 'EXECUTION_MODE', run_contract, 'execution_mode')}",
        f"cache_state: {_summary_or_contract(summary, 'CACHE_STATE', run_contract, 'cache_state')}",
        f"host_architecture: {_summary_or_contract(summary, 'HOST_ARCHITECTURE', run_contract, 'host_architecture')}",
        f"apdr_binary_architecture: {_summary_or_contract(summary, 'APDR_BINARY_ARCHITECTURE', run_contract, 'apdr_binary_architecture')}",
        f"python_architecture: {_summary_or_contract(summary, 'PYTHON_ARCHITECTURE', run_contract, 'python_architecture')}",
        f"llm_context_window: {_summary_or_contract(summary, 'LLM_CONTEXT_WINDOW', run_contract, 'llm_context_window')}",
        f"inference_policy: {_summary_or_contract(summary, 'INFERENCE_POLICY', run_contract, 'inference_policy')}",
        f"build_profile: {_summary_or_contract(summary, 'BUILD_PROFILE', run_contract, 'build_profile')}",
        f"authored_plan_status: {summary.get('AUTHORED_PLAN_STATUS', '')}",
        f"authored_plan_path: {summary.get('AUTHORED_PLAN_PATH', '')}",
        f"authored_plan_authorship: {summary.get('AUTHORED_PLAN_AUTHORSHIP', '')}",
        f"authored_plan_fallback_sections: {summary.get('AUTHORED_PLAN_FALLBACK_SECTIONS', '')}",
        f"intake_failure_class: {summary.get('INTAKE_FAILURE_CLASS', '')}",
        f"intake_failure_path: {summary.get('INTAKE_FAILURE_PATH', '')}",
        f"validation_succeeded: {summary.get('VALIDATION_SUCCEEDED', 'false')}",
        f"validation_status: {summary.get('VALIDATION_STATUS', '')}",
        f"validation_reason: {summary.get('VALIDATION_REASON', '')}",
        f"fallback_invoked: {summary.get('fallback_invoked', 'false')}",
        f"fallback_outcome: {summary.get('fallback_outcome', '')}",
        f"fallback_reason: {summary.get('fallback_reason', '')}",
        f"failure_family: {summary.get('FAILURE_FAMILY', '')}",
        f"failure_bucket: {summary.get('FAILURE_BUCKET', '')}",
        f"skip_candidate: {summary.get('SKIP_CANDIDATE', '')}",
        f"escalated_backend: {summary.get('ESCALATED_BACKEND', '')}",
        f"validation_python: {summary.get('VALIDATION_PYTHON', '')}",
        f"build_image_id: {summary.get('BUILD_IMAGE_ID', summary.get('DOCKER_IMAGE_ID', ''))}",
        f"docker_image_id: {summary.get('BUILD_IMAGE_ID', summary.get('DOCKER_IMAGE_ID', ''))}",
        f"lockfile_key: {summary.get('LOCKFILE_KEY', '')}",
        f"debug_dir: {summary.get('DEBUG_DIR', '')}",
        f"attempts_dir: {summary.get('ATTEMPTS_DIR', '')}",
        f"llm_trace_dir: {summary.get('LLM_TRACE_DIR', '')}",
        f"context_log: {summary.get('CONTEXT_LOG', '')}",
        f"iterations_dir: {summary.get('ITERATIONS_DIR', '')}",
    ]
    output_path.write_text("\n".join(content) + "\n", encoding="utf-8")
    return output_path


def main() -> int:
    args = parse_args()
    run_contract = load_run_contract(args.run_contract_json)
    snippet_path = Path(args.file).expanduser().resolve()
    snippet_dir = snippet_path.parent
    artifact_dir = Path(args.output_dir).expanduser().resolve() if args.output_dir.strip() else snippet_dir
    tool_dir = Path(__file__).resolve().parent

    command, selection_warnings = choose_command(tool_dir, args.build_profile)
    for warning in selection_warnings:
        print(f"APDR binary selection warning: {warning}", file=sys.stderr)
    command.extend(
        [
            "resolve",
            str(snippet_path),
            "--output",
            str(artifact_dir),
            "--range",
            str(args.range),
            "--max-retries",
            str(args.loop),
            "--docker-timeout",
            str(args.docker_timeout),
            "--validation-backend",
            str(args.validation_backend),
            "--llm-validation-policy",
            str(args.llm_validation_policy),
            "--llm-provider",
            "ollama",
            "--llm-model",
            str(args.model),
            "--llm-base-url",
            str(args.base),
        ]
    )
    if str(args.rag).lower() in {"true", "1", "yes", "y"} or args.validation_backend == "llm":
        command.append("--allow-llm")
    if args.no_validate:
        command.append("--no-validate")
    if args.no_execute_snippet:
        command.append("--no-execute-snippet")
    if args.no_parallel_versions:
        command.append("--no-parallel-versions")
    if args.llm_only:
        command.append("--llm-only")
    if args.force_validate:
        command.append("--force-validate")
    if args.validation_timeout and args.validation_timeout > 0:
        command.extend(["--validation-timeout", str(args.validation_timeout)])
    if args.benchmark_context_log.strip():
        command.extend(["--benchmark-context-log", args.benchmark_context_log.strip()])
    if args.run_contract_json.strip():
        command.extend(["--run-contract-json", args.run_contract_json.strip()])
    if args.benchmark_context_log.strip():
        append_context_log(
            args.benchmark_context_log,
            "apdr-command",
            "\n".join(
                [
                    f"snippet={snippet_path}",
                    f"artifact_dir={artifact_dir}",
                    f"build_profile={args.build_profile or 'standard'}",
                    f"binary_selection_warnings={json.dumps(selection_warnings)}",
                    f"command={' '.join(command)}",
                ]
            ),
        )

    completed = subprocess.run(
        command,
        cwd=tool_dir,
        capture_output=True,
        text=True,
        check=False,
    )

    if args.verbose or completed.returncode != 0:
        if completed.stdout:
            print(completed.stdout, end="")
        if completed.stderr:
            print(completed.stderr, end="", file=sys.stderr)
    if args.benchmark_context_log.strip():
        combined = []
        if completed.stdout:
            combined.append("STDOUT:\n" + completed.stdout)
        if completed.stderr:
            combined.append("STDERR:\n" + completed.stderr)
        append_context_log(
            args.benchmark_context_log,
            "apdr-cli-output",
            "\n\n".join(combined) if combined else "(no output)",
        )

    summary = parse_summary(completed.stdout)
    if completed.returncode != 0 and not summary:
        return completed.returncode

    python_version = summary.get("PYTHON_VERSION", "3.11")
    artifact_dir.mkdir(parents=True, exist_ok=True)
    output_path = write_output_file(artifact_dir, python_version, summary, args, run_contract)
    print(f"Wrote APDR output to {output_path}")
    validation_succeeded = str(summary.get("VALIDATION_SUCCEEDED", "false")).strip().lower() == "true"
    validation_status = str(summary.get("VALIDATION_STATUS", "")).strip()
    validation_reason = str(summary.get("VALIDATION_REASON", "")).strip()
    if validation_status.startswith("skipped"):
        if validation_reason:
            print(f"APDR skipped: {validation_reason}", file=sys.stderr)
        else:
            print(f"APDR skipped: {validation_status}", file=sys.stderr)
        if completed.returncode != 0 and validation_status:
            return 0
        return 0
    if not validation_succeeded:
        if validation_reason:
            print(f"APDR validation failed: {validation_reason}", file=sys.stderr)
        else:
            print("APDR validation failed", file=sys.stderr)
        return 1
    if completed.returncode != 0:
        return completed.returncode
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
