#!/usr/bin/env python3
from __future__ import annotations

import argparse
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
    parser.add_argument("-m", "--model", default="phi3:medium", help="LLM model used when APDR's LLM fallback is enabled")
    parser.add_argument("-t", "--temp", default="0.7", help="Compatibility flag retained for benchmark parity")
    parser.add_argument("-l", "--loop", type=int, default=5, help="Maximum APDR recovery retries")
    parser.add_argument("-r", "--range", type=int, default=1, help="Python version search range")
    parser.add_argument("-ra", "--rag", default="true", help="Enable APDR's optional LLM-assisted resolution tier")
    parser.add_argument("--docker-timeout", type=int, default=300, help="Validation install/import timeout in seconds")
    parser.add_argument("--no-validate", action="store_true", help="Skip APDR validation")
    parser.add_argument("--no-execute-snippet", action="store_true", help="Only import resolved packages in smoke tests")
    parser.add_argument("--no-parallel-versions", action="store_true", help="Validate only the selected Python version")
    parser.add_argument("--benchmark-context-log", default="", help="Append benchmark build/run/LLM trace to this file")
    parser.add_argument("-v", "--verbose", action="store_true", help="Print the raw CLI output")
    return parser.parse_args()


def choose_command(tool_dir: Path) -> list[str]:
    release_binary = tool_dir / "target" / "release" / "apdr"
    debug_binary = tool_dir / "target" / "debug" / "apdr"
    release_binary_windows = tool_dir / "target" / "release" / "apdr.exe"
    debug_binary_windows = tool_dir / "target" / "debug" / "apdr.exe"
    source_mtime = newest_source_mtime(tool_dir)
    candidates = [
        path
        for path in (debug_binary_windows, release_binary_windows, debug_binary, release_binary)
        if path.exists()
    ]
    if candidates:
        freshest = max(candidates, key=lambda path: path.stat().st_mtime)
        if freshest.stat().st_mtime >= source_mtime:
            return [str(freshest)]
    return ["cargo", "run", "--quiet", "--"]


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


def write_output_file(snippet_dir: Path, python_version: str, summary: dict[str, str], args: argparse.Namespace) -> Path:
    output_path = snippet_dir / f"output_data_{python_version}.yml"
    content = [
        "---",
        f"python_version: {python_version}",
        "tool: apdr",
        f"model: {args.model}",
        f"base_url: {args.base}",
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
        f"solve_duration_ms: {summary.get('SOLVE_DURATION_MS', '0')}",
        f"validation_duration_ms: {summary.get('VALIDATION_DURATION_MS', '0')}",
        f"env_create_duration_ms: {summary.get('ENV_CREATE_DURATION_MS', '0')}",
        f"install_duration_ms: {summary.get('INSTALL_DURATION_MS', '0')}",
        f"smoke_duration_ms: {summary.get('SMOKE_DURATION_MS', '0')}",
        f"validation_backend: {summary.get('VALIDATION_BACKEND', '')}",
        f"validation_succeeded: {summary.get('VALIDATION_SUCCEEDED', 'false')}",
        f"validation_status: {summary.get('VALIDATION_STATUS', '')}",
        f"validation_reason: {summary.get('VALIDATION_REASON', '')}",
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
    snippet_path = Path(args.file).expanduser().resolve()
    snippet_dir = snippet_path.parent
    artifact_dir = Path(args.output_dir).expanduser().resolve() if args.output_dir.strip() else snippet_dir
    tool_dir = Path(__file__).resolve().parent

    command = choose_command(tool_dir)
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
            "--llm-provider",
            "ollama",
            "--llm-model",
            str(args.model),
            "--llm-base-url",
            str(args.base),
        ]
    )
    if str(args.rag).lower() in {"true", "1", "yes", "y"}:
        command.append("--allow-llm")
    if args.no_validate:
        command.append("--no-validate")
    if args.no_execute_snippet:
        command.append("--no-execute-snippet")
    if args.no_parallel_versions:
        command.append("--no-parallel-versions")
    if args.benchmark_context_log.strip():
        command.extend(["--benchmark-context-log", args.benchmark_context_log.strip()])
        append_context_log(
            args.benchmark_context_log,
            "apdr-command",
            "\n".join(
                [
                    f"snippet={snippet_path}",
                    f"artifact_dir={artifact_dir}",
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
    output_path = write_output_file(artifact_dir, python_version, summary, args)
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
