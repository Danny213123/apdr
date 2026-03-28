#!/usr/bin/env python3
from __future__ import annotations

import argparse
import importlib.util
import json
import os
import shlex
import shutil
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    import resource
except ImportError:  # pragma: no cover - only hit on platforms without resource
    resource = None


REPO_ROOT = Path(__file__).resolve().parents[1]
TEST_EXECUTOR = REPO_ROOT / "tools" / "apdr" / "test_executor.py"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Capture memory for a representative APDR resolve run."
    )
    parser.add_argument("--snippet", required=True, help="Snippet file to profile.")
    parser.add_argument("--output-json", required=True, help="Path for the memory profile JSON artifact.")
    parser.add_argument(
        "--validation-backend",
        choices=("env", "docker", "llm"),
        default="env",
        help="Validation backend forwarded to the APDR resolve command.",
    )
    parser.add_argument(
        "--python-command",
        default=sys.executable,
        help="Retained for backward compatibility with older wrapper-driven captures.",
    )
    parser.add_argument(
        "--test-executor",
        default=str(TEST_EXECUTOR),
        help="Path to the test_executor.py wrapper for the APDR checkout being profiled.",
    )
    parser.add_argument(
        "--apdr-command",
        default="",
        help="Optional direct APDR command override, for example 'tools/apdr/target/debug/apdr.exe'.",
    )
    parser.add_argument("--context-log", default="", help="Optional APDR benchmark context log path.")
    parser.add_argument(
        "--no-validate",
        action="store_true",
        help="Skip validation to isolate resolver-side memory when needed.",
    )
    parser.add_argument(
        "--force-validate",
        action="store_true",
        help="Force validation even when APDR has cached/pre-solved results.",
    )
    return parser.parse_args()


def parse_python_command(value: str) -> list[str]:
    text = str(value or "").strip()
    if not text:
        return [sys.executable]
    return shlex.split(text, posix=os.name != "nt")


def shell_join(parts: list[str]) -> str:
    if os.name == "nt":
        return subprocess.list2cmdline(parts)
    return shlex.join(parts)


def parse_simple_yaml(path: Path) -> dict[str, str]:
    metadata: dict[str, str] = {}
    if not path.exists():
        return metadata
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line == "---" or ":" not in line:
            continue
        key, value = line.split(":", 1)
        metadata[key.strip()] = value.strip()
    return metadata


def parse_bool(value: Any) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def parse_int(value: Any) -> int:
    text = str(value or "").strip()
    if not text:
        return 0
    try:
        return int(float(text))
    except (TypeError, ValueError):
        return 0


def normalize_path_text(value: str) -> str:
    text = str(value or "").strip()
    return str(Path(text).expanduser()) if text else ""


def is_skipped(metadata: dict[str, str]) -> bool:
    status = str(metadata.get("validation_status") or "").strip().lower()
    return status.startswith("skipped") or status == "host-runtime-required"


def classify_status(returncode: int, metadata: dict[str, str], output_exists: bool) -> str:
    if is_skipped(metadata):
        return "skipped"
    if output_exists and returncode == 0 and parse_bool(metadata.get("validation_succeeded")):
        return "passed"
    return "failed"


def latest_output(case_output_dir: Path) -> Path | None:
    outputs = sorted(
        case_output_dir.glob("output_data_*.yml"),
        key=lambda item: item.stat().st_mtime,
        reverse=True,
    )
    return outputs[0] if outputs else None


def query_process_memory_windows(pid: int) -> tuple[int, int]:
    command = [
        "powershell.exe",
        "-NoProfile",
        "-Command",
        (
            f"$p = Get-Process -Id {pid} -ErrorAction SilentlyContinue; "
            "if ($p) { "
            'Write-Output "$($p.PeakWorkingSet64),$($p.PrivateMemorySize64)" '
            "}"
        ),
    ]
    completed = subprocess.run(
        command,
        capture_output=True,
        text=True,
        check=False,
    )
    parts = str(completed.stdout or "").strip().split(",", 1)
    if len(parts) != 2:
        return 0, 0
    return parse_int(parts[0]), parse_int(parts[1])


def normalize_ru_maxrss(ru_maxrss: int) -> int:
    if ru_maxrss <= 0:
        return 0
    if sys.platform == "darwin":
        return int(ru_maxrss)
    return int(ru_maxrss) * 1024


def load_test_executor_module(path: Path) -> Any:
    spec = importlib.util.spec_from_file_location(
        f"apdr_test_executor_{abs(hash(str(path)))}",
        path,
    )
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not load test executor from {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def main() -> int:
    args = parse_args()
    snippet = Path(args.snippet).expanduser().resolve()
    output_json = Path(args.output_json).expanduser().resolve()
    output_json.parent.mkdir(parents=True, exist_ok=True)
    test_executor = Path(args.test_executor).expanduser().resolve()
    test_executor_module = load_test_executor_module(test_executor)
    tool_dir = test_executor.parent
    case_output_dir = output_json.parent / ".memory-profile-run"
    if case_output_dir.exists():
        shutil.rmtree(case_output_dir)
    case_output_dir.mkdir(parents=True, exist_ok=True)

    command = (
        parse_python_command(args.apdr_command)
        if str(args.apdr_command).strip()
        else test_executor_module.choose_command(tool_dir)
    )
    command.extend(
        [
            "resolve",
            str(snippet),
            "--output",
            str(case_output_dir),
            "--range",
            "1",
            "--max-retries",
            "5",
            "--docker-timeout",
            "900",
            "--validation-backend",
            str(args.validation_backend),
            "--llm-provider",
            "ollama",
            "--llm-model",
            "qwen3.5:9b",
            "--llm-base-url",
            "http://localhost:11434",
        ]
    )
    if args.context_log:
        command.extend(["--benchmark-context-log", args.context_log])
    if args.no_validate:
        command.append("--no-validate")
    if args.force_validate:
        command.append("--force-validate")

    command_display = shell_join(command)
    started = time.perf_counter()
    process = subprocess.Popen(
        command,
        cwd=tool_dir,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    peak_rss_bytes = 0
    peak_private_bytes = 0
    if os.name == "nt":
        while process.poll() is None:
            rss_bytes, private_bytes = query_process_memory_windows(process.pid)
            peak_rss_bytes = max(peak_rss_bytes, rss_bytes)
            peak_private_bytes = max(peak_private_bytes, private_bytes)
            time.sleep(0.1)
        rss_bytes, private_bytes = query_process_memory_windows(process.pid)
        peak_rss_bytes = max(peak_rss_bytes, rss_bytes)
        peak_private_bytes = max(peak_private_bytes, private_bytes)

    stdout, stderr = process.communicate()
    duration_ms = int((time.perf_counter() - started) * 1000)

    if os.name != "nt":
        if resource is None:
            raise RuntimeError("resource module is unavailable on this platform")
        peak_rss_bytes = normalize_ru_maxrss(
            resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss
        )

    summary = test_executor_module.parse_summary(stdout)
    output_path = None
    metadata: dict[str, str] = {}
    if summary:
        fake_args = argparse.Namespace(
            base="http://localhost:11434",
            model="qwen3.5:9b",
            temp="0.7",
            loop=5,
            range=1,
            rag="true" if args.validation_backend == "llm" else "false",
        )
        output_path = test_executor_module.write_output_file(
            case_output_dir,
            summary.get("PYTHON_VERSION", "3.11"),
            summary,
            fake_args,
        )
        metadata = parse_simple_yaml(output_path)
    requirements_path = Path(normalize_path_text(metadata.get("requirements_path"))) if metadata.get("requirements_path") else case_output_dir / "requirements.txt"
    report_path = Path(normalize_path_text(metadata.get("report_path"))) if metadata.get("report_path") else case_output_dir / "resolution-report.txt"

    payload = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "snippet": str(snippet),
        "command": command_display,
        "test_executor": str(test_executor),
        "validation_backend": args.validation_backend,
        "duration_ms": duration_ms,
        "peak_rss_bytes": peak_rss_bytes,
        "peak_private_bytes": peak_private_bytes,
        "status": classify_status(process.returncode, metadata, output_path is not None),
        "validation_succeeded": parse_bool(metadata.get("validation_succeeded")),
        "validation_status": str(metadata.get("validation_status") or "").strip(),
        "validation_reason": str(metadata.get("validation_reason") or "").strip(),
        "python_version": output_path.stem[len("output_data_") :] if output_path else "",
        "returncode": process.returncode,
        "solve_duration_ms": parse_int(metadata.get("solve_duration_ms")),
        "validation_duration_ms": parse_int(metadata.get("validation_duration_ms")),
        "env_create_duration_ms": parse_int(metadata.get("env_create_duration_ms")),
        "install_duration_ms": parse_int(metadata.get("install_duration_ms")),
        "smoke_duration_ms": parse_int(metadata.get("smoke_duration_ms")),
        "llm_calls": parse_int(metadata.get("llm_calls")),
        "env_builds": parse_int(metadata.get("env_builds")),
        "retries": parse_int(metadata.get("retries")),
        "output_dir": str(case_output_dir),
        "output_file": str(output_path) if output_path else "",
        "requirements_path": str(requirements_path),
        "report_path": str(report_path),
        "stdout_tail": "\n".join(stdout.splitlines()[-10:]),
        "stderr_tail": "\n".join(stderr.splitlines()[-10:]),
    }
    output_json.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(f"Wrote memory profile to {output_json}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
