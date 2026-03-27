#!/usr/bin/env python3
from __future__ import annotations

import argparse
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


REPO_ROOT = Path(__file__).resolve().parents[1]
TEST_EXECUTOR = REPO_ROOT / "tools" / "apdr" / "test_executor.py"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run a bounded APDR sample and write a machine-readable baseline."
    )
    parser.add_argument("--fixtures-root", default="", help="Root of deterministic fixture snippets.")
    parser.add_argument("--dataset-root", default="", help="Root of dataset snippets, such as hard-gists.")
    parser.add_argument("--limit", type=int, default=10, help="Maximum number of snippets to run.")
    parser.add_argument(
        "--validation-backend",
        choices=("env", "docker", "llm"),
        default="env",
        help="Validation backend forwarded to tools/apdr/test_executor.py.",
    )
    parser.add_argument("--output-json", required=True, help="Path for the aggregate JSON artifact.")
    parser.add_argument("--output-md", default="", help="Optional Markdown summary output.")
    parser.add_argument("--context-log", default="", help="Optional APDR benchmark context log path.")
    parser.add_argument(
        "--python-command",
        default=sys.executable,
        help="Python launcher used to invoke tools/apdr/test_executor.py.",
    )
    parser.add_argument(
        "--force-validate",
        action="store_true",
        help="Force validation even when APDR has cached/pre-solved results.",
    )
    args = parser.parse_args()
    if not args.fixtures_root and not args.dataset_root:
        parser.error("Provide at least one of --fixtures-root or --dataset-root.")
    if args.limit == 0:
        parser.error("--limit must be non-zero.")
    return args


def parse_python_command(value: str) -> list[str]:
    text = str(value or "").strip()
    if not text:
        return [sys.executable]
    return shlex.split(text, posix=os.name != "nt")


def shell_join(parts: list[str]) -> str:
    if os.name == "nt":
        return subprocess.list2cmdline(parts)
    return shlex.join(parts)


def normalize_path_text(value: str) -> str:
    text = str(value or "").strip()
    return str(Path(text).expanduser()) if text else ""


def discover_fixture_snippets(root: Path) -> list[Path]:
    return sorted(
        (path for path in root.rglob("*.py") if path.is_file()),
        key=lambda path: str(path.relative_to(root)).replace("\\", "/"),
    )


def discover_dataset_snippets(root: Path) -> list[Path]:
    preferred = sorted(
        (path for path in root.rglob("snippet.py") if path.is_file()),
        key=lambda path: str(path.relative_to(root)).replace("\\", "/"),
    )
    if preferred:
        return preferred
    return sorted(
        (path for path in root.rglob("*.py") if path.is_file()),
        key=lambda path: str(path.relative_to(root)).replace("\\", "/"),
    )


def collect_snippets(args: argparse.Namespace) -> list[dict[str, Any]]:
    ordered: list[dict[str, Any]] = []
    seen: set[Path] = set()

    if args.fixtures_root:
        root = Path(args.fixtures_root).expanduser().resolve()
        if not root.exists():
            raise FileNotFoundError(f"fixtures root does not exist: {root}")
        for path in discover_fixture_snippets(root):
            resolved = path.resolve()
            if resolved in seen:
                continue
            seen.add(resolved)
            ordered.append(
                {
                    "source": "fixtures",
                    "root": str(root),
                    "snippet": resolved,
                    "relative_path": str(resolved.relative_to(root)).replace("\\", "/"),
                }
            )

    if args.dataset_root:
        root = Path(args.dataset_root).expanduser().resolve()
        if not root.exists():
            raise FileNotFoundError(f"dataset root does not exist: {root}")
        for path in discover_dataset_snippets(root):
            resolved = path.resolve()
            if resolved in seen:
                continue
            seen.add(resolved)
            ordered.append(
                {
                    "source": "dataset",
                    "root": str(root),
                    "snippet": resolved,
                    "relative_path": str(resolved.relative_to(root)).replace("\\", "/"),
                }
            )

    if not ordered:
        raise FileNotFoundError("No snippets found under the provided roots.")
    limit = abs(int(args.limit))
    return ordered[:limit]


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


def is_skipped(metadata: dict[str, str]) -> bool:
    status = str(metadata.get("validation_status") or "").strip().lower()
    return status.startswith("skipped") or status == "host-runtime-required"


def classify_status(returncode: int, metadata: dict[str, str], output_exists: bool) -> str:
    if is_skipped(metadata):
        return "skipped"
    if output_exists and returncode == 0 and parse_bool(metadata.get("validation_succeeded")):
        return "passed"
    return "failed"


def load_requirements(path: Path) -> list[str]:
    if not path.exists():
        return []
    try:
        return [
            line.strip()
            for line in path.read_text(encoding="utf-8").splitlines()
            if line.strip() and not line.lstrip().startswith("#")
        ]
    except OSError:
        return []


def find_latest_output(case_output_dir: Path) -> Path | None:
    outputs = sorted(
        case_output_dir.glob("output_data_*.yml"),
        key=lambda item: item.stat().st_mtime,
        reverse=True,
    )
    return outputs[0] if outputs else None


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def render_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# Pre-optimization baseline",
        "",
        f"Created: {report['created_at']}",
        f"Validation backend: `{report['validation_backend']}`",
        f"Sample count: {report['sample_count']}",
        f"Pass rate: {report['pass_rate_percent']:.2f}%",
        f"Validation duration: {report['validation_duration_ms']} ms total",
        f"Solve duration: {report['solve_duration_ms']} ms total",
        f"Env create duration: {report['env_create_duration_ms']} ms total",
        f"Install duration: {report['install_duration_ms']} ms total",
        f"Smoke duration: {report['smoke_duration_ms']} ms total",
        "Peak memory: See the companion memory profile artifact for this phase.",
        "",
        "## Command",
        "",
        "```text",
        report["command"],
        "```",
        "",
        "## Sample Rule",
        "",
        f"- Deterministic lexicographic ordering across provided roots",
        f"- Limit: {report['sample_count']} case(s)",
        "",
        "## Totals",
        "",
        f"- Passed: {report['passed']}",
        f"- Failed: {report['failed']}",
        f"- Skipped: {report['skipped']}",
        "",
        "## Samples",
        "",
        "| # | Snippet | Source | Status | Python | Solve ms | Validate ms |",
        "|---|---------|--------|--------|--------|----------|-------------|",
    ]
    for sample in report["samples"]:
        lines.append(
            "| {index} | `{snippet}` | {source} | {status} | {python_version} | {solve_duration_ms} | {validation_duration_ms} |".format(
                index=sample["index"],
                snippet=sample["relative_path"],
                source=sample["source"],
                status=sample["status"].upper(),
                python_version=sample["python_version"] or "--",
                solve_duration_ms=sample["solve_duration_ms"],
                validation_duration_ms=sample["validation_duration_ms"],
            )
        )
    lines.extend(
        [
            "",
            "## Per-sample Commands",
            "",
        ]
    )
    for sample in report["samples"]:
        lines.extend(
            [
                f"### {sample['index']}. {sample['relative_path']}",
                "",
                "```text",
                sample["command"],
                "```",
                "",
            ]
        )
    return "\n".join(lines).rstrip() + "\n"


def main() -> int:
    args = parse_args()
    snippets = collect_snippets(args)
    output_json = Path(args.output_json).expanduser().resolve()
    output_md = Path(args.output_md).expanduser().resolve() if args.output_md else None
    sample_root = output_json.parent / ".baseline-runs"
    python_command = parse_python_command(args.python_command)
    started_at = time.perf_counter()
    cases: list[dict[str, Any]] = []

    for index, sample in enumerate(snippets, start=1):
        case_output_dir = sample_root / f"{index:02d}-{sample['snippet'].stem}"
        if case_output_dir.exists():
            shutil.rmtree(case_output_dir)
        case_output_dir.mkdir(parents=True, exist_ok=True)

        command = python_command + [
            str(TEST_EXECUTOR),
            "-f",
            str(sample["snippet"]),
            "--output-dir",
            str(case_output_dir),
            "--validation-backend",
            str(args.validation_backend),
        ]
        if args.context_log:
            command.extend(["--benchmark-context-log", args.context_log])
        if args.force_validate:
            command.append("--force-validate")

        command_display = shell_join(command)
        run_started = time.perf_counter()
        completed = subprocess.run(
            command,
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            check=False,
        )
        wall_duration_ms = int((time.perf_counter() - run_started) * 1000)

        output_path = find_latest_output(case_output_dir)
        metadata = parse_simple_yaml(output_path) if output_path else {}
        requirements_path = Path(normalize_path_text(metadata.get("requirements_path"))) if metadata.get("requirements_path") else case_output_dir / "requirements.txt"
        report_path = Path(normalize_path_text(metadata.get("report_path"))) if metadata.get("report_path") else case_output_dir / "resolution-report.txt"
        status = classify_status(completed.returncode, metadata, output_path is not None)
        cases.append(
            {
                "index": index,
                "source": sample["source"],
                "root": sample["root"],
                "snippet": str(sample["snippet"]),
                "relative_path": sample["relative_path"],
                "command": command_display,
                "output_dir": str(case_output_dir),
                "output_file": str(output_path) if output_path else "",
                "python_version": output_path.stem[len("output_data_") :] if output_path else "",
                "returncode": completed.returncode,
                "status": status,
                "validation_status": str(metadata.get("validation_status") or "").strip(),
                "validation_reason": str(metadata.get("validation_reason") or "").strip(),
                "validation_succeeded": parse_bool(metadata.get("validation_succeeded")),
                "solve_duration_ms": parse_int(metadata.get("solve_duration_ms")),
                "validation_duration_ms": parse_int(metadata.get("validation_duration_ms")),
                "env_create_duration_ms": parse_int(metadata.get("env_create_duration_ms")),
                "install_duration_ms": parse_int(metadata.get("install_duration_ms")),
                "smoke_duration_ms": parse_int(metadata.get("smoke_duration_ms")),
                "llm_calls": parse_int(metadata.get("llm_calls")),
                "env_builds": parse_int(metadata.get("env_builds")),
                "retries": parse_int(metadata.get("retries")),
                "wall_duration_ms": wall_duration_ms,
                "requirements_path": str(requirements_path),
                "report_path": str(report_path),
                "requirements": load_requirements(requirements_path),
                "stdout_tail": "\n".join(completed.stdout.splitlines()[-10:]),
                "stderr_tail": "\n".join(completed.stderr.splitlines()[-10:]),
            }
        )

    passed = sum(1 for case in cases if case["status"] == "passed")
    failed = sum(1 for case in cases if case["status"] == "failed")
    skipped = sum(1 for case in cases if case["status"] == "skipped")
    sample_count = len(cases)
    report = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "repo_root": str(REPO_ROOT),
        "command": shell_join(sys.argv),
        "fixtures_root": str(Path(args.fixtures_root).expanduser().resolve()) if args.fixtures_root else "",
        "dataset_root": str(Path(args.dataset_root).expanduser().resolve()) if args.dataset_root else "",
        "validation_backend": args.validation_backend,
        "context_log": args.context_log,
        "sample_count": sample_count,
        "passed": passed,
        "failed": failed,
        "skipped": skipped,
        "pass_rate": (passed / sample_count) if sample_count else 0.0,
        "pass_rate_percent": round((passed / sample_count) * 100.0, 2) if sample_count else 0.0,
        "solve_duration_ms": sum(case["solve_duration_ms"] for case in cases),
        "validation_duration_ms": sum(case["validation_duration_ms"] for case in cases),
        "env_create_duration_ms": sum(case["env_create_duration_ms"] for case in cases),
        "install_duration_ms": sum(case["install_duration_ms"] for case in cases),
        "smoke_duration_ms": sum(case["smoke_duration_ms"] for case in cases),
        "wall_duration_ms": int((time.perf_counter() - started_at) * 1000),
        "llm_calls": sum(case["llm_calls"] for case in cases),
        "env_builds": sum(case["env_builds"] for case in cases),
        "retries": sum(case["retries"] for case in cases),
        "samples": cases,
    }

    write_json(output_json, report)
    if output_md is not None:
        output_md.parent.mkdir(parents=True, exist_ok=True)
        output_md.write_text(render_markdown(report), encoding="utf-8")
    print(f"Wrote baseline aggregate to {output_json}")
    if output_md is not None:
        print(f"Wrote baseline markdown to {output_md}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
