#!/usr/bin/env python3
"""Phase 14 replay runner: orchestrates cold/warm benchmark captures on a locked slice.

This script provides one replay-oriented entrypoint that can:
- Produce a cold baseline capture (no cache priming)
- Prewarm native caches then produce a warm candidate capture
- Validate wiring without a live dataset via --probe-only
- Enforce build-profile discipline and surface invalidating conditions
- Emit Phase 13 run-contract and timing schema for comparable evidence

Usage examples:

    # Probe-only mode (validate wiring, no live dataset needed):
    python3 scripts/run_phase14_replay.py \\
        --probe-only --output-json /tmp/probe.json

    # Cold baseline capture:
    python3 scripts/run_phase14_replay.py \\
        --manifest-json .planning/phases/14-.../14-macos-replay-slice.json \\
        --dataset-root hard-gists \\
        --baseline-json evidence/macos-before.json \\
        --validation-backend env --build-profile release

    # Warm candidate capture with prewarm:
    python3 scripts/run_phase14_replay.py \\
        --manifest-json .planning/phases/14-.../14-macos-replay-slice.json \\
        --dataset-root hard-gists \\
        --prewarm \\
        --candidate-json evidence/macos-after.json \\
        --validation-backend env --build-profile release
"""
from __future__ import annotations

import argparse
import json
import os
import platform
import shutil
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

# Phase 13 measurement contract reuse
from benchmark_ui.run_contract import (
    REQUIRED_RUN_CONTRACT_KEYS,
    build_run_contract,
    determine_execution_mode,
    normalize_build_profile,
    normalize_cache_state,
    normalize_context_window,
    normalize_inference_policy,
    normalize_run_intent,
)

MEASURE_SCRIPT = REPO_ROOT / "scripts" / "measure_apdr_baseline.py"


# ---------------------------------------------------------------------------
# Preflight / invalidation checks
# ---------------------------------------------------------------------------

def detect_rosetta() -> bool:
    """Return True if running under Rosetta 2 translation on macOS."""
    if sys.platform != "darwin":
        return False
    try:
        result = subprocess.run(
            ["sysctl", "-n", "sysctl.proc_translated"],
            capture_output=True, text=True, timeout=5,
        )
        return result.stdout.strip() == "1"
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return False


def detect_docker_backend_active() -> bool:
    """Return True if Docker daemon is reachable (indicates Docker overhead risk)."""
    if not shutil.which("docker"):
        return False
    try:
        result = subprocess.run(
            ["docker", "info", "--format", "{{.ServerVersion}}"],
            capture_output=True, text=True, timeout=8,
        )
        return result.returncode == 0
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return False


def detect_apdr_binary(tool_dir: Path, build_profile: str) -> tuple[Path | None, list[str]]:
    """Find the APDR binary matching the requested build profile.

    Returns (binary_path, warnings).
    """
    warnings: list[str] = []
    release = tool_dir / "target" / "release" / "apdr"
    debug = tool_dir / "target" / "debug" / "apdr"
    release_exe = tool_dir / "target" / "release" / "apdr.exe"
    debug_exe = tool_dir / "target" / "debug" / "apdr.exe"

    profile_lower = build_profile.lower()
    if profile_lower in ("release", "pgo"):
        preferred = [release, release_exe]
        fallback = [debug, debug_exe]
    elif profile_lower == "debug":
        preferred = [debug, debug_exe]
        fallback = [release, release_exe]
    else:
        # "standard" -- prefer release, accept debug
        preferred = [release, release_exe, debug, debug_exe]
        fallback = []

    for candidate in preferred:
        if candidate.exists():
            return candidate, warnings

    for candidate in fallback:
        if candidate.exists():
            warnings.append(
                f"Requested build_profile={build_profile} but only found "
                f"{candidate.relative_to(tool_dir)}; measurement may include "
                f"wrong optimization level."
            )
            return candidate, warnings

    warnings.append(
        f"No prebuilt APDR binary found for build_profile={build_profile}. "
        f"Execution will fall back to 'cargo run' which adds compilation overhead "
        f"inside the measured run."
    )
    return None, warnings


def preflight_warnings(
    validation_backend: str,
    build_profile: str,
    tool_dir: Path,
) -> list[str]:
    """Collect preflight warnings for invalidating conditions."""
    warnings: list[str] = []

    if detect_rosetta():
        warnings.append(
            "Running under Rosetta 2 translation. Measured timings will not "
            "reflect native ARM64 performance."
        )

    if validation_backend == "env" and detect_docker_backend_active():
        warnings.append(
            "Docker daemon is running while using env validation backend. "
            "Docker Desktop background activity may affect timing stability."
        )

    if validation_backend == "docker":
        warnings.append(
            "Using Docker validation backend for a replay capture. "
            "This adds container startup overhead to every case and is not "
            "recommended for the fast native macOS replay lane."
        )

    _, binary_warnings = detect_apdr_binary(tool_dir, build_profile)
    warnings.extend(binary_warnings)

    return warnings


# ---------------------------------------------------------------------------
# Prewarm
# ---------------------------------------------------------------------------

def prewarm_caches(
    manifest_path: str,
    dataset_root: str,
    fixtures_root: str,
    validation_backend: str,
    build_profile: str,
    python_command: str,
    model_name: str,
    base_url: str,
) -> dict[str, Any]:
    """Run the slice once to prime native caches. Returns prewarm metadata."""
    print("=== Prewarm pass: priming native caches ===")
    started = time.perf_counter()

    command = [
        sys.executable, str(MEASURE_SCRIPT),
        "--output-json", "/dev/null" if os.name != "nt" else "NUL",
        "--validation-backend", validation_backend,
        "--build-profile", build_profile,
        "--run-intent", "prewarm",
        "--cache-state", "cold",
        "--model-name", model_name,
        "--base-url", base_url,
        "--execute-live",
    ]
    if manifest_path:
        command.extend(["--manifest-json", manifest_path])
    if dataset_root:
        command.extend(["--dataset-root", dataset_root])
    if fixtures_root:
        command.extend(["--fixtures-root", fixtures_root])
    if python_command:
        command.extend(["--python-command", python_command])

    result = subprocess.run(
        command,
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    elapsed_ms = int((time.perf_counter() - started) * 1000)
    print(f"Prewarm completed in {elapsed_ms}ms (exit {result.returncode})")
    if result.returncode != 0:
        print(f"Prewarm stderr: {result.stderr[-500:]}", file=sys.stderr)

    return {
        "prewarm_elapsed_ms": elapsed_ms,
        "prewarm_returncode": result.returncode,
        "prewarm_completed_at": datetime.now(timezone.utc).isoformat(),
    }


# ---------------------------------------------------------------------------
# Capture
# ---------------------------------------------------------------------------

def run_capture(
    manifest_path: str,
    dataset_root: str,
    fixtures_root: str,
    output_json: str,
    output_md: str,
    validation_backend: str,
    build_profile: str,
    python_command: str,
    model_name: str,
    base_url: str,
    run_intent: str,
    cache_state: str,
    execute_live: bool,
) -> dict[str, Any]:
    """Run the measurement script and return its output path."""
    command = [
        sys.executable, str(MEASURE_SCRIPT),
        "--output-json", output_json,
        "--validation-backend", validation_backend,
        "--build-profile", build_profile,
        "--run-intent", run_intent,
        "--cache-state", cache_state,
        "--model-name", model_name,
        "--base-url", base_url,
    ]
    if manifest_path:
        command.extend(["--manifest-json", manifest_path])
    if dataset_root:
        command.extend(["--dataset-root", dataset_root])
    if fixtures_root:
        command.extend(["--fixtures-root", fixtures_root])
    if output_md:
        command.extend(["--output-md", output_md])
    if python_command:
        command.extend(["--python-command", python_command])
    if execute_live:
        command.append("--execute-live")

    started = time.perf_counter()
    result = subprocess.run(
        command,
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    elapsed_ms = int((time.perf_counter() - started) * 1000)

    return {
        "capture_elapsed_ms": elapsed_ms,
        "capture_returncode": result.returncode,
        "capture_output_json": output_json,
        "capture_output_md": output_md,
        "capture_stdout_tail": "\n".join(result.stdout.splitlines()[-5:]),
        "capture_stderr_tail": "\n".join(result.stderr.splitlines()[-5:]),
    }


# ---------------------------------------------------------------------------
# Probe-only mode
# ---------------------------------------------------------------------------

def run_probe(
    validation_backend: str,
    build_profile: str,
    output_json: str,
) -> dict[str, Any]:
    """Validate wiring without requiring a live dataset or manifest.

    Checks that the APDR binary is available, preflight conditions are met,
    and the measurement contract can be constructed.
    """
    tool_dir = REPO_ROOT / "tools" / "apdr"
    warnings = preflight_warnings(validation_backend, build_profile, tool_dir)
    binary, _ = detect_apdr_binary(tool_dir, build_profile)

    probe = {
        "probe_only": True,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "validation_backend": validation_backend,
        "build_profile": build_profile,
        "binary_found": binary is not None,
        "binary_path": str(binary) if binary else "",
        "rosetta_detected": detect_rosetta(),
        "docker_active": detect_docker_backend_active(),
        "host_architecture": platform.machine(),
        "platform": sys.platform,
        "warnings": warnings,
        "status": "pass" if not warnings else "warnings",
    }

    output_path = Path(output_json).expanduser().resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(probe, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(f"Probe result written to {output_path}")
    for warning in warnings:
        print(f"  WARNING: {warning}")
    return probe


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Phase 14 replay runner: produce cold/warm benchmark captures on "
            "a locked replay slice using the Phase 13 measurement contract."
        ),
    )
    # Slice inputs
    parser.add_argument(
        "--manifest-json", default="",
        help="Path to the replay-slice manifest JSON.",
    )
    parser.add_argument(
        "--dataset-root", default="",
        help="Root directory of the benchmark dataset (e.g. hard-gists).",
    )
    parser.add_argument(
        "--fixtures-root", default="",
        help="Root directory of deterministic fixture snippets.",
    )

    # Capture outputs
    parser.add_argument(
        "--baseline-json", default="",
        help="Output path for the baseline (cold) capture JSON.",
    )
    parser.add_argument(
        "--candidate-json", default="",
        help="Output path for the candidate (warm) capture JSON.",
    )
    parser.add_argument(
        "--output-json", default="",
        help="Generic output JSON path (used when neither baseline nor candidate is specified).",
    )
    parser.add_argument(
        "--output-md", default="",
        help="Optional Markdown summary output path.",
    )

    # Replay options
    parser.add_argument(
        "--prewarm", action="store_true",
        help="Run a prewarm pass to prime native caches before the candidate capture.",
    )
    parser.add_argument(
        "--probe-only", action="store_true",
        help="Validate wiring and preflight without a live dataset.",
    )
    parser.add_argument(
        "--execute-live", action="store_true",
        help="Execute APDR live instead of using fixture-backed placeholders.",
    )

    # Configuration
    parser.add_argument(
        "--validation-backend", choices=("env", "docker", "llm"),
        default="env",
        help="Validation backend for the replay capture.",
    )
    parser.add_argument(
        "--build-profile", default="release",
        help="Build profile for APDR binary selection (release, debug, pgo, standard).",
    )
    parser.add_argument(
        "--python-command", default="",
        help="Python launcher for tools/apdr/test_executor.py.",
    )
    parser.add_argument(
        "--model-name", default="qwen3.5:9b",
        help="Model name recorded in the run contract.",
    )
    parser.add_argument(
        "--base-url", default="http://localhost:11434",
        help="Base URL recorded in the run contract.",
    )
    parser.add_argument(
        "--workers", type=int, default=1,
        help="Number of parallel workers (default 1 for stable replay timing).",
    )

    return parser.parse_args()


def main() -> int:
    args = parse_args()

    # Probe-only mode
    if args.probe_only:
        output = args.output_json or args.baseline_json or args.candidate_json
        if not output:
            output = "/tmp/phase14-probe.json"
        probe = run_probe(args.validation_backend, args.build_profile, output)
        return 0 if probe["status"] != "fail" else 1

    # Validate inputs
    if not args.manifest_json and not args.dataset_root and not args.fixtures_root:
        print(
            "Error: provide at least one of --manifest-json, --dataset-root, "
            "or --fixtures-root.",
            file=sys.stderr,
        )
        return 1

    if not args.baseline_json and not args.candidate_json and not args.output_json:
        print(
            "Error: provide at least one of --baseline-json, --candidate-json, "
            "or --output-json.",
            file=sys.stderr,
        )
        return 1

    # Preflight
    tool_dir = REPO_ROOT / "tools" / "apdr"
    warnings = preflight_warnings(args.validation_backend, args.build_profile, tool_dir)
    if warnings:
        print("=== Preflight warnings ===")
        for warning in warnings:
            print(f"  WARNING: {warning}")
        print()

    captures: list[dict[str, Any]] = []

    # Baseline capture (cold)
    if args.baseline_json:
        print("=== Baseline capture (cold) ===")
        baseline_result = run_capture(
            manifest_path=args.manifest_json,
            dataset_root=args.dataset_root,
            fixtures_root=args.fixtures_root,
            output_json=args.baseline_json,
            output_md="",
            validation_backend=args.validation_backend,
            build_profile=args.build_profile,
            python_command=args.python_command,
            model_name=args.model_name,
            base_url=args.base_url,
            run_intent="baseline",
            cache_state="cold",
            execute_live=args.execute_live,
        )
        captures.append({"role": "baseline", **baseline_result})
        print(f"Baseline capture: exit {baseline_result['capture_returncode']}, "
              f"{baseline_result['capture_elapsed_ms']}ms")

    # Prewarm pass (if requested)
    prewarm_meta: dict[str, Any] = {}
    if args.prewarm:
        prewarm_meta = prewarm_caches(
            manifest_path=args.manifest_json,
            dataset_root=args.dataset_root,
            fixtures_root=args.fixtures_root,
            validation_backend=args.validation_backend,
            build_profile=args.build_profile,
            python_command=args.python_command,
            model_name=args.model_name,
            base_url=args.base_url,
        )

    # Candidate capture (warm)
    if args.candidate_json:
        print("=== Candidate capture (warm) ===")
        candidate_result = run_capture(
            manifest_path=args.manifest_json,
            dataset_root=args.dataset_root,
            fixtures_root=args.fixtures_root,
            output_json=args.candidate_json,
            output_md=args.output_md,
            validation_backend=args.validation_backend,
            build_profile=args.build_profile,
            python_command=args.python_command,
            model_name=args.model_name,
            base_url=args.base_url,
            run_intent="candidate",
            cache_state="warm" if args.prewarm else "unknown",
            execute_live=args.execute_live,
        )
        captures.append({"role": "candidate", **candidate_result})
        print(f"Candidate capture: exit {candidate_result['capture_returncode']}, "
              f"{candidate_result['capture_elapsed_ms']}ms")

    # Generic output (when neither baseline nor candidate is specified)
    if args.output_json and not args.baseline_json and not args.candidate_json:
        print("=== Capture ===")
        generic_result = run_capture(
            manifest_path=args.manifest_json,
            dataset_root=args.dataset_root,
            fixtures_root=args.fixtures_root,
            output_json=args.output_json,
            output_md=args.output_md,
            validation_backend=args.validation_backend,
            build_profile=args.build_profile,
            python_command=args.python_command,
            model_name=args.model_name,
            base_url=args.base_url,
            run_intent="baseline",
            cache_state="unknown",
            execute_live=args.execute_live,
        )
        captures.append({"role": "capture", **generic_result})

    # Summary
    print()
    print("=== Phase 14 Replay Summary ===")
    print(f"Validation backend: {args.validation_backend}")
    print(f"Build profile:      {args.build_profile}")
    print(f"Manifest:           {args.manifest_json or '(none)'}")
    print(f"Prewarm:            {'yes' if args.prewarm else 'no'}")
    if prewarm_meta:
        print(f"Prewarm time:       {prewarm_meta.get('prewarm_elapsed_ms', 0)}ms")
    for capture in captures:
        print(f"  {capture['role']}: {capture['capture_output_json']} "
              f"(exit {capture['capture_returncode']}, "
              f"{capture['capture_elapsed_ms']}ms)")
    if warnings:
        print(f"Warnings:           {len(warnings)}")

    # Return non-zero if any capture failed
    failed = any(c["capture_returncode"] != 0 for c in captures)
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
