#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import re
import shlex
import shutil
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from benchmark_ui.run_contract import (
    REQUIRED_RUN_CONTRACT_KEYS,
    build_run_contract,
    contract_from_sources,
    determine_execution_mode,
    normalize_build_profile,
    normalize_cache_state,
    normalize_context_window,
    normalize_inference_policy,
    normalize_run_intent,
)
TEST_EXECUTOR = REPO_ROOT / "tools" / "apdr" / "test_executor.py"
REPORT_SECTIONS = {
    "resolved_dependencies",
    "config_dependencies",
    "unresolved",
    "notes",
    "validation_attempts",
}
ATTEMPT_FIELD_RE = re.compile(r'([A-Za-z_]+)=(".*?"|\S+)')
TIMING_KEYS = (
    "solve_duration_ms",
    "validation_duration_ms",
    "llm_duration_ms",
    "env_create_duration_ms",
    "install_duration_ms",
    "docker_startup_duration_ms",
    "smoke_duration_ms",
)
RUN_CONTRACT_FLAT_KEYS = (
    "run_contract_version",
    "model_name",
    "base_url",
    "run_intent",
    "execution_mode",
    "cache_state",
    "host_architecture",
    "apdr_binary_architecture",
    "python_architecture",
    "llm_context_window",
    "inference_policy",
    "build_profile",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate a Phase 13 measurement report from APDR fixture artifacts or live samples."
    )
    parser.add_argument("--fixtures-root", default="", help="Root of deterministic fixture snippets.")
    parser.add_argument("--dataset-root", default="", help="Root of dataset snippets, such as hard-gists.")
    parser.add_argument("--limit", type=int, default=10, help="Maximum number of snippets to include.")
    parser.add_argument(
        "--manifest-json",
        default="",
        help="Path to a replay-slice manifest JSON that defines exact case membership and order.",
    )
    parser.add_argument(
        "--validation-backend",
        choices=("env", "docker", "llm"),
        default="env",
        help="Validation backend used for execution-mode labeling and optional live runs.",
    )
    parser.add_argument("--output-json", required=True, help="Path for the aggregate JSON artifact.")
    parser.add_argument("--output-md", default="", help="Optional Markdown summary output.")
    parser.add_argument("--context-log", default="", help="Optional APDR benchmark context log path.")
    parser.add_argument(
        "--python-command",
        default=sys.executable,
        help="Python launcher used to invoke tools/apdr/test_executor.py for live runs.",
    )
    parser.add_argument(
        "--force-validate",
        action="store_true",
        help="Force validation even when APDR has cached/pre-solved results.",
    )
    parser.add_argument(
        "--execute-live",
        action="store_true",
        help="Execute tools/apdr/test_executor.py instead of using fixture-backed placeholders.",
    )
    parser.add_argument("--model-name", default="qwen3.5:9b", help="Model name recorded in the run contract.")
    parser.add_argument("--base-url", default="http://localhost:11434", help="Base URL recorded in the run contract.")
    parser.add_argument("--run-intent", default="baseline", help="Run intent recorded in the run contract.")
    parser.add_argument("--cache-state", default="unknown", help="Fallback cache state for the report contract.")
    parser.add_argument("--build-profile", default="standard", help="Build profile recorded in the run contract.")
    parser.add_argument(
        "--llm-context-window",
        default="",
        help="Context window recorded in the run contract. Defaults to benchmark_ui normalization.",
    )
    parser.add_argument(
        "--inference-policy",
        default="",
        help="Inference policy recorded in the run contract. Defaults to benchmark_ui normalization.",
    )
    args = parser.parse_args()
    if not args.fixtures_root and not args.dataset_root and not args.manifest_json:
        parser.error("Provide at least one of --fixtures-root, --dataset-root, or --manifest-json.")
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
    if text in {"", "--", "none", "None"}:
        return ""
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


def load_manifest_json(manifest_path: str) -> dict[str, Any]:
    """Load and validate a replay-slice manifest JSON file."""
    path = Path(manifest_path).expanduser().resolve()
    if not path.exists():
        raise FileNotFoundError(f"Manifest JSON not found: {path}")
    with path.open("r", encoding="utf-8") as fh:
        data = json.load(fh)
    if not isinstance(data, dict):
        raise ValueError(f"Manifest must be a JSON object: {path}")
    if not data.get("slice_id"):
        raise ValueError(f"Manifest is missing 'slice_id': {path}")
    cases = data.get("cases")
    if not isinstance(cases, list) or not cases:
        raise ValueError(f"Manifest must contain a non-empty 'cases' array: {path}")
    return data


def collect_snippets_from_manifest(
    manifest: dict[str, Any],
    fixtures_root: Path | None,
    dataset_root: Path | None,
) -> list[dict[str, Any]]:
    """Resolve manifest case entries to snippet records in manifest order.

    Searches fixtures_root first, then dataset_root. Raises ValueError if
    any manifest case is not found in either root.
    """
    ordered: list[dict[str, Any]] = []
    missing: list[str] = []

    for entry in manifest["cases"]:
        rel_path = str(entry["relative_path"])
        found = False

        # Try fixtures root first
        if fixtures_root is not None:
            candidate = fixtures_root / rel_path
            if candidate.exists() and candidate.is_file():
                ordered.append({
                    "source": "fixtures",
                    "root": str(fixtures_root),
                    "snippet": candidate.resolve(),
                    "relative_path": rel_path,
                })
                found = True

        # Try dataset root
        if not found and dataset_root is not None:
            candidate = dataset_root / rel_path
            if candidate.exists() and candidate.is_file():
                ordered.append({
                    "source": "dataset",
                    "root": str(dataset_root),
                    "snippet": candidate.resolve(),
                    "relative_path": rel_path,
                })
                found = True

        if not found:
            missing.append(rel_path)

    if missing:
        roots = []
        if fixtures_root:
            roots.append(str(fixtures_root))
        if dataset_root:
            roots.append(str(dataset_root))
        raise ValueError(
            f"Manifest references {len(missing)} snippets not found in "
            f"{', '.join(roots)}: {', '.join(missing[:10])}"
        )
    return ordered


def collect_snippets(args: argparse.Namespace) -> list[dict[str, Any]]:
    # If a manifest is provided, use it to select and order snippets.
    if args.manifest_json:
        manifest = load_manifest_json(args.manifest_json)
        fixtures_root = Path(args.fixtures_root).expanduser().resolve() if args.fixtures_root else None
        dataset_root = Path(args.dataset_root).expanduser().resolve() if args.dataset_root else None
        if fixtures_root is not None and not fixtures_root.exists():
            raise FileNotFoundError(f"fixtures root does not exist: {fixtures_root}")
        if dataset_root is not None and not dataset_root.exists():
            raise FileNotFoundError(f"dataset root does not exist: {dataset_root}")
        return collect_snippets_from_manifest(manifest, fixtures_root, dataset_root)

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


def parse_resolution_report(path: Path) -> tuple[dict[str, str], list[str], list[dict[str, str]]]:
    metadata: dict[str, str] = {}
    notes: list[str] = []
    attempts: list[dict[str, str]] = []
    if not path.exists():
        return metadata, notes, attempts
    section = ""
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.rstrip()
        stripped = line.strip()
        if not stripped:
            continue
        if stripped.endswith(":") and stripped[:-1] in REPORT_SECTIONS:
            section = stripped[:-1]
            continue
        if not section:
            if ":" not in stripped:
                continue
            key, value = stripped.split(":", 1)
            metadata[key.strip()] = value.strip()
            continue
        if section == "notes" and stripped.startswith("- "):
            notes.append(stripped[2:])
            continue
        if section == "validation_attempts" and stripped.startswith("- "):
            attempts.append(parse_attempt_fields(stripped[2:]))
    return metadata, notes, attempts


def parse_attempt_fields(line: str) -> dict[str, str]:
    fields: dict[str, str] = {}
    for key, raw_value in ATTEMPT_FIELD_RE.findall(line):
        fields[key] = raw_value.strip('"')
    return fields


def metadata_value(primary: Mapping[str, Any], fallback: Mapping[str, Any], key: str) -> str:
    primary_value = str(primary.get(key) or "").strip()
    if primary_value:
        return primary_value
    return str(fallback.get(key) or "").strip()


def dedupe_preserve_order(values: list[str]) -> list[str]:
    ordered: list[str] = []
    seen: set[str] = set()
    for value in values:
        item = str(value or "").strip()
        if not item or item in seen:
            continue
        seen.add(item)
        ordered.append(item)
    return ordered


def summarize_backend_path(validation_backend: str, attempts: list[dict[str, str]]) -> str:
    backends = dedupe_preserve_order([attempt.get("backend", "") for attempt in attempts])
    if validation_backend and validation_backend not in backends:
        backends.append(validation_backend)
    if not backends:
        return validation_backend or "--"
    return " -> ".join(backends)


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


def yes_no(value: bool) -> str:
    return "Yes" if value else "No"


def report_title(report: dict[str, Any]) -> str:
    target = str(report.get("output_md") or report.get("output_json") or "").lower()
    if "validation-candidate" in target:
        return "Validation candidate capture"
    if "resolver-candidate" in target:
        return "Resolver candidate capture"
    return "Phase 13 Measurement Capture"


def build_report_run_contract(
    args: argparse.Namespace,
    python_command: list[str],
) -> dict[str, str]:
    run_config = {
        "run_intent": args.run_intent,
        "cache_state": args.cache_state,
        "llm_context_window": args.llm_context_window,
        "inference_policy": args.inference_policy,
        "build_profile": args.build_profile,
    }
    return build_run_contract(
        repo_root=REPO_ROOT,
        tool="apdr",
        model_name=args.model_name,
        base_url=args.base_url,
        temperature=None,
        validation_backend=args.validation_backend,
        run_config=run_config,
        runner_command=python_command,
    )


def derive_cache_context(
    validation_backend: str,
    validation_status: str,
    notes: list[str],
    attempts: list[dict[str, str]],
) -> dict[str, Any]:
    used_cached_env = any(parse_bool(attempt.get("cached_env")) for attempt in attempts)
    validated_env_cache_hit = any(parse_bool(attempt.get("env_cache_hit")) for attempt in attempts)
    used_cached_lockfile = any(parse_bool(attempt.get("cached_lockfile")) for attempt in attempts)
    import_set_cache_hit = (
        validation_backend == "import-set-cache"
        or validation_status == "passed-cached"
        or any("import-set cache hit" in note.lower() for note in notes)
    )
    validated_env_reused = used_cached_env or validated_env_cache_hit
    cache_labels: list[str] = []
    if import_set_cache_hit:
        cache_labels.append("import-set")
    if validated_env_reused:
        cache_labels.append("validated-env")
    if used_cached_lockfile:
        cache_labels.append("lockfile")
    return {
        "import_set_cache_hit": import_set_cache_hit,
        "used_cached_env": used_cached_env,
        "validated_env_cache_hit": validated_env_cache_hit,
        "validated_env_reused": validated_env_reused,
        "used_cached_lockfile": used_cached_lockfile,
        "cache_summary": ", ".join(cache_labels) if cache_labels else "none",
    }


def inferred_cache_state(cache_context: Mapping[str, Any]) -> str:
    if (
        parse_bool(cache_context.get("import_set_cache_hit"))
        or parse_bool(cache_context.get("validated_env_reused"))
        or parse_bool(cache_context.get("used_cached_lockfile"))
    ):
        return "warm"
    return "cold"


def finalize_run_contract(
    baseline_contract: Mapping[str, Any],
    metadata: Mapping[str, Any],
    report_metadata: Mapping[str, Any],
    validation_backend: str,
    cache_context: Mapping[str, Any],
) -> dict[str, str]:
    contract = {str(key): str(value).strip() for key, value in contract_from_sources(metadata, report_metadata).items()}
    normalized: dict[str, str] = {
        key: str(contract.get(key) or baseline_contract.get(key) or "").strip()
        for key in REQUIRED_RUN_CONTRACT_KEYS
    }
    normalized["tool"] = normalized["tool"] or str(baseline_contract.get("tool") or "apdr")
    normalized["validation_backend"] = (
        str(metadata.get("validation_backend") or report_metadata.get("validation_backend") or validation_backend).strip().lower()
        or str(baseline_contract.get("validation_backend") or validation_backend).strip().lower()
    )
    normalized["run_intent"] = normalize_run_intent(
        normalized.get("run_intent") or baseline_contract.get("run_intent")
    )
    raw_cache_state = normalized.get("cache_state") or inferred_cache_state(cache_context)
    normalized["cache_state"] = normalize_cache_state(raw_cache_state) or inferred_cache_state(cache_context)
    normalized["execution_mode"] = (
        str(normalized.get("execution_mode") or "").strip()
        or determine_execution_mode(normalized["tool"], normalized["validation_backend"])
    )
    normalized["llm_context_window"] = normalize_context_window(
        normalized.get("llm_context_window") or baseline_contract.get("llm_context_window")
    )
    normalized["inference_policy"] = normalize_inference_policy(
        normalized.get("inference_policy") or baseline_contract.get("inference_policy")
    )
    normalized["build_profile"] = normalize_build_profile(
        normalized.get("build_profile") or baseline_contract.get("build_profile")
    )
    for key in REQUIRED_RUN_CONTRACT_KEYS:
        normalized[key] = str(normalized.get(key) or baseline_contract.get(key) or "").strip()
    return normalized


def locate_fixture_artifacts(snippet_path: Path) -> tuple[Path | None, Path | None]:
    case_dir = snippet_path.parent
    output_path = find_latest_output(case_dir)
    report_path = case_dir / "resolution-report.txt"
    if output_path is None or not report_path.exists():
        return None, None
    report_metadata, _, _ = parse_resolution_report(report_path)
    report_snippet = normalize_path_text(report_metadata.get("snippet", ""))
    if report_snippet and Path(report_snippet).resolve() != snippet_path.resolve():
        return None, None
    return output_path, report_path


def build_case_command(
    python_command: list[str],
    snippet_path: Path,
    case_output_dir: Path,
    args: argparse.Namespace,
    run_contract_path: Path | None = None,
) -> list[str]:
    command = python_command + [
        str(TEST_EXECUTOR),
        "-f",
        str(snippet_path),
        "--output-dir",
        str(case_output_dir),
        "--validation-backend",
        str(args.validation_backend),
        "--build-profile",
        str(args.build_profile),
    ]
    if args.context_log:
        command.extend(["--benchmark-context-log", args.context_log])
    if args.force_validate:
        command.append("--force-validate")
    if run_contract_path is not None:
        command.extend(["--run-contract-json", str(run_contract_path)])
    return command


def build_live_case(
    sample: Mapping[str, Any],
    index: int,
    sample_root: Path,
    args: argparse.Namespace,
    python_command: list[str],
    baseline_contract: Mapping[str, Any],
) -> dict[str, Any]:
    case_output_dir = sample_root / f"{index:02d}-{Path(str(sample['snippet'])).stem}"
    if case_output_dir.exists():
        shutil.rmtree(case_output_dir)
    case_output_dir.mkdir(parents=True, exist_ok=True)

    run_contract_path = case_output_dir / "run_contract.json"
    write_json(run_contract_path, dict(baseline_contract))
    command = build_case_command(
        python_command,
        Path(str(sample["snippet"])),
        case_output_dir,
        args,
        run_contract_path,
    )
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
    report_path = Path(
        normalize_path_text(metadata.get("report_path")) or case_output_dir / "resolution-report.txt"
    )
    report_metadata, report_notes, report_attempts = parse_resolution_report(report_path)
    return build_case_record(
        sample=sample,
        index=index,
        command_display=command_display,
        output_dir=case_output_dir,
        output_path=output_path,
        report_path=report_path,
        metadata=metadata,
        report_metadata=report_metadata,
        report_notes=report_notes,
        report_attempts=report_attempts,
        baseline_contract=baseline_contract,
        validation_backend=args.validation_backend,
        returncode=completed.returncode,
        stdout_tail="\n".join(completed.stdout.splitlines()[-10:]),
        stderr_tail="\n".join(completed.stderr.splitlines()[-10:]),
        wall_duration_ms=wall_duration_ms,
    )


def build_fixture_case(
    sample: Mapping[str, Any],
    index: int,
    sample_root: Path,
    args: argparse.Namespace,
    python_command: list[str],
    baseline_contract: Mapping[str, Any],
) -> dict[str, Any]:
    case_output_dir = sample_root / f"{index:02d}-{Path(str(sample['snippet'])).stem}"
    if case_output_dir.exists():
        shutil.rmtree(case_output_dir)
    case_output_dir.mkdir(parents=True, exist_ok=True)

    output_path, report_path = locate_fixture_artifacts(Path(str(sample["snippet"])))
    metadata = parse_simple_yaml(output_path) if output_path else {}
    report_metadata, report_notes, report_attempts = (
        parse_resolution_report(report_path) if report_path else ({}, [], [])
    )
    placeholder_note = ""
    if output_path is None:
        placeholder_note = (
            "No fixture-backed APDR artifact was available for this snippet; "
            "the report uses a deterministic placeholder record instead of a live execution."
        )
        report_notes = [placeholder_note]
    return build_case_record(
        sample=sample,
        index=index,
        command_display=shell_join(
            build_case_command(python_command, Path(str(sample["snippet"])), case_output_dir, args)
        ),
        output_dir=case_output_dir,
        output_path=output_path,
        report_path=report_path or (case_output_dir / "resolution-report.txt"),
        metadata=metadata,
        report_metadata=report_metadata,
        report_notes=report_notes,
        report_attempts=report_attempts,
        baseline_contract=baseline_contract,
        validation_backend=args.validation_backend,
        returncode=0 if output_path else 1,
        stdout_tail="",
        stderr_tail=placeholder_note,
        wall_duration_ms=0,
    )


def build_case_record(
    *,
    sample: Mapping[str, Any],
    index: int,
    command_display: str,
    output_dir: Path,
    output_path: Path | None,
    report_path: Path,
    metadata: Mapping[str, Any],
    report_metadata: Mapping[str, Any],
    report_notes: list[str],
    report_attempts: list[dict[str, str]],
    baseline_contract: Mapping[str, Any],
    validation_backend: str,
    returncode: int,
    stdout_tail: str,
    stderr_tail: str,
    wall_duration_ms: int,
) -> dict[str, Any]:
    output_exists = output_path is not None
    validation_backend_value = metadata_value(metadata, report_metadata, "validation_backend") or validation_backend
    validation_status = metadata_value(metadata, report_metadata, "validation_status")
    validation_reason = metadata_value(metadata, report_metadata, "validation_reason")
    cache_context = derive_cache_context(
        validation_backend_value,
        validation_status,
        report_notes,
        report_attempts,
    )
    run_contract = finalize_run_contract(
        baseline_contract,
        metadata,
        report_metadata,
        validation_backend_value,
        cache_context,
    )
    requirements_path = Path(
        normalize_path_text(metadata_value(metadata, report_metadata, "requirements_path"))
        or output_dir / "requirements.txt"
    )
    status = classify_status(returncode, dict(metadata), output_exists)
    case = {
        "index": index,
        "source": sample["source"],
        "root": sample["root"],
        "snippet": str(sample["snippet"]),
        "relative_path": sample["relative_path"],
        "command": command_display,
        "output_dir": str(output_dir),
        "output_file": str(output_path) if output_path else "",
        "python_version": metadata_value(metadata, report_metadata, "python_version")
        or (output_path.stem[len("output_data_") :] if output_path else ""),
        "returncode": returncode,
        "status": status,
        "validation_backend": validation_backend_value,
        "backend_path": summarize_backend_path(validation_backend_value, report_attempts),
        "validation_status": validation_status,
        "validation_reason": validation_reason,
        "validation_succeeded": parse_bool(metadata_value(metadata, report_metadata, "validation_succeeded")),
        "solve_duration_ms": parse_int(metadata_value(metadata, report_metadata, "solve_duration_ms")),
        "validation_duration_ms": parse_int(metadata_value(metadata, report_metadata, "validation_duration_ms")),
        "llm_duration_ms": parse_int(metadata_value(metadata, report_metadata, "llm_duration_ms")),
        "env_create_duration_ms": parse_int(metadata_value(metadata, report_metadata, "env_create_duration_ms")),
        "install_duration_ms": parse_int(metadata_value(metadata, report_metadata, "install_duration_ms")),
        "docker_startup_duration_ms": parse_int(
            metadata_value(metadata, report_metadata, "docker_startup_duration_ms")
        ),
        "smoke_duration_ms": parse_int(metadata_value(metadata, report_metadata, "smoke_duration_ms")),
        "llm_calls": parse_int(metadata_value(metadata, report_metadata, "llm_calls")),
        "env_builds": parse_int(metadata_value(metadata, report_metadata, "env_builds")),
        "retries": parse_int(metadata_value(metadata, report_metadata, "retries")),
        "wall_duration_ms": wall_duration_ms,
        "requirements_path": str(requirements_path),
        "report_path": str(report_path),
        "requirements": load_requirements(requirements_path),
        "report_notes": report_notes,
        "validation_attempts": report_attempts,
        "stdout_tail": stdout_tail,
        "stderr_tail": stderr_tail,
        "run_contract": run_contract,
        **cache_context,
    }
    for key in RUN_CONTRACT_FLAT_KEYS:
        case[key] = str(run_contract.get(key, "")).strip()
    return case


def aggregate_cache_state(cases: list[dict[str, Any]], fallback: str) -> str:
    states = dedupe_preserve_order([case.get("cache_state", "") for case in cases])
    if not states:
        return normalize_cache_state(fallback)
    if len(states) == 1:
        return normalize_cache_state(states[0])
    return "mixed"


def aggregate_execution_mode(cases: list[dict[str, Any]], fallback: str) -> str:
    modes = dedupe_preserve_order([case.get("execution_mode", "") for case in cases])
    if not modes:
        return fallback
    if len(modes) == 1:
        return modes[0]
    return "mixed"


def render_markdown(report: dict[str, Any]) -> str:
    lines = [
        f"# {report_title(report)}",
        "",
        f"Created: {report['created_at']}",
    ]
    if report.get("slice_id"):
        lines.append(f"Slice ID: `{report['slice_id']}`")
    if report.get("manifest_json"):
        lines.append(f"Manifest: `{report['manifest_json']}`")
    lines += [
        f"Validation backend: `{report['validation_backend']}`",
        f"Execution mode: `{report['execution_mode']}`",
        f"Cache state: `{report['cache_state']}`",
        f"Model: `{report['model_name']}`",
        f"Base URL: `{report['base_url']}`",
        f"Run intent: `{report['run_intent']}`",
        f"Context window: `{report['llm_context_window']}`",
        f"Inference policy: `{report['inference_policy']}`",
        f"Build profile: `{report['build_profile']}`",
        f"Sample count: {report['sample_count']}",
        f"Pass rate: {report['pass_rate_percent']:.2f}%",
        f"Validation duration: {report['validation_duration_ms']} ms total",
        f"Solve duration: {report['solve_duration_ms']} ms total",
        f"LLM duration: {report['llm_duration_ms']} ms total",
        f"Env create duration: {report['env_create_duration_ms']} ms total",
        f"Install duration: {report['install_duration_ms']} ms total",
        f"Docker startup duration: {report['docker_startup_duration_ms']} ms total",
        f"Smoke duration: {report['smoke_duration_ms']} ms total",
        f"LLM calls: {report['llm_calls']}",
        f"Env builds: {report['env_builds']}",
        f"Retries: {report['retries']}",
        "",
        "## Command",
        "",
        "```text",
        report["command"],
        "```",
        "",
        "## Evidence Labels",
        "",
        f"- Execution mode: `{report['execution_mode']}`",
        f"- Cache state: `{report['cache_state']}`",
        f"- Warm samples: {report['cache_state_counts'].get('warm', 0)}",
        f"- Cold samples: {report['cache_state_counts'].get('cold', 0)}",
        "",
        "## Totals",
        "",
        f"- Passed: {report['passed']}",
        f"- Failed: {report['failed']}",
        f"- Skipped: {report['skipped']}",
        "",
        "## Samples",
        "",
        "| # | Snippet | Status | Mode | Cache | Model | LLM ms | Docker start ms | Solve ms | Validate ms |",
        "|---|---------|--------|------|-------|-------|--------|-----------------|----------|-------------|",
    ]
    for sample in report["samples"]:
        lines.append(
            "| {index} | `{snippet}` | {status} | `{execution_mode}` | `{cache_state}` | `{model_name}` | {llm_duration_ms} | {docker_startup_duration_ms} | {solve_duration_ms} | {validation_duration_ms} |".format(
                **sample
            )
        )
    lines.extend(["", "## Per-sample Commands", ""])
    for sample in report["samples"]:
        lines.extend(
            [
                f"### {sample['index']}. {sample['relative_path']}",
                "",
                f"- Status: `{sample['status']}`",
                f"- Validation backend: `{sample['validation_backend'] or '--'}`",
                f"- Execution mode: `{sample['execution_mode']}`",
                f"- Cache state: `{sample['cache_state']}`",
                f"- Model: `{sample['model_name']}`",
                f"- Base URL: `{sample['base_url']}`",
                f"- Context window: `{sample['llm_context_window']}`",
                f"- Inference policy: `{sample['inference_policy']}`",
                f"- Build profile: `{sample['build_profile']}`",
                f"- Warm cache indicators: `{sample['cache_summary']}`",
                f"- LLM duration ms: `{sample['llm_duration_ms']}`",
                f"- Docker startup ms: `{sample['docker_startup_duration_ms']}`",
                f"- Solve ms: `{sample['solve_duration_ms']}`",
                f"- Validation ms: `{sample['validation_duration_ms']}`",
                f"- Resolution report: `{sample['report_path']}`",
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
    baseline_contract = build_report_run_contract(args, python_command)
    cases: list[dict[str, Any]] = []

    for index, sample in enumerate(snippets, start=1):
        if args.execute_live and sample["source"] != "fixtures":
            case = build_live_case(sample, index, sample_root, args, python_command, baseline_contract)
        elif args.execute_live and sample["source"] == "fixtures":
            case = build_live_case(sample, index, sample_root, args, python_command, baseline_contract)
        else:
            case = build_fixture_case(sample, index, sample_root, args, python_command, baseline_contract)
        cases.append(case)

    passed = sum(1 for case in cases if case["status"] == "passed")
    failed = sum(1 for case in cases if case["status"] == "failed")
    skipped = sum(1 for case in cases if case["status"] == "skipped")
    sample_count = len(cases)
    run_contract = dict(baseline_contract)
    run_contract["cache_state"] = aggregate_cache_state(cases, run_contract.get("cache_state", "unknown"))
    run_contract["execution_mode"] = aggregate_execution_mode(
        cases,
        run_contract.get("execution_mode", determine_execution_mode("apdr", args.validation_backend)),
    )
    # Extract manifest metadata if a manifest was used.
    manifest_json = str(args.manifest_json).strip() if args.manifest_json else ""
    slice_id = ""
    if manifest_json:
        manifest_data = load_manifest_json(manifest_json)
        slice_id = str(manifest_data.get("slice_id", ""))

    report = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "repo_root": str(REPO_ROOT),
        "command": shell_join(sys.argv),
        "fixtures_root": str(Path(args.fixtures_root).expanduser().resolve()) if args.fixtures_root else "",
        "dataset_root": str(Path(args.dataset_root).expanduser().resolve()) if args.dataset_root else "",
        "manifest_json": manifest_json,
        "slice_id": slice_id,
        "validation_backend": args.validation_backend,
        "force_validate": bool(args.force_validate),
        "execute_live": bool(args.execute_live),
        "output_json": str(output_json),
        "output_md": str(output_md) if output_md is not None else "",
        "context_log": args.context_log,
        "sample_count": sample_count,
        "passed": passed,
        "failed": failed,
        "skipped": skipped,
        "pass_rate": (passed / sample_count) if sample_count else 0.0,
        "pass_rate_percent": round((passed / sample_count) * 100.0, 2) if sample_count else 0.0,
        "solve_duration_ms": sum(case["solve_duration_ms"] for case in cases),
        "validation_duration_ms": sum(case["validation_duration_ms"] for case in cases),
        "llm_duration_ms": sum(case["llm_duration_ms"] for case in cases),
        "env_create_duration_ms": sum(case["env_create_duration_ms"] for case in cases),
        "install_duration_ms": sum(case["install_duration_ms"] for case in cases),
        "docker_startup_duration_ms": sum(case["docker_startup_duration_ms"] for case in cases),
        "smoke_duration_ms": sum(case["smoke_duration_ms"] for case in cases),
        "wall_duration_ms": int((time.perf_counter() - started_at) * 1000),
        "llm_calls": sum(case["llm_calls"] for case in cases),
        "env_builds": sum(case["env_builds"] for case in cases),
        "retries": sum(case["retries"] for case in cases),
        "cache_state_counts": {
            state: sum(1 for case in cases if case.get("cache_state") == state)
            for state in ("warm", "cold", "mixed", "unknown")
        },
        "run_contract": run_contract,
        "samples": cases,
    }
    for key in RUN_CONTRACT_FLAT_KEYS:
        report[key] = str(run_contract.get(key, "")).strip()

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
