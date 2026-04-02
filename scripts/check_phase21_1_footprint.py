#!/usr/bin/env python3
"""Check Phase 21.1 footprint contracts."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any, Iterable


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Check Phase 21.1 footprint contracts.")
    parser.add_argument("--repo-root", required=True, help="Repository root to inspect.")
    parser.add_argument(
        "--mode",
        required=True,
        choices=("tracked", "local", "delta"),
        help="Footprint check mode to run.",
    )
    parser.add_argument("--status-json", required=True, help="Path to write the status JSON.")
    parser.add_argument("--cache-path", help="Cache path to measure in local mode.")
    parser.add_argument("--baseline-json", help="Baseline artifact for delta mode.")
    parser.add_argument("--candidate-json", help="Candidate artifact for delta mode.")
    parser.add_argument(
        "--tracked-status-json",
        help="Tracked-mode status JSON to merge into a local artifact for pre-fix baselines.",
    )
    parser.add_argument(
        "--probe-only",
        action="store_true",
        help="Validate the contract without requiring cleanup actions.",
    )
    return parser.parse_args()


def run_git(repo_root: Path, args: list[str]) -> str:
    completed = subprocess.run(
        ["git", *args],
        cwd=repo_root,
        check=True,
        capture_output=True,
        text=True,
    )
    return completed.stdout


def tracked_files(repo_root: Path, pattern: str) -> list[Path]:
    output = run_git(repo_root, ["ls-files", pattern])
    files = []
    for line in output.splitlines():
        value = line.strip()
        if value:
            files.append(repo_root / value)
    return files


def tracked_roots(paths: Iterable[Path], repo_root: Path) -> list[str]:
    roots: set[str] = set()
    for path in paths:
        rel = path.relative_to(repo_root)
        parts = rel.parts
        if len(parts) >= 3 and parts[0] == "tools" and parts[1] == "apdr":
            roots.add("/".join(parts[:3]))
        else:
            roots.add(rel.as_posix())
    return sorted(roots)


def tracked_bytes(paths: Iterable[Path]) -> int:
    total = 0
    for path in paths:
        if path.exists() and path.is_file():
            total += path.stat().st_size
    return total


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def write_json(path: Path, payload: dict[str, Any]) -> None:
    ensure_parent(path)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def load_json_object(path_text: str, label: str) -> dict[str, Any]:
    path = Path(path_text).expanduser().resolve()
    if not path.exists():
        raise ValueError(f"{label} not found: {path}")
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"{label} is not valid JSON: {path} ({exc})") from exc
    if not isinstance(payload, dict):
        raise ValueError(f"{label} must be a JSON object: {path}")
    return payload


def tracked_status(repo_root: Path, override: dict[str, Any] | None = None) -> dict[str, Any]:
    if override is not None:
        tracked_target_paths = override.get("tracked_target_paths")
        tracked_cache_paths = override.get("tracked_cache_paths")
        tracked_target_bytes = override.get("tracked_target_bytes")
        if not isinstance(tracked_target_paths, list):
            raise ValueError("tracked_status_json is missing tracked_target_paths")
        if not isinstance(tracked_cache_paths, list):
            raise ValueError("tracked_status_json is missing tracked_cache_paths")
        if not isinstance(tracked_target_bytes, int):
            raise ValueError("tracked_status_json is missing tracked_target_bytes")
        return {
            "tracked_target_paths": [str(value) for value in tracked_target_paths],
            "tracked_target_bytes": tracked_target_bytes,
            "tracked_cache_paths": [str(value) for value in tracked_cache_paths],
        }
    target_files = tracked_files(repo_root, "tools/apdr/target*")
    cache_files = tracked_files(repo_root, "tools/apdr/.apdr-cache*")
    return {
        "tracked_target_paths": tracked_roots(target_files, repo_root),
        "tracked_target_bytes": tracked_bytes(target_files),
        "tracked_cache_paths": tracked_roots(cache_files, repo_root),
    }


def bytes_for_path(path: Path) -> int:
    if not path.exists():
        return 0
    if path.is_file():
        return path.stat().st_size
    total = 0
    for root, _, files in os.walk(path):
        root_path = Path(root)
        for file_name in files:
            file_path = root_path / file_name
            try:
                total += file_path.stat().st_size
            except FileNotFoundError:
                continue
    return total


def resolve_repo_path(repo_root: Path, path_text: str | None) -> Path:
    if not path_text:
        return repo_root / "tools" / "apdr" / ".apdr-cache"
    path = Path(path_text).expanduser()
    if path.is_absolute():
        return path.resolve()
    return (repo_root / path).resolve()


def measured_cache_paths(repo_root: Path, cache_path: Path) -> list[Path]:
    paths: list[Path] = []
    for candidate in (cache_path, repo_root / "tools" / "apdr" / ".apdr-cache"):
        resolved = candidate.resolve()
        if resolved not in paths and resolved.exists():
            paths.append(resolved)
    return paths


def measured_target_paths(repo_root: Path) -> list[Path]:
    apdr_root = repo_root / "tools" / "apdr"
    paths: list[Path] = []
    repo_target = apdr_root / "target"
    if repo_target.exists():
        paths.append(repo_target.resolve())
    for candidate in sorted(apdr_root.glob("target-*")):
        if candidate.exists():
            paths.append(candidate.resolve())
    return paths


def local_status(
    repo_root: Path,
    *,
    cache_path: Path,
    tracked_override: dict[str, Any] | None = None,
) -> dict[str, Any]:
    tracked = tracked_status(repo_root, tracked_override)
    cache_paths = measured_cache_paths(repo_root, cache_path)
    target_paths = measured_target_paths(repo_root)
    notes = [
        f"cache_paths={','.join(path.as_posix() for path in cache_paths) or '(none)'}",
        f"target_paths={','.join(path.as_posix() for path in target_paths) or '(none)'}",
    ]
    return {
        "repo_root": str(repo_root),
        "tracked_target_paths": tracked["tracked_target_paths"],
        "tracked_target_bytes": tracked["tracked_target_bytes"],
        "tracked_cache_paths": tracked["tracked_cache_paths"],
        "local_cache_bytes": sum(bytes_for_path(path) for path in cache_paths),
        "local_target_bytes": sum(bytes_for_path(path) for path in target_paths),
        "tool_tree_bytes": bytes_for_path(repo_root / "tools"),
        "repo_git_bytes": bytes_for_path(repo_root / ".git"),
        "notes": notes,
    }


def validate_artifact(payload: dict[str, Any], label: str) -> dict[str, Any]:
    required_keys = (
        "tracked_target_paths",
        "tracked_target_bytes",
        "tracked_cache_paths",
        "local_cache_bytes",
        "local_target_bytes",
        "tool_tree_bytes",
        "repo_git_bytes",
        "notes",
    )
    missing = [key for key in required_keys if key not in payload]
    if missing:
        raise ValueError(f"{label} is missing keys: {', '.join(missing)}")
    if not isinstance(payload["tracked_target_paths"], list):
        raise ValueError(f"{label} tracked_target_paths must be a list")
    if not isinstance(payload["tracked_cache_paths"], list):
        raise ValueError(f"{label} tracked_cache_paths must be a list")
    if not isinstance(payload["notes"], list):
        raise ValueError(f"{label} notes must be a list")
    for key in ("tracked_target_bytes", "local_cache_bytes", "local_target_bytes", "tool_tree_bytes", "repo_git_bytes"):
        if not isinstance(payload[key], int):
            raise ValueError(f"{label} {key} must be an integer")
    return payload


def delta_status(
    baseline_payload: dict[str, Any],
    candidate_payload: dict[str, Any],
    *,
    probe_only: bool,
) -> dict[str, Any]:
    baseline = validate_artifact(baseline_payload, "baseline artifact")
    candidate = validate_artifact(candidate_payload, "candidate artifact")

    source_delta = candidate["tracked_target_bytes"] - baseline["tracked_target_bytes"]
    cache_delta = candidate["local_cache_bytes"] - baseline["local_cache_bytes"]
    target_delta = candidate["local_target_bytes"] - baseline["local_target_bytes"]

    messages: list[str] = []
    ok = True

    if candidate["tracked_target_paths"]:
        ok = False
        messages.append("candidate artifact still has tracked_target_paths")
    else:
        messages.append("candidate artifact has no tracked target paths")

    if source_delta >= 0:
        ok = False
        messages.append("source-distribution tracked bytes did not decrease")
    else:
        messages.append(f"source distribution delta: {source_delta}")

    if candidate["tool_tree_bytes"] >= baseline["tool_tree_bytes"]:
        ok = False
        messages.append("tools tree bytes did not decrease")
    else:
        messages.append(
            f"tools tree delta: {candidate['tool_tree_bytes'] - baseline['tool_tree_bytes']}"
        )

    if cache_delta > 0:
        ok = False
        messages.append("local cache bytes regressed")
    else:
        messages.append(f"local cache delta: {cache_delta}")

    if target_delta > 0:
        ok = False
        messages.append("local target bytes regressed")
    else:
        messages.append(f"local target delta: {target_delta}")

    if probe_only:
        messages.append("probe-only mode validated the artifact contract without extra live requirements")

    return {
        "ok": ok,
        "source_delta_bytes": source_delta,
        "cache_delta_bytes": cache_delta,
        "target_delta_bytes": target_delta,
        "messages": messages,
    }


def main() -> int:
    args = parse_args()
    repo_root = Path(args.repo_root).expanduser().resolve()
    status_path = Path(args.status_json).expanduser().resolve()

    try:
        if args.mode == "tracked":
            tracked = tracked_status(repo_root)
            status = {
                "mode": args.mode,
                "repo_root": str(repo_root),
                "probe_only": bool(args.probe_only),
                **tracked,
                "ok": not tracked["tracked_target_paths"] and not tracked["tracked_cache_paths"],
            }
        elif args.mode == "local":
            tracked_override = None
            if args.tracked_status_json:
                tracked_override = load_json_object(
                    args.tracked_status_json,
                    "tracked status artifact",
                )
            status = local_status(
                repo_root,
                cache_path=resolve_repo_path(repo_root, args.cache_path),
                tracked_override=tracked_override,
            )
        else:
            if not args.baseline_json or not args.candidate_json:
                raise ValueError("--baseline-json and --candidate-json are required in delta mode")
            status = delta_status(
                load_json_object(args.baseline_json, "baseline artifact"),
                load_json_object(args.candidate_json, "candidate artifact"),
                probe_only=bool(args.probe_only),
            )
    except (subprocess.CalledProcessError, ValueError) as exc:
        error_payload = {
            "mode": args.mode,
            "ok": False,
            "error": str(exc),
        }
        write_json(status_path, error_payload)
        print(str(exc), file=sys.stderr)
        return 1

    write_json(status_path, status)

    if not status.get("ok", True):
        print("phase 21.1 footprint check failed", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
