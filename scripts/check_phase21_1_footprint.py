#!/usr/bin/env python3
"""Check Phase 21.1 footprint contracts."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Iterable


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Check Phase 21.1 footprint contracts.")
    parser.add_argument("--repo-root", required=True, help="Repository root to inspect.")
    parser.add_argument(
        "--mode",
        required=True,
        choices=("tracked",),
        help="Footprint check mode to run.",
    )
    parser.add_argument("--status-json", required=True, help="Path to write the status JSON.")
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


def main() -> int:
    args = parse_args()
    repo_root = Path(args.repo_root).expanduser().resolve()
    status_path = Path(args.status_json).expanduser().resolve()

    target_files = tracked_files(repo_root, "tools/apdr/target*")
    cache_files = tracked_files(repo_root, "tools/apdr/.apdr-cache*")

    status = {
        "mode": args.mode,
        "repo_root": str(repo_root),
        "probe_only": bool(args.probe_only),
        "tracked_target_paths": tracked_roots(target_files, repo_root),
        "tracked_target_bytes": tracked_bytes(target_files),
        "tracked_cache_paths": tracked_roots(cache_files, repo_root),
        "ok": not target_files and not cache_files,
    }

    ensure_parent(status_path)
    status_path.write_text(json.dumps(status, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    if not status["ok"]:
        print("tracked APDR artifacts remain in git", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
