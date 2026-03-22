"""#14: Active learning feedback loop.

After each benchmark run, automatically extract correct/incorrect mappings
and update seed data for future runs.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

logger = logging.getLogger("apdr_llm")


def extract_passed_mappings(run_dir: str) -> list[dict]:
    """Extract (import_name, package_name) pairs from passed cases.

    Looks at resolution-report.txt and resolved-final.txt in each case dir.
    """
    run_path = Path(run_dir)
    cases_dir = run_path / "cases"
    if not cases_dir.exists():
        return []

    mappings = []
    for case_dir in sorted(cases_dir.iterdir()):
        if not case_dir.is_dir():
            continue

        # Check if case passed
        report_path = case_dir / "resolution-report.txt"
        if not report_path.exists():
            continue

        report = report_path.read_text()
        if "status: passed" not in report.lower() and "succeeded: true" not in report.lower():
            continue

        # Extract resolved mappings
        resolved_path = case_dir / "debug" / "resolved-final.txt"
        if not resolved_path.exists():
            resolved_path = case_dir / "resolved-final.txt"
        if not resolved_path.exists():
            continue

        for line in resolved_path.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            # Format: import_name -> package_name==version [strategy]
            if " -> " in line:
                parts = line.split(" -> ", 1)
                import_name = parts[0].strip()
                rest = parts[1].strip()
                # Extract package name (before == or space)
                pkg_name = rest.split("==")[0].split()[0].strip()
                if import_name and pkg_name:
                    mappings.append({
                        "import_name": import_name,
                        "package_name": pkg_name,
                        "source": "active-learning",
                    })

    return mappings


def extract_failed_mappings(run_dir: str) -> list[dict]:
    """Extract failed (import_name, wrong_package, error) from failed cases."""
    run_path = Path(run_dir)
    cases_dir = run_path / "cases"
    if not cases_dir.exists():
        return []

    failures = []
    for case_dir in sorted(cases_dir.iterdir()):
        if not case_dir.is_dir():
            continue

        report_path = case_dir / "resolution-report.txt"
        if not report_path.exists():
            continue

        report = report_path.read_text()
        if "status: passed" in report.lower() or "succeeded: true" in report.lower():
            continue

        # Look for LLM mappings that led to failure
        for line in report.splitlines():
            if "LLM resolved" in line or "llm-retry" in line:
                # Try to extract the mapping
                if " -> " in line:
                    parts = line.split(" -> ", 1)
                    import_name = parts[0].split()[-1].strip()
                    pkg_name = parts[1].split(".")[0].split()[0].strip()
                    failures.append({
                        "import_name": import_name,
                        "package_name": pkg_name,
                        "error": "validation_failed",
                    })

    return failures


def update_seed_file(
    seed_path: str,
    new_mappings: list[dict],
    min_count: int = 2,
) -> int:
    """Append verified mappings to a seed TSV file.

    Only adds mappings that appear at least min_count times across runs
    (to avoid learning from flukes). Returns number of new entries added.
    """
    path = Path(seed_path)
    existing: set[str] = set()

    if path.exists():
        for line in path.read_text().splitlines():
            if line.strip() and not line.startswith("#"):
                parts = line.split("\t")
                if parts:
                    existing.add(parts[0].strip().lower())

    # Count occurrences
    from collections import Counter
    counter: Counter = Counter()
    pkg_map: dict[str, str] = {}
    for m in new_mappings:
        key = m["import_name"].strip().lower()
        counter[key] += 1
        pkg_map[key] = m["package_name"]

    # Add new entries that meet threshold and aren't already in the file
    added = 0
    with path.open("a") as f:
        for imp_lower, count in counter.items():
            if count >= min_count and imp_lower not in existing:
                pkg = pkg_map[imp_lower]
                f.write(f"{imp_lower}\t{pkg}\n")
                added += 1

    if added:
        logger.info("Active learning: added %d new mappings to %s", added, seed_path)

    return added


def update_failure_memory(
    cache_path: str,
    failures: list[dict],
) -> int:
    """Append failed mappings to the failure memory TSV."""
    from .failure_memory import FailureMemory

    if not failures:
        return 0

    fm = FailureMemory.load(cache_path)
    added = 0
    for f in failures:
        imp = f.get("import_name", "")
        pkg = f.get("package_name", "")
        error = f.get("error", "validation_failed")
        if imp and pkg and not fm.has_failed(imp, pkg):
            fm.record_failure(imp, pkg, error, "3.10")
            added += 1

    if added:
        fm.save()
        logger.info("Active learning: recorded %d new failures", added)

    return added
