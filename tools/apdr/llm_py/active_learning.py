"""#14: Active learning feedback loop.

After each benchmark run, automatically extract correct/incorrect mappings
and update seed data for future runs.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

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


def load_benchmark_artifact(artifact_path: str) -> dict[str, Any]:
    path = Path(artifact_path)
    if not path.exists():
        raise FileNotFoundError(f"Benchmark artifact not found: {path}")
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"Benchmark artifact must be a JSON object: {path}")
    return payload


def extract_benchmark_memory(artifact_path: str) -> dict[str, list[dict[str, Any]]]:
    """Extract inspectable active-learning memory from a Phase 15 artifact."""
    payload = load_benchmark_artifact(artifact_path)
    source = "active-learning"
    created_at = str(payload.get("created_at") or "")
    policy_label = str(payload.get("policy_label") or "")
    retrieval_profile = str(payload.get("retrieval_profile") or "none")
    memory_profile = str(payload.get("memory_profile") or "none")

    successes: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    for sample in payload.get("samples", []):
        if not isinstance(sample, dict):
            continue
        status = str(sample.get("tier3_status") or "").strip()
        status_reason = str(sample.get("status_reason") or "").strip()
        imports = [str(item).strip() for item in sample.get("imports", []) if str(item).strip()]
        resolved_mappings = sample.get("resolved_mappings", [])
        if isinstance(resolved_mappings, list):
            for mapping in resolved_mappings:
                if not isinstance(mapping, dict):
                    continue
                import_name = str(mapping.get("import_name") or "").strip()
                package_name = str(mapping.get("package_name") or "").strip()
                if import_name and package_name:
                    successes.append(
                        {
                            "import_name": import_name,
                            "package_name": package_name,
                            "source": source,
                            "status": status or "resolved",
                            "reason": status_reason,
                            "created_at": created_at,
                            "policy_label": policy_label,
                            "retrieval_profile": retrieval_profile,
                            "memory_profile": memory_profile,
                        }
                    )

        if status in {"failed", "abstained"}:
            failure_details = sample.get("failure_details", [])
            if isinstance(failure_details, list) and failure_details:
                for detail in failure_details:
                    if not isinstance(detail, dict):
                        continue
                    import_name = str(detail.get("import_name") or "").strip()
                    package_name = str(detail.get("package_name") or "").strip()
                    if import_name:
                        failures.append(
                            {
                                "import_name": import_name,
                                "package_name": package_name,
                                "error": str(detail.get("error") or status_reason or status or "failed"),
                                "source": source,
                                "created_at": created_at,
                                "policy_label": policy_label,
                                "retrieval_profile": retrieval_profile,
                                "memory_profile": memory_profile,
                            }
                        )
            else:
                for import_name in imports:
                    failures.append(
                        {
                            "import_name": import_name,
                            "package_name": "",
                            "error": status_reason or status or "failed",
                            "source": source,
                            "created_at": created_at,
                            "policy_label": policy_label,
                            "retrieval_profile": retrieval_profile,
                            "memory_profile": memory_profile,
                        }
                    )

    return {"successes": successes, "failures": failures}


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
        source = f.get("source", "active-learning")
        if imp and pkg and not fm.has_failed(imp, pkg):
            fm.record_failure(imp, pkg, error, "3.10", source=source)
            added += 1

    if added:
        fm.save()
        logger.info("Active learning: recorded %d new failures", added)

    return added


def update_success_memory(
    cache_path: str,
    successes: list[dict[str, Any]],
) -> int:
    """Persist benchmark-fed success memory as inspectable JSON."""
    if not successes:
        return 0

    path = Path(cache_path) / "llm_success_memory.json"
    existing: list[dict[str, Any]] = []
    if path.exists():
        try:
            existing_payload = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(existing_payload, list):
                existing = existing_payload
        except json.JSONDecodeError:
            existing = []

    seen = {
        (
            str(item.get("import_name") or "").lower(),
            str(item.get("package_name") or "").lower(),
            str(item.get("policy_label") or "").lower(),
        )
        for item in existing
        if isinstance(item, dict)
    }
    added = 0
    for success in successes:
        key = (
            str(success.get("import_name") or "").lower(),
            str(success.get("package_name") or "").lower(),
            str(success.get("policy_label") or "").lower(),
        )
        if key[0] and key[1] and key not in seen:
            existing.append(success)
            seen.add(key)
            added += 1

    if added:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(existing, indent=2) + "\n", encoding="utf-8")
        logger.info("Active learning: recorded %d new successes", added)

    return added


def load_success_memory_context(
    cache_path: str,
    import_names: list[str],
    max_items_per_import: int = 2,
) -> list[str]:
    """Load benchmark-fed success memory as retrieval context."""
    path = Path(cache_path) / "llm_success_memory.json"
    if not path.exists():
        return []

    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return []
    if not isinstance(payload, list):
        return []

    indexed: dict[str, list[dict[str, Any]]] = {}
    for item in payload:
        if not isinstance(item, dict):
            continue
        import_name = str(item.get("import_name") or "").strip()
        if import_name:
            indexed.setdefault(import_name, []).append(item)

    lines: list[str] = []
    for import_name in import_names:
        entries = indexed.get(import_name, [])[:max_items_per_import]
        for entry in entries:
            package_name = str(entry.get("package_name") or "").strip()
            policy_label = str(entry.get("policy_label") or "").strip() or "unknown"
            retrieval_profile = str(entry.get("retrieval_profile") or "none")
            if package_name:
                lines.append(
                    f"PREVIOUS SUCCESS: import `{import_name}` resolved to `{package_name}` "
                    f"via active-learning policy `{policy_label}` with retrieval `{retrieval_profile}`."
                )
    return lines


def update_memory_from_artifact(cache_path: str, artifact_path: str) -> dict[str, int]:
    """Update success and failure memory from a benchmark artifact."""
    extracted = extract_benchmark_memory(artifact_path)
    return {
        "successes_added": update_success_memory(cache_path, extracted["successes"]),
        "failures_added": update_failure_memory(cache_path, extracted["failures"]),
    }
