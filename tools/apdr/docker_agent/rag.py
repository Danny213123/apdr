"""RAG context assembly from seed files, KGraph, and attempt history."""

from __future__ import annotations

from .tools import import_mapper, pypi_lookup


def assemble_confidence_context(state: dict) -> str:
    """Build context for the Confidence Agent."""
    parts: list[str] = []
    imports = state.get("imports", [])
    cache_path = state.get("config", {}).get("cache_path", "")

    for imp in imports[:20]:  # cap at 20 to keep prompt size reasonable
        mapping = import_mapper.lookup_import(imp)
        if mapping:
            parts.append(f"- import '{imp}' maps to package '{mapping.package_name}' (source: {mapping.source})")
        else:
            parts.append(f"- import '{imp}' has no known package mapping")

    reqs = state.get("requirements_txt", "")
    for line in reqs.splitlines()[:20]:
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        pkg = line.split("==")[0].split(">=")[0].split("<=")[0].strip()
        versions = pypi_lookup.query_versions(pkg, state.get("python_version", "3.11"), cache_path)
        if versions:
            parts.append(f"- package '{pkg}' has {len(versions)} known versions")
        else:
            parts.append(f"- package '{pkg}' has no known versions (may not exist on PyPI)")

    return "\n".join(parts) if parts else "No additional context available."


def assemble_analyst_context(state: dict) -> str:
    """Build context for the Analyst Agent."""
    parts: list[str] = []

    # Include attempt history
    for attempt in state.get("attempts_log", [])[-5:]:  # last 5 attempts
        parts.append(
            f"- Attempt {attempt.get('attempt_index')}: "
            f"Python {attempt.get('python_version')}, "
            f"status={attempt.get('status')}, "
            f"error_type={attempt.get('error_type', 'n/a')}, "
            f"fix_applied={attempt.get('fix_applied', 'none')}"
        )

    # Include tried versions
    for pkg, versions in state.get("tried_versions", {}).items():
        parts.append(f"- Already tried {pkg} versions: {', '.join(versions)}")

    return "\n".join(parts) if parts else "First attempt, no history."


def assemble_recovery_context(
    available_versions: list[str],
    tried_versions: list[str],
    dep_constraints: list[str],
    attempt_history: list[dict],
    error_detail: str,
) -> str:
    """Build context for the Recovery Agent."""
    parts: list[str] = []

    if available_versions:
        # Show a representative sample (first 10, last 10, skip middle)
        if len(available_versions) > 25:
            sample = available_versions[:10] + ["..."] + available_versions[-10:]
        else:
            sample = available_versions
        parts.append(f"Available versions ({len(available_versions)} total): {', '.join(sample)}")

    if tried_versions:
        parts.append(f"Already tried (DO NOT repeat): {', '.join(tried_versions)}")

    if dep_constraints:
        parts.append(f"Known dependency constraints: {'; '.join(dep_constraints[:10])}")

    # Summarize recent attempt history
    for attempt in attempt_history[-3:]:
        parts.append(
            f"Previous attempt: {attempt.get('status')} "
            f"(error: {attempt.get('error_type', 'n/a')}, fix: {attempt.get('fix_applied', 'none')})"
        )

    return "\n".join(parts) if parts else "No additional context."
