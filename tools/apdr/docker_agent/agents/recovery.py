"""Recovery Agent — suggests fixes based on error classification, grounded by tools.

Tools: PyPI version lookup, KGraph query, import→package mapping, version sampler
LLM: Yes (fix suggestion with tool-grounded context)
"""

from __future__ import annotations

from ..prompts.schemas import RecoveryAction
from ..prompts.templates import RECOVERY_PROMPT
from ..rag import assemble_recovery_context
from ..state import AgentState
from ..tools import import_mapper, pypi_lookup
from .llm_utils import llm_invoke


def recovery_node(state: AgentState) -> dict:
    """Suggest a fix based on error classification, using tools for grounding."""
    error_type = state.get("error_type", "Unknown")
    package = state.get("error_package", "unknown")
    python_version = state.get("python_version", "3.11")
    config = state.get("config", {})
    cache_path = config.get("cache_path", "")

    # Check attempt budget
    if state.get("attempt", 0) >= state.get("max_attempts", 5):
        return {
            "fix_action": "give_up",
            "fix_package": package,
            "fix_reason": "Maximum attempts reached",
        }

    # Tool 1: Get available versions for the offending package
    available_versions: list[str] = []
    if package != "unknown":
        available_versions = pypi_lookup.query_versions(package, python_version, cache_path)

    # Tool 2: Get already-tried versions
    tried = state.get("tried_versions", {}).get(package, [])

    # Tool 3: Check import→package mapping (for ModuleNotFound errors)
    mapped_package = None
    if error_type == "ModuleNotFound" and package != "unknown":
        mapping = import_mapper.lookup_import(package)
        if mapping and mapping.package_name.lower() != package.lower():
            mapped_package = mapping.package_name

    # Tool 4: Get dependency constraints from KGraph
    dep_constraints: list[str] = []
    if package != "unknown" and available_versions:
        # Try to get deps for a recent version
        for ver in reversed(available_versions[-5:]):
            deps = pypi_lookup.kgraph_deps(package, ver, cache_path)
            if deps:
                dep_constraints = deps
                break

    # For ModuleNotFound with a known mapping, skip LLM — use deterministic fix
    if mapped_package:
        mapping = import_mapper.lookup_import(package)
        version = mapping.default_version if mapping and mapping.default_version else None
        return {
            "fix_action": "add_package",
            "fix_package": mapped_package,
            "fix_version": version,
            "fix_system_deps": [],
            "fix_reason": f"Import '{package}' maps to package '{mapped_package}' (from seed data)",
        }

    # For system dep errors that the analyst already identified, skip LLM
    if state.get("fix_action") == "add_system_dep" and state.get("fix_system_deps"):
        return {
            "fix_action": "add_system_dep",
            "fix_package": package,
            "fix_system_deps": state["fix_system_deps"],
            "fix_reason": "System dependencies identified from build log headers",
        }

    # Build RAG context from all tool results
    recovery_context = assemble_recovery_context(
        available_versions=available_versions,
        tried_versions=tried,
        dep_constraints=dep_constraints,
        attempt_history=state.get("attempts_log", []),
        error_detail=state.get("error_detail", ""),
    )

    # LLM decides the fix with full tool-grounded context
    result = llm_invoke(
        template=RECOVERY_PROMPT,
        schema=RecoveryAction,
        config=config,
        error_type=error_type,
        error_package=package,
        error_detail=state.get("error_detail", ""),
        requirements_txt=state.get("requirements_txt", ""),
        python_version=python_version,
        recovery_context=recovery_context,
    )

    if result is None:
        # LLM unavailable — try a deterministic fallback
        if error_type == "PythonVersionMismatch":
            return {
                "fix_action": "try_next_python",
                "fix_package": package,
                "fix_reason": "Python version mismatch, trying next candidate",
            }
        if error_type in ("VersionNotFound", "DependencyConflict") and available_versions:
            # Pick a version from the middle that hasn't been tried
            untried = [v for v in available_versions if v not in tried]
            if untried:
                mid = untried[len(untried) // 2]
                return {
                    "fix_action": "change_version",
                    "fix_package": package,
                    "fix_version": mid,
                    "fix_system_deps": [],
                    "fix_reason": f"LLM unavailable, picking middle untried version {mid}",
                }
        return {
            "fix_action": "give_up",
            "fix_package": package,
            "fix_reason": "LLM unavailable and no deterministic fallback available",
        }

    return {
        "fix_action": result.action,
        "fix_package": result.package,
        "fix_version": result.version,
        "fix_system_deps": result.system_deps or [],
        "fix_reason": result.reason,
    }
