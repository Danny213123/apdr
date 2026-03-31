"""LangGraph state machine — the core orchestration graph."""

from __future__ import annotations

import re

from langgraph.graph import END, StateGraph

from .agents.analyst import analyst_node
from .agents.builder import builder_node
from .agents.confidence import confidence_node
from .agents.recovery import recovery_node
from .state import AgentState


# ---------------------------------------------------------------------------
# Conditional edge functions (pure logic — the Orchestrator)
# ---------------------------------------------------------------------------

def _should_build(state: AgentState) -> str:
    """After confidence check: build if confident enough, else skip."""
    if state.get("confidence", 0.0) < 0.4:
        return "end"
    return "build"


def _build_outcome(state: AgentState) -> str:
    """After builder: route based on build/run result."""
    if state.get("status") == "passed":
        return "end"
    error = state.get("error_type", "")
    if error in ("build_failed", "runtime_failed", "runtime_timeout"):
        return "analyze"
    # If no explicit error but no pass either, try analysis
    if state.get("build_log") or state.get("run_log"):
        return "analyze"
    return "end"


def _recovery_outcome(state: AgentState) -> str:
    """After recovery: apply fix, try next python, or give up."""
    action = state.get("fix_action", "give_up")
    attempt = state.get("attempt", 0)
    max_attempts = state.get("max_attempts", 5)

    if action == "give_up" or attempt >= max_attempts:
        return "end"
    if action == "try_next_python":
        return "next_python"
    return "apply_fix"


# ---------------------------------------------------------------------------
# Deterministic nodes (no LLM, pure logic)
# ---------------------------------------------------------------------------

def _apply_fix_node(state: AgentState) -> dict:
    """Apply the Recovery Agent's fix to requirements and state."""
    reqs = state.get("requirements_txt", "")
    action = state.get("fix_action", "")
    pkg = state.get("fix_package", "")
    ver = state.get("fix_version")

    updates: dict = {"attempt": state.get("attempt", 0) + 1}

    if action == "change_version" and pkg and ver:
        updates["requirements_txt"] = _update_version(reqs, pkg, ver)
        tried = dict(state.get("tried_versions", {}))
        tried.setdefault(pkg, []).append(ver)
        updates["tried_versions"] = tried
        updates["fix_applied"] = f"Changed {pkg} to {ver}"

    elif action == "add_package" and pkg:
        line = f"{pkg}=={ver}" if ver else pkg
        updates["requirements_txt"] = reqs.rstrip() + "\n" + line + "\n"
        updates["fix_applied"] = f"Added {line}"

    elif action == "remove_package" and pkg:
        updates["requirements_txt"] = _remove_package(reqs, pkg)
        updates["fix_applied"] = f"Removed {pkg}"

    elif action == "add_system_dep":
        new_deps = sorted(set(
            state.get("system_deps", []) + state.get("fix_system_deps", [])
        ))
        updates["system_deps"] = new_deps
        updates["fix_applied"] = f"Added system deps: {', '.join(state.get('fix_system_deps', []))}"

    # Record fix in last attempt log entry
    attempts_log = list(state.get("attempts_log", []))
    if attempts_log:
        attempts_log[-1]["fix_applied"] = updates.get("fix_applied", "")
    updates["attempts_log"] = attempts_log

    # Clear error state for next build
    updates["error_type"] = ""
    updates["error_package"] = ""
    updates["error_detail"] = ""
    updates["fix_action"] = ""
    updates["fix_package"] = ""
    updates["fix_version"] = None
    updates["fix_system_deps"] = []

    return updates


def _next_python_node(state: AgentState) -> dict:
    """Advance to the next candidate Python version."""
    idx = state.get("current_python_idx", 0) + 1
    candidates = state.get("candidate_versions", [])

    updates: dict = {
        "current_python_idx": idx,
        "attempt": state.get("attempt", 0) + 1,
        "error_type": "",
        "error_package": "",
        "error_detail": "",
        "fix_action": "",
        "fix_package": "",
        "fix_version": None,
        "fix_system_deps": [],
    }

    if idx >= len(candidates):
        updates["status"] = "failed"

    return updates


def _mark_failed(state: AgentState) -> dict:
    """Terminal node that marks the overall status as failed."""
    if state.get("status") not in ("passed", "skipped"):
        return {"status": "failed"}
    return {}


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------

def _update_version(reqs: str, package: str, new_version: str) -> str:
    """Replace a package version in requirements text."""
    lines = reqs.splitlines()
    norm = package.lower().replace("-", "_").replace(".", "_")
    updated = []
    found = False
    for line in lines:
        stripped = line.strip()
        if stripped and not stripped.startswith("#"):
            pkg_part = re.split(r"[=><!\[; ]", stripped, maxsplit=1)[0].strip()
            if pkg_part.lower().replace("-", "_").replace(".", "_") == norm:
                updated.append(f"{pkg_part}=={new_version}")
                found = True
                continue
        updated.append(line)
    if not found:
        updated.append(f"{package}=={new_version}")
    return "\n".join(updated) + "\n"


def _remove_package(reqs: str, package: str) -> str:
    """Remove a package from requirements text."""
    lines = reqs.splitlines()
    norm = package.lower().replace("-", "_").replace(".", "_")
    kept = []
    for line in lines:
        stripped = line.strip()
        if stripped and not stripped.startswith("#"):
            pkg_part = re.split(r"[=><!\[; ]", stripped, maxsplit=1)[0].strip()
            if pkg_part.lower().replace("-", "_").replace(".", "_") == norm:
                continue
        kept.append(line)
    return "\n".join(kept) + "\n"


# ---------------------------------------------------------------------------
# Graph construction
# ---------------------------------------------------------------------------

def build_graph():
    """Build and compile the LangGraph state machine."""
    graph = StateGraph(AgentState)

    # Add nodes (each is an agent or logic node)
    graph.add_node("confidence_check", confidence_node)
    graph.add_node("build", builder_node)
    graph.add_node("analyze", analyst_node)
    graph.add_node("recover", recovery_node)
    graph.add_node("apply_fix", _apply_fix_node)
    graph.add_node("next_python", _next_python_node)
    graph.add_node("mark_failed", _mark_failed)

    # Entry point
    graph.set_entry_point("confidence_check")

    # Conditional edges
    graph.add_conditional_edges(
        "confidence_check",
        _should_build,
        {"build": "build", "end": END},
    )
    graph.add_conditional_edges(
        "build",
        _build_outcome,
        {"end": END, "analyze": "analyze"},
    )
    graph.add_edge("analyze", "recover")
    graph.add_conditional_edges(
        "recover",
        _recovery_outcome,
        {"end": "mark_failed", "apply_fix": "apply_fix", "next_python": "next_python"},
    )
    graph.add_edge("apply_fix", "build")
    graph.add_conditional_edges(
        "next_python",
        lambda s: "end" if s.get("status") == "failed" else "build",
        {"end": END, "build": "build"},
    )
    graph.add_edge("mark_failed", END)

    return graph.compile()
