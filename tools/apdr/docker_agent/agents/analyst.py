"""Analyst Agent — classifies build/run failure logs into error types.

Tools: pattern matcher (16 built-in patterns), header-file extractor, log parser
LLM: Yes (fallback for unrecognized errors)
"""

from __future__ import annotations

from ..prompts.schemas import ErrorClassification
from ..prompts.templates import ERROR_CLASSIFICATION_PROMPT
from ..rag import assemble_analyst_context
from ..state import AgentState
from ..tools import log_parser, system_deps
from .llm_utils import llm_invoke


def analyst_node(state: AgentState) -> dict:
    """Classify build/run failure into an actionable error type."""
    build_log = state.get("build_log", "")
    run_log = state.get("run_log", "")
    combined_log = build_log + "\n" + run_log

    # Tool 1: Deterministic pattern matching (16 built-in patterns)
    match = log_parser.match_known_patterns(combined_log)
    if match:
        package = log_parser.extract_package_from_log(combined_log, match.error_type)
        return {
            "error_type": match.error_type,
            "error_package": package,
            "error_detail": f"{match.fix_hint} (pattern: {match.matched_pattern})",
        }

    # Tool 2: Header-file extraction for system dep errors
    missing_deps = system_deps.extract_system_deps_from_log(combined_log)
    if missing_deps:
        return {
            "error_type": "BuildFailure",
            "error_package": "unknown",
            "error_detail": f"Missing system deps: {', '.join(missing_deps)}",
            "fix_system_deps": missing_deps,
            "fix_action": "add_system_dep",
        }

    # Tool 3: LLM fallback for unrecognized errors
    rag_context = assemble_analyst_context(state)
    log_tail = "\n".join(combined_log.splitlines()[-100:])

    result = llm_invoke(
        template=ERROR_CLASSIFICATION_PROMPT,
        schema=ErrorClassification,
        config=state.get("config", {}),
        build_log_tail=log_tail,
        requirements_txt=state.get("requirements_txt", ""),
        python_version=state.get("python_version", "3.11"),
        rag_context=rag_context,
    )

    if result is None:
        return {
            "error_type": "Unknown",
            "error_package": "unknown",
            "error_detail": "LLM unavailable and no deterministic pattern matched",
        }

    return {
        "error_type": result.error_type,
        "error_package": result.offending_package,
        "error_detail": result.detail,
    }
