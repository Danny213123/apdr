"""Confidence Agent — scores snippet+requirements plausibility before building.

Tools: snippet analyzer, unsolvable pattern checker
LLM: Yes (confidence scoring)
"""

from __future__ import annotations

from ..prompts.schemas import ConfidenceAssessment
from ..prompts.templates import CONFIDENCE_PROMPT, UNSOLVABLE_IMPORTS
from ..rag import assemble_confidence_context
from ..state import AgentState
from .llm_utils import llm_invoke


def _check_unsolvable_patterns(source: str, imports: list[str]) -> str | None:
    """Deterministic check for known unsolvable imports."""
    lower_imports = {imp.lower().split(".")[0] for imp in imports}
    hits = lower_imports & UNSOLVABLE_IMPORTS
    if hits:
        return f"Snippet uses host-runtime imports: {', '.join(sorted(hits))}"

    lower_source = source.lower()
    if "cuda" in lower_source and ("torch.cuda" in lower_source or "numba.cuda" in lower_source):
        return "Snippet requires CUDA GPU hardware not available in generic Docker"

    return None


def confidence_node(state: AgentState) -> dict:
    """Score the plausibility of resolving this snippet in Docker."""
    # Tool 1: Check for known unsolvable patterns (deterministic, instant)
    unsolvable = _check_unsolvable_patterns(
        state.get("snippet_source", ""),
        state.get("imports", []),
    )
    if unsolvable:
        return {
            "confidence": 0.0,
            "confidence_reason": unsolvable,
            "status": "skipped",
        }

    # Tool 2: Assemble RAG context
    rag_context = assemble_confidence_context(state)

    # Tool 3: LLM confidence assessment
    result = llm_invoke(
        template=CONFIDENCE_PROMPT,
        schema=ConfidenceAssessment,
        config=state.get("config", {}),
        imports=", ".join(state.get("imports", [])),
        python_version=state.get("python_version", "3.11"),
        requirements_txt=state.get("requirements_txt", ""),
        rag_context=rag_context,
    )

    if result is None:
        # LLM unavailable — assume solvable, proceed with deterministic build
        return {"confidence": 0.5, "confidence_reason": "LLM unavailable, proceeding optimistically"}

    return {
        "confidence": result.confidence,
        "confidence_reason": result.reason,
    }
