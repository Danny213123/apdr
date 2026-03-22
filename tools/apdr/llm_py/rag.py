"""RAG context assembly for LLM prompts.

Ported from tools/apdr/src/llm/rag.rs.
Since the Python LLM service receives pre-assembled context from Rust,
this module provides helpers for any Python-side context enrichment.
"""

from __future__ import annotations


def assemble_context_from_parts(
    context: list[str],
    failure_context: str = "",
) -> list[str]:
    """Combine context parts and optional failure memory context."""
    result = list(context)
    if failure_context:
        result.append(failure_context)
    return result
