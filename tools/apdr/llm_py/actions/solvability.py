"""Solvability assessment action handler."""

from __future__ import annotations

import logging

from pydantic import BaseModel

from ..client import LlmClient
from ..models import ResolutionRequest, ResolutionResponse
from .. import prompts

logger = logging.getLogger("apdr_llm")


class SolvabilityResult(BaseModel):
    decision: str
    confidence: float = 0.6
    reason: str = ""
    unsolvable_modules: list[str] = []


def handle(req: ResolutionRequest) -> ResolutionResponse:
    client = LlmClient(req.provider, req.model, req.base_url)
    if not client.is_available():
        return ResolutionResponse(error="LLM provider not available")

    user_prompt = prompts.solvability_user(
        source=req.snippet_source,
        imports=req.imports,
        import_paths=[],  # import_paths not sent separately; included in snippet
        benchmark_context=req.benchmark_context,
    )

    result = client.complete_json(
        system_prompt=prompts.SOLVABILITY_SYSTEM,
        user_prompt=user_prompt,
        response_model=SolvabilityResult,
        max_tokens=512,
    )

    if result is None:
        return ResolutionResponse(
            error="LLM solvability assessment returned no output",
            prompts_issued=1,
        )

    return ResolutionResponse(
        decision=result.decision.lower(),
        confidence=max(0.0, min(1.0, result.confidence)),
        reason=result.reason,
        unsolvable_modules=result.unsolvable_modules,
        prompts_issued=1,
    )
