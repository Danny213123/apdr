"""Recovery package hint action handler.

Uses tolerant JSON completion for better compatibility with small models.
Integrates #12 build error pattern library for RAG-enriched recovery prompts.
"""

from __future__ import annotations

import logging

from pydantic import BaseModel

from ..build_error_patterns import format_error_context
from ..client import LlmClient
from ..models import ResolutionRequest, ResolutionResponse
from ..pypi_checker import package_exists_on_pypi
from .. import prompts

logger = logging.getLogger("apdr_llm")


class RecoveryResult(BaseModel):
    fix_possible: bool = False
    wrong_package: str = ""
    correct_package: str = ""
    reasoning: str = ""


def handle(req: ResolutionRequest) -> ResolutionResponse:
    client = LlmClient(req.provider, req.model, req.base_url)
    if not client.is_available():
        return ResolutionResponse(error="LLM provider not available")

    notes: list[str] = []

    # --- #12: Enrich recovery context with known build error patterns ---
    error_pattern_ctx = ""
    if req.error_log:
        error_pattern_ctx = format_error_context(req.error_log)
        if error_pattern_ctx:
            notes.append("Build error pattern library matched")

    user_prompt = prompts.recovery_user(
        resolved_packages=req.resolved_packages,
        error_log=req.error_log,
        snippet_source=req.snippet_source,
        python_version=req.python_version,
        error_type=req.error_type,
        previous_attempts=req.previous_attempts,
    )

    # Prepend error pattern context if we have matches
    if error_pattern_ctx:
        user_prompt = f"{error_pattern_ctx}\n\n{user_prompt}"

    # Tolerant JSON call — no field validators during generation
    result = client.complete_json(
        system_prompt=prompts.RECOVERY_SYSTEM,
        user_prompt=user_prompt,
        response_model=RecoveryResult,
        max_tokens=512,
    )

    if result is None:
        return ResolutionResponse(
            fix_possible=False,
            error="LLM recovery returned no output",
            prompts_issued=1,
        )

    # Post-hoc PyPI validation
    if result.fix_possible and result.correct_package:
        if not package_exists_on_pypi(result.correct_package):
            notes.append(f"Recovery suggestion '{result.correct_package}' not found on PyPI")
            result.fix_possible = False

    if not result.fix_possible:
        return ResolutionResponse(fix_possible=False, notes=notes, prompts_issued=1)

    if (
        not result.wrong_package
        or not result.correct_package
        or result.wrong_package == result.correct_package
    ):
        return ResolutionResponse(fix_possible=False, notes=notes, prompts_issued=1)

    if result.reasoning:
        notes.append(result.reasoning)

    return ResolutionResponse(
        fix_possible=True,
        wrong_package=result.wrong_package,
        correct_package=result.correct_package,
        notes=notes,
        prompts_issued=1,
    )
