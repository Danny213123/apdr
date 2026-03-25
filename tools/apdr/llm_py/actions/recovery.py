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
    version: str = ""           # specific version pin (e.g. "1.8.3")
    add_package: str = ""       # new transitive dep to add (e.g. "protobuf==3.20.3")
    remove_package: str = ""    # package to remove entirely (local module / wrong package)
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
        max_tokens=1024,  # Larger for thinking mode output
        num_ctx=8192,  # Recovery needs enough context for error log + snippet
    )

    if result is None:
        return ResolutionResponse(
            fix_possible=False,
            error="LLM recovery returned no output",
            prompts_issued=1,
        )

    # Post-hoc PyPI validation
    if result.fix_possible and result.correct_package:
        pkg_name = result.correct_package.split("==")[0].split(">=")[0].split("<=")[0]
        if not package_exists_on_pypi(pkg_name):
            notes.append(f"Recovery suggestion '{result.correct_package}' not found on PyPI")
            result.fix_possible = False

    if result.fix_possible and result.add_package:
        add_pkg_name = result.add_package.split("==")[0].split(">=")[0].split("<=")[0]
        if not package_exists_on_pypi(add_pkg_name):
            notes.append(f"Recovery add_package '{result.add_package}' not found on PyPI")
            result.add_package = ""

    # Validate remove_package against PyPI too — but removal doesn't need PyPI check
    if result.fix_possible and result.remove_package:
        # remove_package is always valid — it just removes from the resolved list
        pass

    if not result.fix_possible:
        # Even if swap isn't possible, adding a dep or removing one might help
        if result.add_package or result.remove_package:
            if result.reasoning:
                notes.append(result.reasoning)
            return ResolutionResponse(
                fix_possible=True,
                add_package=result.add_package,
                remove_package=result.remove_package,
                notes=notes,
                prompts_issued=1,
            )
        return ResolutionResponse(fix_possible=False, notes=notes, prompts_issued=1)

    # Allow same-package version pin: wrong_package == correct_package + version
    has_swap = (
        result.wrong_package
        and result.correct_package
        and result.wrong_package != result.correct_package
    )
    has_version_pin = (
        result.wrong_package
        and result.correct_package
        and result.version
        and result.wrong_package == result.correct_package
    )

    if not has_swap and not has_version_pin:
        # No valid swap or version pin, but maybe there's an add_package or remove_package
        if result.add_package or result.remove_package:
            if result.reasoning:
                notes.append(result.reasoning)
            return ResolutionResponse(
                fix_possible=True,
                add_package=result.add_package,
                remove_package=result.remove_package,
                notes=notes,
                prompts_issued=1,
            )
        return ResolutionResponse(fix_possible=False, notes=notes, prompts_issued=1)

    if result.reasoning:
        notes.append(result.reasoning)

    return ResolutionResponse(
        fix_possible=True,
        wrong_package=result.wrong_package,
        correct_package=result.correct_package,
        version=result.version,
        add_package=result.add_package,
        remove_package=result.remove_package,
        notes=notes,
        prompts_issued=1,
    )
