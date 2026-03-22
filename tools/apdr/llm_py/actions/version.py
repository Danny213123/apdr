"""Version inference action handler."""

from __future__ import annotations

import logging

from pydantic import BaseModel

from ..client import LlmClient
from ..models import ResolutionRequest, ResolutionResponse
from .. import prompts

logger = logging.getLogger("apdr_llm")


class VersionResult(BaseModel):
    version: str = "NONE"


def handle(req: ResolutionRequest) -> ResolutionResponse:
    client = LlmClient(req.provider, req.model, req.base_url)
    if not client.is_available():
        return ResolutionResponse(error="LLM provider not available")

    if not req.versions:
        return ResolutionResponse(version="", prompts_issued=0)

    user_prompt = prompts.version_user(
        package_name=req.package_name,
        versions=req.versions,
        python_version=req.python_version,
        benchmark_context=req.benchmark_context,
    )

    result = client.complete_json(
        system_prompt="Choose a version for a Python package. Return JSON with a single 'version' field.",
        user_prompt=user_prompt,
        response_model=VersionResult,
        max_tokens=256,
    )

    if result is None:
        return ResolutionResponse(version="", prompts_issued=1)

    version = result.version.strip()
    if version.upper() == "NONE" or version not in req.versions:
        return ResolutionResponse(version="", prompts_issued=1)

    return ResolutionResponse(version=version, prompts_issued=1)
