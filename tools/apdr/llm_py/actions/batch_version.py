"""Batch version selection action handler.

#12: Instead of N separate LLM calls for N packages, select versions for
all packages in a single prompt. Saves ~(N-1) * 2-4s of LLM round-trip time.
"""

from __future__ import annotations

import logging

from pydantic import BaseModel

from ..client import LlmClient
from ..models import ResolutionRequest, ResolutionResponse
from .. import prompts

logger = logging.getLogger("apdr_llm")


class VersionEntry(BaseModel):
    package: str
    version: str = "NONE"


class BatchVersionResult(BaseModel):
    versions: list[VersionEntry] = []


def handle(req: ResolutionRequest) -> ResolutionResponse:
    """Select versions for multiple packages in a single LLM call."""
    client = LlmClient(req.provider, req.model, req.base_url)
    if not client.is_available():
        return ResolutionResponse(error="LLM provider not available")

    # batch_packages is a dict: {package_name: [versions...]}
    batch = req.batch_packages
    if not batch:
        return ResolutionResponse(version="", prompts_issued=0)

    # Build a combined prompt for all packages
    bm_str = prompts.compress_benchmark_context(req.benchmark_context, 4096)
    lines = [
        f"Target Python version: {req.python_version}",
        f"Benchmark context:\n{bm_str}",
        "",
        "Choose one installable version for EACH of the following Python packages.",
        "For each package, pick the best version from its allowed list.",
        'Return a JSON object with a "versions" array. Each entry has "package" and "version" fields.',
        'If none look viable for a package, set version to "NONE".',
        "",
    ]
    for pkg_name, versions in batch.items():
        lines.append(f"  {pkg_name}: {', '.join(versions)}")

    user_prompt = "\n".join(lines)

    result = client.complete_json(
        system_prompt="Choose versions for Python packages. Return JSON with a 'versions' array.",
        user_prompt=user_prompt,
        response_model=BatchVersionResult,
        max_tokens=512,
        num_ctx=4096,
    )

    if result is None:
        return ResolutionResponse(error="Batch version selection returned no output", prompts_issued=1)

    # Convert to a dict response for Rust to parse
    version_map = {}
    for entry in result.versions:
        v = entry.version.strip()
        if v.upper() != "NONE" and v:
            # Validate version is in the allowed list
            allowed = batch.get(entry.package, [])
            if v in allowed:
                version_map[entry.package] = v

    return ResolutionResponse(
        batch_versions=version_map,
        prompts_issued=1,
    )
