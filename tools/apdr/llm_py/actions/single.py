"""Single package hint action handler.

Uses two-pass architecture and PyPI-validated mappings for single imports.
Integrates local module detection (#4) and constrained decoding (#2).
"""

from __future__ import annotations

import logging

from pydantic import BaseModel, Field

from ..client import LlmClient
from ..local_detector import filter_imports
from ..models import PackageMapping, ResolutionRequest, ResolutionResponse
from ..pypi_checker import preload_known_packages
from ..reverse_index import enrich_context as reverse_index_enrich, load as load_reverse_index
from .. import prompts
from .resolve import MappingsResult, _build_constrained_model

logger = logging.getLogger("apdr_llm")


def handle(req: ResolutionRequest) -> ResolutionResponse:
    client = LlmClient(req.provider, req.model, req.base_url)
    if not client.is_available():
        return ResolutionResponse(error="LLM provider not available")

    if not req.imports:
        return ResolutionResponse(error="No import specified")

    notes: list[str] = []

    # --- #4: Pre-LLM local module detection ---
    llm_imports, local_skips, framework_mappings, known_mappings = filter_imports(req.imports)

    if local_skips:
        notes.append(f"Skipped local module: {', '.join(local_skips)}")
        return ResolutionResponse(notes=notes, prompts_issued=0)

    if framework_mappings:
        result_mappings = [
            PackageMapping(import_name=imp, package_name=pkg)
            for imp, pkg in framework_mappings.items()
        ]
        notes.append(f"Framework submodule resolved: {list(framework_mappings.keys())}")
        return ResolutionResponse(
            mappings=result_mappings, notes=notes, prompts_issued=0,
        )

    if known_mappings:
        result_mappings = [
            PackageMapping(import_name=imp, package_name=pkg)
            for imp, pkg in known_mappings.items()
        ]
        notes.append(f"Known mapping resolved: {list(known_mappings.keys())}")
        return ResolutionResponse(
            mappings=result_mappings, notes=notes, prompts_issued=0,
        )

    import_name = llm_imports[0] if llm_imports else req.imports[0]
    prompts_issued = 0

    # Load reverse index
    try:
        load_reverse_index()
    except Exception:
        pass

    # Pre-populate PyPI cache
    if req.context:
        known_pkgs = [
            ctx_line[len("Known package: "):]
            for ctx_line in req.context
            if ctx_line.startswith("Known package: ")
        ]
        if known_pkgs:
            preload_known_packages(known_pkgs)

    # Enrich context with reverse index
    context_parts = list(req.context)
    reverse_ctx = reverse_index_enrich([import_name])
    if reverse_ctx:
        context_parts.extend(reverse_ctx)

    user_prompt = prompts.package_resolution_user(
        unresolved_imports=[import_name],
        python_version=req.python_version,
        context=context_parts,
        benchmark_context=req.benchmark_context,
        attribute_usage=req.attribute_usage,
        tier2_candidates=req.tier2_candidates,
    )

    # --- #2: Try constrained decoding if tier2 candidates available ---
    if req.tier2_candidates:
        constrained_model = _build_constrained_model(req.tier2_candidates)
        if constrained_model is not None:
            result = client.complete_ollama_native(
                system_prompt=prompts.RESOLUTION_SYSTEM,
                user_prompt=user_prompt,
                response_model=constrained_model,
                max_tokens=512,
            )
            prompts_issued += 1
            if result is not None:
                notes.append("Used constrained decoding with tier2 candidates")
                mapped = import_name
                for m in result.mappings:
                    if m.import_name == import_name:
                        mapped = m.package_name
                        break
                return ResolutionResponse(
                    mappings=[PackageMapping(import_name=import_name, package_name=mapped)],
                    notes=notes,
                    prompts_issued=prompts_issued,
                )

    # Tolerant JSON call
    result = client.complete_json(
        system_prompt=prompts.RESOLUTION_SYSTEM,
        user_prompt=user_prompt,
        response_model=MappingsResult,
        max_tokens=512,
    )
    prompts_issued += 1

    if result is None:
        diagnostics_raw = client.last_failure_reason()
        diagnostics = diagnostics_raw.strip() if isinstance(diagnostics_raw, str) else ""
        if diagnostics:
            notes.append(f"LLM diagnostics: {diagnostics}")
        return ResolutionResponse(
            mappings=[PackageMapping(import_name=import_name, package_name=import_name)],
            notes=notes,
            failure_reason=diagnostics,
            prompts_issued=prompts_issued,
        )

    # Find the mapping for our import
    mapped = import_name
    for m in result.mappings:
        if m.import_name == import_name:
            mapped = m.package_name
            break

    return ResolutionResponse(
        mappings=[PackageMapping(import_name=import_name, package_name=mapped)],
        notes=notes,
        prompts_issued=prompts_issued,
    )
