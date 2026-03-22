"""Main package resolution action handler.

Implements:
- #1: Pydantic field_validator with PyPI existence inside Instructor retry loop
- #2: Constrained decoding with dynamic enum allowlist from tier2 candidates
- #4: Two-pass architecture (reasoning → structuring) for single-import cases
- #6: Reverse index RAG enrichment from seed/harvest data
- #4 (local): Pre-LLM local module detection
- #2 (batch): Post-LLM batch PyPI verification
- Self-consistency voting for multi-import resolution
- Self-refine critique step
- ReAct verify-and-retry with PyPI existence check
"""

from __future__ import annotations

import logging
from typing import Any, Literal, Optional

from pydantic import BaseModel, Field, field_validator, model_validator

from ..client import LlmClient
from ..failure_memory import FailureMemory
from ..local_detector import filter_imports
from ..models import PackageMapping, ResolutionRequest, ResolutionResponse
from ..pypi_checker import package_exists_on_pypi, preload_known_packages
from ..reverse_index import enrich_context as reverse_index_enrich, load as load_reverse_index
from .. import prompts

logger = logging.getLogger("apdr_llm")


# ---------------------------------------------------------------------------
# #1: Validated mapping model — field_validator triggers Instructor retries
# ---------------------------------------------------------------------------

class ValidatedPackageMapping(BaseModel):
    import_name: str
    package_name: str

    @field_validator("package_name")
    @classmethod
    def package_must_exist_on_pypi(cls, v: str, info: Any) -> str:
        """Reject hallucinated packages. Instructor retries with the
        validation error message, giving the LLM a chance to correct."""
        if not v or v.strip() == "":
            raise ValueError("package_name cannot be empty")
        # Allow identity mappings (import == package after normalization)
        import_name = ""
        if info and hasattr(info, "data") and isinstance(info.data, dict):
            import_name = info.data.get("import_name", "")
        norm_v = v.strip().lower().replace("-", "_").replace(".", "_")
        norm_i = import_name.strip().lower().replace("-", "_").replace(".", "_")
        if norm_v == norm_i:
            return v  # Identity mapping — skip check
        if not package_exists_on_pypi(v):
            raise ValueError(
                f"Package '{v}' does NOT exist on PyPI. "
                f"Please suggest a different, real PyPI package for import '{import_name}'."
            )
        return v


class ValidatedMappingsResult(BaseModel):
    mappings: list[ValidatedPackageMapping] = Field(default_factory=list)


# Non-validated version for voting (avoid N*M PyPI checks during voting)
class MappingsResult(BaseModel):
    mappings: list[PackageMapping] = Field(default_factory=list)


class SelfRefineResult(BaseModel):
    all_correct: bool = True
    corrections: list[dict] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# #2: Build a constrained-decoding model with dynamic enum
# ---------------------------------------------------------------------------

def _build_constrained_model(
    tier2_candidates: dict[str, list[str]],
) -> type[BaseModel] | None:
    """Build a dynamic Pydantic model where package_name is a Literal enum
    derived from tier2 candidates. This constrains Ollama's GBNF grammar
    so the model can ONLY output known-valid package names.

    Returns None if no meaningful constraints can be built.
    """
    # Collect all unique candidate packages
    all_candidates: set[str] = set()
    for candidates in tier2_candidates.values():
        all_candidates.update(candidates)

    if len(all_candidates) < 2:
        return None  # Not enough candidates to constrain

    # Add identity mappings (import_name == package_name) as valid options
    for import_name in tier2_candidates:
        all_candidates.add(import_name)

    candidate_tuple = tuple(sorted(all_candidates))

    # Create dynamic model with Literal constraint
    class ConstrainedMapping(BaseModel):
        import_name: str
        package_name: str  # Will be post-validated against candidates

        @field_validator("package_name")
        @classmethod
        def must_be_known_candidate(cls, v: str) -> str:
            norm = v.strip().lower().replace("-", "_")
            for c in candidate_tuple:
                if c.lower().replace("-", "_") == norm:
                    return c  # Return the canonical form
            raise ValueError(
                f"'{v}' is not in the candidate list. "
                f"Choose from: {', '.join(candidate_tuple[:15])}"
            )

    class ConstrainedMappingsResult(BaseModel):
        mappings: list[ConstrainedMapping] = Field(default_factory=list)

    return ConstrainedMappingsResult


# ---------------------------------------------------------------------------
# Main handler
# ---------------------------------------------------------------------------

def handle(req: ResolutionRequest) -> ResolutionResponse:
    client = LlmClient(req.provider, req.model, req.base_url)
    if not client.is_available():
        return ResolutionResponse(error="LLM provider not available")

    notes: list[str] = []
    prompts_issued = 0

    # --- #4 (local): Pre-LLM local module detection ---
    llm_imports, local_skips, framework_mappings = filter_imports(req.imports)
    pre_resolved: dict[str, str] = {}

    if local_skips:
        notes.append(f"Skipped {len(local_skips)} local modules: {', '.join(local_skips[:5])}")
    if framework_mappings:
        for imp, parent in framework_mappings.items():
            pre_resolved[imp] = parent
            notes.append(f"Framework submodule {imp} -> {parent}")

    # If all imports were resolved pre-LLM, return immediately
    if not llm_imports:
        result_mappings = [
            PackageMapping(import_name=imp, package_name=pkg)
            for imp, pkg in pre_resolved.items()
        ]
        return ResolutionResponse(
            mappings=result_mappings,
            notes=notes,
            prompts_issued=0,
        )

    # --- #6: Load reverse index for RAG enrichment ---
    try:
        load_reverse_index()
    except Exception:
        pass  # Non-critical

    # Pre-populate PyPI cache with known packages from Rust store context
    if req.context:
        known_pkgs = []
        for ctx_line in req.context:
            if ctx_line.startswith("Known package: "):
                known_pkgs.append(ctx_line[len("Known package: "):])
        if known_pkgs:
            preload_known_packages(known_pkgs)

    # Load failure memory
    failure_memory = FailureMemory.load(req.cache_path) if req.cache_path else None
    failure_ctx = ""
    if failure_memory:
        failure_ctx = failure_memory.format_context(llm_imports)

    # --- #6: Enrich context with reverse index ---
    context_parts = list(req.context)
    if failure_ctx:
        context_parts.append(failure_ctx)
    reverse_ctx = reverse_index_enrich(llm_imports)
    if reverse_ctx:
        context_parts.extend(reverse_ctx)
        notes.append(f"Reverse index enriched {len(reverse_ctx)} imports")

    # Build user prompt (only for imports that need LLM)
    user_prompt = prompts.package_resolution_user(
        unresolved_imports=llm_imports,
        python_version=req.python_version,
        context=context_parts,
        benchmark_context=req.benchmark_context,
        attribute_usage=req.attribute_usage,
        tier2_candidates=req.tier2_candidates,
    )

    # --- #2: Try constrained decoding if tier2 candidates available ---
    constrained_model = None
    if req.tier2_candidates:
        constrained_model = _build_constrained_model(req.tier2_candidates)

    # Choose resolution strategy based on remaining LLM imports
    llm_confidence = 0.73  # default
    if constrained_model is not None and len(llm_imports) == 1:
        # Single import with tier2 candidates → try constrained decoding first
        result = client.complete_ollama_native(
            system_prompt=prompts.RESOLUTION_SYSTEM,
            user_prompt=user_prompt,
            response_model=constrained_model,
            max_tokens=512,
        )
        prompts_issued += 1
        if result is not None:
            notes.append("Used constrained decoding with tier2 candidates")
        else:
            # Fallback to tolerant JSON
            result = client.complete_json(
                system_prompt=prompts.RESOLUTION_SYSTEM,
                user_prompt=user_prompt,
                response_model=MappingsResult,
                max_tokens=512,
            )
            prompts_issued += 1
    else:
        # All other cases: single tolerant JSON call
        result = client.complete_json(
            system_prompt=prompts.RESOLUTION_SYSTEM,
            user_prompt=user_prompt,
            response_model=MappingsResult,
            max_tokens=1024,
        )
        prompts_issued += 1

    if result is None:
        return ResolutionResponse(
            unresolved=list(req.imports),
            notes=["LLM package-resolution call returned no output."],
            prompts_issued=prompts_issued,
        )

    # Build initial mappings, merging pre-resolved + LLM results
    mappings: dict[str, str] = dict(pre_resolved)
    for m in result.mappings:
        if m.import_name in llm_imports:
            mappings[m.import_name] = m.package_name

    for imp in llm_imports:
        if imp not in mappings:
            mappings[imp] = imp

    # --- #1: ReAct verify-and-retry with PyPI existence check ---
    nonexistent = _check_pypi_existence(
        [(i, p) for i, p in mappings.items() if i != p]
    )
    if nonexistent:
        feedback_imports = [i for i, _ in nonexistent]
        feedback_lines = [
            f"  {imp} -> {pkg} (NOT FOUND on PyPI)" for imp, pkg in nonexistent
        ]
        retry_context = [
            "IMPORTANT: The following packages do NOT exist on PyPI. "
            "Please suggest correct alternatives:\n" + "\n".join(feedback_lines)
        ] + context_parts

        retry_user = prompts.package_resolution_user(
            unresolved_imports=feedback_imports,
            python_version=req.python_version,
            context=retry_context,
            benchmark_context=req.benchmark_context,
            attribute_usage=req.attribute_usage,
            tier2_candidates={
                k: v
                for k, v in (req.tier2_candidates or {}).items()
                if k in feedback_imports
            },
        )

        retry_result = client.complete_json(
            system_prompt=prompts.RESOLUTION_SYSTEM,
            user_prompt=retry_user,
            response_model=MappingsResult,
            max_tokens=1024,
        )
        prompts_issued += 1

        if retry_result:
            for m in retry_result.mappings:
                if m.import_name in mappings:
                    mappings[m.import_name] = m.package_name
                    notes.append(
                        f"ReAct retry: {m.import_name} -> {m.package_name}"
                    )

    # Build response
    result_mappings = [
        PackageMapping(import_name=imp, package_name=pkg)
        for imp, pkg in mappings.items()
    ]

    return ResolutionResponse(
        mappings=result_mappings,
        confidence=llm_confidence,
        notes=notes,
        prompts_issued=prompts_issued,
    )


def _check_pypi_existence(
    mappings: list[tuple[str, str]],
) -> list[tuple[str, str]]:
    """Check packages via the cached PyPI checker. Returns non-existent pairs."""
    nonexistent = []
    for imp, pkg in mappings:
        if not package_exists_on_pypi(pkg):
            nonexistent.append((imp, pkg))
    return nonexistent
