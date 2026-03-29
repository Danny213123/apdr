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
from typing import Any

from pydantic import BaseModel, Field, field_validator, model_validator

from ..active_learning import load_success_memory_context
from ..client import LlmClient
from ..failure_memory import FailureMemory
from ..local_detector import filter_imports
from ..models import PackageMapping, ResolutionRequest, ResolutionResponse
from ..pypi_checker import package_exists_on_pypi, preload_known_packages
from ..rag import assemble_retrieval_context
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
    actual_agent_mode = "direct"
    if not client.is_available():
        return ResolutionResponse(
            error="LLM provider not available",
            failure_reason="LLM provider not available",
            **_response_metadata(req, actual_agent_mode),
        )

    notes: list[str] = []
    prompts_issued = 0
    explicit_agent_requested = _uses_explicit_agent_mode(req)

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
            **_response_metadata(req, actual_agent_mode),
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
    success_memory_context: list[str] = []
    if req.cache_path:
        success_memory_context = load_success_memory_context(req.cache_path, llm_imports)
        if success_memory_context:
            context_parts.extend(success_memory_context)
            notes.append(
                f"Active-learning success memory added {len(success_memory_context)} context lines"
            )
    reverse_ctx = reverse_index_enrich(llm_imports)
    if reverse_ctx:
        context_parts.extend(reverse_ctx)
        notes.append(f"Reverse index enriched {len(reverse_ctx)} imports")

    retrieval_bundle = assemble_retrieval_context(
        import_names=llm_imports,
        context=context_parts,
        failure_context=failure_ctx,
        benchmark_context=req.benchmark_context,
        retrieval_profile=req.retrieval_profile,
    )
    context_parts = retrieval_bundle["context"]
    summarized_benchmark_context = retrieval_bundle["benchmark_context"]
    notes.extend(retrieval_bundle["notes"])

    # Build user prompt (only for imports that need LLM)
    user_prompt = prompts.package_resolution_user(
        unresolved_imports=llm_imports,
        python_version=req.python_version,
        context=context_parts,
        benchmark_context=summarized_benchmark_context,
        attribute_usage=req.attribute_usage,
        tier2_candidates=req.tier2_candidates,
        retrieval_profile=req.retrieval_profile,
    )

    # --- #2: Try constrained decoding if tier2 candidates available ---
    constrained_model = None
    if req.tier2_candidates:
        constrained_model = _build_constrained_model(req.tier2_candidates)

    # Choose an agentic strategy based on ambiguity.
    llm_confidence = 0.73
    result = None
    if constrained_model is not None and len(llm_imports) == 1:
        # Easy single-import cases still benefit from grammar-constrained decoding.
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
            result = client.complete_two_pass(
                system_prompt=prompts.RESOLUTION_SYSTEM,
                user_prompt=user_prompt,
                response_model=MappingsResult,
                max_tokens=512,
            )
            prompts_issued += 2
            if result is not None:
                notes.append("Used two-pass reasoning for single-import recovery")
    elif len(llm_imports) == 1:
        result = client.complete_two_pass(
            system_prompt=prompts.RESOLUTION_SYSTEM,
            user_prompt=user_prompt,
            response_model=MappingsResult,
            max_tokens=768,
        )
        prompts_issued += 2
        if result is not None:
            notes.append("Used two-pass reasoning for single-import recovery")
    else:
        result, llm_confidence = client.complete_with_entropy(
            system_prompt=prompts.RESOLUTION_SYSTEM,
            user_prompt=user_prompt,
            response_model=MappingsResult,
            n=3,
            temperature=0.2,
            max_tokens=1024,
        )
        prompts_issued += 3
        if result is not None:
            notes.append(f"Used self-consistency voting (confidence {llm_confidence:.2f})")
        else:
            notes.append("Self-consistency voting returned no usable mapping draft")

    if result is None:
        return ResolutionResponse(
            unresolved=list(req.imports),
            notes=["LLM package-resolution call returned no output."],
            prompts_issued=prompts_issued,
            abstain_reason="LLM package-resolution call returned no output.",
            **_response_metadata(req, actual_agent_mode),
        )

    # Build initial mappings, merging pre-resolved + LLM results
    mappings: dict[str, str] = dict(pre_resolved)
    for m in result.mappings:
        if m.import_name in llm_imports:
            mappings[m.import_name] = m.package_name
    missing_mappings = [imp for imp in llm_imports if imp not in mappings]
    if missing_mappings:
        notes.append(
            "Initial reasoning left imports unresolved: "
            + ", ".join(missing_mappings[:5])
        )

    # Critique the draft before shipping it.
    self_refine_result = _self_refine_mappings(
        client=client,
        req=req,
        llm_imports=llm_imports,
        mappings=mappings,
    )
    if self_refine_result is not None:
        prompts_issued += 1
        if not self_refine_result.all_correct:
            for correction in self_refine_result.corrections:
                import_name = str(correction.get("import_name") or "").strip()
                corrected_package = str(correction.get("corrected_package") or "").strip()
                if import_name in mappings and corrected_package:
                    mappings[import_name] = corrected_package
                    notes.append(f"Self-refine corrected {import_name} -> {corrected_package}")
        else:
            notes.append("Self-refine accepted the draft mappings")

    # --- Agentic fallback: if confidence is low or PyPI existence rejects the draft,
    # run the tool-using resolver rather than hardcoding more package rules.
    nonexistent = _check_pypi_existence([(i, p) for i, p in mappings.items() if i != p])
    suspicious = _suspicious_identity_mappings(mappings, llm_imports)
    needs_agent_fallback = (
        len(llm_imports) > 1 and llm_confidence < 0.72
    ) or bool(nonexistent) or bool(suspicious) or bool(missing_mappings) or explicit_agent_requested

    response_abstain_reason = ""
    response_failure_reason = ""

    if needs_agent_fallback:
        from . import react_agent

        agent_req = req.model_copy(deep=True)
        agent_req.imports = list(llm_imports)
        agent_req.context = context_parts + _agent_seed_context(
            mappings=mappings,
            llm_imports=llm_imports,
            llm_confidence=llm_confidence,
            missing_mappings=missing_mappings,
            nonexistent=nonexistent,
            suspicious=suspicious,
        )
        agent_response = react_agent.handle(agent_req)
        prompts_issued += agent_response.prompts_issued
        notes.extend(agent_response.notes)
        actual_agent_mode = agent_response.agent_mode or _normalized_agent_mode(req.agent_mode)
        response_abstain_reason = str(agent_response.abstain_reason or "").strip()
        response_failure_reason = str(agent_response.failure_reason or agent_response.error or "").strip()

        if agent_response.tool_profile:
            notes.append(
                "Agent seam executed with "
                f"agent_mode={actual_agent_mode}, "
                f"tool_profile={agent_response.tool_profile}, "
                f"retrieval_profile={agent_response.retrieval_profile or 'none'}"
            )
        if response_abstain_reason:
            notes.append(f"Agent abstain: {response_abstain_reason}")
        if response_failure_reason:
            notes.append(f"Agent failure: {response_failure_reason}")

        if explicit_agent_requested:
            seeded = dict(pre_resolved)
            for mapping in agent_response.mappings:
                if mapping.import_name in llm_imports and mapping.package_name:
                    seeded[mapping.import_name] = mapping.package_name
                    notes.append(
                        f"Explicit agent resolved {mapping.import_name} -> {mapping.package_name}"
                    )
            mappings = seeded
        else:
            for mapping in agent_response.mappings:
                if mapping.import_name in mappings and mapping.package_name:
                    mappings[mapping.import_name] = mapping.package_name
                    notes.append(
                        f"Agent fallback resolved {mapping.import_name} -> {mapping.package_name}"
                    )

            unresolved_from_agent = {
                name for name in agent_response.unresolved if name in llm_imports
            }
            for import_name in unresolved_from_agent:
                mappings.pop(import_name, None)

        if nonexistent:
            rejected = ", ".join(f"{imp}->{pkg}" for imp, pkg in nonexistent[:5])
            notes.append(f"Agent fallback triggered by PyPI rejection: {rejected}")
        if suspicious:
            notes.append(
                "Agent fallback triggered by low-confidence or identity-heavy mappings"
            )
        if explicit_agent_requested:
            notes.append(
                f"Explicit agent seam requested via agent_mode={_normalized_agent_mode(req.agent_mode)}"
            )

    # Build response
    result_mappings = [
        PackageMapping(import_name=imp, package_name=pkg)
        for imp, pkg in mappings.items()
    ]
    unresolved = [imp for imp in req.imports if imp not in mappings]

    return ResolutionResponse(
        mappings=result_mappings,
        unresolved=unresolved,
        confidence=llm_confidence,
        notes=notes,
        prompts_issued=prompts_issued,
        abstain_reason=response_abstain_reason,
        failure_reason=response_failure_reason,
        **_response_metadata(req, actual_agent_mode),
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


def _self_refine_mappings(
    client: LlmClient,
    req: ResolutionRequest,
    llm_imports: list[str],
    mappings: dict[str, str],
) -> SelfRefineResult | None:
    review_pairs = [
        (imp, pkg)
        for imp, pkg in mappings.items()
        if imp in llm_imports and pkg and pkg != imp
    ]
    if not review_pairs:
        return None

    snippet_excerpt = req.snippet_source.strip()
    if not snippet_excerpt:
        snippet_excerpt = "\n".join(f"import {imp}" for imp in llm_imports)

    return client.complete_json(
        system_prompt=prompts.SELF_REFINE_SYSTEM,
        user_prompt=prompts.self_refine_user(
            mappings=review_pairs,
            python_version=req.python_version,
            snippet_excerpt=snippet_excerpt,
        ),
        response_model=SelfRefineResult,
        max_tokens=768,
    )


def _suspicious_identity_mappings(
    mappings: dict[str, str],
    llm_imports: list[str],
) -> list[str]:
    suspicious: list[str] = []
    for import_name in llm_imports:
        package_name = mappings.get(import_name)
        if not package_name:
            continue
        if package_name == import_name and not package_exists_on_pypi(package_name):
            suspicious.append(import_name)
    return suspicious


def _normalized_agent_mode(value: str) -> str:
    text = str(value or "").strip().lower()
    if text in {"manual", "langchain", "langgraph", "auto"}:
        return text
    return "direct"


def _uses_explicit_agent_mode(req: ResolutionRequest) -> bool:
    return _normalized_agent_mode(req.agent_mode) != "direct"


def _response_metadata(req: ResolutionRequest, agent_mode: str) -> dict[str, str]:
    policy_label = str(req.policy_label or "").strip()
    if not policy_label:
        policy_label = f"{agent_mode}-{str(req.tool_profile or 'full').strip() or 'full'}"
    return {
        "agent_mode": agent_mode,
        "tool_profile": str(req.tool_profile or "").strip() or "full",
        "retrieval_profile": str(req.retrieval_profile or "").strip() or "none",
        "policy_label": policy_label,
    }


def _agent_seed_context(
    *,
    mappings: dict[str, str],
    llm_imports: list[str],
    llm_confidence: float,
    missing_mappings: list[str],
    nonexistent: list[tuple[str, str]],
    suspicious: list[str],
) -> list[str]:
    seeded: list[str] = [
        f"Draft tier3 confidence: {llm_confidence:.2f}",
    ]
    for import_name in llm_imports:
        package_name = mappings.get(import_name)
        if package_name:
            seeded.append(f"Draft mapping candidate: {import_name} -> {package_name}")
    if missing_mappings:
        seeded.append("Draft left unresolved: " + ", ".join(missing_mappings[:5]))
    if nonexistent:
        seeded.append(
            "Draft rejected by PyPI existence check: "
            + ", ".join(f"{imp}->{pkg}" for imp, pkg in nonexistent[:5])
        )
    if suspicious:
        seeded.append("Draft contained suspicious identity mappings: " + ", ".join(suspicious[:5]))
    return seeded
