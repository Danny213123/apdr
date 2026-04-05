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
import re
from typing import Any

from pydantic import BaseModel, Field, field_validator, model_validator

from ..active_learning import load_success_memory_context
from ..client import LlmClient, classify_failure_reason
from ..failure_memory import FailureMemory
from ..local_detector import filter_imports
from ..models import (
    AuthoredCasePlan,
    AuthoredPlanPackageMapping,
    IntakeFailureRecord,
    PackageMapping,
    ResolutionRequest,
    ResolutionResponse,
    SmokeStrategy,
)
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
    actual_agent_mode = "direct"

    notes: list[str] = []
    prompts_issued = 0
    explicit_agent_requested = _uses_explicit_agent_mode(req)
    deterministic_mappings: set[str] = set()

    # --- #4 (local): Pre-LLM local module detection ---
    llm_imports, local_skips, framework_mappings, known_mappings = filter_imports(req.imports)
    pre_resolved: dict[str, str] = {}

    if local_skips:
        notes.append(f"Skipped {len(local_skips)} local modules: {', '.join(local_skips[:5])}")
    if framework_mappings:
        for imp, parent in framework_mappings.items():
            pre_resolved[imp] = parent
            notes.append(f"Framework submodule {imp} -> {parent}")
    if known_mappings:
        for imp, pkg in known_mappings.items():
            pre_resolved[imp] = pkg
            deterministic_mappings.add(imp)
            notes.append(f"Known mapping {imp} -> {pkg}")

    llm_imports, evidence_mappings, evidence_notes = _seed_evidence_backed_mappings(
        req,
        llm_imports,
    )
    if evidence_mappings:
        pre_resolved.update(evidence_mappings)
        deterministic_mappings.update(evidence_mappings)
        notes.extend(evidence_notes)

    # If all imports were resolved pre-LLM, return immediately
    if not llm_imports:
        result_mappings = [
            PackageMapping(import_name=imp, package_name=pkg)
            for imp, pkg in pre_resolved.items()
        ]
        if not req.imports:
            notes.append("No third-party imports required package resolution.")
        authored_plan = _build_authored_case_plan(
            req=req,
            mappings={mapping.import_name: mapping.package_name for mapping in result_mappings},
            unresolved=[],
            local_skips=local_skips,
            framework_mappings=framework_mappings,
            deterministic_mappings=deterministic_mappings,
            llm_confidence=1.0,
        )
        return ResolutionResponse(
            mappings=result_mappings,
            authored_plan=authored_plan,
            authored_plan_status="available",
            notes=notes,
            prompts_issued=0,
            **_response_metadata(req, actual_agent_mode),
        )

    client = LlmClient(req.provider, req.model, req.base_url)
    if not client.is_available():
        intake_failure = _build_intake_failure(
            reason="LLM provider not available",
            diagnostics="LLM provider not available",
        )
        return ResolutionResponse(
            error="LLM provider not available",
            failure_reason="LLM provider not available",
            authored_plan_status=intake_failure.authored_plan_status,
            intake_failure=intake_failure,
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
        snippet_source=req.snippet_source,
    )

    # --- #2: Try constrained decoding if tier2 candidates available ---
    constrained_model = None
    if req.tier2_candidates:
        constrained_model = _build_constrained_model(req.tier2_candidates)

    # Tailor system prompt to the specific imports (progressive context disclosure)
    system_prompt = prompts.resolution_system_for_imports(llm_imports)

    # Single-pass JSON completion — no instructor, no multi-pass chains.
    llm_confidence = 0.73
    result = None
    if constrained_model is not None and len(llm_imports) == 1:
        # Easy single-import cases still benefit from grammar-constrained decoding.
        result = client.complete_ollama_native(
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            response_model=constrained_model,
            max_tokens=512,
        )
        prompts_issued += 1
        if result is not None:
            notes.append("Used constrained decoding with tier2 candidates")

    # For small ambiguous cases that survived tier1/tier2 (retrieval_profile=full),
    # use entropy voting to get real confidence. Skip in LLM-only mode (no tier2
    # ran, so "no candidates" doesn't mean ambiguous — it means we skipped tiers).
    use_entropy = (
        result is None
        and 1 <= len(llm_imports) <= 2
        and not req.tier2_candidates
        and (req.retrieval_profile or "none") == "full"
    )

    if use_entropy:
        entropy_result, entropy_confidence = client.complete_with_entropy(
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            response_model=MappingsResult,
            n=3,
            max_tokens=1024,
        )
        prompts_issued += 3
        if entropy_result is not None:
            result = entropy_result
            llm_confidence = entropy_confidence
            notes.append(f"Entropy voting confidence: {llm_confidence:.2f}")

    if result is None:
        result = client.complete_json(
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            response_model=MappingsResult,
            max_tokens=1024,
        )
        prompts_issued += 1
        if result is not None:
            notes.append("Used tolerant JSON completion")

    if result is None:
        diagnostics_raw = client.last_failure_reason()
        diagnostics = diagnostics_raw.strip() if isinstance(diagnostics_raw, str) else ""
        intake_failure = _build_intake_failure(
            reason="LLM package-resolution call returned no output.",
            diagnostics=diagnostics,
            raw_response_preview=diagnostics,
        )
        response_notes = ["LLM package-resolution call returned no output."]
        if diagnostics:
            response_notes.append(f"LLM diagnostics: {diagnostics}")
        return ResolutionResponse(
            unresolved=list(req.imports),
            notes=response_notes,
            prompts_issued=prompts_issued,
            abstain_reason="LLM package-resolution call returned no output.",
            failure_reason=diagnostics,
            authored_plan_status=intake_failure.authored_plan_status,
            intake_failure=intake_failure,
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
    authored_plan = _build_authored_case_plan(
        req=req,
        mappings=mappings,
        unresolved=unresolved,
        local_skips=local_skips,
        framework_mappings=framework_mappings,
        deterministic_mappings=deterministic_mappings,
        llm_confidence=llm_confidence,
    )

    return ResolutionResponse(
        mappings=result_mappings,
        unresolved=unresolved,
        authored_plan=authored_plan,
        authored_plan_status="available",
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


def _normalize_name(value: str) -> str:
    return value.strip().lower().replace("-", "_").replace(".", "_")


def _parse_context_evidence_lines(
    context_lines: list[str],
) -> tuple[dict[str, str], dict[str, str]]:
    import_mappings: dict[str, str] = {}
    known_packages: dict[str, str] = {}

    for line in context_lines:
        mapping_match = re.match(
            r"^\s*known import mapping:\s*(?P<imp>.+?)\s*->\s*(?P<pkg>.+?)\s*$",
            line,
            flags=re.IGNORECASE,
        )
        if mapping_match:
            import_name = mapping_match.group("imp").strip()
            package_name = mapping_match.group("pkg").strip()
            if import_name and package_name:
                import_mappings[_normalize_name(import_name)] = package_name
            continue

        if line.startswith("Known package: "):
            package_name = line[len("Known package: ") :].strip()
            if package_name:
                known_packages[_normalize_name(package_name)] = package_name

    return import_mappings, known_packages


def _canonical_candidate_name(
    package_name: str,
    import_name: str,
    tier2_candidates: dict[str, list[str]],
) -> str:
    normalized_target = _normalize_name(package_name)
    for candidate in tier2_candidates.get(import_name, []):
        if _normalize_name(candidate) == normalized_target:
            return candidate
    return package_name


def _seed_evidence_backed_mappings(
    req: ResolutionRequest,
    llm_imports: list[str],
) -> tuple[list[str], dict[str, str], list[str]]:
    import_mappings, known_packages = _parse_context_evidence_lines(req.context)
    deterministic: dict[str, str] = {}
    notes: list[str] = []
    remaining: list[str] = []

    for import_name in llm_imports:
        normalized_import = _normalize_name(import_name)
        package_name = ""

        if normalized_import in import_mappings:
            package_name = _canonical_candidate_name(
                import_mappings[normalized_import],
                import_name,
                req.tier2_candidates,
            )
        else:
            for candidate in req.tier2_candidates.get(import_name, []):
                if _normalize_name(candidate) == normalized_import and normalized_import in known_packages:
                    package_name = known_packages[normalized_import]
                    break

        if package_name:
            deterministic[import_name] = package_name
            notes.append(
                f"Deterministic evidence-backed mapping {import_name} -> {package_name}"
            )
        else:
            remaining.append(import_name)

    return remaining, deterministic, notes


def _build_authored_case_plan(
    *,
    req: ResolutionRequest,
    mappings: dict[str, str],
    unresolved: list[str],
    local_skips: list[str],
    framework_mappings: dict[str, str],
    deterministic_mappings: set[str],
    llm_confidence: float,
) -> AuthoredCasePlan:
    deterministic_fallback_sections: list[str] = []
    if not req.imports:
        deterministic_fallback_sections.append("no-third-party-imports")
    if local_skips:
        deterministic_fallback_sections.append("local-import-filter")
    if framework_mappings:
        deterministic_fallback_sections.append("framework-submodule-detection")
    if deterministic_mappings:
        deterministic_fallback_sections.append("evidence-backed-import-mapping")

    package_mappings: list[AuthoredPlanPackageMapping] = []
    for import_name in sorted(mappings):
        source = "llm"
        confidence = llm_confidence
        if import_name in framework_mappings:
            source = "deterministic-framework"
            confidence = 1.0
        elif import_name in deterministic_mappings:
            source = "deterministic-evidence"
            confidence = 1.0
        package_mappings.append(
            AuthoredPlanPackageMapping(
                import_name=import_name,
                package_name=mappings[import_name],
                source=source,
                confidence=round(max(0.0, min(1.0, confidence)), 2),
            )
        )

    runtime_assumptions = [f"python_version={req.python_version}"]
    if not req.imports:
        runtime_assumptions.append(
            "no third-party imports were detected, so dependency resolution is empty by design"
        )
    if local_skips:
        runtime_assumptions.append(
            "local helper imports are treated as project-local and excluded from PyPI resolution"
        )
    if framework_mappings:
        runtime_assumptions.append(
            "framework submodules are expected to resolve through their parent framework package"
        )
    if deterministic_mappings:
        runtime_assumptions.append(
            "exact import-to-package matches with benchmark evidence are resolved deterministically before LLM intake"
        )

    import_targets = sorted(
        import_name for import_name in req.imports if import_name in mappings
    )
    smoke_strategy = SmokeStrategy(
        mode="import",
        import_targets=import_targets,
        commands=[],
        rationale=(
            "Verify that the snippet's third-party imports can be imported before "
            "broader validation continues."
        ),
    )

    section_confidence = {
        "imports": 1.0,
        "package_mappings": round(max(0.0, min(1.0, llm_confidence)), 2),
        "runtime_assumptions": round(max(0.5, min(1.0, llm_confidence)), 2),
        "smoke_strategy": round(max(0.5, min(1.0, llm_confidence)), 2),
    }

    authorship = (
        "llm-authored-with-deterministic-preprocessing"
        if deterministic_fallback_sections
        else "llm-authored"
    )

    return AuthoredCasePlan(
        extracted_imports=list(req.imports),
        package_mappings=package_mappings,
        unresolved_imports=list(unresolved),
        system_dependency_hints=_system_dependency_hints(mappings),
        runtime_assumptions=runtime_assumptions,
        smoke_strategy=smoke_strategy,
        section_confidence=section_confidence,
        authorship=authorship,
        deterministic_fallback_sections=deterministic_fallback_sections,
    )


def _system_dependency_hints(mappings: dict[str, str]) -> list[str]:
    hints: list[str] = []
    known_hints = {
        "lxml": "lxml may require libxml2-dev and libxslt1-dev in Docker or env validation.",
        "mysqlclient": "mysqlclient may require default-libmysqlclient-dev during build.",
        "psycopg2": "psycopg2 may require libpq-dev during build.",
        "opencv-python": "opencv-python may require libgl1 for import smoke in slim images.",
        "opencv-python-headless": "opencv-python-headless may still require image codec libraries in some environments.",
    }
    seen_packages = {package_name.lower() for package_name in mappings.values()}
    for package_name, hint in known_hints.items():
        if package_name in seen_packages:
            hints.append(hint)
    return hints


def _build_intake_failure(
    *,
    reason: str,
    diagnostics: str,
    raw_response_preview: str = "",
) -> IntakeFailureRecord:
    diagnostic_preview = _truncate_preview(diagnostics or reason)
    return IntakeFailureRecord(
        failure_class=classify_failure_reason(diagnostics or reason),
        reason=reason,
        diagnostic_preview=diagnostic_preview,
        raw_response_preview=_truncate_preview(raw_response_preview),
        authored_plan_status="unusable",
        llm_only_behavior="fail",
    )


def _truncate_preview(value: str, limit: int = 280) -> str:
    text = str(value or "").strip().replace("\n", "\\n")
    if len(text) <= limit:
        return text
    return text[: limit - 3] + "..."
