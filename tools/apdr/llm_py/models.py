"""Pydantic data models for the APDR LLM service IPC protocol."""

from __future__ import annotations

from pydantic import BaseModel, Field


class PackageMapping(BaseModel):
    import_name: str
    package_name: str


class SmokeStrategy(BaseModel):
    mode: str = "import"
    import_targets: list[str] = Field(default_factory=list)
    commands: list[str] = Field(default_factory=list)
    rationale: str = ""


class AuthoredPlanPackageMapping(BaseModel):
    import_name: str
    package_name: str
    source: str = "llm"
    confidence: float = 0.0


class AuthoredCasePlan(BaseModel):
    plan_version: str = "1"
    extracted_imports: list[str] = Field(default_factory=list)
    package_mappings: list[AuthoredPlanPackageMapping] = Field(default_factory=list)
    unresolved_imports: list[str] = Field(default_factory=list)
    system_dependency_hints: list[str] = Field(default_factory=list)
    runtime_assumptions: list[str] = Field(default_factory=list)
    smoke_strategy: SmokeStrategy = Field(default_factory=SmokeStrategy)
    section_confidence: dict[str, float] = Field(default_factory=dict)
    authorship: str = "llm-authored"
    deterministic_fallback_sections: list[str] = Field(default_factory=list)


class IntakeFailureRecord(BaseModel):
    failure_class: str = ""
    reason: str = ""
    diagnostic_preview: str = ""
    raw_response_preview: str = ""
    authored_plan_status: str = "unusable"
    llm_only_behavior: str = "fail"


class ResolutionRequest(BaseModel):
    """JSON-line request sent from Rust to Python over stdin."""

    action: str  # "resolve" | "solvability" | "recovery" | "version" | "single"

    # Common fields
    imports: list[str] = Field(default_factory=list)
    python_version: str = "3.10"
    context: list[str] = Field(default_factory=list)
    benchmark_context: str = ""
    attribute_usage: dict[str, list[str]] = Field(default_factory=dict)
    snippet_source: str = ""

    # For recovery
    resolved_packages: list[str] = Field(default_factory=list)
    error_log: str = ""
    error_type: str = ""
    previous_attempts: list[list[str]] = Field(default_factory=list)

    # For version
    package_name: str = ""
    versions: list[str] = Field(default_factory=list)

    # For batch_version (#12)
    batch_packages: dict[str, list[str]] = Field(default_factory=dict)

    # For resolve — tier2 candidates per import
    tier2_candidates: dict[str, list[str]] = Field(default_factory=dict)

    # LLM config
    provider: str = "ollama"
    model: str = "gemma3:12b"
    base_url: str = "http://localhost:11434"
    model_override: str = ""   # per-action model override (e.g. smaller model for version)
    num_ctx_override: int = 0  # per-action context window override (0 = use default)
    agent_mode: str = "direct"
    tool_profile: str = "full"
    retrieval_profile: str = "none"
    policy_label: str = ""

    # Paths for persistence
    cache_path: str = ""
    output_dir: str = ""
    benchmark_context_log: str = ""


class ResolutionResponse(BaseModel):
    """JSON-line response sent from Python to Rust over stdout."""

    # For resolve / single
    mappings: list[PackageMapping] = Field(default_factory=list)
    unresolved: list[str] = Field(default_factory=list)
    authored_plan: AuthoredCasePlan | None = None
    authored_plan_status: str = ""
    intake_failure: IntakeFailureRecord | None = None

    # For solvability
    decision: str = ""
    confidence: float = 0.0
    reason: str = ""
    unsolvable_modules: list[str] = Field(default_factory=list)

    # For recovery
    fix_possible: bool = False
    wrong_package: str = ""
    correct_package: str = ""
    add_package: str = ""       # new transitive dep to add (e.g. "protobuf==3.20.3")
    remove_package: str = ""    # package to remove entirely (local module / wrong package)

    # For version
    version: str = ""

    # For batch_version (#12)
    batch_versions: dict[str, str] = Field(default_factory=dict)

    # Metadata
    notes: list[str] = Field(default_factory=list)
    prompts_issued: int = 0
    agent_mode: str = ""
    tool_profile: str = ""
    retrieval_profile: str = ""
    policy_label: str = ""
    abstain_reason: str = ""
    failure_reason: str = ""
    error: str = ""
