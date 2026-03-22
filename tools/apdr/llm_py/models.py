"""Pydantic data models for the APDR LLM service IPC protocol."""

from __future__ import annotations

from pydantic import BaseModel, Field


class PackageMapping(BaseModel):
    import_name: str
    package_name: str


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

    # For resolve — tier2 candidates per import
    tier2_candidates: dict[str, list[str]] = Field(default_factory=dict)

    # LLM config
    provider: str = "ollama"
    model: str = "gemma3:12b"
    base_url: str = "http://localhost:11434"

    # Paths for persistence
    cache_path: str = ""
    output_dir: str = ""
    benchmark_context_log: str = ""


class ResolutionResponse(BaseModel):
    """JSON-line response sent from Python to Rust over stdout."""

    # For resolve / single
    mappings: list[PackageMapping] = Field(default_factory=list)
    unresolved: list[str] = Field(default_factory=list)

    # For solvability
    decision: str = ""
    confidence: float = 0.0
    reason: str = ""
    unsolvable_modules: list[str] = Field(default_factory=list)

    # For recovery
    fix_possible: bool = False
    wrong_package: str = ""
    correct_package: str = ""

    # For version
    version: str = ""

    # Metadata
    notes: list[str] = Field(default_factory=list)
    prompts_issued: int = 0
    error: str = ""
