from __future__ import annotations

from typing import Optional, TypedDict


class AgentState(TypedDict, total=False):
    # --- Input (set once at start) ---
    snippet_path: str
    snippet_source: str
    requirements_txt: str
    imports: list[str]
    python_version: str
    candidate_versions: list[str]
    config: dict  # llm_provider, llm_model, llm_base_url, cache_path, output_dir, …

    # --- Confidence assessment ---
    confidence: float
    confidence_reason: str

    # --- Build state ---
    system_deps: list[str]
    dockerfile_content: str
    image_tag: str
    work_dir: str

    # --- Attempt tracking ---
    attempt: int
    max_attempts: int  # default 5
    current_python_idx: int
    attempts_log: list[dict]

    # --- Error state (set by Analyst) ---
    build_log: str
    run_log: str
    error_type: str  # VersionNotFound, DependencyConflict, ModuleNotFound, BuildFailure, …
    error_package: str
    error_detail: str

    # --- Recovery state (set by Recovery Agent) ---
    fix_action: str  # change_version, add_package, remove_package, add_system_dep, try_next_python, give_up
    fix_package: str
    fix_version: Optional[str]
    fix_system_deps: list[str]
    fix_reason: str

    # --- Recovery history (for RAG context) ---
    tried_versions: dict  # {package: [versions_tried]}
    tried_packages: list[str]

    # --- Result ---
    status: str  # pending, passed, failed, skipped
    selected_python_version: Optional[str]
    final_requirements: Optional[str]
