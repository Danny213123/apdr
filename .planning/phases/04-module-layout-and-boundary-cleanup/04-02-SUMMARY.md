---
phase: 04-module-layout-and-boundary-cleanup
plan: 02
subsystem: validation
tags:
  - apdr
  - rust
  - validation
  - docker
  - module-layout
dependency_graph:
  requires:
    - 04-01
  provides:
    - validation-builder-facade
    - env-backend-submodule
    - docker-backend-submodule
    - agent-backend-submodule
    - python-runtime-submodule
    - process-submodule
  affects:
    - 04-03
tech_stack:
  added: []
  patterns:
    - facade-plus-sibling-modules
    - backend-specific-ownership
    - extracted-runtime-and-process-helpers
key_files:
  created:
    - tools/apdr/src/docker/builder/mod.rs
    - tools/apdr/src/docker/builder/env_backend.rs
    - tools/apdr/src/docker/builder/docker_backend.rs
    - tools/apdr/src/docker/builder/agent_backend.rs
    - tools/apdr/src/docker/builder/python_runtime.rs
    - tools/apdr/src/docker/builder/process.rs
  modified:
    - tools/apdr/src/docker/mod.rs
key-decisions:
  - Kept `crate::docker::builder::validate_requirements(...)` stable and moved backend-specific implementation behind a directory-backed module rooted at `builder/mod.rs`.
  - Left only backend selection, summary merge behavior, and shared types in the facade so reviewers can follow the validation pipeline without paging through helper details.
  - Kept the builder test module at the facade layer because the tests cover cross-backend behavior rather than one implementation file.
patterns-established:
  - "Large validation entrypoints should stay as facades while backend and runtime helpers move into named sibling modules."
  - "Cross-backend retry behavior should be verified at the orchestration layer even after implementation moves out."
requirements-completed:
  - ARCH-01
  - ARCH-03
  - ARCH-04
  - ARCH-05
metrics:
  completed_date: "2026-03-27"
  tasks_completed: 3
  verification_tests: 4
---

# Phase 4 Plan 02 Summary

**Split the validation builder into a directory-backed facade with named backend and runtime modules while keeping validation behavior stable.**

## Accomplishments

- Replaced the flat [`builder.rs`](D:\apdr\tools\apdr\src\docker\builder.rs) file with [`builder/mod.rs`](D:\apdr\tools\apdr\src\docker\builder\mod.rs) plus focused siblings for env, Docker, validation-agent, runtime, and process responsibilities.
- Kept [`validate_requirements(...)`](D:\apdr\tools\apdr\src\docker\builder\mod.rs) as the public entrypoint while delegating env validation to [`env_backend.rs`](D:\apdr\tools\apdr\src\docker\builder\env_backend.rs), Docker retry/cleanup logic to [`docker_backend.rs`](D:\apdr\tools\apdr\src\docker\builder\docker_backend.rs), and validation-agent logic to [`agent_backend.rs`](D:\apdr\tools\apdr\src\docker\builder\agent_backend.rs).
- Moved interpreter discovery and auto-install helpers into [`python_runtime.rs`](D:\apdr\tools\apdr\src\docker\builder\python_runtime.rs) and command/process helpers into [`process.rs`](D:\apdr\tools\apdr\src\docker\builder\process.rs).
- Reduced the top-level builder orchestrator to `356` lines, well below the phase target, while preserving the validation history merge and backend retry behavior.

## Verification Results

- `cargo test --manifest-path tools/apdr/Cargo.toml validation_pipeline_ -- --nocapture` passed
- `cargo clippy --manifest-path tools/apdr/Cargo.toml --all-targets -- -D warnings` passed
- `rg -n "mod env_backend;|mod docker_backend;|mod agent_backend;|mod python_runtime;|mod process;" tools/apdr/src/docker/builder/mod.rs` passed
- `@(Get-Content tools/apdr/src/docker/builder/mod.rs).Count` returned `356`

## Files Created/Modified

- [`mod.rs`](D:\apdr\tools\apdr\src\docker\builder\mod.rs) - validation facade with backend selection, summary merge behavior, and shared types.
- [`env_backend.rs`](D:\apdr\tools\apdr\src\docker\builder\env_backend.rs) - env validation, validated-env cache handling, and attempt metadata helpers.
- [`docker_backend.rs`](D:\apdr\tools\apdr\src\docker\builder\docker_backend.rs) - Docker validation, fallback eligibility checks, cleanup, and backend-unavailable reporting.
- [`agent_backend.rs`](D:\apdr\tools\apdr\src\docker\builder\agent_backend.rs) - validation-agent execution, probe caching, and result parsing.
- [`python_runtime.rs`](D:\apdr\tools\apdr\src\docker\builder\python_runtime.rs) - interpreter discovery, launcher handling, and auto-install support.
- [`process.rs`](D:\apdr\tools\apdr\src\docker\builder\process.rs) - command execution, timeout handling, install helpers, and repository cataloging.

## Decisions Made

- Used `pub(super)` boundaries inside the builder directory so helpers stayed internal to the validation subsystem instead of widening the crate API.
- Kept the builder tests in the facade module because they exercise cache-source detection, retry ordering, and probe caching across submodules.
- Accepted a small cleanup pass after the mechanical split to fix misplaced attributes and doc comments rather than hand-copying the entire file upfront.

## Deviations from Plan

None. The split landed with the planned module names and preserved the public entrypoint path.

## Issues Encountered

- The first extraction pass misplaced one `#[allow(clippy::too_many_arguments)]` attribute and a stray doc comment, which Clippy caught immediately.
- Git diff stat initially showed only the deletion of the old file until the new builder directory was staged, so verification relied on direct file checks rather than diff summary alone.
- Existing unrelated local changes in [`lib.rs`](D:\apdr\tools\apdr\src\lib.rs) and [`test_llm_integration.py`](D:\apdr\tools\apdr\llm_py\tests\test_llm_integration.py) were left untouched.

## Next Phase Readiness

- The support-heavy resolver modules in `04-03` can now reuse the same facade-plus-sibling-module pattern used in both Waves 1 and 2.
- Full-phase verification can now review resolver and validation boundaries as separate concerns instead of navigating two monolithic files.

## Self-Check: PASSED

- [`mod.rs`](D:\apdr\tools\apdr\src\docker\builder\mod.rs) contains `mod env_backend;`, `mod docker_backend;`, `mod agent_backend;`, `mod python_runtime;`, and `mod process;`
- [`env_backend.rs`](D:\apdr\tools\apdr\src\docker\builder\env_backend.rs) contains `fn validate_requirements_env(`
- [`docker_backend.rs`](D:\apdr\tools\apdr\src\docker\builder\docker_backend.rs) contains `fn validate_requirements_docker(`
- [`agent_backend.rs`](D:\apdr\tools\apdr\src\docker\builder\agent_backend.rs) contains `fn attempt_langgraph_agent(`
- All planned Wave 2 verification commands passed

---
*Phase: 04-module-layout-and-boundary-cleanup*
*Completed: 2026-03-27*
