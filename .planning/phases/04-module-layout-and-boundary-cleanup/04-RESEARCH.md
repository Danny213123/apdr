# Phase 4: Module Layout & Boundary Cleanup - Research

**Researched:** 2026-03-27
**Domain:** APDR Rust module decomposition, ownership boundaries, reviewability, and structural guardrails
**Confidence:** Medium

## Summary

Phase 4 should convert the largest Rust hotspots from monolithic files into directory-backed modules without changing the benchmark behavior established in Phases 2 and 3. The current evidence is clear: `tools/apdr/src/resolver/mod.rs` is now `4,940` lines, `tools/apdr/src/docker/builder.rs` is `3,218` lines, `tools/apdr/src/resolver/family_knowledge.rs` is `2,235` lines, `tools/apdr/src/resolver/pypi_client.rs` is `1,393` lines, and `tools/apdr/src/resolver/tier3_llm.rs` is `1,264` lines. Those files combine orchestration, recovery logic, diagnostics, persistence helpers, and backend-specific details in a way that makes code review and change isolation harder than it should be.

Primary recommendation: plan the phase as three sequential structural refactors. First, split `resolver/mod.rs` into an orchestrator plus focused submodules for retry-loop mutation, recovery diagnostics, and artifact writing while keeping `resolve_path(...)` stable. Second, convert `docker/builder.rs` into a directory-backed builder module with explicit backend, runtime, and process helpers while keeping `validate_requirements(...)` stable. Third, decompose the remaining support-heavy resolver modules so family bundles, PyPI access, and Tier 3 LLM plumbing each have clearer internal boundaries and names.

## Phase Requirements

| ID | Requirement | Research Support |
|----|-------------|------------------|
| ARCH-01 | Oversized Rust modules are split into smaller files with coherent responsibilities | The five largest Rust files in the active path all exceed `1,200` lines, and two exceed `3,000`, which is enough evidence to justify deliberate file extraction instead of more local edits. |
| ARCH-02 | Public and internal APIs between Rust modules are easier to follow and less entangled | `resolve_path(...)` and `validate_requirements(...)` are the right stable entrypoints; most other helpers can move behind submodules without changing callers. |
| ARCH-03 | Complex recovery and validation logic is extracted behind named helpers or submodules instead of giant functions | `apply_recovery_fix(...)`, `validate_with_retries(...)`, `validate_requirements_env(...)`, and `validate_requirements_docker(...)` already reveal clean extraction seams. |
| ARCH-04 | File and module naming better reflects responsibility and ownership boundaries | Current filenames mix orchestration, backends, persistence, IPC, version logic, and bundle definitions in single files. |
| ARCH-05 | Refactors reduce cognitive load for code review on the most active Rust areas | Reviewers currently have to scan thousands of lines to follow retry loops, failure classification, backend escalation, or legacy family bundles. |

## Evidence That Should Drive Planning

### File-size and hotspot evidence

- `tools/apdr/src/resolver/mod.rs`: `4,940` lines and `203` `.clone(` calls
- `tools/apdr/src/docker/builder.rs`: `3,218` lines and `19` `.clone(` calls
- `tools/apdr/src/resolver/family_knowledge.rs`: `2,235` lines
- `tools/apdr/src/resolver/pypi_client.rs`: `1,393` lines
- `tools/apdr/src/resolver/tier3_llm.rs`: `1,264` lines

Phase 1 already captured these areas as the benchmark-critical Rust hotspots. Phases 2 and 3 improved performance behavior inside them, but they did not materially reduce review surface area. That makes Phase 4 a structural cleanup phase, not another algorithm phase.

### Resolver split points already visible in code

- `resolve_path(...)` at the top of `resolver/mod.rs` should remain the main public orchestration entrypoint.
- `validate_with_retries(...)`, `RetryLoopState`, `update_package_version(...)`, `ensure_dependency(...)`, `try_build_failure_alternatives(...)`, and `upsert_dependency(...)` form a retry-mutation cluster that can move together.
- `failure_signature(...)`, `apply_llm_recovery_hint(...)`, `extract_missing_module(...)`, `extract_build_dependency(...)`, and related `extract_*` helpers form a recovery-diagnostics cluster.
- `write_parse_artifacts(...)`, `write_solver_artifacts(...)`, `write_iteration_snapshot(...)`, and formatting helpers form an artifact-writing cluster.

### Validation builder split points already visible in code

- `validate_requirements(...)` should remain the stable entrypoint from callers.
- `validate_requirements_env(...)`, `prepare_env_validation_attempt(...)`, `materialize_env_for_attempt(...)`, and `create_and_install_env(...)` form the env-backend cluster.
- `validate_requirements_docker(...)`, `cleanup_docker_image(...)`, `cleanup_docker_dangling(...)`, and Docker retry helpers form the Docker-backend cluster.
- `attempt_langgraph_agent(...)`, `docker_agent_importable(...)`, `run_docker_agent_import_probe(...)`, and `parse_agent_result(...)` form the agent-backend cluster.
- `find_python_interpreter(...)`, `ensure_python_interpreter(...)`, `attempt_python_auto_install(...)`, and interpreter-path helpers form the runtime-resolution cluster.
- `run_command_with_timeout(...)`, `combined_output(...)`, `truncate_log(...)`, and command-install helpers form the process/command cluster.

### Support-module split points

- `resolver/family_knowledge.rs` mixes detection (`uses_legacy_*`), bundle application (`apply_*_bundle`), bundle definitions (`legacy_*_bundle`), and learned-family persistence (`load_*`, `save_*`, `add_*`).
- `resolver/pypi_client.rs` mixes cache persistence, version-constraint math, SmartPip server orchestration, and host-Python process discovery.
- `resolver/tier3_llm.rs` mixes process spawning, request context assembly, failure-memory persistence, and public resolution entrypoints.

## Implementation Recommendations

### 1. Keep top-level public entrypoints stable

Phase 4 should not force broad caller churn. Keep these entrypoints where downstream code already expects them:

- `resolver::resolve_path(...)`
- `docker::builder::validate_requirements(...)`
- public `family_knowledge`, `pypi_client`, and `tier3_llm` functions already used outside their files

The top-level modules should become orchestration facades that delegate to smaller sibling modules.

### 2. Prefer directory-backed modules over renamed flat files

For the largest files, the cleanest layout is:

- `tools/apdr/src/resolver/mod.rs`
- `tools/apdr/src/resolver/retry_loop.rs`
- `tools/apdr/src/resolver/recovery_diagnostics.rs`
- `tools/apdr/src/resolver/artifacts.rs`
- `tools/apdr/src/docker/builder/mod.rs`
- `tools/apdr/src/docker/builder/env_backend.rs`
- `tools/apdr/src/docker/builder/docker_backend.rs`
- `tools/apdr/src/docker/builder/agent_backend.rs`
- `tools/apdr/src/docker/builder/python_runtime.rs`
- `tools/apdr/src/docker/builder/process.rs`

That keeps import paths legible and avoids inventing ambiguous names.

### 3. Use Phase 4 to lower cognitive load, not to rewrite behavior

Each split should preserve existing tests before any optional cleanup. Recommended guardrail:

- move code with the smallest behavior delta first
- keep function signatures stable until after the file split lands
- only rename helpers when the new module boundary makes the old name actively misleading

### 4. Add structural verification, not just behavioral verification

Phase 4 needs more than tests. The phase should also prove that:

- the targeted top-level files shrink materially
- the extracted modules exist with the planned names
- entrypoint modules read as orchestration layers instead of implementation dumps

Simple `rg` and line-count checks are enough for this phase and make review easier.

## Validation Architecture

### Quick checks

- `cargo test --manifest-path tools/apdr/Cargo.toml test_resolver -- --nocapture`
- `cargo test --manifest-path tools/apdr/Cargo.toml validation_pipeline_ -- --nocapture`
- `cargo clippy --manifest-path tools/apdr/Cargo.toml --all-targets -- -D warnings`

### Structural checks

- `rg -n "^mod |^pub\\(crate\\) mod |^pub mod " tools/apdr/src/resolver tools/apdr/src/docker`
- `rg -n "resolve_path|validate_requirements|apply_family_knowledge|fetch_versions|resolve_with_context" tools/apdr/src/resolver tools/apdr/src/docker`
- line-count checks on the top-level orchestrator files to confirm they shrink after extraction

### Phase-close checks

- a Phase 4 layout note should summarize which clusters moved into which files
- Phase 1 and Phase 3 artifacts remain the benchmark reference; Phase 4 does not claim performance gains unless measured

## Canonical Files For Planning

- `.planning/PROJECT.md`
- `.planning/ROADMAP.md`
- `.planning/REQUIREMENTS.md`
- `.planning/STATE.md`
- `.planning/codebase/CONCERNS.md`
- `.planning/phases/01-baseline-and-guardrails/01-HOTSPOT-AUDIT.md`
- `.planning/phases/02-resolver-memory-and-algorithm-efficiency/02-RESOLVER-DELTA.md`
- `.planning/phases/03-validation-pipeline-throughput/03-VALIDATION-DELTA.md`
- `tools/apdr/src/lib.rs`
- `tools/apdr/src/resolver/mod.rs`
- `tools/apdr/src/docker/builder.rs`
- `tools/apdr/src/resolver/family_knowledge.rs`
- `tools/apdr/src/resolver/pypi_client.rs`
- `tools/apdr/src/resolver/tier3_llm.rs`

## Out-of-Scope For This Phase

- new benchmark claims that are not measured against the existing baseline artifacts
- broad algorithm changes already reserved for Phase 2
- documentation and panic-path hardening reserved for Phase 5
- changing the benchmark dataset, sample rule, or milestone success metrics
- reworking the Python LLM bridge protocol beyond structural extraction needed for module clarity

---
*Research created: 2026-03-27*
*Phase: 04-module-layout-and-boundary-cleanup*
