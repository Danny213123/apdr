---
phase: 04-module-layout-and-boundary-cleanup
plan: 01
subsystem: resolver
tags:
  - apdr
  - rust
  - resolver
  - module-layout
  - reviewability
dependency_graph:
  requires: []
  provides:
    - resolver-orchestrator-facade
    - retry-loop-submodule
    - recovery-diagnostics-submodule
    - resolver-artifacts-submodule
  affects:
    - 04-02
    - 04-03
tech_stack:
  added: []
  patterns:
    - facade-plus-sibling-modules
    - resolver-debug-wrapper-stability
    - extracted-artifact-and-diagnostics-helpers
key_files:
  created:
    - tools/apdr/src/resolver/retry_loop.rs
    - tools/apdr/src/resolver/recovery_diagnostics.rs
    - tools/apdr/src/resolver/artifacts.rs
  modified:
    - tools/apdr/src/resolver/mod.rs
key-decisions:
  - Kept `resolve_path(...)` in `resolver/mod.rs` and moved the heavy retry, diagnostics, and artifact code behind sibling modules instead of changing the resolver entrypoint path.
  - Preserved the public debug helper API by leaving thin wrappers in `resolver/mod.rs` that forward into `retry_loop.rs`.
  - Let the diagnostics module own normalization, extraction, and failure-metadata helpers so the retry loop can focus on control flow and dependency mutation.
patterns-established:
  - "Large Rust orchestrators should keep the public entrypoint and delegate implementation families to sibling modules."
  - "When integration tests rely on debug helpers, preserve the outer API with wrappers during structural refactors."
requirements-completed:
  - ARCH-01
  - ARCH-02
  - ARCH-05
metrics:
  completed_date: "2026-03-27"
  tasks_completed: 3
  verification_tests: 4
---

# Phase 4 Plan 01 Summary

**Split the resolver monolith into an orchestration facade plus retry-loop, diagnostics, and artifact modules without changing resolver behavior.**

## Accomplishments

- Created [`retry_loop.rs`](D:\apdr\tools\apdr\src\resolver\retry_loop.rs), [`recovery_diagnostics.rs`](D:\apdr\tools\apdr\src\resolver\recovery_diagnostics.rs), and [`artifacts.rs`](D:\apdr\tools\apdr\src\resolver\artifacts.rs) under [`resolver`](D:\apdr\tools\apdr\src\resolver).
- Reduced [`resolver/mod.rs`](D:\apdr\tools\apdr\src\resolver\mod.rs) from `4,940` lines to `1,679` lines while keeping `resolve_path(...)` as the main resolver entrypoint.
- Moved retry-loop mutation, dependency updates, and the debug helper implementations into the retry-loop module.
- Moved failure-signature, extraction, normalization, and validation-status helpers into the diagnostics module.
- Moved parse, solver, and iteration artifact writers into the artifacts module.

## Verification Results

- `cargo test --manifest-path tools/apdr/Cargo.toml --test test_resolver resolver_ -- --nocapture` passed
- `cargo clippy --manifest-path tools/apdr/Cargo.toml --all-targets -- -D warnings` passed
- `rg -n "mod retry_loop;|mod recovery_diagnostics;|mod artifacts;" tools/apdr/src/resolver/mod.rs` passed
- `@(Get-Content tools/apdr/src/resolver/mod.rs).Count` returned `1679`

## Files Created/Modified

- [`mod.rs`](D:\apdr\tools\apdr\src\resolver\mod.rs) - now acts as the resolver facade and preserves the public debug helper surface.
- [`retry_loop.rs`](D:\apdr\tools\apdr\src\resolver\retry_loop.rs) - owns `validate_with_retries(...)`, dependency mutation helpers, and retry-loop debug helpers.
- [`recovery_diagnostics.rs`](D:\apdr\tools\apdr\src\resolver\recovery_diagnostics.rs) - owns failure extraction, normalization, LLM recovery-note handling, and validation metadata helpers.
- [`artifacts.rs`](D:\apdr\tools\apdr\src\resolver\artifacts.rs) - owns parse, solver, and iteration artifact writers plus dependency-state formatting.

## Decisions Made

- Kept the retry-loop module internal and re-exported only the debug helpers through wrappers so integration tests did not need a path change.
- Used `pub(super)` visibility for extracted implementation helpers to keep the split tight to the `resolver` module instead of widening the crate API.
- Left later skip-detection and LLM retry helpers in `resolver/mod.rs` for now because they are part of the remaining orchestration flow rather than the extracted retry core.

## Deviations from Plan

None. The resolver split landed with the planned module names and met the line-count target on the top-level file.

## Issues Encountered

- The first mechanical split dropped module-local imports; that was resolved by adding explicit imports inside the new submodules rather than relying on parent-module `use` items.
- Windows PowerShell wrote the new files with BOM-aware UTF-8, so the initial cleanup pass had to patch around encoding-sensitive file headers.
- Existing unrelated local changes in [`lib.rs`](D:\apdr\tools\apdr\src\lib.rs) and [`test_llm_integration.py`](D:\apdr\tools\apdr\llm_py\tests\test_llm_integration.py) were left untouched.

## Next Phase Readiness

- The validation builder split in `04-02` can now follow the same facade-plus-sibling-module pattern used here.
- The support-module cleanup in `04-03` can reuse the same visibility and wrapper strategy when public helper paths must remain stable.

## Self-Check: PASSED

- [`mod.rs`](D:\apdr\tools\apdr\src\resolver\mod.rs) contains `mod retry_loop;`, `mod recovery_diagnostics;`, and `mod artifacts;`
- [`retry_loop.rs`](D:\apdr\tools\apdr\src\resolver\retry_loop.rs) contains `fn validate_with_retries(`
- [`recovery_diagnostics.rs`](D:\apdr\tools\apdr\src\resolver\recovery_diagnostics.rs) contains `fn failure_signature(`
- [`artifacts.rs`](D:\apdr\tools\apdr\src\resolver\artifacts.rs) contains `fn write_iteration_snapshot(`
- All planned Wave 1 verification commands passed

---
*Phase: 04-module-layout-and-boundary-cleanup*
*Completed: 2026-03-27*
