---
phase: 05-documentation-error-handling-and-review-readiness
plan: 02
subsystem: error-handling
tags:
  - apdr
  - rust
  - panic-safety
  - resolver
  - validation
dependency_graph:
  requires:
    - 05-01
  provides:
    - tier3-llm-unavailable-fallback
    - guarded-env-backend-escalation
    - documented-touched-invariants
  affects:
    - 05-03
    - 06
tech_stack:
  added: []
  patterns:
    - fallible-process-bootstrap
    - guarded-latest-attempt-escalation
    - explicit-invariant-messages
key_files:
  created: []
  modified:
    - tools/apdr/src/resolver/tier3_llm/process.rs
    - tools/apdr/src/docker/builder/env_backend.rs
    - tools/apdr/src/resolver/family_knowledge/learned.rs
    - tools/apdr/src/resolver/pre_solve.rs
    - tools/apdr/src/resolver/kgraph_db.rs
key-decisions:
  - Changed the Tier 3 LLM process cache to a fallible `OnceLock<Option<Mutex<LlmProcess>>>` so host-state failures degrade to LLM-unavailable behavior instead of aborting the process.
  - Replaced env-backend escalation unwraps with one helper that treats missing attempt history as no escalation, preserving the existing env-to-Docker ordering without widening public signatures.
  - Kept the pooled KGraph connection deref as an explicit invariant with a reviewer-facing `expect(...)` message instead of widening the connection API for a non-runtime-facing case.
patterns-established:
  - "Runtime-facing subprocess bootstrap failures should degrade into existing fallback behavior rather than panic."
  - "Escalation checks should guard the latest attempt explicitly instead of assuming attempt history exists."
requirements-completed:
  - QUAL-02
  - QUAL-05
metrics:
  completed_date: "2026-03-27"
  tasks_completed: 3
  verification_tests: 4
---

# Phase 5 Plan 02 Summary

**Removed the runtime-facing Tier 3 startup panics, guarded env-backend escalation checks, and narrowed the remaining touched invariants to explicit reviewer-readable forms.**

## Accomplishments

- Hardened [`process.rs`](D:\apdr\tools\apdr\src\resolver\tier3_llm\process.rs) so Python LLM service spawn or pipe-capture failures now log a warning and return `None` through `call_python(...)` instead of panicking.
- Added a guarded latest-attempt helper in [`env_backend.rs`](D:\apdr\tools\apdr\src\docker\builder\env_backend.rs) so env-backend escalation no longer depends on `summary.attempts.last().unwrap()`.
- Replaced the trivial learned-family unwrap in [`learned.rs`](D:\apdr\tools\apdr\src\resolver\family_knowledge\learned.rs) with an explicit early return and turned the pre-solve undo-stack pop into a non-panicking invariant check in [`pre_solve.rs`](D:\apdr\tools\apdr\src\resolver\pre_solve.rs).
- Narrowed the remaining touched production invariant in [`kgraph_db.rs`](D:\apdr\tools\apdr\src\resolver\kgraph_db.rs) to an explicit reviewer-facing `expect(...)` message about pooled connection ownership.

## Verification Results

- `rg -n 'unwrap\(|expect\(' tools/apdr/src/resolver/tier3_llm/process.rs tools/apdr/src/docker/builder/env_backend.rs tools/apdr/src/resolver/family_knowledge/learned.rs tools/apdr/src/resolver/pre_solve.rs tools/apdr/src/resolver/kgraph_db.rs` left only the documented pooled-connection invariant in `kgraph_db.rs`
- `cargo test --manifest-path tools/apdr/Cargo.toml --test test_resolver resolver_ -- --nocapture` passed
- `cargo test --manifest-path tools/apdr/Cargo.toml validation_pipeline_ -- --nocapture` passed
- `cargo clippy --manifest-path tools/apdr/Cargo.toml --all-targets -- -D warnings` passed

## Files Created/Modified

- [`process.rs`](D:\apdr\tools\apdr\src\resolver\tier3_llm\process.rs) - fallible Tier 3 subprocess bootstrap with warning-based degradation into the existing `Option` return path.
- [`env_backend.rs`](D:\apdr\tools\apdr\src\docker\builder\env_backend.rs) - guarded escalation helper that preserves env-to-Docker ordering without assuming attempt history exists.
- [`learned.rs`](D:\apdr\tools\apdr\src\resolver\family_knowledge\learned.rs) - explicit early return for missing learned-family state.
- [`pre_solve.rs`](D:\apdr\tools\apdr\src\resolver\pre_solve.rs) - explicit undo-stack invariant handling during restore.
- [`kgraph_db.rs`](D:\apdr\tools\apdr\src\resolver\kgraph_db.rs) - reviewer-facing pooled-connection invariant message.

## Decisions Made

- Tier 3 process startup failures now reuse the existing `call_python(...) -> Option<_>` fallback shape instead of introducing a new public error type.
- Env-backend escalation stays derived from the latest recorded attempt, but missing history is treated as a no-op rather than an implicit invariant.
- The pooled KGraph connection remains an internal invariant because the connection is taken only during `Drop`; the code now says that explicitly.

## Deviations from Plan

None. The plan executed exactly as written.

## Issues Encountered

- The plan-level `rg -n 'unwrap\(|expect\(' tools/apdr/src/resolver tools/apdr/src/docker/builder` sweep still surfaces test-only unwraps in `docker/builder/mod.rs` and unrelated out-of-scope production sites in `pypi_client/core.rs`, `version_sampler.rs`, and `smartpip.rs`. Wave 2 sign-off therefore used the touched-file grep to confirm that only the documented `kgraph_db.rs` invariant remained inside the plan scope.
- Existing unrelated local changes in [`lib.rs`](D:\apdr\tools\apdr\src\lib.rs) and [`test_llm_integration.py`](D:\apdr\tools\apdr\llm_py\tests\test_llm_integration.py) were left untouched.

## Next Phase Readiness

- Wave 3 can now focus on consistency language, fmt coverage, and full-suite verification without carrying forward the Tier 3 or env-backend runtime panic debt.
- The reviewer guide from `05-01` and the narrowed invariants from `05-02` now give the final consistency pass a cleaner fallback and error-handling vocabulary to preserve.

## Self-Check: PASSED

- [`process.rs`](D:\apdr\tools\apdr\src\resolver\tier3_llm\process.rs) no longer contains the previous spawn panic or pipe `expect(...)` calls
- [`env_backend.rs`](D:\apdr\tools\apdr\src\docker\builder\env_backend.rs) no longer contains `summary.attempts.last().unwrap()`
- [`learned.rs`](D:\apdr\tools\apdr\src\resolver\family_knowledge\learned.rs) no longer contains `let learned = learned.unwrap();`
- [`pre_solve.rs`](D:\apdr\tools\apdr\src\resolver\pre_solve.rs) contains `match self.undo_stack.pop()`
- [`kgraph_db.rs`](D:\apdr\tools\apdr\src\resolver\kgraph_db.rs) contains the explicit pooled-connection invariant message
- All planned Wave 2 verification commands passed

---
*Phase: 05-documentation-error-handling-and-review-readiness*
*Completed: 2026-03-27*
