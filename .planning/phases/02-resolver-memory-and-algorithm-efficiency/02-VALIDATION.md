---
phase: 02
slug: resolver-memory-and-algorithm-efficiency
status: planned
nyquist_compliant: true
wave_0_complete: true
created: 2026-03-27
---

# Phase 2 - Validation Strategy

> Validation contract for resolver ownership cleanup, retry-loop simplification, and bounded before/after benchmark comparison.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | `cargo test`, `cargo clippy`, and the Phase 1 benchmark/regression scripts |
| **Config file** | `tools/apdr/Cargo.toml` |
| **Quick run command** | `cargo test --manifest-path tools/apdr/Cargo.toml --test test_resolver pre_solver_ -- --nocapture` |
| **Full suite command** | `cargo test --manifest-path tools/apdr/Cargo.toml --test test_resolver && cargo clippy --manifest-path tools/apdr/Cargo.toml --all-targets -- -D warnings` |
| **Phase comparison command** | `python scripts/check_apdr_regression.py --baseline .planning/phases/01-baseline-and-guardrails/01-baseline.json --candidate .planning/phases/02-resolver-memory-and-algorithm-efficiency/02-resolver-candidate.json` |
| **Estimated runtime** | ~2-4 minutes for the Rust suite; longer only when generating the candidate baseline artifact |

---

## Sampling Rate

- **After every task commit:** Run the quick command plus the task-specific verify command from the active plan
- **After every plan wave:** Run the full suite command
- **Before `$gsd-verify-work`:** Generate `02-resolver-candidate.json`, run the phase comparison command, and confirm the delta doc references the same sample set as the candidate artifact
- **Max feedback latency:** keep code-path feedback under 90 seconds; only the bounded candidate benchmark may exceed that

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|-----------|-------------------|-------------|--------|
| 02-01-01 | 01 | 1 | EFF-02 | targeted Rust tests | `cargo test --manifest-path tools/apdr/Cargo.toml --test test_resolver pre_solver_ -- --nocapture` | yes | pending |
| 02-01-02 | 01 | 1 | EFF-03 | targeted Rust tests | `cargo test --manifest-path tools/apdr/Cargo.toml --test test_resolver pre_solver_ -- --nocapture` | yes | pending |
| 02-01-03 | 01 | 1 | EFF-05 | artifact + behavior verification | `rg -n "PythonSolveAttempt|persist_dependency_specs|pre_solver_preserves_candidate_order_without_mutex_aggregation" tools/apdr/src/resolver/pre_solve.rs tools/apdr/src/resolver/pypi_client.rs tools/apdr/tests/test_resolver.rs` | yes | pending |
| 02-02-01 | 02 | 2 | EFF-01 | targeted Rust tests | `cargo test --manifest-path tools/apdr/Cargo.toml --test test_resolver resolver_ -- --nocapture` | yes | pending |
| 02-02-02 | 02 | 2 | EFF-04 | lint + targeted Rust tests | `cargo clippy --manifest-path tools/apdr/Cargo.toml --all-targets -- -D warnings && cargo test --manifest-path tools/apdr/Cargo.toml --test test_resolver resolver_ -- --nocapture` | yes | pending |
| 02-02-03 | 02 | 2 | EFF-05 | artifact + behavior verification | `rg -n "RetryLoopState|render_requirements_if_dirty|dependency_index_by_package" tools/apdr/src/resolver/mod.rs` | yes | pending |
| 02-03-01 | 03 | 3 | EFF-05 | bounded benchmark capture | `python scripts/measure_apdr_baseline.py --fixtures-root tools/apdr/tests/fixtures --limit 3 --validation-backend env --output-json .planning/phases/02-resolver-memory-and-algorithm-efficiency/02-resolver-candidate.json --output-md .planning/phases/02-resolver-memory-and-algorithm-efficiency/02-RESOLVER-CANDIDATE.md` | no | pending |
| 02-03-02 | 03 | 3 | EFF-05 | regression gate | `python scripts/check_apdr_regression.py --baseline .planning/phases/01-baseline-and-guardrails/01-baseline.json --candidate .planning/phases/02-resolver-memory-and-algorithm-efficiency/02-resolver-candidate.json` | no | pending |

---

## Wave 0 Requirements

- Existing Rust test infrastructure is already present from Phase 1 and the repo baseline.
- No new framework install is required before execution.
- The phase must reuse the bounded fixture sample already committed in Phase 1 rather than inventing a new comparison sample.

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| The written delta explains any remaining validation-heavy noise separately from resolver hot-path improvements | EFF-05 | The comparison script reports numeric deltas, but a reviewer still needs a short explanation of host-specific noise such as Windows Docker permission limits | Read `02-RESOLVER-DELTA.md`, confirm it cites the Phase 1 baseline file and the new candidate artifact, and verify the narrative does not claim resolver wins from unrelated Docker or host-runtime behavior |

---

## Validation Sign-Off

- [x] All planned tasks have an automated verify step or explicit artifact check
- [x] Sampling continuity reuses the committed Phase 1 baseline instead of changing the benchmark target
- [x] Existing infrastructure covers the phase
- [x] No watch-mode commands are required
- [x] Quick feedback latency stays bounded for code changes
- [x] `nyquist_compliant: true` is set in frontmatter

**Approval:** passed
