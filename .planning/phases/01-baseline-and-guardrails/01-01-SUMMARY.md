---
phase: 01-baseline-and-guardrails
plan: 01
subsystem: infra
tags:
  - apdr
  - benchmarking
  - profiling
  - regression-baseline
dependency_graph:
  requires: []
  provides:
    - deterministic-baseline-harness
    - memory-profile-wrapper
    - pre-optimization-baseline-artifacts
    - cli-summary-contract-guard
  affects:
    - phase-02-resolver-memory-and-algorithm-efficiency
    - phase-03-validation-pipeline-throughput
tech_stack:
  added:
    - python-cli-scripts
    - serde-derive
  patterns:
    - output-data-contract-reuse
    - wrapper-based-memory-capture
    - committed-baseline-artifacts
key_files:
  created:
    - scripts/measure_apdr_baseline.py
    - scripts/profile_apdr_memory.py
    - .planning/phases/01-baseline-and-guardrails/01-baseline.json
    - .planning/phases/01-baseline-and-guardrails/01-memory-profile.json
    - .planning/phases/01-baseline-and-guardrails/01-BASELINE.md
  modified:
    - tools/apdr/Cargo.toml
    - tools/apdr/Cargo.lock
    - tools/apdr/src/resolver/family_knowledge.rs
    - tools/apdr/tests/test_cli.rs
key-decisions:
  - Reused APDR's existing output_data_<python>.yml contract instead of inventing a second benchmark output format.
  - Captured memory through a wrapper around test_executor.py so Phase 1 avoids temporary Rust instrumentation.
  - Kept the initial baseline sample deterministic by lexicographic fixture ordering with a hard limit of three cases.
patterns-established:
  - "Baseline harnesses should emit machine-readable JSON plus a concise Markdown snapshot."
  - "Representative memory measurements should be recorded alongside the exact APDR command that produced them."
requirements-completed:
  - BASE-01
  - BASE-02
metrics:
  duration_seconds: 1500
  completed_date: "2026-03-26"
  tasks_completed: 3
  baseline_sample_count: 3
  pass_rate_percent: 33.33
  peak_rss_bytes: 19595264
---

# Phase 1 Plan 01 Summary

**Deterministic APDR baseline capture, cross-platform memory profiling, and committed pre-optimization artifacts for the v2.0 modernization milestone.**

## Performance

- **Duration:** ~25 min
- **Started:** 2026-03-26T23:31:00-04:00
- **Completed:** 2026-03-26T23:56:09-04:00
- **Tasks:** 3
- **Files modified:** 9

## Accomplishments

- Added `scripts/measure_apdr_baseline.py` to run deterministic APDR samples and aggregate timing, pass-rate, retry, and LLM-call metrics from `output_data_<python>.yml`.
- Added `scripts/profile_apdr_memory.py` to capture `peak_rss_bytes` for a representative APDR run without instrumenting the Rust core.
- Committed the first v2.0 baseline artifacts: `01-baseline.json`, `01-memory-profile.json`, and `01-BASELINE.md`.

## Baseline Results

- Sample rule: first 3 fixture snippets in lexicographic order from `tools/apdr/tests/fixtures`
- Status totals: 1 passed, 1 failed, 1 skipped
- Pass rate: 33.33%
- Solve duration: 553 ms total
- Validation duration: 40,237 ms total
- Install duration: 1,077 ms total
- Peak memory: 19,595,264 bytes on `tools/apdr/tests/fixtures/sample_snippet.py`

## Task Commits

Each task was committed atomically:

1. **Task 1: Add the deterministic baseline harness and protect the summary contract** - `468403e` (`feat(01-01): add baseline harness and cli contract guard`)
2. **Task 2: Add the cross-platform memory capture wrapper** - `a02694d` (`feat(01-01): add cross-platform memory profile wrapper`)
3. **Task 3: Capture and document the initial baseline artifacts** - `f957892` (`docs(01-01): capture baseline and memory artifacts`)

## Files Created/Modified

- `scripts/measure_apdr_baseline.py` - Runs bounded APDR samples and emits aggregate JSON/Markdown outputs.
- `scripts/profile_apdr_memory.py` - Captures a representative `peak_rss_bytes` measurement for APDR executions.
- `.planning/phases/01-baseline-and-guardrails/01-baseline.json` - Current baseline totals and per-sample metrics.
- `.planning/phases/01-baseline-and-guardrails/01-memory-profile.json` - Representative memory snapshot for the phase.
- `.planning/phases/01-baseline-and-guardrails/01-BASELINE.md` - Human-readable baseline snapshot for milestone v2.0.
- `tools/apdr/tests/test_cli.rs` - Locks the CLI summary fields consumed by the new harness.
- `tools/apdr/src/resolver/family_knowledge.rs` - Restored buildability for the learned-family path so APDR could execute.
- `tools/apdr/Cargo.toml` - Added `serde` derive support needed by the learned-family structs.
- `tools/apdr/Cargo.lock` - Captured the dependency graph update for the serde addition.

## Decisions Made

- Reused `test_executor.py` and the existing YAML output contract rather than duplicating benchmark parsing logic elsewhere.
- Kept the first baseline intentionally small and deterministic so later phases can rerun it quickly.
- Accepted that Phase 1 should record environment-specific failures in the baseline rather than filtering them out.

## Deviations from Plan

### Auto-fixed Issues

**1. Build blocker in learned-family persistence path**
- **Found during:** Task 1 verification (`cargo test --manifest-path tools/apdr/Cargo.toml --test test_cli`)
- **Issue:** `family_knowledge.rs` referenced `serde`, `lazy_static`, and `chrono` without working crate wiring, and the member-update logic failed the borrow checker.
- **Fix:** Added `serde` derive support in `Cargo.toml`, switched the global family cache to `once_cell::sync::Lazy`, replaced timestamp formatting with a stdlib helper, and made member-package checks own their package names before mutation.
- **Files modified:** `tools/apdr/Cargo.toml`, `tools/apdr/Cargo.lock`, `tools/apdr/src/resolver/family_knowledge.rs`
- **Verification:** `cargo test --manifest-path tools/apdr/Cargo.toml --test test_cli`
- **Committed in:** `468403e`

---

**Total deviations:** 1 auto-fixed build blocker
**Impact on plan:** Necessary to make APDR executable again; no scope expansion beyond restoring the planned verification path.

## Issues Encountered

- `cfscrape_snippet.py` failed on this Windows host after env validation escalated to Docker because `C:\Users\danny\.docker\buildx\instances` was inaccessible. The failure is preserved in the baseline as real machine evidence, not normalized away.
- `apple_private_framework_snippet.py` was correctly recorded as a host-runtime skip because it requires macOS Objective-C frameworks.

## User Setup Required

None - no external service configuration required for this plan's committed outputs.

## Next Phase Readiness

- `01-baseline.json` and `01-memory-profile.json` are ready for Plan `01-02` to consume.
- Phase 2 and Phase 3 now have a concrete before-state for timing, pass rate, and representative memory.
- The current baseline includes one environment-specific Docker permission failure on Windows, so later comparisons should interpret that case as host evidence rather than a general APDR correctness regression.

## Self-Check: PASSED

- `python -m py_compile scripts/measure_apdr_baseline.py scripts/profile_apdr_memory.py` passed
- `cargo test --manifest-path tools/apdr/Cargo.toml --test test_cli` passed
- `01-baseline.json` contains the required timing keys
- `01-memory-profile.json` contains `peak_rss_bytes`
- `01-BASELINE.md` contains the milestone baseline narrative and measured outputs

---
*Phase: 01-baseline-and-guardrails*
*Completed: 2026-03-26*
