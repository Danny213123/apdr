---
phase: 01-baseline-and-guardrails
plan: 02
subsystem: infra
tags:
  - apdr
  - regression-gates
  - hotspot-audit
  - documentation
dependency_graph:
  requires:
    - phase: 01-01
      provides: deterministic-baseline-harness
  provides:
    - baseline-regression-gate
    - hotspot-priority-audit
    - modernization-guardrail-command-set
  affects:
    - phase-02-resolver-memory-and-algorithm-efficiency
    - phase-03-validation-pipeline-throughput
tech_stack:
  added:
    - json-regression-checker
  patterns:
    - threshold-based-regression-gating
    - evidence-backed-hotspot-ranking
    - repo-root-guardrail-commands
key_files:
  created:
    - scripts/check_apdr_regression.py
    - .planning/phases/01-baseline-and-guardrails/01-HOTSPOT-AUDIT.md
  modified:
    - tools/apdr/README.md
key-decisions:
  - Kept the regression gate JSON-first so later phases compare the same committed baseline artifact directly.
  - Ranked hotspots with a mix of baseline timing, representative memory, and static complexity signals instead of static code size alone.
  - Published repo-root commands in the APDR README so future phases have one canonical checklist.
patterns-established:
  - "Candidate benchmark results should be compared against `01-baseline.json` before claiming wins."
  - "Hotspot prioritization should cite measured baseline evidence plus at least one static complexity indicator."
requirements-completed:
  - BASE-03
  - BASE-04
  - BASE-05
metrics:
  duration_seconds: 900
  completed_date: "2026-03-27"
  tasks_completed: 3
  hotspot_count: 5
  regression_gate_smoke_passed: true
---

# Phase 1 Plan 02 Summary

**Threshold-based regression gating, evidence-backed hotspot ranking, and a published modernization command set for the remaining Rust optimization phases.**

## Performance

- **Duration:** ~15 min
- **Started:** 2026-03-26T23:47:00-04:00
- **Completed:** 2026-03-27T00:02:54-04:00
- **Tasks:** 3
- **Files modified:** 3

## Accomplishments

- Added `scripts/check_apdr_regression.py` so candidate runs can be checked against the committed Phase 1 baseline with explicit pass-rate and duration thresholds.
- Wrote `01-HOTSPOT-AUDIT.md` to rank the next five Rust targets using baseline timing, representative memory, and static complexity signals.
- Added a `Modernization guardrails` section to `tools/apdr/README.md` so future phases have one canonical command set for fmt, clippy, tests, baseline refresh, memory profiling, and regression checks.

## Verification Results

- `python -m py_compile scripts/measure_apdr_baseline.py scripts/profile_apdr_memory.py scripts/check_apdr_regression.py` passed
- `cargo test --manifest-path tools/apdr/Cargo.toml --test test_cli` passed
- All three script `--help` checks passed
- Hotspot audit file references were verified with `rg`
- README guardrail commands were verified with `rg`
- `python scripts/check_apdr_regression.py --baseline ... --candidate ...` passed on a same-baseline comparison

## Task Commits

Each task was committed atomically:

1. **Task 1: Create the baseline regression gate script** - `c55144d` (`feat(01-02): add baseline regression gate`)
2. **Task 2: Write the evidence-backed hotspot audit** - `622999f` (`docs(01-02): rank modernization hotspots`)
3. **Task 3: Publish the modernization guardrail command set** - `e36cd48` (`docs(01-02): publish modernization guardrails`)

## Files Created/Modified

- `scripts/check_apdr_regression.py` - Compares candidate baseline JSON artifacts against the committed Phase 1 baseline.
- `.planning/phases/01-baseline-and-guardrails/01-HOTSPOT-AUDIT.md` - Ranked the next five Rust optimization targets for Phase 2 and Phase 3.
- `tools/apdr/README.md` - Added the modernization guardrail section with exact repo-root commands.

## Decisions Made

- Used the committed `01-baseline.json` shape as the contract for all future regression checks.
- Split the hotspot attack order by milestone phase: resolver-heavy work first in Phase 2, validation-heavy work first in Phase 3.
- Kept the README command set repo-root relative to reduce ambiguity across future sessions.

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered

None beyond the already-captured baseline evidence from `01-01`. Plan `01-02` consumed those artifacts directly as intended.

## User Setup Required

None - no external service configuration required for this plan's committed outputs.

## Next Phase Readiness

- Phase 1 is complete and Phase 2 can now plan against a committed baseline, memory snapshot, audit, and regression gate.
- `tools/apdr/src/resolver/mod.rs`, `tools/apdr/src/resolver/pre_solve.rs`, and `tools/apdr/src/resolver/pypi_client.rs` are now the recommended starting order for Phase 2.

## Self-Check: PASSED

- Regression gate script exists and exits 0 on a no-change candidate
- Hotspot audit contains the agreed target files
- README contains the exact modernization guardrail commands
- Phase 1 now has both required summaries and committed baseline artifacts

---
*Phase: 01-baseline-and-guardrails*
*Completed: 2026-03-27*
