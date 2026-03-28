---
phase: 12-live-benchmark-proof-and-requirement-reconciliation
plan: 01
subsystem: benchmark
tags: [apdr, live-proof, preflight, benchmark, requirements]
requires:
  - phase: 10-benchmark-verification-accuracy-closeout
    provides: locked canonical/watchlist manifest, dry-run rerun wrapper, and Phase 10 proof inputs
  - phase: 11-verification-backfill-and-state-repair
    provides: refreshed audit defining the live-rerun versus hard-blocker routing
provides:
  - explicit Phase 12 live-proof flags and status artifact support in the targeted rerun wrapper
  - probe-only readiness JSON for the locked Phase 10 surface
  - reviewer-facing live proof contract and blocking conditions note
affects: [12-02, 12-03, milestone-closeout]
tech-stack:
  added: []
  patterns:
    - explicit-live-proof-contract
    - probe-only-readiness-artifact
    - output-metadata-driven-rerun-classification
key-files:
  created:
    - .planning/phases/12-live-benchmark-proof-and-requirement-reconciliation/12-live-proof-status.json
    - .planning/phases/12-live-benchmark-proof-and-requirement-reconciliation/12-LIVE-PROOF.md
    - .planning/phases/12-live-benchmark-proof-and-requirement-reconciliation/12-01-SUMMARY.md
  modified:
    - scripts/run_phase10_targeted_benchmark.py
key-decisions:
  - "Phase 12 live proof must be opt-in via --require-live so missing live inputs produce a blocker instead of an implicit dry-run."
  - "Probe-only verification writes the readiness artifact without rewriting the locked Phase 10 rerun outputs."
  - "REC-02, REC-03, and REC-04 remain open after 12-01 because this plan establishes readiness, not measured recovery improvement."
patterns-established:
  - "Phase 12 proof tooling writes a machine-readable readiness JSON before it claims a live-proof or blocker terminal state."
  - "Live rerun classification reads emitted output_data_*.yml metadata and refreshed requirements.txt to mirror benchmark_ui status rules."
requirements-completed: []
duration: 11min
completed: 2026-03-28
---

# Phase 12 Plan 01: Live Proof Readiness Summary

**Explicit live-proof contract for the targeted rerun wrapper, with probe-only readiness status and blocker-branch documentation for the locked Phase 10 benchmark surface**

## Performance

- **Duration:** 11 min
- **Started:** 2026-03-28T23:13:06Z
- **Completed:** 2026-03-28T23:23:45Z
- **Tasks:** 2
- **Files modified:** 4

## Accomplishments

- Hardened `scripts/run_phase10_targeted_benchmark.py` so live proof is explicit via `--require-live`, `--probe-only`, and `--status-json` instead of silently falling back to dry-run.
- Updated the live rerun path to ingest emitted `output_data_*.yml` metadata and refreshed `requirements.txt`, matching the benchmark runner's classification inputs.
- Recorded `.planning/phases/12-live-benchmark-proof-and-requirement-reconciliation/12-live-proof-status.json` showing `requested_mode: live-proof`, `actual_mode: probe-only`, and `live_ready: true` for the locked Phase 10 manifest, baseline summary, and `pllm` CSV.
- Added `.planning/phases/12-live-benchmark-proof-and-requirement-reconciliation/12-LIVE-PROOF.md` documenting the explicit command contract and the two allowed terminal states: ready-for-live-rerun or hard-blocker.

## Verification Results

- `python -m py_compile scripts/run_phase10_targeted_benchmark.py` passed.
- `python scripts/run_phase10_targeted_benchmark.py --manifest-json .planning/phases/10-benchmark-verification-accuracy-closeout/10-targeted-rerun-manifest.json --baseline-summary runs/20260327-150339-apdr/summary.json --pllm-csv pllm_results/csv/summary-all-runs.csv --output-json .planning/phases/10-benchmark-verification-accuracy-closeout/10-targeted-rerun.json --case-delta-json .planning/phases/10-benchmark-verification-accuracy-closeout/10-case-delta.json --output-md .planning/phases/10-benchmark-verification-accuracy-closeout/10-TARGETED-RERUN.md --context-log .planning/phases/10-benchmark-verification-accuracy-closeout/10-targeted-benchmark-context.log --status-json .planning/phases/12-live-benchmark-proof-and-requirement-reconciliation/12-live-proof-status.json --probe-only --require-live --apdr-command tools/apdr/target/debug/apdr.exe` passed.
- `rg -n 'requested_mode|actual_mode|live_ready|blocker_reason' .planning/phases/12-live-benchmark-proof-and-requirement-reconciliation/12-live-proof-status.json` passed.
- `rg -n '## Live Readiness|## Command Contract|## Blocking Conditions' .planning/phases/12-live-benchmark-proof-and-requirement-reconciliation/12-LIVE-PROOF.md` passed.

## Task Commits

Each task was committed atomically:

1. **Task 1: Upgrade the targeted rerun wrapper from dry-run helper to explicit live-proof tool** - `d8a5ecf` (feat)
2. **Task 2: Run a live-proof preflight probe and record the terminal-state prerequisites** - `7bf88fa` (feat)

## Files Created/Modified

- `scripts/run_phase10_targeted_benchmark.py` - adds explicit live-proof flags, probe-only readiness mode, status JSON output, and live artifact ingestion from emitted metadata files.
- `.planning/phases/12-live-benchmark-proof-and-requirement-reconciliation/12-live-proof-status.json` - machine-readable readiness artifact with requested/actual mode, live readiness, counts, and blocker fields.
- `.planning/phases/12-live-benchmark-proof-and-requirement-reconciliation/12-LIVE-PROOF.md` - reviewer-facing live proof contract and blocking conditions note.

## Decisions Made

- Preserve the file-ownership boundary by verifying probe-only behavior with Phase 10 output arguments present but unused, rather than regenerating locked Phase 10 outputs in this plan.
- Keep `REC-02`, `REC-03`, and `REC-04` open after this summary because readiness evidence is not the same as measured live recovery proof.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Removed hardcoded canonical and watchlist totals from rerun outputs**
- **Found during:** Task 1 (wrapper hardening)
- **Issue:** The wrapper still hardcoded `70` and `17` in one delta artifact path instead of trusting the manifest it already loaded.
- **Fix:** Switched the case-delta totals to use the manifest-driven canonical and watchlist lengths.
- **Files modified:** `scripts/run_phase10_targeted_benchmark.py`
- **Verification:** `python -m py_compile scripts/run_phase10_targeted_benchmark.py`
- **Committed in:** `d8a5ecf`

**2. [Rule 2 - Missing Critical] Mirrored benchmark_ui requirement fallback when inline result requirements are empty**
- **Found during:** Task 1 (wrapper hardening)
- **Issue:** Status normalization could miss artifact-backed `requirements.txt` data when inline result arrays were empty, which would drift from the benchmark UI's host-runtime pass/skip logic.
- **Fix:** Added artifact-directory requirement loading so rerun classification can use the same fallback evidence path as `benchmark_ui/service.py`.
- **Files modified:** `scripts/run_phase10_targeted_benchmark.py`
- **Verification:** `python -m py_compile scripts/run_phase10_targeted_benchmark.py`
- **Committed in:** `d8a5ecf`

---

**Total deviations:** 2 auto-fixed (1 bug, 1 missing critical)
**Impact on plan:** Both fixes were in-scope correctness work. They tightened the proof contract without expanding the plan beyond readiness hardening.

## Issues Encountered

None.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

- `12-02` can consume the new readiness artifact and live-proof contract to run the actual live rerun or document a hard blocker if conditions regress.
- `REC-02`, `REC-03`, and `REC-04` are still pending. This plan makes the proof path trustworthy; it does not yet prove measured recovery improvement.

## Self-Check: PASSED

- Verified required files exist on disk: `scripts/run_phase10_targeted_benchmark.py`, `12-live-proof-status.json`, `12-LIVE-PROOF.md`, and `12-01-SUMMARY.md`.
- Verified task commits exist in git history: `d8a5ecf` and `7bf88fa`.

---
*Phase: 12-live-benchmark-proof-and-requirement-reconciliation*
*Completed: 2026-03-28*
