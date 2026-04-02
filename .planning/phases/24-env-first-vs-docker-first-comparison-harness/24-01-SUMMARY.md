---
phase: 24-env-first-vs-docker-first-comparison-harness
plan: 01
subsystem: benchmarking
tags: [python, benchmark, comparison, llm, proof]
requires:
  - phase: 22-docker-first-policy-and-safe-degradation
    provides: "docker-first and env-first llm policy control with stable llm_validation_policy semantics"
  - phase: 23-policy-truth-and-failure-semantics
    provides: "stable requested policy, validation path, route, and failure-family truth surfaces"
provides:
  - "A locked Phase 24 comparison slice with real snippet paths for paired env-first and docker-first evaluation"
  - "Deterministic env-first and docker-first fixture summaries for fast probe-mode validation"
  - "A reusable harness script that extracts or replays paired llm artifacts while holding validation_backend=llm constant"
affects: [24-02, 24-03, phase-25-docker-first-decision-closeout]
tech-stack:
  added: [scripts/run_phase24_policy_comparison.py]
  patterns:
    - "Policy comparison artifacts preserve a fixed slice and matched run contracts while varying only llm_validation_policy"
    - "Probe-first benchmark tooling can validate paired-policy contracts without requiring a live replay on every iteration"
key-files:
  created:
    - .planning/phases/24-env-first-vs-docker-first-comparison-harness/24-01-SUMMARY.md
    - scripts/run_phase24_policy_comparison.py
    - .planning/phases/24-env-first-vs-docker-first-comparison-harness/24-comparison-slice.json
    - .planning/phases/24-env-first-vs-docker-first-comparison-harness/24-env-first-fixture-summary.json
    - .planning/phases/24-env-first-vs-docker-first-comparison-harness/24-docker-first-fixture-summary.json
  modified: []
key-decisions:
  - "Keep Phase 24 comparison artifacts on validation_backend=llm and vary only llm_validation_policy so policy deltas stay interpretable."
  - "Use deterministic fixture summaries for probe-mode validation rather than reusing older runs whose contracts predate the Phase 22-23 policy work."
patterns-established:
  - "Phase comparison harnesses should lazily import heavy benchmark runtime modules so probe-only checks stay fast and dependency-light."
  - "Fixed-slice comparison fixtures should use real snippet paths even when the deterministic probe data is synthetic."
requirements-completed: [CMP-01]
duration: 15min
completed: 2026-04-02
---

# Phase 24 Plan 01: Comparison Harness Summary

**The repo now has a fixed slice, paired env-first/docker-first probe fixtures, and a reusable harness that can materialize matched llm-policy artifacts from saved summaries or live replay**

## Performance

- **Duration:** 15 min
- **Started:** 2026-04-02T06:30:00Z
- **Completed:** 2026-04-02T06:45:00Z
- **Tasks:** 2
- **Files modified:** 4

## Accomplishments

- Locked a real-snippet Phase 24 comparison slice so env-first and docker-first runs can be judged on the same case set.
- Added deterministic env-first and docker-first fixture summaries that preserve matching llm contracts while demonstrating policy-sensitive route, bucket, and timing differences.
- Built `scripts/run_phase24_policy_comparison.py`, which can either extract artifacts from saved summaries or replay the locked slice through the benchmark worker while keeping `validation_backend=llm` constant.

## Task Commits

Each task was committed atomically:

1. **Task 1: Lock the Phase 24 comparison slice and paired probe fixtures** - `39f90b8` (`feat`)
2. **Task 2: Build the Phase 24 paired extraction and replay harness** - `a8edb40` (`feat`)

## Files Created/Modified

- `.planning/phases/24-env-first-vs-docker-first-comparison-harness/24-comparison-slice.json` - Freezes the five-case comparison slice with real snippet paths and comparison focus notes.
- `.planning/phases/24-env-first-vs-docker-first-comparison-harness/24-env-first-fixture-summary.json` - Provides the deterministic env-first probe fixture summary.
- `.planning/phases/24-env-first-vs-docker-first-comparison-harness/24-docker-first-fixture-summary.json` - Provides the deterministic docker-first probe fixture summary.
- `scripts/run_phase24_policy_comparison.py` - Extracts or replays env-first/docker-first artifacts and preserves comparable policy, path, bucket, provenance, and timing fields.

## Decisions Made

- Kept the comparison harness contract centered on `validation_backend=llm` so later delta checks do not conflate backend changes with first-hop policy changes.
- Treated the Phase 24 fixture summaries as deterministic probes, not as substitute live evidence, so the later runbook can still demand a real paired replay.

## Deviations from Plan

None.

## Issues Encountered

None.

## User Setup Required

None for probe mode. Live replay still needs the normal APDR runtime plus a supported Docker-capable host.

## Next Phase Readiness

- Phase 24 plan 02 can now freeze sample artifacts, compute contract parity, and report pass, bucket, and timing deltas on top of a stable paired harness.
- No blockers from plan 01 remain; `/tmp/phase24-env-artifact.json` and `/tmp/phase24-docker-artifact.json` are already being generated successfully in probe mode.

## Self-Check: PASSED

- Found `.planning/phases/24-env-first-vs-docker-first-comparison-harness/24-01-SUMMARY.md`
- Verified task commits `39f90b8` and `a8edb40` exist in git history

---
*Phase: 24-env-first-vs-docker-first-comparison-harness*
*Completed: 2026-04-02*
