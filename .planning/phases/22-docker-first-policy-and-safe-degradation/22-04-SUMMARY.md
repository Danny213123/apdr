---
phase: 22-docker-first-policy-and-safe-degradation
plan: 04
subsystem: validation
tags: [rust, python, docker, llm, proof, benchmark-ui]
requires:
  - phase: 22-01
    provides: Docker-first llm routing policy categories and normalized llm_validation_policy handling
  - phase: 22-02
    provides: Operator-visible llm policy and degradation wording across benchmark surfaces
  - phase: 22-03
    provides: Deterministic docker policy proof packaging and per-case docker-bypass artifacts
provides:
  - Docker-first `llm` routing now gates on usable Docker instead of PATH presence alone
  - Unusable-Docker bypass metadata now distinguishes `docker cli unavailable` from `docker daemon unavailable`
  - Phase 22 proof contract now freezes both missing-CLI and daemon-unavailable env-fallback cases
affects: [phase-22-verification, phase-23-policy-truth-and-failure-semantics, phase-24-env-first-vs-docker-first-comparison-harness]
tech-stack:
  added: []
  patterns:
    - Docker-first `llm` route selection uses a concrete Docker usability probe before choosing Docker as the first hop
    - Phase proof slices freeze distinct bypass reasons when operator/runtime contracts need to stay machine-checkable
key-files:
  created:
    - .planning/phases/22-docker-first-policy-and-safe-degradation/22-04-SUMMARY.md
  modified:
    - tools/apdr/src/docker/builder/process.rs
    - tools/apdr/src/docker/builder/agent_backend.rs
    - tools/apdr/src/docker/builder/mod.rs
    - tools/apdr/src/lib.rs
    - benchmark_ui/state.py
    - benchmark_ui/test_state_backend_doctor.py
    - scripts/check_phase22_docker_policy.py
    - .planning/phases/22-docker-first-policy-and-safe-degradation/22-docker-policy-slice.json
    - .planning/phases/22-docker-first-policy-and-safe-degradation/22-docker-policy-proof-status.json
    - .planning/phases/22-docker-first-policy-and-safe-degradation/22-DOCKER-POLICY-PROOF.md
key-decisions:
  - "Kept the unusable-Docker fallback inside the existing `env-first-docker-bypass` route while persisting the exact bypass reason separately."
  - "Expanded the fixed Phase 22 proof package instead of inventing a live replay requirement for this gap closure."
patterns-established:
  - "Phase 22 safe degradation is only considered complete when runtime routing, Doctor copy, and the deterministic proof slice all describe the same Docker-unavailability states."
  - "Fixed-slice proof contracts should distinguish missing tooling from installed-but-unusable tooling when the runtime does."
requirements-completed: [GDR-01]
duration: 2min
completed: 2026-04-01
---

# Phase 22 Plan 04: Unusable Docker Gap Closure Summary

**Docker-first `llm` now falls back cleanly for both missing Docker CLI and unusable Docker daemon, and the Phase 22 proof contract freezes both cases**

## Performance

- **Duration:** 2 min
- **Started:** 2026-04-01T22:11:03-04:00
- **Completed:** 2026-04-01T22:12:57-04:00
- **Tasks:** 2
- **Files modified:** 10

## Accomplishments

- Added a real Docker usability probe so docker-first `llm` only takes the Docker path when the CLI exists and `docker info --format {{.ServerVersion}}` succeeds.
- Persisted exact bypass reasons through route metadata and `docker-bypass.txt`, distinguishing `docker cli unavailable` from `docker daemon unavailable`.
- Expanded the deterministic Phase 22 proof contract to five cases so the checker now freezes both missing-CLI and daemon-unavailable env fallback behavior.

## Task Commits

Each task was committed atomically:

1. **Task 1: Gate docker-first `llm` on real Docker usability and persist the unusable-Docker bypass reason** - `447502d` (`fix`)
2. **Task 2: Freeze the unusable-Docker case in Doctor wording and the Phase 22 proof contract** - `ff779a1` (`fix`)

## Files Created/Modified

- `tools/apdr/src/docker/builder/process.rs` - Added Docker usability probing and distinct unavailability reasons for CLI-missing versus daemon-unavailable states.
- `tools/apdr/src/docker/builder/agent_backend.rs` - Routes docker-first `llm` through env fallback when Docker is unusable and stamps the exact bypass reason into metadata and bypass notes.
- `tools/apdr/src/docker/builder/mod.rs` - Added Phase 22 regression coverage for daemon-unavailable route selection and bypass-note contents.
- `tools/apdr/src/lib.rs` - Preserved the exact Docker bypass reason in the existing top-level reporting surfaces used by Phase 22 outputs.
- `benchmark_ui/state.py` - Clarified APDR `llm` backend and Doctor messaging around missing Docker CLI versus unavailable Docker daemon.
- `benchmark_ui/test_state_backend_doctor.py` - Locked the daemon-unavailable Doctor warning behavior in unit tests.
- `scripts/check_phase22_docker_policy.py` - Expanded the frozen Phase 22 proof checker to the five-case unusable-Docker contract.
- `.planning/phases/22-docker-first-policy-and-safe-degradation/22-docker-policy-slice.json` - Added the `docker-daemon-unavailable` contract case and updated the missing-CLI case to the exact bypass reason.
- `.planning/phases/22-docker-first-policy-and-safe-degradation/22-docker-policy-proof-status.json` - Stores the passing five-case probe result for the updated proof package.
- `.planning/phases/22-docker-first-policy-and-safe-degradation/22-DOCKER-POLICY-PROOF.md` - Updated the reviewer note to describe the full five-case Docker-unavailability contract.

## Decisions Made

- Reused the existing `env-first-docker-bypass` route label for both unusable-Docker cases so downstream consumers only need one route category plus the exact reason field.
- Treated the proof gap as a contract-alignment problem, not a new benchmark-evidence problem, so the closure stayed deterministic and phase-scoped.

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered

- The earlier Phase 22 verification report was stale once the runtime/router fix landed, so this gap closure had to complete the proof package and then rely on fresh verification rather than the original report text.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

- Phase 22 can now be re-verified against the actual unusable-Docker degradation contract instead of the earlier PATH-only assumption.
- Phase 23 can build on truthful requested-policy and actual-route metadata without reopening the safe-degradation semantics.

## Self-Check: PASSED

- Found summary file: `.planning/phases/22-docker-first-policy-and-safe-degradation/22-04-SUMMARY.md`
- Found task commit: `447502d`
- Found task commit: `ff779a1`
