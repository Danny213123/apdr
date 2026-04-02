---
phase: 22-docker-first-policy-and-safe-degradation
plan: 03
subsystem: proof
tags: [rust, python, docker, llm, proof]
requires:
  - phase: 22-01
    provides: Docker-first llm routing policy categories and normalized llm_validation_policy handling
  - phase: 22-02
    provides: Operator-visible llm policy and degradation wording across benchmark surfaces
provides:
  - Deterministic `docker-bypass.txt` notes plus top-level llm route metadata for non-Docker `llm` first hops
  - Frozen Phase 22 policy slice covering docker-first, env-first control, Docker bypass, and host-runtime pre-skip cases
  - Probeable checker and reviewer note for the Phase 22 routing and debug-artifact contract
affects: [phase-23-policy-truth-and-failure-semantics, phase-24-env-first-vs-docker-first-comparison-harness]
tech-stack:
  added: []
  patterns:
    - Non-Docker `llm` first hops leave a deterministic `docker-bypass.txt` note under `.apdr-debug`
    - Phase proof contracts use a fixed slice JSON plus committed probe-status JSON to lock routing promises without a live replay
key-files:
  created:
    - scripts/check_phase22_docker_policy.py
    - .planning/phases/22-docker-first-policy-and-safe-degradation/22-docker-policy-slice.json
    - .planning/phases/22-docker-first-policy-and-safe-degradation/22-docker-policy-proof-status.json
    - .planning/phases/22-docker-first-policy-and-safe-degradation/22-DOCKER-POLICY-PROOF.md
  modified:
    - tools/apdr/src/lib.rs
    - tools/apdr/src/docker/builder/agent_backend.rs
    - tools/apdr/src/docker/builder/mod.rs
key-decisions:
  - "Persisted requested llm policy, route category, and bypass details in top-level APDR outputs instead of relying on debug-folder inspection alone."
  - "Used a contract-shaped Phase 22 proof slice with archetype identifiers rather than implying a live comparison harness before Phase 24."
patterns-established:
  - "Phase 22 `llm` case review starts from top-level route metadata, then falls through to either Docker attempt artifacts or a single `docker-bypass.txt` note."
  - "Policy proof scripts validate requested policy, first hop, bypass reason, and required debug artifacts together as one deterministic contract."
requirements-completed: [DFV-01, GDR-01]
duration: 5min
completed: 2026-04-01
---

# Phase 22 Plan 03: Docker Policy Proof Summary

**Per-case `llm` Docker bypass notes, top-level route metadata, and a fixed four-case policy proof contract for docker-first degradation behavior**

## Performance

- **Duration:** 5 min
- **Started:** 2026-04-01T21:18:34-04:00
- **Completed:** 2026-04-01T21:24:00-0400
- **Tasks:** 2
- **Files modified:** 7

## Accomplishments

- Added deterministic `docker-bypass.txt` writing plus top-level `requested_llm_validation_policy`, `llm_validation_route`, `docker_bypass_reason`, and `docker_bypass_note` fields so non-Docker `llm` paths are explicit and machine-readable.
- Locked the Rust contract with `phase22_policy_*` coverage for bypass-note creation and the new top-level report fields while preserving docker-first attempt artifacts from the existing Docker backend layout.
- Added `scripts/check_phase22_docker_policy.py`, the fixed `22-docker-policy-slice.json`, the generated probe status JSON, and `22-DOCKER-POLICY-PROOF.md` so Phase 22 closes on a deterministic routing/degradation proof instead of an implied future comparison harness.

## Task Commits

Each task was committed atomically:

1. **Task 1: Guarantee Docker attempt artifacts or explicit bypass notes for every `llm` case** - `2d44a9e` (`fix`)
2. **Task 2: Freeze a deterministic Phase 22 policy proof contract** - `5aa6e05` (`feat`)

## Files Created/Modified

- `tools/apdr/src/lib.rs` - Added persisted llm route/bypass metadata to `ValidationSummary` reporting and summary surfaces.
- `tools/apdr/src/docker/builder/agent_backend.rs` - Writes deterministic `docker-bypass.txt` notes for non-Docker first-hop `llm` routes and stamps route metadata onto final summaries.
- `tools/apdr/src/docker/builder/mod.rs` - Added `phase22_policy_*` tests covering bypass-note creation and Docker-first no-note behavior.
- `scripts/check_phase22_docker_policy.py` - Validates the fixed Phase 22 policy slice and emits the machine-readable proof status artifact.
- `.planning/phases/22-docker-first-policy-and-safe-degradation/22-docker-policy-slice.json` - Freezes the four required policy archetypes and their expected metadata/debug artifacts.
- `.planning/phases/22-docker-first-policy-and-safe-degradation/22-docker-policy-proof-status.json` - Stores the passing probe-only checker result for the frozen contract.
- `.planning/phases/22-docker-first-policy-and-safe-degradation/22-DOCKER-POLICY-PROOF.md` - Gives reviewers the Phase 22 contract, probe command, and interpretation rules.

## Decisions Made

- Kept the new truth surface narrow: top-level outputs now expose requested policy and bypass metadata, while the debug folder still carries the detailed Docker attempt or bypass artifact.
- Treated the Phase 22 proof as a contract package, not a performance comparison, so the checker freezes policy semantics and required artifacts without overstating later comparison evidence.

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered

- `cargo fmt` widened beyond the task files and touched unrelated Rust files; those accidental formatter changes were restored before task commits so the final work stayed scoped to the plan.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

- Phase 22 is now closed on policy/degradation proof, so Phase 23 can focus on richer end-to-end truth surfaces without reopening the Phase 22 contract.
- Phase 24 can compare env-first versus docker-first on a locked expectation set instead of re-defining what docker-first and bypass behavior are supposed to mean.

## Self-Check: PASSED

- Found summary file: `.planning/phases/22-docker-first-policy-and-safe-degradation/22-03-SUMMARY.md`
- Found task commit: `2d44a9e`
- Found task commit: `5aa6e05`
