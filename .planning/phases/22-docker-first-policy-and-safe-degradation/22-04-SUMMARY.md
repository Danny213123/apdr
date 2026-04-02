---
phase: 22-docker-first-policy-and-safe-degradation
plan: 04
subsystem: infra
tags: [rust, python, docker, llm, proof]
requires:
  - phase: 22-02
    provides: Docker-first llm Doctor wording and operator-facing degradation copy
  - phase: 22-03
    provides: Top-level llm route metadata plus the initial Phase 22 policy proof contract
provides:
  - Real Docker-usability gating for docker-first `llm` route selection using a `docker info` probe
  - Exact `docker cli unavailable` and `docker daemon unavailable` bypass reasons in APDR metadata and `docker-bypass.txt`
  - A five-case Phase 22 proof contract and Doctor regression coverage for installed-but-unusable Docker
affects: [phase-23-policy-truth-and-failure-semantics, phase-24-env-first-vs-docker-first-comparison-harness]
tech-stack:
  added: []
  patterns:
    - Docker-first `llm` routing probes real Docker usability before choosing the first validation hop
    - Phase proof contracts pin exact bypass reasons for distinct Docker failure classes instead of one generic fallback bucket
key-files:
  created: []
  modified:
    - tools/apdr/src/docker/builder/process.rs
    - tools/apdr/src/docker/builder/agent_backend.rs
    - tools/apdr/src/docker/builder/mod.rs
    - tools/apdr/src/lib.rs
    - benchmark_ui/state.py
    - benchmark_ui/test_state_backend_doctor.py
    - scripts/check_phase22_docker_policy.py
    - .planning/phases/22-docker-first-policy-and-safe-degradation/22-docker-policy-slice.json
    - .planning/phases/22-docker-first-policy-and-safe-degradation/22-DOCKER-POLICY-PROOF.md
    - .planning/phases/22-docker-first-policy-and-safe-degradation/22-docker-policy-proof-status.json
key-decisions:
  - "Kept `llm_validation_route` stable as `env-first-docker-bypass` while splitting the bypass reason itself into exact CLI versus daemon-unavailable strings."
  - "Extended the proof slice to five archetypes so installed-but-unusable Docker is frozen as a first-class contract case instead of implied by generic wording."
patterns-established:
  - "Docker-first `llm` degradation is decided by a structured Docker-availability probe, not just PATH presence."
  - "Doctor copy, top-level APDR outputs, and proof artifacts now share the same exact bypass reason strings."
requirements-completed: [GDR-01]
duration: 6min
completed: 2026-04-02
---

# Phase 22 Plan 04: Docker Usability Gap Closure Summary

**Docker-first `llm` now probes `docker info` before routing, degrades to env when the daemon is unusable, and freezes that contract across Rust tests, Doctor copy, and the Phase 22 proof slice**

## Performance

- **Duration:** 6 min
- **Started:** 2026-04-02T02:08:30Z
- **Completed:** 2026-04-02T02:14:43Z
- **Tasks:** 2
- **Files modified:** 10

## Accomplishments

- Replaced Docker-on-PATH gating with a real Docker-usability probe so docker-first `llm` falls back to env validation when Docker is installed but unusable.
- Persisted exact `docker cli unavailable` versus `docker daemon unavailable` reasons through Phase 22 route metadata, top-level APDR outputs, and `docker-bypass.txt`.
- Extended the deterministic Phase 22 proof package and Doctor regression tests to freeze the installed-but-unusable Docker case alongside the existing env-first control, CLI-missing, and host-runtime paths.

## Task Commits

Each task was committed atomically:

1. **Task 1: Gate docker-first `llm` on real Docker usability and persist the unusable-Docker bypass reason** - `447502d` (`fix`)
2. **Task 2: Freeze the unusable-Docker case in Doctor wording and the Phase 22 proof contract** - `ff779a1` (`fix`)
3. **Verification follow-up: freeze the literal Docker probe command required by the plan acceptance grep** - `883495b` (`fix`)

## Files Created/Modified

- `tools/apdr/src/docker/builder/process.rs` - Added structured Docker availability probing and sourced it from the literal `docker info --format {{.ServerVersion}}` command string.
- `tools/apdr/src/docker/builder/agent_backend.rs` - Routed docker-first `llm` through the structured probe and wrote exact CLI-versus-daemon bypass reasons into metadata and bypass notes.
- `tools/apdr/src/docker/builder/mod.rs` - Added Phase 22 regression tests for daemon-unavailable fallback and bypass-note content while preserving the missing-CLI coverage.
- `tools/apdr/src/lib.rs` - Updated the Phase 22 report/summary-line fixture tests to expect the exact CLI-unavailable reason.
- `benchmark_ui/state.py` - Updated APDR `llm` backend and Doctor wording to spell out env degradation for both Docker CLI and Docker daemon failures.
- `benchmark_ui/test_state_backend_doctor.py` - Added daemon-unavailable Doctor assertions and tightened existing wording checks around exact bypass reasons.
- `scripts/check_phase22_docker_policy.py` - Expanded the deterministic contract to five cases and aligned the status artifact to plan `04`.
- `.planning/phases/22-docker-first-policy-and-safe-degradation/22-docker-policy-slice.json` - Added the `docker-daemon-unavailable` case and made the CLI-missing case explicit.
- `.planning/phases/22-docker-first-policy-and-safe-degradation/22-DOCKER-POLICY-PROOF.md` - Updated the reviewer proof note to call out installed-but-unusable Docker explicitly.
- `.planning/phases/22-docker-first-policy-and-safe-degradation/22-docker-policy-proof-status.json` - Stores the passing probe-only result for the final five-case Phase 22 contract.

## Decisions Made

- Preserved the existing `env-first-docker-bypass` route label so downstream readers keep a stable route category while the exact reason is carried separately in `docker_bypass_reason`.
- Treated installed-but-unusable Docker as a proof-contract case, not just a runtime edge case, so future verification can detect drift without needing a live benchmark replay.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] Acceptance grep required the literal Docker probe command in source**
- **Found during:** Final verification after Task 2
- **Issue:** `process.rs` built the `docker info --format {{.ServerVersion}}` probe correctly from args, but the plan’s grep-based acceptance check required that literal command string to appear in source.
- **Fix:** Introduced `DOCKER_VALIDATION_PROBE_COMMAND` and used it to build the probe command so behavior stayed the same while the acceptance contract became explicit.
- **Files modified:** `tools/apdr/src/docker/builder/process.rs`
- **Verification:** `cargo test --manifest-path tools/apdr/Cargo.toml phase22_policy_` and the required `rg -n 'docker daemon unavailable|docker-daemon-unavailable|docker info --format' ...`
- **Committed in:** `883495b`

---

**Total deviations:** 1 auto-fixed (1 blocking)
**Impact on plan:** The follow-up fix was required to satisfy the plan’s final acceptance contract. No scope creep beyond the gap closure.

## Issues Encountered

None.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

- Phase 22 can now close on truthful Docker degradation behavior; Phase 23 can build richer policy-truth surfaces on exact bypass reasons instead of a generic Docker-unavailable bucket.
- Phase 24 can reuse the five-case proof contract as its routing baseline before comparing env-first versus docker-first outcomes.

## Self-Check: PASSED

- Found summary file: `.planning/phases/22-docker-first-policy-and-safe-degradation/22-04-SUMMARY.md`
- Found task commit: `447502d`
- Found task commit: `ff779a1`
- Found task commit: `883495b`

---
*Phase: 22-docker-first-policy-and-safe-degradation*
*Completed: 2026-04-02*
