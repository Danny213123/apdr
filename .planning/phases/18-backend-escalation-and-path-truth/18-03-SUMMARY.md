---
phase: 18-backend-escalation-and-path-truth
plan: 03
subsystem: proof
tags: [python, benchmark-ui, doctor, proof, validation-path]
requires:
  - phase: 18-01
    provides: deterministic llm docker escalation
  - phase: 18-02
    provides: validation_path artifact contract and benchmark reader support
provides:
  - truthful Doctor and runtime messaging for APDR llm mode
  - fixed March 30 live-derived backend-path replay slice and proof note
  - deterministic Phase 18 checker and probe status artifact
affects: [phase-18-verification, benchmark-ui, reviewer-evidence]
tech-stack:
  added: []
  patterns:
    - "Doctor warns for optional-but-useful Docker paths instead of treating them as either fully required or irrelevant"
    - "Phase proof uses a fixed live-derived slice plus a probeable machine-readable status file"
key-files:
  created:
    - benchmark_ui/test_state_backend_doctor.py
    - scripts/check_phase18_backend_path.py
    - .planning/phases/18-backend-escalation-and-path-truth/18-live-backend-slice.json
    - .planning/phases/18-backend-escalation-and-path-truth/18-backend-path-proof-status.json
    - .planning/phases/18-backend-escalation-and-path-truth/18-BACKEND-PROOF.md
  modified:
    - benchmark_ui/state.py
    - benchmark_ui/service.py
key-decisions:
  - "Treat Docker as targeted-but-optional for APDR llm mode: warn when unavailable, but keep Docker-only mode as a hard requirement."
  - "Anchor Phase 18 proof to a frozen March 30 slice and make the checker enforce requested-backend-versus-routed-path truth."
patterns-established:
  - "Doctor copy for mixed routing modes should describe the whole route, not just the first backend hop."
  - "Probe-only proof status files are committed reviewer artifacts, not transient temp outputs."
requirements-completed: [VAL-01, VAL-02, WIN-02]
duration: 8 min
completed: 2026-03-31
---

# Phase 18 Plan 03: Backend Escalation and Path Truth Summary

**Doctor now tells the truth about APDR `llm` mode’s targeted Docker middle hop, and Phase 18 has a fixed live-derived proof contract for backend-path routing**

## Performance

- **Duration:** 8 min
- **Started:** 2026-03-31T02:53:00Z
- **Completed:** 2026-03-31T03:00:45Z
- **Tasks:** 2
- **Files modified:** 7

## Accomplishments

- Updated Doctor/runtime language so APDR `llm` mode is described as local env validation plus targeted Docker escalation plus agent fallback.
- Changed Doctor behavior so missing Docker in APDR `llm` mode produces a warning explaining that eligible cases cannot take the Docker middle path, while pure Docker mode still fails hard.
- Fixed APDR `llm` Doctor checks so local interpreters and env tooling are still validated because the route remains env-first.
- Added a dedicated Doctor test module covering the targeted warning path, the continued env-tooling checks for `llm`, the pure-Docker hard-failure path, and the service intro copy.
- Added the fixed March 30 live-derived backend-path slice manifest, the deterministic `check_phase18_backend_path.py` checker, the generated probe status JSON, and the reviewer-facing `18-BACKEND-PROOF.md` note.

## Task Commits

Each task was committed atomically:

1. **Task 1: Add operator-facing Docker escalation messaging** - `d0b300f` (feat)
2. **Task 2: Freeze the live-derived Phase 18 proof contract** - `376ae8d` (feat)

## Files Created/Modified

- `benchmark_ui/state.py` - Updates Doctor and runtime semantics for APDR `llm` mode so Docker is targeted and optional while env tooling remains required.
- `benchmark_ui/service.py` - Updates APDR validation labels and Doctor intro copy to mention targeted Docker escalation and agent fallback explicitly.
- `benchmark_ui/test_state_backend_doctor.py` - Adds focused tests for the `llm` warning path, env-tooling checks, pure-Docker failure semantics, and Doctor copy.
- `scripts/check_phase18_backend_path.py` - Adds the deterministic Phase 18 proof checker with probe-only and live replay modes.
- `.planning/phases/18-backend-escalation-and-path-truth/18-live-backend-slice.json` - Freezes the five March 30 live-derived backend-path review cases.
- `.planning/phases/18-backend-escalation-and-path-truth/18-backend-path-proof-status.json` - Captures the probe-only checker result for the fixed slice contract.
- `.planning/phases/18-backend-escalation-and-path-truth/18-BACKEND-PROOF.md` - Documents the before/after reviewer contract for requested backend versus routed backend path.

## Decisions Made

- Kept Docker optional for APDR `llm` mode in Doctor because env-first validation can still run, but made the warning concrete about what capability is lost when Docker is absent.
- Wrote the proof checker so live replay mode will intentionally fail against baseline artifacts that still report `validation_backend: env` and omit `validation_path`, which keeps the proof surface honest.

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered

- The repo did not yet contain `benchmark_ui/test_state_backend_doctor.py`, so the plan’s verification target needed to be created from scratch before the Doctor semantics could be locked.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

- Phase 18 now has deterministic routing, artifact truth, runtime messaging, and a fixed replay-proof surface, so phase-close verification can evaluate the whole contract directly.
- Phase 19 can build on explicit backend-path truth instead of inferring route semantics from raw logs or UI wording.

## Self-Check: PASSED

- Found `.planning/phases/18-backend-escalation-and-path-truth/18-03-SUMMARY.md`
- Found task commit `d0b300f`
- Found task commit `376ae8d`

---
*Phase: 18-backend-escalation-and-path-truth*
*Completed: 2026-03-31*
