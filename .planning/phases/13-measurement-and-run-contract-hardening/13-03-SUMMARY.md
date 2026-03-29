---
phase: 13-measurement-and-run-contract-hardening
plan: 03
subsystem: measurement-evidence
tags: [measurement-contract, reporting, checker, samples, markdown, json]

requires:
  - phase: 13-measurement-and-run-contract-hardening
    provides: Per-case timing and run-contract metadata from Plan 13-02
provides:
  - fixture-safe Phase 13 measurement report generation in scripts/measure_apdr_baseline.py
  - deterministic report validation in scripts/check_phase13_measurement_contract.py
  - bounded env-fast and docker-proof example artifacts for reviewer reference
  - reviewer-facing Phase 13 measurement contract note
affects: [phase-14-baselines, reviewer-proof, benchmark-comparison-audits]

tech-stack:
  added: []
  patterns: [fixture-backed reporting, deterministic contract validation, explicit evidence labels]

key-files:
  created:
    - scripts/check_phase13_measurement_contract.py
    - .planning/phases/13-measurement-and-run-contract-hardening/13-env-fast-sample.json
    - .planning/phases/13-measurement-and-run-contract-hardening/13-docker-proof-sample.json
    - .planning/phases/13-measurement-and-run-contract-hardening/13-MEASUREMENT-CONTRACT.md
  modified:
    - scripts/measure_apdr_baseline.py

key-decisions:
  - "Make fixture-backed reporting the default verification path so Phase 13 can validate in restricted environments without depending on live package installs"
  - "Use one checker for both generated reports and hand-authored example artifacts so reviewer docs and machine validation cannot drift apart"
  - "Treat execution mode and cache state as first-class evidence labels in JSON and Markdown output, not just secondary notes"

patterns-established:
  - "Phase 13 report JSON carries a nested run_contract plus flattened comparison fields at top-level and per-sample scope"
  - "Phase 13 checker validates timing keys, run-contract completeness, and reviewer-facing labels from the same schema"

requirements-completed: [MAC-02, EVD-03, EVD-05]

duration: 9min
completed: 2026-03-29
---

# Phase 13 Plan 03: Measurement Checker and Evidence Summary

**Phase 13 now closes with a fixture-safe reporting path, a deterministic measurement-contract checker, and reviewer-facing example artifacts**

## Performance

- **Duration:** 9 min
- **Started:** 2026-03-29T04:20:27Z
- **Completed:** 2026-03-29T04:29:06Z
- **Tasks:** 2
- **Files modified:** 5

## Accomplishments
- Rewrote `scripts/measure_apdr_baseline.py` to emit the full Phase 13 run contract and timing surface at top-level and per-sample scope
- Added a fixture-safe default path so the Phase 13 verification command can generate a comparable report from `tools/apdr/tests/fixtures` without depending on live package-install network access
- Added `scripts/check_phase13_measurement_contract.py` to validate required run-contract keys, timing keys, and explicit `execution_mode` / `cache_state` labels
- Added bounded example artifacts for `env-fast` and `docker-proof` captures
- Added `13-MEASUREMENT-CONTRACT.md` with the exact reviewer-facing contract sections required by the plan

## Task Commits

1. **Plan 13-03 implementation** - pending commit during phase execution

## Files Created/Modified
- `scripts/measure_apdr_baseline.py` - Generates normalized Phase 13 report JSON and Markdown from fixture-backed or live captures
- `scripts/check_phase13_measurement_contract.py` - Deterministic contract validator for generated and sample reports
- `.planning/phases/13-measurement-and-run-contract-hardening/13-env-fast-sample.json` - Example `env-fast` artifact
- `.planning/phases/13-measurement-and-run-contract-hardening/13-docker-proof-sample.json` - Example `docker-proof` artifact
- `.planning/phases/13-measurement-and-run-contract-hardening/13-MEASUREMENT-CONTRACT.md` - Reviewer-facing measurement contract note

## Decisions Made
- Defaulted the reporting script to fixture-backed artifact generation so the Phase 13 checker can run deterministically in CI-like or sandboxed environments
- Kept live execution available through `--execute-live` for future baseline captures once a network-capable environment is available
- Used the same checker against generated reports and hand-authored sample artifacts so the documentation examples are held to the exact same machine contract

## Deviations from Plan

- No functional deviation. The plan asked for a fixture-backed generated report and checker validation; that path now intentionally avoids live dependency installs by default so the verification gate stays deterministic.

## Issues Encountered

- Direct execution from `scripts/` did not initially resolve `benchmark_ui` imports, so both Phase 13 scripts were updated to prepend the repo root to `sys.path` before the final verification pass.

## User Setup Required

None.

## Next Phase Readiness
- Phase 13 is complete and the measurement contract is now documented, validated, and backed by generated artifacts
- Phase 14 can start with comparable benchmark evidence instead of ad hoc timing notes
- macOS before/after and Windows non-regression work can now reuse the same JSON/Markdown contract and checker

## Self-Check: PASSED

- FOUND: `scripts/measure_apdr_baseline.py` emits `llm_duration_ms`, `docker_startup_duration_ms`, `run_contract`, `execution_mode`, and `cache_state`
- FOUND: `scripts/check_phase13_measurement_contract.py` validates generated reports and sample artifacts
- FOUND: `13-MEASUREMENT-CONTRACT.md` contains `## Required Run Contract`, `## Stage Timings`, `## Evidence Labels`, and `## Comparison Metadata`
- PASSED: `python3 scripts/measure_apdr_baseline.py --fixtures-root tools/apdr/tests/fixtures --limit 2 --validation-backend env --output-json /tmp/phase13-generated-report.json --output-md /tmp/phase13-generated-report.md && python3 scripts/check_phase13_measurement_contract.py --sample-json /tmp/phase13-generated-report.json`
- PASSED: `python3 scripts/check_phase13_measurement_contract.py --sample-json .planning/phases/13-measurement-and-run-contract-hardening/13-env-fast-sample.json --sample-json .planning/phases/13-measurement-and-run-contract-hardening/13-docker-proof-sample.json`

---
*Phase: 13-measurement-and-run-contract-hardening*
*Completed: 2026-03-29*
