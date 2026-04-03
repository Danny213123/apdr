---
phase: 26-llm-case-intake-and-plan-authoring
plan: 01
subsystem: llm
tags: [llm, intake, schema, rust, python]
requires: []
provides:
  - "A first-class authored case-plan schema for LLM intake"
  - "A structured intake-failure contract for no-output and schema-failure paths"
  - "Cross-language protocol coverage for the new intake payload"
affects: [26-02, 26-03, 27-llm-authored-docker-validation-and-artifact-truth]
tech-stack:
  added: []
  patterns:
    - "LLM intake must return an authored case plan or a classified intake failure, never only free-form notes"
    - "The Rust tier3 seam should parse authored-plan truth directly from the Python JSON response"
key-files:
  created:
    - tools/apdr/llm_py/tests/test_client_fallbacks.py
  modified:
    - tools/apdr/llm_py/models.py
    - tools/apdr/llm_py/actions/resolve.py
    - tools/apdr/llm_py/client.py
    - tools/apdr/src/resolver/tier3_llm/core.rs
    - tools/apdr/tests/test_resolver.rs
key-decisions:
  - "Intake now persists a classified failure object instead of collapsing no-output cases into generic notes."
  - "The authored case plan includes smoke strategy and runtime assumptions because later phases should render Docker inputs from plan intent."
patterns-established:
  - "Section-level authorship truth belongs in the intake artifact itself, not only in debug logs."
  - "Failure diagnostics should preserve a safe preview so later phases can separate model failure from infrastructure failure."
requirements-completed: [LLM-01, TRU-02]
duration: 35min
completed: 2026-04-02
---

# Phase 26 Plan 01: Authoring Contract Summary

**Phase 26 now has a first-class LLM-authored case-plan contract and a structured intake-failure record instead of opaque mapping output**

## Performance

- **Duration:** 35 min
- **Started:** 2026-04-02T23:46:00Z
- **Completed:** 2026-04-03T00:21:34Z
- **Tasks:** 2
- **Files modified:** 6

## Accomplishments

- Added explicit Python models for authored plans, package-mapping provenance, smoke strategy, and classified intake failures.
- Upgraded the Python resolve path so successful intake returns an authored case plan and no-output paths return a structured failure object with diagnostics.
- Extended the Rust tier3 LLM seam and protocol tests so the authored-plan and intake-failure payloads survive the Python-to-Rust boundary intact.

## Task Commits

Each task was committed atomically where the shared intake seam allowed:

1. **Task 1: Add authored-plan and intake-failure IPC models** - `47d6b0e` (`feat`)
2. **Task 2: Parse the new intake payload on the Rust side and add regression coverage** - `47d6b0e` (`feat`)

## Files Created/Modified

- `tools/apdr/llm_py/models.py` - Adds `AuthoredCasePlan`, `SmokeStrategy`, and `IntakeFailureRecord`.
- `tools/apdr/llm_py/actions/resolve.py` - Builds authored plans on success and structured intake failures on no-output paths.
- `tools/apdr/llm_py/client.py` - Classifies failure reasons and exposes diagnostic previews.
- `tools/apdr/src/resolver/tier3_llm/core.rs` - Parses authored-plan and intake-failure payloads on the Rust side.
- `tools/apdr/llm_py/tests/test_resolve_agentic.py` - Covers authored-plan success and no-output failure behavior.
- `tools/apdr/llm_py/tests/test_client_fallbacks.py` - Covers failure classification and diagnostic-preview helpers.
- `tools/apdr/tests/test_resolver.rs` - Covers the Rust protocol contract for authored plans and intake failures.

## Decisions Made

- Preserved intake truth as structured fields instead of expanding `notes` or `failure_reason` into a pseudo-schema.
- Treated smoke strategy as part of the authored plan so downstream Docker authoring can consume a stable contract in Phase 27.

## Deviations from Plan

None - plan executed as written.

## Issues Encountered

- `pytest` was not installed in the local interpreters that had the needed Phase 26 Python dependencies, so the Python-side contract was spot-checked with direct `python3.12` assertions instead of the exact planned `pytest` command.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

- Plan 26-02 can now persist authored-plan truth into case artifacts, benchmark metadata, and strict `llm-only` outcomes.
- The intake contract is stable enough for later phases to render Docker inputs from it instead of guessing from raw LLM text.

---
*Phase: 26-llm-case-intake-and-plan-authoring*
*Completed: 2026-04-02*
