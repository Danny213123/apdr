---
phase: 09-targeted-tier3-recovery-accuracy
plan: 02
subsystem: resolver
tags: [targeted-recovery, module-not-found, stop-reasons, provider-rules, retry-loop]

requires:
  - phase: 09-01
    provides: "Bounded Phase 9 policy layer with module_rules.json and targeted_recovery.rs"
provides:
  - "Runtime module-provider recovery path in retry loop for pkg_resources, Image, rest_framework"
  - "Inspectable stop-reason early-outs for removed-runtime, project-local, and internal-extension modules"
  - "LLM recovery gating to skip burned retries after deterministic stop-reason classification"
  - "Five phase9_targeted_module_ regression tests"
affects: [09-03, targeted-tier3-recovery-accuracy]

tech-stack:
  added: []
  patterns: ["targeted_recovery policy consultation before generic fallbacks", "stop-reason early-out before LLM recovery"]

key-files:
  created: []
  modified:
    - "tools/apdr/src/resolver/retry_loop.rs"
    - "tools/apdr/src/resolver/recovery_diagnostics.rs"
    - "tools/apdr/src/resolver/tier3_llm/core.rs"
    - "tools/apdr/tests/test_resolver.rs"

key-decisions:
  - "Insert targeted provider check in apply_recovery_fix between family_knowledge and pip/stdlib recovery"
  - "Gate mapping-failure break on absence of targeted provider rules so deterministic aliases get a chance"
  - "Add stop-reason check before both mapping-failure break and apply_recovery_fix to prevent wasted retries"
  - "Gate LLM recovery_package_hint on stop-reason rules to avoid burned LLM calls for non-recoverable modules"

patterns-established:
  - "targeted_stop_reason_for_module: centralized stop-reason lookup in recovery_diagnostics.rs"
  - "extract_module_from_error_log: lightweight module extraction in tier3_llm/core.rs for gating"

requirements-completed: [REC-02, REC-04]

duration: 5min
completed: 2026-03-28
---

# Phase 9 Plan 02: Targeted Module Recovery and Stop Reasons Summary

**Wired Phase 9 module-provider recovery and inspectable stop-reason policies into the retry loop, gating LLM retries for non-recoverable cases**

## Performance

- **Duration:** 5 min
- **Started:** 2026-03-28T19:24:22Z
- **Completed:** 2026-03-28T19:29:05Z
- **Tasks:** 2
- **Files modified:** 4

## Accomplishments
- Retry loop now consults targeted_recovery module-provider rules (pkg_resources -> setuptools, Image -> Pillow, rest_framework -> djangorestframework) before falling through to generic mapping-failure exits
- Non-recoverable modules (imp, numpy.distutils, elementtree, api, taggit_autocomplete, _distance_wrap, etc.) now stop with inspectable reason strings instead of generic mapping failure
- LLM recovery_package_hint is gated on stop-reason rules so it does not fire for modules already classified by deterministic policy
- Five regression tests cover both provider recovery and stop-reason classification

## Task Commits

Each task was committed atomically:

1. **Task 1: Apply targeted module-provider recovery before generic mapping failure** - `7e148b0` (feat)
2. **Task 2: Add inspectable stop reasons and narrow LLM retries for non-recoverable module cases** - `ce0e743` (feat)

## Files Created/Modified
- `tools/apdr/src/resolver/retry_loop.rs` - Added targeted_recovery provider and stop-reason checks before mapping-failure break and in apply_recovery_fix
- `tools/apdr/src/resolver/recovery_diagnostics.rs` - Added targeted_stop_reason_for_module function
- `tools/apdr/src/resolver/tier3_llm/core.rs` - Added stop-reason gate at top of recovery_package_hint, plus extract_module_from_error_log helper
- `tools/apdr/tests/test_resolver.rs` - Added 5 phase9_targeted_module_ tests

## Decisions Made
- Placed targeted provider check after family_knowledge but before pip/stdlib/backport recovery to give curated family data priority while still catching canonical alias cases
- Used the same stop-reason check in both the early-out break path and apply_recovery_fix to ensure consistent behavior regardless of retry iteration count
- Added a lightweight extract_module_from_error_log in tier3_llm/core.rs rather than making recovery_diagnostics::extract_missing_module public, keeping module boundaries clean

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered

None.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness
- Module-provider recovery and stop-reason policies are live in the retry loop
- Phase 9 plan 03 (compatibility policies for version-not-found and dependency-conflict) can proceed
- The Phase 8 family-runtime boundary is untouched

---
*Phase: 09-targeted-tier3-recovery-accuracy*
*Completed: 2026-03-28*
