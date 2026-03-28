---
phase: 09-targeted-tier3-recovery-accuracy
plan: 01
subsystem: resolver
tags: [recovery, policy, serde, targeted-recovery, parity-slice]

requires:
  - phase: 07-failure-baseline-parity-slice
    provides: canonical 70-case parity manifest and 17-case watchlist for case-ID validation
  - phase: 08-data-driven-family-knowledge-runtime
    provides: curated family runtime and Phase 8 migration boundary
provides:
  - targeted recovery policy schema with serde-backed structs
  - validated module-provider and stop-reason rules in data/recovery/module_rules.json
  - validated compatibility clusters, companion rules, and Python-ceiling rules in data/recovery/compatibility_rules.json
  - init_targeted_recovery_policy(tool_root) loader wired into resolve_path
affects: [09-02, 09-03, retry_loop, recovery_diagnostics]

tech-stack:
  added: []
  patterns: [OnceCell/Mutex policy cache matching Phase 8 family knowledge pattern, deterministic case-ID validation against parity manifest]

key-files:
  created:
    - tools/apdr/src/resolver/targeted_recovery.rs
    - tools/apdr/data/recovery/module_rules.json
    - tools/apdr/data/recovery/compatibility_rules.json
    - tools/apdr/data/recovery/README.md
  modified:
    - tools/apdr/src/resolver/mod.rs
    - tools/apdr/tests/test_resolver.rs

key-decisions:
  - "Keep targeted recovery policy separate from Phase 8 curated family knowledge to preserve the migration boundary"
  - "Allow compatibility rules to reference Phase 8 family IDs via family_ref field without crossing the data boundary"
  - "Skip case-ID validation when parity manifest is absent to support isolated test environments"

patterns-established:
  - "Targeted recovery policy uses the same OnceCell/Mutex singleton pattern as curated family knowledge"
  - "Policy validation rejects duplicate IDs, duplicate aliases, duplicate anchors, empty trigger sets, and unknown case IDs with deterministic error messages"

requirements-completed: [REC-02, REC-03]

duration: 5min
completed: 2026-03-28
---

# Phase 9 Plan 01: Targeted Recovery Policy Surface Summary

**Serde-backed targeted recovery policy schema with seeded module-provider, stop-reason, and compatibility rules anchored to the canonical Phase 7 parity slice**

## Performance

- **Duration:** 5 min
- **Started:** 2026-03-28T19:15:39Z
- **Completed:** 2026-03-28T19:21:02Z
- **Tasks:** 2
- **Files modified:** 6

## Accomplishments
- Created `targeted_recovery.rs` with full policy schema covering module-provider rules, stop-reason rules, compatibility clusters, companion package rules, and Python-ceiling rules
- Seeded `module_rules.json` with 3 provider rules (pkg_resources, Image, rest_framework) and 9 stop-reason rules (imp, numpy.distutils, elementtree, _distance_wrap, api, taggit_autocomplete, clips, pizzanuvola_teaser, gisutils)
- Seeded `compatibility_rules.json` with 8 compatibility clusters (torch, tensorflow, scikit-learn, pymc, mitmproxy, odfpy, setuptools, numpy), 2 companion rules (PyJWT, python-dateutil), and 2 Python ceiling rules (pymc3, numpy-legacy)
- Added 3 loader-validation tests exercising seed data loading, duplicate alias rejection, and unknown case-ID rejection

## Task Commits

Each task was committed atomically:

1. **Task 1: Add the targeted recovery policy schema, loader, and validator** - `ac2096c` (feat)
2. **Task 2: Seed the Phase 9 module and compatibility policy files and add loader tests** - `840ac87` (feat)

## Files Created/Modified
- `tools/apdr/src/resolver/targeted_recovery.rs` - Policy schema, OnceCell loader, validator, and read-only accessors
- `tools/apdr/src/resolver/mod.rs` - Wires targeted_recovery module and init call into resolve_path
- `tools/apdr/data/recovery/module_rules.json` - Module-provider and stop-reason rules for canonical Phase 9 surface
- `tools/apdr/data/recovery/compatibility_rules.json` - Compatibility clusters, companion rules, Python ceiling rules
- `tools/apdr/data/recovery/README.md` - Scope documentation anchoring to Phase 7 manifest and Phase 8 runtime
- `tools/apdr/tests/test_resolver.rs` - 3 phase9_targeted_policy_ tests

## Decisions Made
- Kept targeted recovery policy in a separate data directory (`data/recovery/`) from Phase 8 family knowledge (`data/family_knowledge/`) to preserve the migration boundary
- Added a `family_ref` field on compatibility clusters so rules can reference Phase 8 families without crossing the data boundary
- Made case-ID validation optional when the parity manifest is absent, enabling isolated test environments without requiring the full .planning directory

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered

None.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness
- The targeted recovery policy surface is ready for Phase 9 Plans 02 and 03 to wire module-not-found and compatibility handling into the retry loop
- The Phase 8 family-runtime boundary remains untouched
- All 3 loader-validation tests pass

## Self-Check: PASSED

- FOUND: tools/apdr/src/resolver/targeted_recovery.rs
- FOUND: tools/apdr/data/recovery/module_rules.json
- FOUND: tools/apdr/data/recovery/compatibility_rules.json
- FOUND: tools/apdr/data/recovery/README.md
- FOUND: ac2096c
- FOUND: 840ac87

---
*Phase: 09-targeted-tier3-recovery-accuracy*
*Completed: 2026-03-28*
