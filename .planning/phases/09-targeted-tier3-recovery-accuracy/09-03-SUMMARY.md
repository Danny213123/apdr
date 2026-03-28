---
phase: 09-targeted-tier3-recovery-accuracy
plan: 03
subsystem: resolver
tags: [recovery, compatibility, version-not-found, dependency-conflict, pep508, targeted-policy]

# Dependency graph
requires:
  - phase: 09-02
    provides: targeted module-provider and stop-reason rules wired into the retry loop
provides:
  - bounded compatibility recovery for torch, tensorflow, scikit-learn, and other canonical clusters
  - transitive specifier normalization (PyJWT>=2.0.0 style) with companion rule lookups
  - deterministic Phase 9 checker and reviewer handoff note
affects: [phase-10-benchmark-rerun]

# Tech tracking
tech-stack:
  added: []
  patterns: [targeted-compatibility-recovery, transitive-specifier-normalization, phase-checker-pattern]

key-files:
  created:
    - scripts/check_phase9_targeted_recovery.py
    - .planning/phases/09-targeted-tier3-recovery-accuracy/09-TARGETED-RECOVERY.md
  modified:
    - tools/apdr/src/resolver/recovery_diagnostics.rs
    - tools/apdr/src/resolver/retry_loop.rs
    - tools/apdr/src/resolver/targeted_recovery.rs
    - tools/apdr/tests/test_resolver.rs

key-decisions:
  - "Compatibility recovery fires before broad contradictory-pin and strip-all fallbacks"
  - "TensorFlow cluster references Phase 8 curated family_ref instead of separate hardcoded path"
  - "Transitive specifier normalization handles all PEP 508 comparator operators"

patterns-established:
  - "Compatibility cluster lookup by log trigger substrings from data file, not hardcoded patterns"
  - "Companion-rule lookup by normalized package key for transitive specifier recovery"

requirements-completed: [REC-03, REC-04]

# Metrics
duration: 11min
completed: 2026-03-28
---

# Phase 09 Plan 03: Bounded Compatibility Recovery and Phase 9 Closeout Summary

**Bounded compatibility policies for torch, tensorflow, and other canonical clusters now fire before generic pin stripping, with transitive specifier normalization for PyJWT/python-dateutil, plus deterministic checker and reviewer note for Phase 10 handoff.**

## Performance

- **Duration:** 11 min
- **Started:** 2026-03-28T19:32:25Z
- **Completed:** 2026-03-28T19:43:11Z
- **Tasks:** 2/2
- **Files modified:** 6

## Accomplishments

### Task 1: Bounded compatibility recovery for canonical version and conflict clusters

- Added `normalize_requirement_spec` to `recovery_diagnostics.rs` that parses PEP 508 requirement strings (e.g. `PyJWT>=2.0.0`, `python-dateutil<2.0,>=2.1`) into a usable package key plus constraint
- Added `compatibility_cluster_for_log`, `companion_rule_for_package`, and `python_ceiling_for_package` lookup methods to `TargetedRecoveryPolicy`
- Wired `try_targeted_compatibility_recovery` into the retry loop before the existing DependencyConflict and VersionNotFound fallback paths -- applies preferred versions from the cluster policy file for torch/torchvision, tensorflow/keras/tensorboard, scikit-learn, and other canonical anchors
- Wired `try_targeted_transitive_specifier_recovery` to normalize requirement strings from the log and look up companion rules (PyJWT -> cryptography) and cluster policies
- Added 3 regression tests: `phase9_targeted_compatibility_recovers_torch_cluster`, `phase9_targeted_compatibility_recovers_tensorflow_cluster`, `phase9_targeted_compatibility_normalizes_transitive_specifier`
- **Commit:** f795f39

### Task 2: Deterministic Phase 9 checker and reviewer handoff note

- Created `scripts/check_phase9_targeted_recovery.py` with `--parity-manifest`, `--phase8-md`, `--phase9-md`, `--module-rules`, and `--compatibility-rules` arguments
- Checker validates module rule coverage, compatibility cluster coverage, companion rules, Phase 9 note headings, and Phase 8 boundary references
- Created `09-TARGETED-RECOVERY.md` with required headings: Target Scope, Module Recovery Coverage, Compatibility Recovery Coverage, Diagnostics, and Phase 10 Handoff
- The note preserves the Phase 8 runtime boundary and describes the Phase 10 measurement targets
- **Commit:** aefd3b4

## Verification Results

- `cargo test phase9_targeted_compatibility_` -- 3/3 passed
- `cargo test phase7_family_` -- 5/5 passed
- `cargo test data_driven_family_` -- 9/9 passed
- `python scripts/check_phase8_family_runtime.py` -- passed
- `python scripts/check_phase9_targeted_recovery.py` -- 5/5 passed

## Deviations from Plan

None -- plan executed exactly as written.

## Known Stubs

None -- all data lookups are wired to live policy files, no placeholder data.

## Decisions Made

1. **Compatibility recovery placement**: The targeted compatibility recovery runs before both family-knowledge recovery and generic pin-stripping fallbacks. This ensures the bounded policy gets first shot at known clusters.

2. **TensorFlow family_ref**: The `compat-tensorflow` cluster uses `family_ref: "legacy-tensorflow"` to root its behavior in the Phase 8 curated family runtime, as required by the plan's must_haves.

3. **Transitive specifier approach**: Rather than modifying `extract_package_and_version` (which would affect all callers), a separate `normalize_requirement_spec` function handles the broader PEP 508 syntax and is used only in the targeted recovery path.

## Self-Check: PASSED

All created files exist. All commit hashes verified.
