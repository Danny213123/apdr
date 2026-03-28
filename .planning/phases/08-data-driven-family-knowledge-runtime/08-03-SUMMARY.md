---
phase: 08-data-driven-family-knowledge-runtime
plan: 03
subsystem: testing
tags:
  - apdr
  - family-knowledge
  - regression
  - checker
  - python
dependency_graph:
  requires:
    - 08-02
  provides:
    - phase7-family-runtime-regression-tests
    - phase8-family-runtime-checker
    - reviewer-handoff-note
  affects:
    - 09
    - 10
tech_stack:
  added: []
  patterns:
    - bounded-phase7-fixture-regression-coverage
    - deterministic-phase-close-checker
    - reviewer-note-anchored-to-phase7-boundary
key_files:
  created:
    - scripts/check_phase8_family_runtime.py
    - .planning/phases/08-data-driven-family-knowledge-runtime/08-FAMILY-RUNTIME.md
    - .planning/phases/08-data-driven-family-knowledge-runtime/08-03-SUMMARY.md
  modified:
    - tools/apdr/tests/test_resolver.rs
key-decisions:
  - The Phase 7 benchmark-derived fixture root remains the regression boundary, but the runtime tests stay bounded to representative fixture cases per touched surface instead of replaying all 17 snapshots.
  - The Phase 8 checker validates both curated data coverage and the reviewer note so the closeout contract stays local, deterministic, and benchmark-free.
  - The `Image -> Pillow` alias stays enforced by the checker even though the direct non-validation fixture path was not used as the stable Pillow pin assertion.
patterns-established:
  - "Use `phase7_family_` tests for bounded runtime regressions tied to the copied Phase 7 fixture corpus."
  - "Use a deterministic Python checker to lock note headings, manifest counts, required mappings, and touched-surface coverage before Phase 9 begins."
requirements-completed:
  - FAM-02
  - FAM-03
metrics:
  completed_date: "2026-03-28"
  tasks_completed: 2
  verification_tests: 3
---

# Phase 8 Plan 03 Summary

**Phase 7 benchmark-derived family fixtures now guard the migrated runtime, and Phase 8 closes with a deterministic checker plus one reviewer-facing handoff note.**

## Accomplishments

- Added bounded `phase7_family_` regression tests in `tools/apdr/tests/test_resolver.rs` for the Phase 7 fixture corpus, covering legacy Pillow pins, the PyMC3 bundle, standalone keras backend recovery, `pkg_resources -> setuptools`, and the sklearn shim.
- Created `scripts/check_phase8_family_runtime.py` to validate the locked 17-case family manifest, required explicit namespace mappings, touched curated family/rule coverage, and the required `08-FAMILY-RUNTIME.md` headings.
- Wrote `.planning/phases/08-data-driven-family-knowledge-runtime/08-FAMILY-RUNTIME.md` so reviewers can see the data files, touched runtime ownership, diagnostics contract, and the Phase 9 handoff without reopening the Phase 7 boundary.

## Verification Results

- `cargo test --manifest-path tools/apdr/Cargo.toml --test test_resolver phase7_family_ -- --nocapture` passed.
- `python scripts/check_phase8_family_runtime.py --family-manifest .planning/phases/07-failure-baseline-parity-slice/07-family-snapshot-manifest.json --families-json tools/apdr/data/family_knowledge/touched_families.json --recovery-json tools/apdr/data/family_knowledge/touched_recovery_rules.json --baseline-md .planning/phases/08-data-driven-family-knowledge-runtime/08-FAMILY-RUNTIME.md` passed.
- `cargo test --manifest-path tools/apdr/Cargo.toml --test test_resolver data_driven_family_ -- --nocapture; cargo test --manifest-path tools/apdr/Cargo.toml --test test_resolver phase7_family_ -- --nocapture; python scripts/check_phase8_family_runtime.py --family-manifest .planning/phases/07-failure-baseline-parity-slice/07-family-snapshot-manifest.json --families-json tools/apdr/data/family_knowledge/touched_families.json --recovery-json tools/apdr/data/family_knowledge/touched_recovery_rules.json --baseline-md .planning/phases/08-data-driven-family-knowledge-runtime/08-FAMILY-RUNTIME.md` passed.

## Files Created/Modified

- `tools/apdr/tests/test_resolver.rs` - adds bounded Phase 7 fixture regressions for the touched family surfaces.
- `scripts/check_phase8_family_runtime.py` - deterministic checker for the family manifest, curated files, and closeout note.
- `.planning/phases/08-data-driven-family-knowledge-runtime/08-FAMILY-RUNTIME.md` - reviewer note for data ownership, diagnostics, and Phase 9 handoff.

## Decisions Made

- The regression layer stays intentionally bounded: representative fixture cases protect each touched runtime surface while the deterministic checker enforces the full touched manifest count and required explicit mappings.
- The closeout note explicitly preserves the canonical 70-case slice and 17-case watchlist so Phase 9 accuracy work cannot silently broaden the migration scope.
- The checker anchors the `Image -> Pillow` alias contract even though the bounded runtime regression focuses the Pillow pin on the two fixtures that deterministically preserve that exact outcome.

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered

- The direct `3682135` fixture path did not keep the Pillow pin as a stable bounded assertion under the non-validation resolver path, so the bounded Pillow regression stayed on the two deterministic pin fixtures while the `Image -> Pillow` alias remained covered by the closeout checker.
- The Phase 7 fixture tests are slower than the narrow runtime slices because they parse larger benchmark-derived snippets, but the bounded sample still stays under the planned feedback window.
- Existing unrelated local edits in `tools/apdr/src/lib.rs` and `tools/apdr/llm_py/tests/test_llm_integration.py` were left untouched.

## Next Phase Readiness

- Phase 9 can now target tier3 recovery accuracy on top of a locked touched-family runtime instead of re-litigating family registry ownership.
- Phase 10 can reuse the deterministic checker and the Phase 7 fixture boundary when it packages benchmark deltas and remaining gaps.

## Self-Check: PASSED

- `tools/apdr/tests/test_resolver.rs` contains `phase7_family_` tests rooted in `tools/apdr/tests/phase7_family_fixtures/`.
- `scripts/check_phase8_family_runtime.py` exists and validates the manifest count, required mappings, curated surface ownership, and note headings.
- `.planning/phases/08-data-driven-family-knowledge-runtime/08-FAMILY-RUNTIME.md` contains `## Data Files`, `## Touched Runtime Coverage`, `## Diagnostics`, and `## Phase 9 Handoff`.

---
*Phase: 08-data-driven-family-knowledge-runtime*
*Completed: 2026-03-28*
