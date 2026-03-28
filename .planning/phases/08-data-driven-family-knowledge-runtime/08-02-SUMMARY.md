---
phase: 08-data-driven-family-knowledge-runtime
plan: 02
subsystem: resolver
tags:
  - apdr
  - family-knowledge
  - curated-runtime
  - recovery
  - rust
dependency_graph:
  requires:
    - 08-01
  provides:
    - curated-runtime-family-registry
    - curated-bundle-and-retry-wiring
    - touched-family-runtime-guardrails
  affects:
    - 08-03
    - 09
tech_stack:
  added: []
  patterns:
    - runtime-family-overlay-for-touched-scope
    - curated-recovery-rule-driven-bundle-application
    - shared-curated-missing-module-recovery
key_files:
  created:
    - .planning/phases/08-data-driven-family-knowledge-runtime/08-02-SUMMARY.md
  modified:
    - tools/apdr/src/resolver/mod.rs
    - tools/apdr/src/resolver/family_knowledge/core.rs
    - tools/apdr/src/resolver/family_knowledge/detection.rs
    - tools/apdr/src/resolver/family_knowledge/legacy_bundles.rs
    - tools/apdr/src/resolver/retry_loop.rs
    - tools/apdr/tests/test_resolver.rs
key-decisions:
  - Curated touched families overlay the existing static family table instead of replacing it wholesale, so untouched family behavior stays on the established runtime path.
  - Touched recovery-note prefixes, locked notes, bundle members, and preferred Python order now come from curated recovery rules with hardcoded fallbacks only as a resilience backstop.
  - The `pkg_resources` retry shortcut now shares the same curated recovery-rule source of truth as the rest of the touched family runtime.
patterns-established:
  - "Touched registry lookups should use `RuntimeFamily` so curated and static families can coexist without widening Phase 8 scope."
  - "Bundle-oriented family recovery should pull note text, triggers, and bundle members from curated recovery rules before consulting fallback constants."
requirements-completed:
  - FAM-01
  - FAM-02
  - FAM-03
metrics:
  completed_date: "2026-03-28"
  tasks_completed: 2
  verification_tests: 4
---

# Phase 8 Plan 02 Summary

**Resolver runtime wiring now loads the touched Phase 7 family boundary from curated data and applies the same curated source across registry, bundle, and retry behavior.**

## Accomplishments

- Initialized curated family knowledge at resolver entry and added a `RuntimeFamily` overlay so touched families can come from curated data while untouched families continue using the static `FAMILIES` table.
- Routed touched registry lookups, explicit namespace mappings, bundle application, recovery-note rendering, preferred Python ordering, and conflict pruning through curated family and recovery-rule data.
- Replaced the direct `pkg_resources -> setuptools` retry special case with curated missing-module recovery and added targeted runtime tests that lock the touched/static split and preserved family outcomes.

## Verification Results

- `cargo test --manifest-path tools/apdr/Cargo.toml --test test_resolver data_driven_family_runtime_registry_ -- --nocapture` passed.
- `cargo test --manifest-path tools/apdr/Cargo.toml --test test_resolver data_driven_family_runtime_behavior_ -- --nocapture` passed.
- `rg -n "init_curated_family_knowledge" tools/apdr/src/resolver/mod.rs tools/apdr/src/resolver/family_knowledge/core.rs` passed.
- `rg -n "pkg_resources|curated" tools/apdr/src/resolver/retry_loop.rs` passed.

## Files Created/Modified

- `tools/apdr/src/resolver/mod.rs` - initializes curated family knowledge from `tool_root` before resolver family logic runs.
- `tools/apdr/src/resolver/family_knowledge/core.rs` - adds `RuntimeFamily`, curated runtime lookup helpers, curated missing-module recovery, and curated-aware family conflict handling.
- `tools/apdr/src/resolver/family_knowledge/detection.rs` - resolves touched namespace allowances from curated mappings before static fallbacks.
- `tools/apdr/src/resolver/family_knowledge/legacy_bundles.rs` - drives touched bundle members, trigger matching, note rendering, and Python-order selection from curated recovery rules.
- `tools/apdr/src/resolver/retry_loop.rs` - routes `pkg_resources` recovery through curated family knowledge.
- `tools/apdr/tests/test_resolver.rs` - adds runtime registry/behavior tests plus a shared lock for curated-family cache isolation.

## Decisions Made

- `RuntimeFamily` carries either a curated touched family or a static family entry so conflict pruning and namespace checks can use one path without widening the Phase 8 boundary.
- Curated registry families suppress the overlapping static entry by normalized family name, preventing touched lookups from silently falling back to stale hardcoded data.
- Touched recovery behavior preserves the existing reviewer-facing note language by reading templates from curated rules first and then using prior strings as deterministic fallbacks.

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered

- The first 08-02 behavior run poisoned the shared mutex when the new PyMC3 assertion failed. The helper was updated to recover from poison so one targeted test failure does not cascade into unrelated runtime tests.
- The PyMC3 runtime output normalized the requirement name casing to match the existing resolver tests, so the new assertion was aligned to the stable lowercase requirement check rather than treating casing as a behavior change.
- Existing unrelated local edits in `tools/apdr/src/lib.rs` and `tools/apdr/llm_py/tests/test_llm_integration.py` were left untouched.

## Next Phase Readiness

- `08-03` can now lock the migrated runtime behind the Phase 7 touched-family fixture corpus because the resolver consumes curated touched data end-to-end.
- Phase 9 can build on the new runtime/data split instead of re-opening touched registry and retry wiring.

## Self-Check: PASSED

- `tools/apdr/src/resolver/mod.rs` contains `init_curated_family_knowledge`.
- `tools/apdr/src/resolver/family_knowledge/core.rs` contains curated runtime overlay logic and the shared `pkg_resources` recovery entrypoint.
- `tools/apdr/src/resolver/retry_loop.rs` contains the curated missing-module routing path.
- `cargo test --manifest-path tools/apdr/Cargo.toml --test test_resolver data_driven_family_runtime_registry_ -- --nocapture` and `data_driven_family_runtime_behavior_ -- --nocapture` both passed.

---
*Phase: 08-data-driven-family-knowledge-runtime*
*Completed: 2026-03-28*
