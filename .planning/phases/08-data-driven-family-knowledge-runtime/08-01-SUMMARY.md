---
phase: 08-data-driven-family-knowledge-runtime
plan: 01
subsystem: resolver
tags:
  - apdr
  - family-knowledge
  - curated-data
  - validation
  - rust
dependency_graph:
  requires: []
  provides:
    - curated-touched-family-schema
    - repo-seeded-family-knowledge-data
    - deterministic-loader-validation
  affects:
    - 08-02
    - 08-03
tech_stack:
  added: []
  patterns:
    - split-registry-vs-bundle-curated-family-scopes
    - json-backed-family-runtime-validation
    - actionable-curated-data-errors
key_files:
  created:
    - tools/apdr/src/resolver/family_knowledge/data.rs
    - tools/apdr/data/family_knowledge/touched_families.json
    - tools/apdr/data/family_knowledge/touched_recovery_rules.json
    - tools/apdr/data/family_knowledge/README.md
    - .planning/phases/08-data-driven-family-knowledge-runtime/08-01-SUMMARY.md
  modified:
    - tools/apdr/src/resolver/family_knowledge/mod.rs
    - tools/apdr/tests/test_resolver.rs
key-decisions:
  - Curated touched families are split into `registry` and `bundle` scopes so Phase 8 can move bundle metadata without polluting conflict-pruning lookups for unrelated packages.
  - Recovery rules carry explicit anchor IDs, trigger substrings, and bundle members so malformed touched runtime data fails before runtime wiring begins.
  - Loader diagnostics stay deterministic by validating with ordered maps and exact conflicting family, alias, or rule identifiers.
patterns-established:
  - "Touched family runtime data should live under `tools/apdr/data/family_knowledge/` with one loader module owning validation."
  - "Bundle-only curated families may share package names with registry families as long as registry lookups are indexed separately."
requirements-completed:
  - FAM-01
  - FAM-03
metrics:
  completed_date: "2026-03-28"
  tasks_completed: 2
  verification_tests: 4
---

# Phase 8 Plan 01 Summary

**Curated touched-family schema, repo-seeded JSON data, and deterministic validation errors for the Phase 8 migration boundary.**

## Accomplishments

- Added `tools/apdr/src/resolver/family_knowledge/data.rs` with serde-backed curated family, mapping, and recovery-rule structs plus `init_curated_family_knowledge(...)`, deterministic validation, and read-only accessors.
- Seeded `tools/apdr/data/family_knowledge/touched_families.json` and `tools/apdr/data/family_knowledge/touched_recovery_rules.json` with the bounded Phase 7 touched scope: `setuptools`, `pil`, `sklearn`, `legacy-pymc3`, `legacy-tensorflow`, `legacy-ggplot`, and the six touched recovery rules.
- Documented the touched-only scope in `tools/apdr/data/family_knowledge/README.md` and added targeted resolver tests that prove the seed loads and malformed curated data fails with exact actionable messages.

## Verification Results

- `rg -n "struct CuratedFamilyKnowledge|fn init_curated_family_knowledge|duplicate explicit namespace mapping" tools/apdr/src/resolver/family_knowledge/data.rs` passed.
- `rg -n "legacy-pymc3|legacy-tensorflow|legacy-ggplot|pkg_resources|Image|sklearn" tools/apdr/data/family_knowledge/touched_families.json` passed.
- `rg -n "pkg-resources|legacy-pillow|legacy-pymc3|legacy-tensorflow|keras-backend|legacy-ggplot" tools/apdr/data/family_knowledge/touched_recovery_rules.json` passed.
- `cargo test --manifest-path tools/apdr/Cargo.toml --test test_resolver data_driven_family_loader_ -- --nocapture` passed.

## Files Created/Modified

- `tools/apdr/src/resolver/family_knowledge/data.rs` - curated touched-family schema, loader, validator, and cached accessors.
- `tools/apdr/src/resolver/family_knowledge/mod.rs` - exports the new curated data API.
- `tools/apdr/data/family_knowledge/touched_families.json` - repo-seeded touched family definitions and namespace mappings.
- `tools/apdr/data/family_knowledge/touched_recovery_rules.json` - repo-seeded touched recovery rules, triggers, and bundle variants.
- `tools/apdr/data/family_knowledge/README.md` - Phase 8 scope note for the curated data directory.
- `tools/apdr/tests/test_resolver.rs` - targeted loader/validator tests for the new curated data surface.

## Decisions Made

- Bundle-oriented touched families are represented in the curated data but kept out of registry indexing until Wave 2 wiring explicitly consumes them.
- Recovery-rule validation rejects duplicate aliases, unknown families, empty trigger sets, and bundle members that do not belong to the referenced curated family.
- The initial seed remains anchored to the Phase 7 17-case snapshot corpus and does not widen into untouched family surfaces.

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered

- The initial multi-file patch exceeded the Windows command-length limit, so the Wave 1 edit set was applied in smaller patches without changing the resulting files.
- Existing unrelated local changes in `tools/apdr/src/lib.rs` and `tools/apdr/llm_py/tests/test_llm_integration.py` were left untouched.

## Next Phase Readiness

- `08-02` can now initialize curated touched-family data from `tool_root` and wire touched registry and recovery behavior through the new accessors.
- `08-03` can rely on the seeded JSON files and exact error strings as the deterministic baseline for the phase-close checker.

## Self-Check: PASSED

- `tools/apdr/src/resolver/family_knowledge/data.rs` contains `struct CuratedFamilyKnowledge`, `fn init_curated_family_knowledge`, and the duplicate namespace-mapping diagnostic.
- `tools/apdr/data/family_knowledge/touched_families.json` contains the touched family names and required alias mappings.
- `tools/apdr/data/family_knowledge/touched_recovery_rules.json` contains the six touched recovery-rule IDs from the plan.
- `cargo test --manifest-path tools/apdr/Cargo.toml --test test_resolver data_driven_family_loader_ -- --nocapture` passed with `3` targeted tests.

---
*Phase: 08-data-driven-family-knowledge-runtime*
*Completed: 2026-03-28*
