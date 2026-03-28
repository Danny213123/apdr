# Phase 8: Data-Driven Family Knowledge Runtime - Research

**Researched:** 2026-03-28
**Domain:** Migrating the touched family-knowledge runtime from hardcoded Rust tables into validated repo data while preserving the Phase 7 regression boundary
**Confidence:** Medium

## Summary

Phase 8 should move only the family-knowledge behavior already protected by Phase 7's 17-case touched-family corpus into repo-shipped data files. The current runtime is split across three hardcoded surfaces: static registry tables and explicit namespace mappings in `tools/apdr/src/resolver/family_knowledge/core.rs`, bundle and note generation in `tools/apdr/src/resolver/family_knowledge/legacy_bundles.rs`, and one remaining direct recovery shortcut in `tools/apdr/src/resolver/retry_loop.rs` for `pkg_resources -> setuptools`. The repo already has deterministic data-loading patterns for TSV seed files in `tools/apdr/src/cache/store.rs` and JSON persistence for learned families in `tools/apdr/src/resolver/family_knowledge/learned.rs`, so the planning problem is not how to add a new configuration mechanism from scratch. The real Phase 8 work is to define one curated schema for the touched family surfaces, validate it strictly, and wire the existing family registry and recovery entrypoints to consume that data without widening scope beyond the Phase 7 boundary.

Primary recommendation: plan Phase 8 as three sequential plans. First, add a curated touched-family data model, loader, and validator with repo data files that encode family members, explicit namespace mappings, recovery triggers, and bundle pins for the touched surfaces only. Second, wire the runtime so `FamilyRegistry`, namespace checks, bundle application, validation-candidate ordering, and the `pkg_resources` retry shortcut all consult the curated data for touched families while leaving untouched hardcoded families alone for now. Third, add regression coverage that exercises the Phase 7 family fixture corpus plus a deterministic phase-close checker that proves the curated data still covers the touched boundary and fails loudly when the data is malformed.

## Phase Requirements

| ID | Requirement | Research Support |
|----|-------------|------------------|
| FAM-01 | Maintainers can define touched family aliases, package mappings, and rejection hints in data files instead of hardcoded Rust tables | The touched surfaces are concentrated in `core.rs`, `legacy_bundles.rs`, and `retry_loop.rs`, and can be represented as repo JSON files loaded by a new `family_knowledge/data.rs` module. |
| FAM-02 | APDR loads and applies data-driven family knowledge for the touched families used in the milestone accuracy slice | Phase 7 already bounded the migration to 17 cases clustered around a small set of touched families: TensorFlow/Keras, Pillow, PyMC3, setuptools/pkg_resources, ggplot, and sklearn. |
| FAM-03 | Invalid or conflicting family-knowledge data fails with actionable validation errors before it can silently change recovery behavior | The repo already uses deterministic parsing and error-return patterns (`Result<T, String>`, `BTreeMap`, `BTreeSet`) that fit a strict loader/validator with explicit diagnostics. |

## Evidence That Should Drive Planning

### The touched Phase 8 migration boundary is already fixed by Phase 7

`07-family-snapshot-manifest.json` and `07-FAMILY-SNAPSHOTS.md` already identify the exact cases that Phase 8 is allowed to migrate. Those 17 cases cluster into a small number of runtime-owned surfaces:

- `8` TensorFlow/Keras cases tied to `family:keras-backend`, `tensorflow`, or `keras`
- `3` Pillow/PIL cases tied to `family:legacy-pillow` and `Image`
- `2` PyMC3-family cases tied to `family:legacy-pymc3`, `pymc3`, `Theano-PyMC`, `Lasagne`, `arviz`, and `xarray-einstats`
- `3` setuptools and legacy ggplot cases tied to `pkg_resources` and `ggplot`
- `1` sklearn namespace-mapping case tied to `sklearn -> scikit-learn`

That is narrow enough that Phase 8 does not need to migrate the entire `FAMILIES` table. It should migrate only the touched family entries and keep the rest of the hardcoded registry intact for this milestone.

### The current family runtime is spread across registry, bundle, and retry surfaces

The touched behavior does not live in one place:

- `core.rs` holds the static `FAMILIES` registry, `EXPLICIT_NAMESPACE_MAPPINGS`, `FamilyRegistry`, conflict pruning, and the public family entrypoints.
- `legacy_bundles.rs` hardcodes bundle members, exact pins, and reviewer-facing note text for legacy stacks such as PyMC3, Pillow, ggplot, and TensorFlow/Keras.
- `detection.rs` uses `FamilyRegistry` and explicit mappings to decide whether a namespace swap is allowed.
- `retry_loop.rs` still contains a direct `pkg_resources -> setuptools` recovery branch outside the family module.

If Phase 8 migrates only the registry structs and ignores the retry and bundle paths, the runtime will still have split sources of truth. Plans should therefore treat `retry_loop.rs` as part of the touched family surface.

### The repo already exposes two reusable data-loading patterns

There are two adjacent patterns worth reusing instead of inventing a new one:

1. `tools/apdr/src/cache/store.rs` loads repo-shipped deterministic seed data from `tools/apdr/data/seed/*.tsv` with sorted maps and line-oriented validation.
2. `tools/apdr/src/resolver/family_knowledge/learned.rs` already uses `serde` and `serde_json` for persisted family metadata and returns actionable `Result<_, String>` errors.

The touched family runtime needs nested structures such as members, bundle pins, trigger substrings, and note templates. That makes JSON a better fit than another ad hoc TSV surface. The simplest Phase 8 shape is therefore a small repo data directory such as:

- `tools/apdr/data/family_knowledge/touched_families.json`
- `tools/apdr/data/family_knowledge/touched_recovery_rules.json`
- `tools/apdr/data/family_knowledge/README.md`

### Validation must fail before runtime behavior changes

FAM-03 is not satisfied by "best effort" parsing. The loader needs explicit preflight validation before the runtime applies any touched family rule. At minimum, the validator should reject:

- duplicate family names
- duplicate preferred members inside one family
- duplicate explicit namespace mappings for the same import alias
- recovery rules that reference an unknown family or mapping ID
- recovery rules with empty trigger lists
- bundle members or pinned packages that do not exist in the curated family/member set

Those failures should surface as actionable error strings rather than quiet fallback to changed recovery behavior.

### Phase-close validation should prove both runtime coverage and regression stability

Phase 7 already gave Phase 8 two useful anchors:

- the machine-readable touched boundary in `07-family-snapshot-manifest.json`
- the copied benchmark snippets under `tools/apdr/tests/phase7_family_fixtures/`

Phase 8 should add one deterministic checker that reads those artifacts plus the curated data files and a reviewer-facing note. That keeps Phase 8 rerunnable without requiring a live benchmark rerun and makes the Phase 9 handoff explicit.

## Implementation Recommendations

### 1. Add one curated touched-family schema, loader, and validator

Recommended new module:

- `tools/apdr/src/resolver/family_knowledge/data.rs`

Recommended repo data files:

- `tools/apdr/data/family_knowledge/touched_families.json`
- `tools/apdr/data/family_knowledge/touched_recovery_rules.json`
- `tools/apdr/data/family_knowledge/README.md`

Recommended responsibilities:

- Define serde-backed structs for curated touched families, family members, explicit namespace mappings, recovery rules, and rule triggers.
- Add one initialization path such as `init_curated_family_knowledge(tool_root: &Path)` so the loader can read repo data using the same `tool_root` already available in `resolver::resolve_path(...)`.
- Cache the validated curated data behind a deterministic once-cell style singleton instead of reparsing the JSON on every lookup.
- Return `Result<_, String>` validation errors with exact family or rule identifiers in the message.
- Keep `BTreeMap` and `BTreeSet` ordering so diagnostics are deterministic and diff-friendly.

Recommended touched scope for the initial seed:

- family entries covering `setuptools`, `pil`, `sklearn`, `legacy-pymc3`, `legacy-tensorflow`, and `legacy-ggplot`
- explicit namespace mappings at least for `pkg_resources`, `PIL`, `Image`, `ImageDraw`, `ImageFont`, `ImageEnhance`, `ImageGrab`, and `sklearn`
- recovery rules for `pkg-resources`, `legacy-pillow`, `legacy-pymc3`, `legacy-tensorflow`, `keras-backend`, and `legacy-ggplot`

### 2. Drive touched-family runtime behavior from curated data, not duplicated hardcoded branches

Recommended runtime entrypoints to update:

- `tools/apdr/src/resolver/mod.rs`
- `tools/apdr/src/resolver/family_knowledge/mod.rs`
- `tools/apdr/src/resolver/family_knowledge/core.rs`
- `tools/apdr/src/resolver/family_knowledge/detection.rs`
- `tools/apdr/src/resolver/family_knowledge/legacy_bundles.rs`
- `tools/apdr/src/resolver/recovery_diagnostics.rs`
- `tools/apdr/src/resolver/retry_loop.rs`

Recommended migration rule:

- Touched families use curated data.
- Untouched families continue using the static `FAMILIES` table for this phase.

That avoids widening Phase 8 into a full family-registry rewrite while still satisfying the milestone requirements for the touched boundary.

Important runtime behaviors that must become data-driven for touched families:

- `FamilyRegistry` package/member lookups and conflict pruning
- `namespace_mapping_allowed(...)`
- `apply_family_knowledge(...)`
- `recover_family_knowledge(...)`
- `protects_family_version(...)`
- `validation_candidate_versions(...)`
- the direct `pkg_resources -> setuptools` branch in `retry_loop.rs`

The reviewer-facing note strings for the touched families should stay stable unless the curated data explicitly changes them. Phase 7 fixtures rely on the current behaviors remaining recognizable.

### 3. Add one regression layer and one phase-close checker

Recommended new closeout artifacts:

- `scripts/check_phase8_family_runtime.py`
- `.planning/phases/08-data-driven-family-knowledge-runtime/08-FAMILY-RUNTIME.md`

Recommended regression approach:

- Extend `tools/apdr/tests/test_resolver.rs` with a Phase 8 fixture helper that reads snippets from `tools/apdr/tests/phase7_family_fixtures/`.
- Keep the tests bounded to the touched Phase 7 families instead of broadening into a full benchmark replay.
- Assert concrete family outcomes for the protected surfaces:
  - Pillow cases still pin `Pillow==6.2.2`
  - PyMC3 cases still keep the coherent pinned bundle
  - keras/TensorFlow cases still carry the backend companion behavior
  - `pkg_resources` cases still add `setuptools`
  - `sklearn` cases still resolve to `scikit-learn`

Recommended checker responsibilities:

- verify that every touched family surfaced by `07-family-snapshot-manifest.json` has a corresponding curated family or recovery rule
- verify required explicit namespace mappings are present in the curated data
- verify the reviewer note lists the data files, touched runtime coverage, diagnostics contract, and Phase 9 handoff
- run or gate on the targeted resolver tests rather than a full benchmark rerun

## Validation Architecture

### Quick checks

- `cargo test --manifest-path tools/apdr/Cargo.toml --test test_resolver data_driven_family_ -- --nocapture`
- `cargo test --manifest-path tools/apdr/Cargo.toml --test test_resolver phase7_family_ -- --nocapture`
- `python -m py_compile scripts/check_phase8_family_runtime.py`

### Artifact checks

- `rg -n 'setuptools|legacy-pymc3|legacy-tensorflow|legacy-ggplot|pil|sklearn' tools/apdr/data/family_knowledge/touched_families.json`
- `rg -n 'pkg-resources|legacy-pillow|legacy-pymc3|legacy-tensorflow|keras-backend|legacy-ggplot' tools/apdr/data/family_knowledge/touched_recovery_rules.json`
- `python scripts/check_phase8_family_runtime.py --family-manifest .planning/phases/07-failure-baseline-parity-slice/07-family-snapshot-manifest.json --families-json tools/apdr/data/family_knowledge/touched_families.json --recovery-json tools/apdr/data/family_knowledge/touched_recovery_rules.json --baseline-md .planning/phases/08-data-driven-family-knowledge-runtime/08-FAMILY-RUNTIME.md`

### Phase-close checks

- `cargo test --manifest-path tools/apdr/Cargo.toml --test test_resolver data_driven_family_ -- --nocapture`
- `cargo test --manifest-path tools/apdr/Cargo.toml --test test_resolver phase7_family_ -- --nocapture`
- `cargo test --manifest-path tools/apdr/Cargo.toml --test test_resolver resolver_ -- --nocapture`
- `python scripts/check_phase8_family_runtime.py --family-manifest .planning/phases/07-failure-baseline-parity-slice/07-family-snapshot-manifest.json --families-json tools/apdr/data/family_knowledge/touched_families.json --recovery-json tools/apdr/data/family_knowledge/touched_recovery_rules.json --baseline-md .planning/phases/08-data-driven-family-knowledge-runtime/08-FAMILY-RUNTIME.md`

## Canonical Files For Planning

- `.planning/PROJECT.md`
- `.planning/ROADMAP.md`
- `.planning/REQUIREMENTS.md`
- `.planning/STATE.md`
- `.planning/phases/07-failure-baseline-parity-slice/07-BASELINE.md`
- `.planning/phases/07-failure-baseline-parity-slice/07-FAMILY-SNAPSHOTS.md`
- `.planning/phases/07-failure-baseline-parity-slice/07-family-snapshot-manifest.json`
- `tools/apdr/src/resolver/mod.rs`
- `tools/apdr/src/resolver/family_knowledge/mod.rs`
- `tools/apdr/src/resolver/family_knowledge/core.rs`
- `tools/apdr/src/resolver/family_knowledge/detection.rs`
- `tools/apdr/src/resolver/family_knowledge/legacy_bundles.rs`
- `tools/apdr/src/resolver/family_knowledge/learned.rs`
- `tools/apdr/src/resolver/recovery_diagnostics.rs`
- `tools/apdr/src/resolver/retry_loop.rs`
- `tools/apdr/src/cache/store.rs`
- `tools/apdr/tests/test_resolver.rs`
- `tools/apdr/tests/phase7_family_fixtures/README.md`

## Out-of-Scope For This Phase

- migrating untouched families outside the Phase 7 touched-family boundary
- changing the canonical 70-case Phase 7 parity slice or the 17-case watchlist split
- broad recovery-accuracy work on non-family failures from the parity slice
- rewriting learned family persistence in `.apdr-cache`
- touching unrelated local edits in `tools/apdr/src/lib.rs` or `tools/apdr/llm_py/tests/test_llm_integration.py`

---
*Research created: 2026-03-28*
*Phase: 08-data-driven-family-knowledge-runtime*
