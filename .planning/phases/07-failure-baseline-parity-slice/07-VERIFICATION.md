---
phase: 07-failure-baseline-parity-slice
verified: 2026-03-28T22:29:44Z
status: passed
score: 4/4 automated must-haves verified
re_verification: true
---

# Phase 7: Failure Baseline & Parity Slice Verification Report

**Phase Goal:** Turn the stopped APDR run and `pllm` comparison into a reproducible, bounded milestone target before changing behavior.
**Verified:** 2026-03-28T22:29:44Z
**Status:** passed
**Re-verification:** Yes - manual approval backfill

## Goal Achievement

### Observable Truths

| # | Truth | Status | Evidence |
| --- | --- | --- | --- |
| 1 | A reproducible case list exists for the APDR-failed and `pllm`-passing slice shared by `runs\20260327-150339-apdr` and `pllm_results` | ✓ VERIFIED | `07-tier3-parity-manifest.json` fixes the `70` tier3 canonical cases and the `17` tier1 watchlist; `scripts/check_phase7_baseline.py` re-derives the same IDs from the raw inputs |
| 2 | Target cases are labeled by tier and dominant failure bucket so later fixes can be measured instead of guessed | ✓ VERIFIED | The parity manifest preserves raw APDR fields, `tier`, and `normalized_bucket`; `normalized_bucket_totals` matches the per-case values |
| 3 | Touched family-knowledge cases have regression fixtures or snapshots that lock current intended behavior before migration | ✓ VERIFIED | `07-family-snapshot-manifest.json` records `17` touched-family cases with `selection_reasons` and `fixture_path`; `tools/apdr/tests/phase7_family_fixtures/` contains the copied benchmark-derived `snippet.py` fixtures |
| 4 | The milestone has a bounded improvement target rather than an open-ended accuracy wish list | ✓ VERIFIED | `07-BASELINE.md` states the canonical 70-case slice, the touched-family 17-case subset, the 17-case watchlist, and the Phase 8 migration boundary |

**Score:** 4/4 automated must-haves verified

### Required Artifacts

| Artifact | Expected | Status | Details |
| --- | --- | --- | --- |
| `scripts/build_phase7_parity_manifest.py` | Canonical slice generator | ✓ VERIFIED | Generates the fixed 70-case parity manifest and Markdown summary |
| `.planning/phases/07-failure-baseline-parity-slice/07-tier3-parity-manifest.json` | Canonical source of truth | ✓ VERIFIED | Contains `canonical_case_count: 70`, `tier1_watchlist_count: 17`, and `normalized_bucket_totals` |
| `scripts/build_phase7_family_snapshots.py` | Touched-family selector and copier | ✓ VERIFIED | Selects from the canonical manifest only and records per-case `selection_reasons` |
| `.planning/phases/07-failure-baseline-parity-slice/07-family-snapshot-manifest.json` | Machine-readable family snapshot contract | ✓ VERIFIED | Contains `selected_case_count: 17` and fixture paths rooted under `tools/apdr/tests/phase7_family_fixtures/` |
| `tools/apdr/tests/phase7_family_fixtures/README.md` | Isolated fixture-root explanation | ✓ VERIFIED | Explains the benchmark-derived fixture root and why it stays outside the legacy continuity root |
| `scripts/check_phase7_baseline.py` | Deterministic phase-close checker | ✓ VERIFIED | Re-derives the overlap contract, validates manifests, and checks the baseline note |
| `.planning/phases/07-failure-baseline-parity-slice/07-BASELINE.md` | Reviewer-facing closeout note | ✓ VERIFIED | Includes commands, artifact links, canonical slice, normalized buckets, family snapshot boundary, watchlist, verification, and Phase 8 handoff |

### Behavioral Spot-Checks

| Behavior | Command | Result | Status |
| --- | --- | --- | --- |
| Parity generator rerun succeeds | `python scripts/build_phase7_parity_manifest.py ...` | Regenerated the canonical manifest and Markdown summary successfully | ✓ PASS |
| Family snapshot generator rerun succeeds | `python scripts/build_phase7_family_snapshots.py ...` | Regenerated the 17-case family snapshot manifest, summary, README, and fixture corpus successfully | ✓ PASS |
| Phase-close checker succeeds | `python scripts/check_phase7_baseline.py ...` | `Phase 7 baseline check passed` | ✓ PASS |
| Targeted resolver guardrail stays green | `cargo test --manifest-path tools/apdr/Cargo.toml --test test_resolver resolver_ -- --nocapture` | `19` tests passed, `0` failed | ✓ PASS |

### Requirements Coverage

| Requirement | Source Plans | Description | Status | Evidence |
| --- | --- | --- | --- | --- |
| REC-01 | 07-01, 07-02, 07-03 | The repo has a reproducible target slice derived from the stopped APDR run and `pllm` CSV | ✓ SATISFIED | Canonical 70-case parity manifest, checker, and baseline note all reference the locked raw inputs and counts |
| FAM-04 | 07-02, 07-03 | Touched family-knowledge behavior is covered by regression fixtures or tests so the data migration preserves intended outcomes | ✓ SATISFIED | 17-case family snapshot corpus, copied fixtures, README boundary, and green targeted `resolver_` test slice |

### Human Verification Required

The previously pending document-review items were approved on 2026-03-28:

1. `07-TIER3-PARITY-MANIFEST.md` and `07-BASELINE.md` keep the 17 tier1 overlap cases outside the Phase 7 contract as a watchlist, not part of the canonical baseline.
2. `07-FAMILY-SNAPSHOTS.md` keeps the touched-family selection rationale anchored to `family:` markers, `Family knowledge` notes, and the explicit namespace or bundle anchors owned by the current family runtime.

### Gaps Summary

No automated gaps found. Phase 7's generated artifacts, checker, and targeted resolver gate all passed, and there are no remaining human verification blockers after the approved 2026-03-28 review.

---

_Verified: 2026-03-28T22:29:44Z_
_Verifier: Codex inline backfill_
