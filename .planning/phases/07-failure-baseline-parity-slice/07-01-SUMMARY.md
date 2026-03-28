---
phase: 07-failure-baseline-parity-slice
plan: 01
subsystem: benchmarking
tags:
  - apdr
  - benchmarking
  - parity
  - baseline
  - python
dependency_graph:
  requires: []
  provides:
    - phase-7-canonical-tier3-parity-manifest
    - normalized-failure-bucket-baseline
    - reviewer-readable-tier1-watchlist
  affects:
    - 07-02
    - 07-03
    - 08
tech_stack:
  added: []
  patterns:
    - deterministic-benchmark-manifest-generation
    - summary-report-log-tail-bucket-normalization
    - json-first-phase-baseline-artifacts
key_files:
  created:
    - .planning/phases/07-failure-baseline-parity-slice/07-tier3-parity-manifest.json
    - .planning/phases/07-failure-baseline-parity-slice/07-TIER3-PARITY-MANIFEST.md
    - .planning/phases/07-failure-baseline-parity-slice/07-01-SUMMARY.md
  modified:
    - scripts/build_phase7_parity_manifest.py
key-decisions:
  - Used the stored summary `tier` field, not `llm_calls`, to define the canonical Phase 7 slice so the 70-case tier3 contract matches the stopped benchmark data exactly.
  - Normalized failure buckets with summary fields first, then `resolution-report.txt`, then `log_tail`, which preserves deterministic classification for the blank-summary case `4145581`.
  - Kept the 17 overlapping tier1 cases in an explicit watchlist section instead of absorbing them into the canonical Phase 7 baseline.
patterns-established:
  - "Benchmark-derived milestone baselines should preserve raw APDR fields alongside one deterministic normalized bucket."
  - "Phase 7 artifact generation is JSON-first, with Markdown summaries derived from the same machine-readable manifest."
requirements-completed:
  - REC-01
metrics:
  completed_date: "2026-03-28"
  tasks_completed: 2
  verification_tests: 4
---

# Phase 7 Plan 01 Summary

**Deterministic 70-case tier3 parity manifest with normalized failure buckets and a reviewer summary anchored to the March 27, 2026 stopped benchmark.**

## Accomplishments

- Added `scripts/build_phase7_parity_manifest.py` to join the stopped APDR summary with the `pllm` CSV on case ID, preserve raw APDR fields, and emit deterministic ordering by `case_id`.
- Generated `.planning/phases/07-failure-baseline-parity-slice/07-tier3-parity-manifest.json` with the fixed `70` canonical tier3 cases, `17` excluded tier1 watchlist cases, per-case normalized buckets, and aggregate bucket totals.
- Generated `.planning/phases/07-failure-baseline-parity-slice/07-TIER3-PARITY-MANIFEST.md` with reviewer-facing source inputs, canonical-slice rules, normalized bucket totals, representative cases, and the explicit Phase 7 watchlist boundary.

## Verification Results

- `python -m py_compile scripts/build_phase7_parity_manifest.py` passed.
- `python scripts/build_phase7_parity_manifest.py --summary-json runs/20260327-150339-apdr/summary.json --pllm-csv pllm_results/csv/summary-all-runs.csv --output-json .planning/phases/07-failure-baseline-parity-slice/07-tier3-parity-manifest.json --output-md .planning/phases/07-failure-baseline-parity-slice/07-TIER3-PARITY-MANIFEST.md` passed.
- `rg -n 'canonical_case_count|tier1_watchlist_count|normalized_bucket' .planning/phases/07-failure-baseline-parity-slice/07-tier3-parity-manifest.json` passed.
- `rg -n '## Canonical Slice|## Normalized Buckets|## Tier1 Watchlist' .planning/phases/07-failure-baseline-parity-slice/07-TIER3-PARITY-MANIFEST.md` passed.

## Files Created/Modified

- `scripts/build_phase7_parity_manifest.py` - deterministic generator for the canonical Phase 7 tier3 parity slice and reviewer summary.
- `.planning/phases/07-failure-baseline-parity-slice/07-tier3-parity-manifest.json` - machine-readable canonical slice with raw APDR fields, `pllm` pass counts, and normalized bucket totals.
- `.planning/phases/07-failure-baseline-parity-slice/07-TIER3-PARITY-MANIFEST.md` - reviewer-readable summary of the canonical slice, representative cases, and tier1 watchlist.

## Decisions Made

- The canonical Phase 7 target is the APDR-failed, non-skipped, `pllm`-passing overlap filtered by the stored `tier3` summary field, yielding the fixed `70`-case slice.
- Bucket normalization prefers summary metadata but falls back to the case report and then `log_tail`, so blank summary metadata cannot silently drop a case into `unclassified`.
- Tier1 overlap cases remain documented for future work but are outside the Phase 7 contract and therefore excluded from the canonical manifest.

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered

- Case `4145581` had an empty `output_metadata` block in `summary.json`, so the generator had to fall back to `resolution-report.txt` to preserve its `import-error` classification deterministically.
- Existing unrelated local changes in `tools/apdr/src/lib.rs` and `tools/apdr/llm_py/tests/test_llm_integration.py` were left untouched.

## Next Phase Readiness

- `07-02` can now select the touched-family subset from the canonical manifest instead of rescanning the raw benchmark data.
- `07-03` can verify the Phase 7 baseline against one fixed machine-readable contract and cite the Markdown summary for reviewer-facing context.

## Self-Check: PASSED

- `.planning/phases/07-failure-baseline-parity-slice/07-tier3-parity-manifest.json` contains `"canonical_case_count": 70`.
- `.planning/phases/07-failure-baseline-parity-slice/07-tier3-parity-manifest.json` contains `"tier1_watchlist_count": 17`.
- `.planning/phases/07-failure-baseline-parity-slice/07-tier3-parity-manifest.json` contains per-case `"normalized_bucket"` entries and a `"normalized_bucket_totals"` object.
- `.planning/phases/07-failure-baseline-parity-slice/07-TIER3-PARITY-MANIFEST.md` contains `## Canonical Slice`, `## Normalized Buckets`, and `## Tier1 Watchlist`.

---
*Phase: 07-failure-baseline-parity-slice*
*Completed: 2026-03-28*
