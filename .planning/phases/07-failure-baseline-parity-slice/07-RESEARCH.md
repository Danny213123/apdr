# Phase 7: Failure Baseline & Parity Slice - Research

**Researched:** 2026-03-28
**Domain:** Reproducible parity-slice baselining, failure-bucket normalization, and benchmark-derived family snapshots for the v2.1 accuracy milestone
**Confidence:** Medium

## Summary

Phase 7 should lock the v2.1 accuracy target into reproducible artifacts before any recovery behavior changes. The repo already has the needed raw inputs: `runs/20260327-150339-apdr/summary.json` carries the benchmark case records and explicit `tier` field, `pllm_results/csv/summary-all-runs.csv` carries the parity pass counts, and each case directory already contains a `resolution-report.txt` with richer failure details than the summary alone. The planning problem is therefore not how to invent a new benchmark format, but how to turn the stopped run into a stable case-level contract that later phases can compare against without reinterpreting the raw March 27, 2026 data every time.

Primary recommendation: plan Phase 7 as three sequential plans. First, generate a canonical Phase 7 manifest for the fixed 70-case tier3 slice, preserving raw APDR fields and adding one deterministic normalized bucket per case. Second, generate a bounded family-snapshot corpus for the subset of those cases that touch the current family-knowledge runtime, and keep those fixtures outside `tools/apdr/tests/fixtures/` so the legacy continuity harness does not change. Third, add a lightweight baseline checker plus a reviewer-facing baseline note that proves the generated artifacts still match the stopped run, the `pllm` CSV, and the copied family fixtures.

## Phase Requirements

| ID | Requirement | Research Support |
|----|-------------|------------------|
| REC-01 | The repo has a reproducible target slice derived from `runs\\20260327-150339-apdr` and `pllm_results\\csv\\summary-all-runs.csv` for APDR-failed but `pllm`-passing cases | The stopped-run summary and `pllm` CSV already join cleanly on the APDR case ID, which is the `artifact_dir` basename in the summary and the `name` column in the CSV. |
| FAM-04 | Touched family-knowledge behavior is covered by regression fixtures or tests so the data migration preserves intended outcomes | Each canonical case already has a `resolution-report.txt`, and the current family runtime exposes both direct `family:` markers and stable namespace-mapping surfaces that can drive a deterministic touched-family fixture set. |

## Evidence That Should Drive Planning

### The canonical parity slice is fixed and should use the stored `tier` field

The March 27, 2026 stopped run and the `pllm` CSV join into:

- `87` APDR-failed cases where `pllm` passed at least once
- `70` of those cases are `tier3`
- `17` are `tier1` watchlist cases outside the Phase 7 contract
- `72` of the `87` overlap cases have `pllm` pass counts >= `5`
- `51` of the `87` overlap cases have `pllm` pass counts of `10`

The important planning detail is that tier classification must come from `summary.json`'s explicit `tier` field. Inferring tier from `llm_calls > 0` undercounts the canonical slice because some tier3 failures still record `llm_calls: 0`.

### Bucket normalization needs a report fallback, not just summary fields

For the canonical 70-case slice, the raw stopped-run fields currently distribute as:

- `environment-build-failed`: `21`
- `module-not-found`: `19`
- `dependency-conflict`: `12`
- `version-not-found`: `11`
- `syntax-error`: `5`
- `import-error`: `1`
- one additional case has blank summary bucket fields even though its `resolution-report.txt` still reports `import-error`

That last case matters. Case `4145581` has empty `output_metadata` in `summary.json`, but its on-disk `resolution-report.txt` still contains `validation_status: import-error` and `failure_bucket: import-error`. Phase 7 should therefore normalize buckets with a deterministic fallback order that prefers raw summary fields but falls back to the case report before giving up.

### The stopped run already has the exact case-level details the manifest needs

The summary JSON carries:

- `artifact_dir`, which yields the canonical case ID
- `tier`
- `snippet`
- `requirements`
- `succeeded` and `skipped`
- `output_metadata.validation_status`
- `output_metadata.validation_reason`
- `output_metadata.failure_bucket`
- `output_metadata.report_path`
- `log_tail`

The report files add:

- `resolved_dependencies`
- `notes`
- `missing_module`
- `failing_package`
- detailed `validation_attempts`

That is enough to generate a machine-readable manifest and a reviewer-readable Markdown summary without rerunning any benchmark cases.

### Touched family behavior is visible through both report markers and runtime surfaces

The current family runtime already exposes two useful signal types:

1. Direct report markers:
   - `resolved_dependencies` strategies such as `family:legacy-pymc3`
   - `notes:` entries beginning with `Family knowledge ...`
2. Runtime-owned identifiers:
   - explicit namespace mappings in `family_knowledge/core.rs` such as `pkg_resources -> setuptools`, `PIL/Image -> Pillow`, `cv2 -> opencv-python`, `rest_framework -> djangorestframework`, `sklearn -> scikit-learn`, and `bs4 -> beautifulsoup4`
   - legacy bundle anchors in `family_knowledge/legacy_bundles.rs` for stacks such as legacy PyMC3, legacy Flask, johnny-cache, Scrapy, cfscrape, legacy ggplot, SimpleCV/OpenCV, and legacy TensorFlow/Keras

A rough scan over the canonical 70-case slice found `17` likely family-touching cases, including `11` cases that already show direct `family:` or `Family knowledge` report markers. That supports a deterministic touched-family rule that combines report markers with narrowly scoped package and namespace anchors.

### Benchmark-derived fixtures must stay out of the legacy continuity fixture root

`scripts/measure_apdr_baseline.py` recursively enumerates `tools/apdr/tests/fixtures/`. If Phase 7 adds new benchmark-derived family files under that existing root, later continuity commands would silently change their lexicographic sample set. The benchmark-derived family fixtures should therefore live in a separate root such as `tools/apdr/tests/phase7_family_fixtures/`, with a README that explains why they are isolated.

### Example cases already show the two main artifact patterns

Representative cases worth grounding the plan in:

- `1433392`: `tier3`, requirements `['proj']`, `module-not-found`, missing module `gisutils`, report path `runs\\20260327-150339-apdr\\cases\\1433392\\resolution-report.txt`
- `1068868`: `tier3`, requirements `['Django==5.1.3', 'django-taggit']`, `module-not-found`, missing module `taggit_autocomplete`
- `1440754`: `tier3`, requirements `['github2']`, `version-not-found`, no automatic fix found for `python-dateutil<2.0,>=2.1`
- `2de2e9a156fe619dbdad762fe1cf84e1`: `tier3`, report contains both `family:legacy-pymc3` strategies and `Family knowledge targeted the legacy PyMC3 stack ...`
- `035dc3b722b7f89cce66520dde285c9a`: `tier3`, `environment-build-failed`, repeated `BuildFailure|TPL-OS||pyeclib` signature

These examples support the three required Phase 7 outputs: a canonical manifest, a family-snapshot subset, and a checker that keeps both tied back to the stopped run.

## Implementation Recommendations

### 1. Generate one JSON-first canonical manifest and one reviewer summary

Recommended script:

- `scripts/build_phase7_parity_manifest.py`

Recommended outputs:

- `.planning/phases/07-failure-baseline-parity-slice/07-tier3-parity-manifest.json`
- `.planning/phases/07-failure-baseline-parity-slice/07-TIER3-PARITY-MANIFEST.md`

Recommended JSON contract:

- root metadata: source file paths, generation date, canonical count `70`, tier1 watchlist count `17`
- one case entry per canonical case with:
  - `case_id`
  - `tier`
  - `snippet`
  - `requirements`
  - `pllm_pass_count`
  - `raw_validation_status`
  - `raw_failure_bucket`
  - `normalized_bucket`
  - `validation_reason`
  - `report_path`
  - `log_tail`
- aggregate bucket totals keyed by `normalized_bucket`

Recommended normalization precedence:

1. `failure_bucket` from `output_metadata` when present and not blank or `--`
2. `validation_status` from `output_metadata` when present and not blank or `--`
3. `failure_bucket:` or `validation_status:` parsed from `resolution-report.txt`
4. known `log_tail` patterns such as `error: module-not-found:` or `error: import-error:`
5. `unclassified`

### 2. Build the family-snapshot subset from the canonical manifest, not from a fresh benchmark scan

Recommended script:

- `scripts/build_phase7_family_snapshots.py`

Recommended outputs:

- `.planning/phases/07-failure-baseline-parity-slice/07-family-snapshot-manifest.json`
- `.planning/phases/07-failure-baseline-parity-slice/07-FAMILY-SNAPSHOTS.md`
- `tools/apdr/tests/phase7_family_fixtures/README.md`
- copied snippets under `tools/apdr/tests/phase7_family_fixtures/<case_id>/snippet.py`

Recommended touched-family rule:

- Start from the canonical 70-case manifest only.
- Mark a case as touched-family when at least one of the following is true:
  - the report contains `family:` in `resolved_dependencies`
  - the report notes contain `Family knowledge`
  - the case requirements, failing package, or missing module hit one of the explicit namespace-mapping surfaces: `pkg_resources`, `PIL`, `Image`, `ImageDraw`, `ImageFont`, `ImageEnhance`, `ImageGrab`, `cv2`, `rest_framework`, `sklearn`, or `bs4`
  - the case requirements, failing package, or missing module hit one of the family-bundle anchors that are specific enough to avoid generic over-selection: `pymc3`, `Theano`, `Theano-PyMC`, `Lasagne`, `arviz`, `xarray-einstats`, `flask_security`, `flask_principal`, `flask_admin`, `flask_sqlalchemy`, `johnny`, `johnny-cache`, `scrapy`, `cfscrape`, `ggplot`, `SimpleCV`, `tensorflow`, or `keras`

The snapshot manifest should preserve `selection_reasons` so later Phase 8 work can show exactly why a case entered the migration-protection set.

### 3. Add one checker script so Phase 7 stays rerunnable without live LLM calls

Recommended script:

- `scripts/check_phase7_baseline.py`

Recommended responsibilities:

- re-derive the canonical overlap from `summary.json` plus the `pllm` CSV
- verify the generated parity manifest still contains exactly the same 70 canonical case IDs and 17 excluded tier1 watchlist IDs
- verify the normalized bucket totals in the manifest match the case entries
- verify the family-snapshot manifest only references canonical cases
- verify every recorded fixture path exists
- verify the reviewer note `07-BASELINE.md` includes the canonical slice, normalized buckets, touched-family subset, excluded tier1 watchlist, and Phase 8 handoff

This lets Phase 7 execution rely on deterministic local artifacts plus the existing targeted resolver test surface instead of any fresh benchmark rerun.

## Validation Architecture

### Quick checks

- `python -m py_compile scripts/build_phase7_parity_manifest.py scripts/build_phase7_family_snapshots.py scripts/check_phase7_baseline.py`
- `rg -n 'canonical_case_count|normalized_bucket|tier1_watchlist_count' .planning/phases/07-failure-baseline-parity-slice/07-tier3-parity-manifest.json`
- `cargo test --manifest-path tools/apdr/Cargo.toml --test test_resolver resolver_ -- --nocapture`

### Artifact checks

- `python scripts/build_phase7_parity_manifest.py --summary-json runs/20260327-150339-apdr/summary.json --pllm-csv pllm_results/csv/summary-all-runs.csv --output-json .planning/phases/07-failure-baseline-parity-slice/07-tier3-parity-manifest.json --output-md .planning/phases/07-failure-baseline-parity-slice/07-TIER3-PARITY-MANIFEST.md`
- `python scripts/build_phase7_family_snapshots.py --parity-manifest .planning/phases/07-failure-baseline-parity-slice/07-tier3-parity-manifest.json --cases-root runs/20260327-150339-apdr/cases --fixtures-root tools/apdr/tests/phase7_family_fixtures --output-json .planning/phases/07-failure-baseline-parity-slice/07-family-snapshot-manifest.json --output-md .planning/phases/07-failure-baseline-parity-slice/07-FAMILY-SNAPSHOTS.md`
- `python scripts/check_phase7_baseline.py --summary-json runs/20260327-150339-apdr/summary.json --pllm-csv pllm_results/csv/summary-all-runs.csv --parity-manifest .planning/phases/07-failure-baseline-parity-slice/07-tier3-parity-manifest.json --family-manifest .planning/phases/07-failure-baseline-parity-slice/07-family-snapshot-manifest.json --baseline-md .planning/phases/07-failure-baseline-parity-slice/07-BASELINE.md`

### Phase-close checks

- `python -m py_compile scripts/build_phase7_parity_manifest.py scripts/build_phase7_family_snapshots.py scripts/check_phase7_baseline.py`
- `python scripts/check_phase7_baseline.py --summary-json runs/20260327-150339-apdr/summary.json --pllm-csv pllm_results/csv/summary-all-runs.csv --parity-manifest .planning/phases/07-failure-baseline-parity-slice/07-tier3-parity-manifest.json --family-manifest .planning/phases/07-failure-baseline-parity-slice/07-family-snapshot-manifest.json --baseline-md .planning/phases/07-failure-baseline-parity-slice/07-BASELINE.md`
- `cargo test --manifest-path tools/apdr/Cargo.toml --test test_resolver resolver_ -- --nocapture`

## Canonical Files For Planning

- `.planning/PROJECT.md`
- `.planning/ROADMAP.md`
- `.planning/REQUIREMENTS.md`
- `.planning/STATE.md`
- `.planning/phases/07-failure-baseline-parity-slice/07-CONTEXT.md`
- `.planning/codebase/CONVENTIONS.md`
- `.planning/codebase/TESTING.md`
- `runs/20260327-150339-apdr/summary.json`
- `runs/20260327-150339-apdr/benchmark-context.log`
- `pllm_results/csv/summary-all-runs.csv`
- `tools/apdr/src/resolver/family_knowledge/core.rs`
- `tools/apdr/src/resolver/family_knowledge/detection.rs`
- `tools/apdr/src/resolver/family_knowledge/legacy_bundles.rs`
- `tools/apdr/src/resolver/recovery_diagnostics.rs`
- `tools/apdr/tests/test_resolver.rs`
- `scripts/measure_apdr_baseline.py`

## Out-of-Scope For This Phase

- any change to APDR recovery behavior or family-knowledge runtime logic
- migrating hardcoded family bundles into data files before the baseline exists
- adding new fixtures under `tools/apdr/tests/fixtures/`
- rerunning the March 27, 2026 benchmark as part of the baseline proof
- broad accuracy work on the 17 tier1 watchlist cases
- touching unrelated local edits in `tools/apdr/src/lib.rs` or `tools/apdr/llm_py/tests/test_llm_integration.py`

---
*Research created: 2026-03-28*
*Phase: 07-failure-baseline-parity-slice*
