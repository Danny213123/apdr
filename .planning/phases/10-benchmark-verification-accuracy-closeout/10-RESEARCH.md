# Phase 10: Benchmark Verification & Accuracy Closeout - Research

**Researched:** 2026-03-28
**Domain:** Manifest-driven targeted benchmark rerun, case-level delta reporting, and milestone closeout for the bounded Phase 7 through Phase 9 accuracy work
**Confidence:** Medium

## Summary

Phase 10 should not invent a new benchmark framework or reopen recovery logic. The repo already has the locked comparison boundary from Phase 7, the stabilized Phase 8 family-runtime checker, the bounded Phase 9 targeted-recovery checker, the stopped March 27, 2026 benchmark summary, and working case-comparison logic in the benchmark UI. The missing work is to turn those into one deterministic rerun of the exact targeted surface, emit the split machine-readable and reviewer-readable artifacts the user requested, preserve an explicit pass-and-skip guard set for `REC-05`, and finish with a deterministic closeout checker plus milestone note.

Primary recommendation: plan Phase 10 as three sequential plans. First, create a deterministic rerun manifest plus a manifest-driven benchmark wrapper that executes only the canonical `70` cases, the separate `17`-case watchlist, and a separate `REC-05` preservation guard set, then write a machine-readable rerun or delta artifact. Second, turn that machine artifact into the split evidence package the user asked for: a canonical-slice benchmark-verification note, a watchlist appendix, a preservation-guard note, and an unrecovered-gap report grouped by dominant bucket with case IDs and follow-on notes. Third, add a deterministic Phase 10 checker, rerun the carried-forward Phase 8 and Phase 9 validation commands, and write the milestone closeout note without touching unrelated local edits.

## Phase Requirements

| ID | Requirement | Research Support |
|----|-------------|------------------|
| REC-05 | Recovery changes preserve existing passed cases and expected skip behavior for host-runtime, unsolvable, and local-helper cases on the rerun | The canonical `70`-case slice is all baseline failures, so preservation needs a separate explicit guard subset drawn from the same March 27, 2026 stopped run. |
| EVD-01 | Benchmark artifacts report case-level APDR versus baseline versus `pllm` deltas for the targeted slice | `runs/20260327-150339-apdr/summary.json`, `pllm_results/csv/summary-all-runs.csv`, and the benchmark UI comparison helpers already provide the source data needed for a deterministic per-case delta artifact. |
| EVD-02 | Milestone closeout records remaining unrecovered parity cases by dominant failure bucket with enough detail for follow-on planning | The Phase 7 manifest already stores normalized bucket ownership for the canonical slice, so the closeout can group unrecovered cases by dominant bucket without redefining the boundary. |

## Evidence That Should Drive Planning

### The comparison boundary is already locked and must stay split

`07-tier3-parity-manifest.json` already fixes the Phase 10 comparison surface:

- `70` canonical tier3 cases in the main contract
- `17` watchlist cases outside the main contract
- `87` APDR-failed but `pllm`-passing overlap cases total

The user reaffirmed this in `10-CONTEXT.md`: the canonical `70`-case slice is the main report, and the `17` watchlist stays in a separate appendix or companion artifact. Phase 10 therefore needs to consume the existing manifest directly instead of rebuilding a new target slice from the raw benchmark.

### Phase 8 and Phase 9 already define the measurement contract

`08-FAMILY-RUNTIME.md` and `09-TARGETED-RECOVERY.md` already tell Phase 10 what must remain stable and what must be measured:

- the Phase 8 family-runtime boundary stays locked
- the Phase 9 measurement targets are targeted compatibility recovery, transitive specifier normalization, module stop reasons, and Phase 8 boundary preservation
- the operating rule is explicit: measure the bounded changes, do not reopen the Phase 7 or Phase 8 boundaries

That means Phase 10 is a verification-and-closeout phase, not another resolver-design phase.

### Existing benchmark artifacts already expose the needed case-level surfaces

The stopped run in `runs/20260327-150339-apdr/summary.json` already contains, per case:

- `snippet`
- `artifact_dir`
- `requirements`
- `validation_status`
- `validation_reason`
- `output_metadata`
- per-case output and trace locations

The same run also wrote `runs/20260327-150339-apdr/benchmark-context.log`, which captures the exact baseline invocation contract:

- tool: `apdr`
- dataset: `hard-gists`
- total snippets: `2890`
- command flags including `--range 5`, `--max-retries 5`, `--docker-timeout 900`, `--validation-backend llm`, `--allow-llm`, `--no-execute-snippet`, and `--force-validate`
- model `qwen3.5:9b` and base URL `http://localhost:11434`

Phase 10 should reuse this command shape for apples-to-apples reruns rather than inventing a new execution contract.

### The repo already has status-comparison logic that should be mirrored, not replaced

`benchmark_ui/service.py` already defines how historical results should be normalized for comparison:

- `_load_pllm_baseline` reduces `summary-all-runs.csv` into per-case `PASS` or `FAIL`
- `_result_succeeded` and `_result_skipped` treat `skipped-*` and `host-runtime-required` carefully, including the special case where host-runtime snippets with valid requirements count as effective passes
- `_comparison_entry` already expresses a clean `MATCH` versus `DIFF` comparison between current status and baseline status

Phase 10 should mirror this status logic in its new benchmark script or shared helper rather than inventing a new interpretation of pass, fail, and skip.

### `measure_apdr_baseline.py` is useful, but its input model is too broad for the locked slice

`scripts/measure_apdr_baseline.py` already has several Phase 10-friendly properties:

- JSON-first output
- optional Markdown output
- support for dataset-root reruns
- benchmark context log plumbing

But it currently discovers snippets by fixture root or dataset root plus a lexicographic limit. That is good for broad slices such as the old Phase 6 hard-gists sample, but it is not sufficient for Phase 10 because the Phase 7 target is a manifest-selected set of case IDs, not a lexicographic prefix of `hard-gists/`.

The safest Phase 10 shape is therefore a thin manifest-driven wrapper script that:

- reads the Phase 7 manifest
- resolves exact snippet paths from the baseline summary
- executes only those exact cases
- keeps watchlist and preservation guards separate from the main canonical report

### `REC-05` needs an explicit preservation guard subset outside the canonical failure slice

The canonical `70`-case slice is entirely composed of baseline APDR failures, so it cannot by itself prove that previously passed or expected-skip behavior stayed intact. A separate preservation guard set is needed.

The March 27, 2026 stopped run already contains stable examples for each required status. Recommended explicit guard case IDs:

**Passed guards**

- `015e2ce27cecdea63564`
- `00056d4304c58a035c87cdf5ff1e5e3e`
- `011004bcac763eaf6f28`

**Host-runtime guards**

- `00a4835bf36513ca58a3`
- `00135b0dfee0ae165ad2`
- `0115e0ce312f26ff59f4fbf4f5821ca2`

**Local-helper guards**

- `005ceac0483fc5a581cc`
- `06649145d7e6c4c147c02459fd2bc5af`

**Unsolvable guards**

- `0b677b13fca6cd0905ca`
- `1029870`
- `1160696`

This guard set should live in a Phase 10 rerun manifest and be reported separately from the canonical delta. It enforces `REC-05` without reopening the canonical `70`-case contract.

### Existing skip and unsolvable sources should stay authoritative

The repo already has stable skip and unsolvable classification sources:

- `tools/apdr/src/resolver/recovery_diagnostics.rs` maps unsolvable categories to `skipped-host-runtime` or `skipped-unsolvable`
- `tools/apdr/data/seed/unsolvable_modules.tsv` already labels concrete host-runtime, unsolvable, and local-helper imports
- `tools/apdr/src/resolver/mod.rs` and `retry_loop.rs` already emit `skipped-host-runtime` and `skipped-local-helper`

Phase 10 should compare rerun statuses against these existing categories. It should not introduce a new skip taxonomy just for closeout reporting.

### Phase 8 and Phase 9 checkers should remain in the closeout suite

Phase 10 is supposed to measure the new accuracy behavior without reopening older boundaries. The existing checker suite already encodes those boundaries:

- `scripts/check_phase8_family_runtime.py`
- `scripts/check_phase9_targeted_recovery.py`
- the targeted Rust regression families `phase9_targeted_`, `phase7_family_`, and `data_driven_family_`

That suite should remain part of the final Phase 10 validation loop so the new benchmark artifacts cannot silently claim a win while the Phase 8 or Phase 9 contracts drift.

## Implementation Recommendations

### 1. Create one explicit rerun manifest and one manifest-driven benchmark wrapper

Recommended new artifacts:

- `.planning/phases/10-benchmark-verification-accuracy-closeout/10-targeted-rerun-manifest.json`
- `scripts/run_phase10_targeted_benchmark.py`
- `.planning/phases/10-benchmark-verification-accuracy-closeout/10-targeted-rerun.json`
- `.planning/phases/10-benchmark-verification-accuracy-closeout/10-case-delta.json`

Recommended responsibilities:

- copy `canonical_case_ids` and `tier1_watchlist_case_ids` directly from `07-tier3-parity-manifest.json`
- add the explicit `REC-05` guard IDs listed above under separate `passed_case_ids`, `host_runtime_case_ids`, `local_helper_case_ids`, and `unsolvable_case_ids`
- resolve repo-relative snippet paths from `runs/20260327-150339-apdr/summary.json`
- rerun only those exact snippets with the baseline-like APDR command shape
- normalize rerun statuses using the same pass-or-skip rules already implemented in `benchmark_ui/service.py`
- write one raw rerun artifact and one compact per-case delta artifact

### 2. Keep the evidence package split exactly as the user requested

Recommended reviewer-facing artifacts:

- `.planning/phases/10-benchmark-verification-accuracy-closeout/10-BENCHMARK-VERIFICATION.md`
- `.planning/phases/10-benchmark-verification-accuracy-closeout/10-WATCHLIST-APPENDIX.md`
- `.planning/phases/10-benchmark-verification-accuracy-closeout/10-PRESERVATION-GUARDS.md`
- `.planning/phases/10-benchmark-verification-accuracy-closeout/10-UNRECOVERED-GAPS.md`

Recommended reporting responsibilities:

- `10-BENCHMARK-VERIFICATION.md` should cover the canonical `70`-case slice only and summarize recovery deltas plus requirement verdicts
- `10-WATCHLIST-APPENDIX.md` should report the `17` watchlist cases separately and explicitly restate that they remain outside the main contract
- `10-PRESERVATION-GUARDS.md` should record the passed and expected-skip guard outcomes for `REC-05`
- `10-UNRECOVERED-GAPS.md` should group remaining canonical failures by dominant bucket, then list case IDs plus short follow-on notes

### 3. Use the guard subset as a blocking preservation gate, not as narrative-only evidence

The preservation guard set should be treated as a hard gate:

- any passed guard that regresses is blocking
- any host-runtime, unsolvable, or local-helper guard that changes classification is blocking
- guard cases are verification-only and must not be folded into the canonical `70`-case success-rate math

That keeps `REC-05` concrete without diluting the main accuracy delta report.

### 4. Close the phase with one deterministic checker plus the carried-forward suite

Recommended new artifacts:

- `scripts/check_phase10_benchmark_closeout.py`
- `.planning/phases/10-benchmark-verification-accuracy-closeout/10-MILESTONE-CLOSEOUT.md`

Recommended checker responsibilities:

- verify the canonical delta artifact still covers exactly `70` canonical cases
- verify the separate watchlist appendix still covers exactly `17` watchlist cases
- verify the explicit guard IDs are present under the correct preservation categories
- verify the benchmark and closeout notes contain required headings and the boundary text that keeps the watchlist outside the main contract
- verify every remaining unrecovered canonical case appears under a dominant bucket with a non-empty follow-on note
- rerun the existing Phase 8 and Phase 9 checker commands as part of final validation

## Validation Architecture

### Quick checks

- `python -m py_compile scripts/run_phase10_targeted_benchmark.py scripts/check_phase10_benchmark_closeout.py`
- `python scripts/run_phase10_targeted_benchmark.py --manifest-json .planning/phases/10-benchmark-verification-accuracy-closeout/10-targeted-rerun-manifest.json --baseline-summary runs/20260327-150339-apdr/summary.json --pllm-csv pllm_results/csv/summary-all-runs.csv --output-json .planning/phases/10-benchmark-verification-accuracy-closeout/10-targeted-rerun.json --case-delta-json .planning/phases/10-benchmark-verification-accuracy-closeout/10-case-delta.json --output-md .planning/phases/10-benchmark-verification-accuracy-closeout/10-TARGETED-RERUN.md --context-log .planning/phases/10-benchmark-verification-accuracy-closeout/10-targeted-benchmark-context.log --dry-run`

### Artifact checks

- `rg -n 'canonical_case_ids|tier1_watchlist_case_ids|passed_case_ids|host_runtime_case_ids|local_helper_case_ids|unsolvable_case_ids' .planning/phases/10-benchmark-verification-accuracy-closeout/10-targeted-rerun-manifest.json`
- `rg -n '## Canonical Slice Delta|## Preservation Guards|## Requirement Verdicts' .planning/phases/10-benchmark-verification-accuracy-closeout/10-BENCHMARK-VERIFICATION.md`
- `python scripts/check_phase10_benchmark_closeout.py --parity-manifest .planning/phases/07-failure-baseline-parity-slice/07-tier3-parity-manifest.json --baseline-summary runs/20260327-150339-apdr/summary.json --rerun-json .planning/phases/10-benchmark-verification-accuracy-closeout/10-targeted-rerun.json --case-delta-json .planning/phases/10-benchmark-verification-accuracy-closeout/10-case-delta.json --benchmark-md .planning/phases/10-benchmark-verification-accuracy-closeout/10-BENCHMARK-VERIFICATION.md --watchlist-md .planning/phases/10-benchmark-verification-accuracy-closeout/10-WATCHLIST-APPENDIX.md --guards-md .planning/phases/10-benchmark-verification-accuracy-closeout/10-PRESERVATION-GUARDS.md --gaps-md .planning/phases/10-benchmark-verification-accuracy-closeout/10-UNRECOVERED-GAPS.md`

### Phase-close checks

- `python scripts/run_phase10_targeted_benchmark.py --manifest-json .planning/phases/10-benchmark-verification-accuracy-closeout/10-targeted-rerun-manifest.json --baseline-summary runs/20260327-150339-apdr/summary.json --pllm-csv pllm_results/csv/summary-all-runs.csv --output-json .planning/phases/10-benchmark-verification-accuracy-closeout/10-targeted-rerun.json --case-delta-json .planning/phases/10-benchmark-verification-accuracy-closeout/10-case-delta.json --output-md .planning/phases/10-benchmark-verification-accuracy-closeout/10-TARGETED-RERUN.md --context-log .planning/phases/10-benchmark-verification-accuracy-closeout/10-targeted-benchmark-context.log`
- `cargo test --manifest-path tools/apdr/Cargo.toml --test test_resolver phase9_targeted_module_ -- --nocapture`
- `cargo test --manifest-path tools/apdr/Cargo.toml --test test_resolver phase9_targeted_compatibility_ -- --nocapture`
- `cargo test --manifest-path tools/apdr/Cargo.toml --test test_resolver phase7_family_ -- --nocapture`
- `cargo test --manifest-path tools/apdr/Cargo.toml --test test_resolver data_driven_family_ -- --nocapture`
- `python scripts/check_phase8_family_runtime.py --family-manifest .planning/phases/07-failure-baseline-parity-slice/07-family-snapshot-manifest.json --families-json tools/apdr/data/family_knowledge/touched_families.json --recovery-json tools/apdr/data/family_knowledge/touched_recovery_rules.json --baseline-md .planning/phases/08-data-driven-family-knowledge-runtime/08-FAMILY-RUNTIME.md`
- `python scripts/check_phase9_targeted_recovery.py --parity-manifest .planning/phases/07-failure-baseline-parity-slice/07-tier3-parity-manifest.json --phase8-md .planning/phases/08-data-driven-family-knowledge-runtime/08-FAMILY-RUNTIME.md --phase9-md .planning/phases/09-targeted-tier3-recovery-accuracy/09-TARGETED-RECOVERY.md --module-rules tools/apdr/data/recovery/module_rules.json --compatibility-rules tools/apdr/data/recovery/compatibility_rules.json`
- `python scripts/check_phase10_benchmark_closeout.py --parity-manifest .planning/phases/07-failure-baseline-parity-slice/07-tier3-parity-manifest.json --baseline-summary runs/20260327-150339-apdr/summary.json --rerun-json .planning/phases/10-benchmark-verification-accuracy-closeout/10-targeted-rerun.json --case-delta-json .planning/phases/10-benchmark-verification-accuracy-closeout/10-case-delta.json --benchmark-md .planning/phases/10-benchmark-verification-accuracy-closeout/10-BENCHMARK-VERIFICATION.md --watchlist-md .planning/phases/10-benchmark-verification-accuracy-closeout/10-WATCHLIST-APPENDIX.md --guards-md .planning/phases/10-benchmark-verification-accuracy-closeout/10-PRESERVATION-GUARDS.md --gaps-md .planning/phases/10-benchmark-verification-accuracy-closeout/10-UNRECOVERED-GAPS.md`

## Canonical Files For Planning

- `.planning/PROJECT.md`
- `.planning/REQUIREMENTS.md`
- `.planning/ROADMAP.md`
- `.planning/STATE.md`
- `.planning/phases/10-benchmark-verification-accuracy-closeout/10-CONTEXT.md`
- `.planning/phases/07-failure-baseline-parity-slice/07-BASELINE.md`
- `.planning/phases/07-failure-baseline-parity-slice/07-TIER3-PARITY-MANIFEST.md`
- `.planning/phases/07-failure-baseline-parity-slice/07-tier3-parity-manifest.json`
- `.planning/phases/08-data-driven-family-knowledge-runtime/08-FAMILY-RUNTIME.md`
- `.planning/phases/09-targeted-tier3-recovery-accuracy/09-TARGETED-RECOVERY.md`
- `runs/20260327-150339-apdr/summary.json`
- `runs/20260327-150339-apdr/benchmark-context.log`
- `pllm_results/csv/summary-all-runs.csv`
- `benchmark_ui/service.py`
- `benchmark_ui/runner.py`
- `scripts/measure_apdr_baseline.py`
- `scripts/check_phase8_family_runtime.py`
- `scripts/check_phase9_targeted_recovery.py`

## Out-of-Scope For This Phase

- changing the canonical `70`-case slice, the `17`-case watchlist split, or the `87`-case overlap definition
- reopening the Phase 8 family-runtime migration boundary or editing Phase 9 recovery policy behavior as part of closeout reporting
- rerunning all `2890` stopped-run snippets; Phase 10 should stay manifest-driven and bounded
- new benchmark UI or workflow features
- modifying unrelated local edits in `benchmark_ui/service.py`, `web/src/main.js`, `tools/apdr/src/lib.rs`, or `tools/apdr/llm_py/tests/test_llm_integration.py`

---
*Research created: 2026-03-28*
*Phase: 10-benchmark-verification-accuracy-closeout*
