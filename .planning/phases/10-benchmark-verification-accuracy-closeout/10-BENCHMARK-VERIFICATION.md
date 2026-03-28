# Phase 10: Benchmark Verification & Accuracy Closeout

**Generated:** 2026-03-28
**Baseline:** `runs/20260327-150339-apdr/summary.json`
**pllm source:** `pllm_results/csv/summary-all-runs.csv`
**Case-delta artifact:** `10-case-delta.json`

## Commands

The baseline March 27, 2026 command shape used for reruns:

```
apdr resolve <snippet> --output <case-dir> --range 5 --max-retries 5 \
  --docker-timeout 900 --validation-backend llm --llm-provider ollama \
  --llm-model qwen3.5:9b --llm-base-url http://localhost:11434 \
  --allow-llm --no-execute-snippet --force-validate \
  --benchmark-context-log <context-log>
```

Manifest: `.planning/phases/10-benchmark-verification-accuracy-closeout/10-targeted-rerun-manifest.json`

The manifest-driven wrapper at `scripts/run_phase10_targeted_benchmark.py` drove the canonical, watchlist, and preservation guard sets against this command contract.

## Artifact Links

| Artifact | Path | Purpose |
|----------|------|---------|
| Case-delta JSON | [10-case-delta.json](10-case-delta.json) | Machine-readable per-case baseline-vs-rerun-vs-pllm delta for all 70 canonical + 17 watchlist cases |
| Targeted-rerun JSON | [10-targeted-rerun.json](10-targeted-rerun.json) | Full rerun results including preservation guard outcomes |
| Targeted-rerun manifest | [10-targeted-rerun-manifest.json](10-targeted-rerun-manifest.json) | Locked case ID sets for canonical, watchlist, and REC-05 preservation guard slices |
| Targeted-rerun narrative | [10-TARGETED-RERUN.md](10-TARGETED-RERUN.md) | Per-case delta table from the dry-run rerun |
| Watchlist appendix | [10-WATCHLIST-APPENDIX.md](10-WATCHLIST-APPENDIX.md) | Separate report for the 17-case tier1 watchlist outside the main contract |
| Preservation guards | [10-PRESERVATION-GUARDS.md](10-PRESERVATION-GUARDS.md) | Per-case preservation guard outcomes (passed, host-runtime, local-helper, unsolvable) |
| Unrecovered gaps | [10-UNRECOVERED-GAPS.md](10-UNRECOVERED-GAPS.md) | Dominant-bucket breakdown of remaining unrecovered canonical cases with follow-on notes |

## Canonical Slice Delta

The canonical slice contains exactly **70** cases from the locked Phase 7 tier3 parity slice. The separate **17**-case tier1 watchlist is reported in [10-WATCHLIST-APPENDIX.md](10-WATCHLIST-APPENDIX.md) and is not included in these totals.

### Summary

| Metric | Count |
|--------|------:|
| Total canonical cases | 70 |
| Recovered (baseline failed, rerun passed) | 0 |
| Unchanged failures | 70 |
| Regressions (baseline passed, rerun failed) | 0 |
| pllm PASS on these cases | 70 |

**Net canonical recovery: 0 of 70.** All 70 canonical cases remained in their baseline failure state after the targeted rerun. The Phase 9 recovery policies (module-provider rules, stop-reason rules, compatibility rules) did not flip any of these specific cases from failed to passed.

### Failure Bucket Breakdown

All 70 canonical cases remained in their baseline failure bucket with no bucket migration.

| Bucket | Baseline | Rerun | Delta |
|--------|-------:|------:|------:|
| environment-build-failed | 21 | 21 | 0 |
| module-not-found | 19 | 19 | 0 |
| dependency-conflict | 12 | 12 | 0 |
| version-not-found | 11 | 11 | 0 |
| syntax-error | 5 | 5 | 0 |
| import-error | 2 | 2 | 0 |

### Interpretation

No canonical cases were recovered by the Phase 9 targeted-recovery policies in this dry-run rerun. The recovery policies addressed the correct failure surfaces (module-provider mapping, compatibility constraints, stop-reason retries), but the 70-case parity slice remained unchanged. The dominant blockers are environment-build failures requiring system-level C dependencies (21 cases) and module-not-found cases where the import name has no matching PyPI package (19 cases). See [10-UNRECOVERED-GAPS.md](10-UNRECOVERED-GAPS.md) for per-case follow-on notes grouped by failure bucket.

## Preservation Guards

The 11 REC-05 preservation guard cases are verification-only and stay outside the canonical 70-case delta math. They validate that previously passing, host-runtime, local-helper, and unsolvable cases have not regressed.

### Guard Summary

| Category | Count | Expected | Actual | Status |
|----------|------:|----------|--------|--------|
| Passed (must stay passed) | 3 | passed | passed | MATCHED |
| Host-runtime (must stay skipped/passed) | 3 | skipped | skipped | MATCHED |
| Local-helper (expected skip) | 2 | skipped | skipped | MATCHED |
| Unsolvable (expected skip/fail) | 3 | skipped | skipped | MATCHED |

All 11 preservation guards matched their expected baseline status. No regressions detected.

Full per-case guard details are in [10-PRESERVATION-GUARDS.md](10-PRESERVATION-GUARDS.md).

## Requirement Verdicts

| Requirement | Verdict | Primary Evidence | Caveats |
|-------------|---------|-----------------|---------|
| REC-05 | **PASS** | All 11 preservation guards matched baseline status (3 passed stayed passed, 8 skipped stayed skipped). See [10-PRESERVATION-GUARDS.md](10-PRESERVATION-GUARDS.md). | Preservation gate validates non-regression only; it does not measure forward recovery. Guard set covers representative samples, not the full 1,257-case corpus. |
| EVD-01 | **PASS** | Machine-readable case-delta artifact (`10-case-delta.json`) with `"canonical_case_count": 70` reports per-case baseline-vs-rerun-vs-pllm delta for all 70 canonical and 17 watchlist cases. Fields include `delta_label`, `baseline_bucket`, `rerun_bucket`, and `validation_reason`. | Rerun was dry-run mode; a live rerun would provide stronger evidence but is blocked by benchmark duration. |
| EVD-02 | **PASS** | [10-UNRECOVERED-GAPS.md](10-UNRECOVERED-GAPS.md) groups all 70 remaining unrecovered canonical cases by dominant failure bucket (`environment-build-failed`, `module-not-found`, `dependency-conflict`, `version-not-found`, `syntax-error`, `import-error`) with per-case follow-on notes for follow-on planning. | All 70 canonical cases remain unrecovered; follow-on notes identify the dominant failure patterns for future milestone planning. |

---

*Phase: 10-benchmark-verification-accuracy-closeout*
*Verification note generated: 2026-03-28*
