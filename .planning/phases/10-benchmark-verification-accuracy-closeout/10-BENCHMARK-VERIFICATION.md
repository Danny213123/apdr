# Phase 10: Benchmark Verification & Accuracy Closeout

Generated: 2026-03-28
Baseline: `runs/20260327-150339-apdr/summary.json`
pllm source: `pllm_results/csv/summary-all-runs.csv`
Case-delta artifact: `10-case-delta.json`

## Commands

The baseline March 27, 2026 command shape used for reruns:

```
apdr resolve <snippet> --output <case-dir> --range 5 --max-retries 5 --docker-timeout 900 --validation-backend llm --llm-provider ollama --llm-model qwen3.5:9b --llm-base-url http://localhost:11434 --allow-llm --no-execute-snippet --force-validate --benchmark-context-log <context-log>
```

Manifest: `.planning/phases/10-benchmark-verification-accuracy-closeout/10-targeted-rerun-manifest.json`

## Artifact Links

| Artifact | Path | Description |
|----------|------|-------------|
| Case-delta JSON | [10-case-delta.json](10-case-delta.json) | Machine-readable per-case delta data for all 98 rerun cases |
| Watchlist appendix | [10-WATCHLIST-APPENDIX.md](10-WATCHLIST-APPENDIX.md) | Separate report for the 17 tier1 watchlist cases outside the main contract |
| Preservation guards | [10-PRESERVATION-GUARDS.md](10-PRESERVATION-GUARDS.md) | Per-guard status report for the 11 REC-05 preservation cases |
| Unrecovered gaps | [10-UNRECOVERED-GAPS.md](10-UNRECOVERED-GAPS.md) | Dominant-bucket grouping and follow-on notes for remaining unrecovered canonical cases |
| Targeted-rerun manifest | [10-targeted-rerun-manifest.json](10-targeted-rerun-manifest.json) | Canonical, watchlist, and guard case ID definitions |
| Targeted-rerun narrative | [10-TARGETED-RERUN.md](10-TARGETED-RERUN.md) | Per-case delta table from the dry-run rerun |

## Canonical Slice Delta

The canonical slice contains exactly **70** cases from the locked Phase 7 tier3 parity slice. The 17 watchlist cases are reported separately in [10-WATCHLIST-APPENDIX.md](10-WATCHLIST-APPENDIX.md) and are not included in these totals.

### Summary

| Metric | Count |
|--------|------:|
| Canonical cases | 70 |
| Recovered (failed -> passed) | 0 |
| Unchanged failures | 70 |
| Regressions (passed -> failed) | 0 |

### Failure Bucket Breakdown

All 70 canonical cases remained in their baseline failure bucket with no bucket migration.

| Bucket | Cases |
|--------|------:|
| environment-build-failed | 21 |
| module-not-found | 19 |
| dependency-conflict | 12 |
| version-not-found | 11 |
| syntax-error | 5 |
| import-error | 2 |

### Interpretation

No canonical cases were recovered by the Phase 9 targeted-recovery policies in this dry-run rerun. The recovery policies addressed the correct failure surfaces (module-provider mapping, compatibility constraints, stop-reason retries), but the 70-case parity slice remained unchanged. The dominant blockers are environment-build failures requiring system-level C dependencies (21 cases) and module-not-found cases where the import name has no matching PyPI package (19 cases).

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
| REC-05 | **PASS** | All 11 preservation guards matched baseline status (3 passed stayed passed, 8 skipped stayed skipped). See [10-PRESERVATION-GUARDS.md](10-PRESERVATION-GUARDS.md). | Preservation gate validates non-regression only; it does not measure forward recovery. |
| EVD-01 | **PASS** | Machine-readable case-delta artifact (`10-case-delta.json`) with `"canonical_case_count": 70` covers the full canonical slice. Reviewer-facing summary and bucket-grouped gap report provide inspectable evidence. | All 70 canonical cases remain unrecovered; the evidence package documents the gap surface rather than recovery wins. |
| EVD-02 | **PASS** | The 17 watchlist cases are reported in a separate appendix ([10-WATCHLIST-APPENDIX.md](10-WATCHLIST-APPENDIX.md)) and are explicitly excluded from the canonical contract totals. The canonical/watchlist boundary from Phase 7 is preserved. | Watchlist cases have not been triaged for future milestone inclusion. |

---

*Phase: 10-benchmark-verification-accuracy-closeout*
*Verification note generated: 2026-03-28*
