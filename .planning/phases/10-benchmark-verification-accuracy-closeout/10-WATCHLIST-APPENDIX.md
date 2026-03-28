# Phase 10: Watchlist Appendix

**Generated:** 2026-03-28
**Source:** `10-case-delta.json`, `10-targeted-rerun-manifest.json`
**Parity manifest:** `.planning/phases/07-failure-baseline-parity-slice/07-tier3-parity-manifest.json`

## Scope Boundary

The 17 tier1 watchlist cases remain outside the main contract for the Phase 10 benchmark verification closeout. These cases were identified during Phase 7 as APDR-failed and pllm-passing overlap cases that did not meet the tier3 inclusion rule. They are outside the Phase 7 contract and are not part of the canonical 70-case parity slice.

The canonical slice contains only the 70 tier3 cases. The watchlist is tracked here as a separate appendix for visibility and future milestone planning. Watchlist totals are never mixed into the canonical delta math or requirement verdicts reported in [10-BENCHMARK-VERIFICATION.md](10-BENCHMARK-VERIFICATION.md).

## Watchlist Cases

| # | Case ID | Baseline | Rerun | pllm | Delta |
|---|---------|----------|-------|------|-------|
| 1 | `1025525` | failed | failed | PASS | unchanged |
| 2 | `10589494` | failed | failed | PASS | unchanged |
| 3 | `125559` | failed | failed | PASS | unchanged |
| 4 | `1329319` | failed | failed | PASS | unchanged |
| 5 | `143e65a425722dc2f3d0` | failed | failed | PASS | unchanged |
| 6 | `1727204` | failed | failed | PASS | unchanged |
| 7 | `23585f7f50005408fc72` | failed | failed | PASS | unchanged |
| 8 | `2636213` | failed | failed | PASS | unchanged |
| 9 | `3018527` | failed | failed | PASS | unchanged |
| 10 | `3077639` | failed | failed | PASS | unchanged |
| 11 | `35164461db4da79f7d56` | failed | failed | PASS | unchanged |
| 12 | `3725741` | failed | failed | PASS | unchanged |
| 13 | `3750774` | failed | failed | PASS | unchanged |
| 14 | `3803003` | failed | failed | PASS | unchanged |
| 15 | `4225456` | failed | failed | PASS | unchanged |
| 16 | `426829` | failed | failed | PASS | unchanged |
| 17 | `4451253` | failed | failed | PASS | unchanged |

**Totals:** 17 cases, 0 recovered, 17 unchanged failures, 0 regressions.

## Interpretation

All 17 watchlist cases remained in their baseline failure state after the targeted rerun. These cases were not targeted by Phase 9 recovery policies because they fall outside the Phase 7 tier3 contract boundary. They remain candidates for future milestone work if the tier1 recovery surface is prioritized.

---

*Phase: 10-benchmark-verification-accuracy-closeout*
*Generated: 2026-03-28*
