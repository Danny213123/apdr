# Phase 10: Preservation Guards

**Generated:** 2026-03-28
**Source:** `10-targeted-rerun-manifest.json`, `10-targeted-rerun.json`
**Total guards:** 11

Preservation guards are verification-only and stay outside the canonical 70-case delta math. They validate that previously passing, host-runtime, local-helper, and unsolvable cases have not regressed after the Phase 9 recovery changes landed.

## Passed Guards

These cases passed in the baseline and must stay passed in the rerun.

| Case ID | Baseline Status | Rerun Status | Matched |
|---------|----------------|--------------|---------|
| `015e2ce27cecdea63564` | passed | passed | Yes |
| `00056d4304c58a035c87cdf5ff1e5e3e` | passed | passed | Yes |
| `011004bcac763eaf6f28` | passed | passed | Yes |

**Result:** All 3 passed guards matched. No regressions.

## Host Runtime Guards

These cases were skipped in the baseline due to host-runtime dependencies (platform-specific modules like microbit, RPi.GPIO, etc.) and must stay skipped or passed in the rerun.

| Case ID | Baseline Status | Rerun Status | Matched |
|---------|----------------|--------------|---------|
| `00a4835bf36513ca58a3` | skipped | skipped | Yes |
| `00135b0dfee0ae165ad2` | skipped | skipped | Yes |
| `0115e0ce312f26ff59f4fbf4f5821ca2` | skipped | skipped | Yes |

**Result:** All 3 host-runtime guards matched. No drift in skip behavior.

## Local Helper Guards

These cases were skipped in the baseline because the snippet depends on local helper modules not available on PyPI. They are expected to stay skipped.

| Case ID | Baseline Status | Rerun Status | Matched |
|---------|----------------|--------------|---------|
| `005ceac0483fc5a581cc` | skipped | skipped | Yes |
| `06649145d7e6c4c147c02459fd2bc5af` | skipped | skipped | Yes |

**Result:** All 2 local-helper guards matched. No drift in skip behavior.

## Unsolvable Guards

These cases were skipped in the baseline because the snippet's dependencies are known to be unsolvable (legacy packages, withdrawn distributions, etc.). They are expected to stay skipped or failed.

| Case ID | Baseline Status | Rerun Status | Matched |
|---------|----------------|--------------|---------|
| `0b677b13fca6cd0905ca` | skipped | skipped | Yes |
| `1029870` | skipped | skipped | Yes |
| `1160696` | skipped | skipped | Yes |

**Result:** All 3 unsolvable guards matched. No drift in skip behavior.

---

**Overall REC-05 Verdict:** All 11 preservation guard cases matched their expected baseline behavior. No regressions detected. The Phase 9 recovery changes did not introduce any backward-incompatible behavior on the guard surface.

---

*Phase: 10-benchmark-verification-accuracy-closeout*
*Generated: 2026-03-28*
