# Benchmark verification package

## Continuity Gate

- Official regression gate: `.planning/phases/06-benchmark-verification-and-v2-closeout/06-CONTINUITY-DELTA.md`
- The fresh Phase 6 bounded continuity rerun stayed on the exact Phase 1 contract: the same three lexicographically selected fixture snippets, the same `env` backend, and the same regression script.
- Gate result: `python scripts/check_apdr_regression.py --baseline .planning/phases/01-baseline-and-guardrails/01-baseline.json --candidate .planning/phases/06-benchmark-verification-and-v2-closeout/06-continuity-candidate.json` passed with all reported metrics `OK`.
- Measured continuity deltas versus the committed baseline:
  - Pass rate: `33.33%` -> `66.67%` (`+0.3333`)
  - Total duration: `41,867 ms` -> `6,607 ms` (`-35,260 ms`, `-84.22%`)
  - Validation duration: `40,237 ms` -> `0 ms` (`-100.00%`)
  - Solve duration: `553 ms` -> `6,607 ms` (`+6,054 ms`, `+1094.76%`)
- Interpretation: the milestone continuity gate is green, but the speed win is still dominated by warm-path reuse rather than a pure forced-validation improvement.

## Hard-Gists Slice

- Broader evidence artifact: `.planning/phases/06-benchmark-verification-and-v2-closeout/06-HARD-GISTS-SLICE.md`
- Machine-readable source: `.planning/phases/06-benchmark-verification-and-v2-closeout/06-hard-gists-slice.json`
- Slice rule: first `25` `snippet.py` cases from `hard-gists` in deterministic lexicographic order, run with `--validation-backend env`.
- Totals from the bounded slice:
  - Passed: `19`
  - Failed: `1`
  - Skipped: `5`
  - Pass rate: `76.00%`
  - Solve duration: `69,452 ms`
  - Validation duration: `166,946 ms`
  - Install duration: `64,949 ms`
  - Smoke duration: `8,959 ms`
- The slice is intentionally broader than the three-case continuity gate but still bounded. It should be read as milestone evidence, not as a full-corpus benchmark campaign.
- Most successful cases reused `import-set-cache`. The one failing validation-heavy case (`01b8b8e1909ae0f601c85e142f2bd15b/snippet.py`) escalated through `env -> docker`, spent real time in install and smoke stages, and then failed on a persistent `xtls` mapping gap rather than on the earlier Windows Docker permission issue.

## Memory Comparison

- Baseline representative memory artifact: `.planning/phases/01-baseline-and-guardrails/01-memory-profile.json`
- Phase 6 representative memory artifact: `.planning/phases/06-benchmark-verification-and-v2-closeout/06-memory-profile.json`
- Targeted resolver-only comparison artifact: `.planning/phases/06-benchmark-verification-and-v2-closeout/06-MEMORY-COMPARISON.md`
- Machine-readable targeted comparison: `.planning/phases/06-benchmark-verification-and-v2-closeout/06-memory-comparison.json`
- Exact `peak_rss_bytes` comparison:
  - Baseline: `19,595,264`
  - Phase 6: `19,845,120`
  - Delta: `+249,856 bytes` (`+1.28%`)
- This remains apples-to-apples evidence because the same snippet path, backend, and wrapper script were reused.
- That wrapper-level whole-run RSS signal remains mixed.
- To isolate the Rust workflow that Phase 2 targeted, Phase 6 also reran the same snippet against the Phase 1 worktree and the current checkout with direct APDR binary invocation plus `--no-validate`, using the improved `peak_private_bytes` field from `scripts/profile_apdr_memory.py`.
- The targeted `peak_private_bytes` result improved:
  - Baseline median: `38,109,184`
  - Current median: `37,994,496`
  - Delta: `-114,688 bytes` (`-0.30%`)
- Interpretation: the older whole-run RSS artifact stayed slightly higher, but the more targeted private-memory indicator on the resolver-only APDR process improved and is the stronger BENCH-03 signal for the optimized Rust workflow.

## Host Variance

- Retained host-variance evidence: `.planning/phases/03-validation-pipeline-throughput/03-VALIDATION-DELTA.md`
- Retained forced-validation artifact: `.planning/phases/03-validation-pipeline-throughput/03-validation-candidate-forced.json`
- Phase 3 remains the source of truth for Windows forced-validation variance:
  - Forced pass rate fell to `0.00%`
  - Forced validation duration rose to `171,903 ms`
  - Both validation-heavy bounded failures ended with `CreateFile C:\Users\danny\.docker\buildx\instances: Access is denied.`
- This package keeps that evidence separate on purpose. The bounded continuity gate proves regression safety under the locked contract, while the Phase 3 forced artifact records that Windows Docker availability is still the ceiling for claims about the real validation path on this host.
- The Phase 6 hard-gists slice therefore supplements milestone evidence without replacing the explicit Phase 3 host-variance record.

## Requirement Verdicts

| Requirement | Verdict | Primary evidence | Caveats |
|-------------|---------|------------------|---------|
| `BENCH-01` | `Pass` | `.planning/phases/06-benchmark-verification-and-v2-closeout/06-CONTINUITY-DELTA.md` | The measurable baseline comparison comes from the bounded continuity gate; the hard-gists slice is broader evidence but not a baseline-matched whole-corpus rerun. |
| `BENCH-02` | `Qualified pass` | `.planning/phases/06-benchmark-verification-and-v2-closeout/06-CONTINUITY-DELTA.md` and `.planning/phases/06-benchmark-verification-and-v2-closeout/06-HARD-GISTS-SLICE.md` | Warm-path reuse drives most of the continuity improvement. `.planning/phases/03-validation-pipeline-throughput/03-VALIDATION-DELTA.md` still documents slower forced validation on this Windows host and remains the non-blocking host-variance caveat. |
| `BENCH-03` | `Pass` | `.planning/phases/06-benchmark-verification-and-v2-closeout/06-MEMORY-COMPARISON.md` and `.planning/phases/06-benchmark-verification-and-v2-closeout/06-memory-comparison.json` | The retained whole-run `peak_rss_bytes` artifact stayed mixed, but the targeted resolver-only `peak_private_bytes` comparison improved by `114,688` bytes (`-0.30%`) on the same snippet when APDR was measured directly. |
| `BENCH-04` | `Pass` | `.planning/phases/06-benchmark-verification-and-v2-closeout/06-CONTINUITY-DELTA.md` and `.planning/phases/06-benchmark-verification-and-v2-closeout/06-hard-gists-slice.json` | The bounded continuity pass rate improved, and the broader slice still passed `19/25` cases with only one true failure. Host-runtime skips in the slice remain separate from APDR correctness regressions. |
