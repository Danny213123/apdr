---
phase: 10
slug: benchmark-verification-accuracy-closeout
status: planned
nyquist_compliant: true
wave_0_complete: true
created: 2026-03-28
---

# Phase 10 - Validation Strategy

> Validation contract for the manifest-driven targeted rerun, case-level delta artifacts, preservation guards, and milestone closeout against the locked Phase 7, Phase 8, and Phase 9 boundaries.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | Python rerun or checker scripts, structural `rg` checks, and the existing Rust targeted resolver tests plus Phase 8 and Phase 9 checkers |
| **Config file** | `tools/apdr/Cargo.toml` |
| **Quick run command** | `python scripts/run_phase10_targeted_benchmark.py --manifest-json .planning/phases/10-benchmark-verification-accuracy-closeout/10-targeted-rerun-manifest.json --baseline-summary runs/20260327-150339-apdr/summary.json --pllm-csv pllm_results/csv/summary-all-runs.csv --output-json .planning/phases/10-benchmark-verification-accuracy-closeout/10-targeted-rerun.json --case-delta-json .planning/phases/10-benchmark-verification-accuracy-closeout/10-case-delta.json --output-md .planning/phases/10-benchmark-verification-accuracy-closeout/10-TARGETED-RERUN.md --context-log .planning/phases/10-benchmark-verification-accuracy-closeout/10-targeted-benchmark-context.log --dry-run` |
| **Full suite command** | `python scripts/run_phase10_targeted_benchmark.py --manifest-json .planning/phases/10-benchmark-verification-accuracy-closeout/10-targeted-rerun-manifest.json --baseline-summary runs/20260327-150339-apdr/summary.json --pllm-csv pllm_results/csv/summary-all-runs.csv --output-json .planning/phases/10-benchmark-verification-accuracy-closeout/10-targeted-rerun.json --case-delta-json .planning/phases/10-benchmark-verification-accuracy-closeout/10-case-delta.json --output-md .planning/phases/10-benchmark-verification-accuracy-closeout/10-TARGETED-RERUN.md --context-log .planning/phases/10-benchmark-verification-accuracy-closeout/10-targeted-benchmark-context.log && cargo test --manifest-path tools/apdr/Cargo.toml --test test_resolver phase9_targeted_module_ -- --nocapture && cargo test --manifest-path tools/apdr/Cargo.toml --test test_resolver phase9_targeted_compatibility_ -- --nocapture && cargo test --manifest-path tools/apdr/Cargo.toml --test test_resolver phase7_family_ -- --nocapture && cargo test --manifest-path tools/apdr/Cargo.toml --test test_resolver data_driven_family_ -- --nocapture && python scripts/check_phase8_family_runtime.py --family-manifest .planning/phases/07-failure-baseline-parity-slice/07-family-snapshot-manifest.json --families-json tools/apdr/data/family_knowledge/touched_families.json --recovery-json tools/apdr/data/family_knowledge/touched_recovery_rules.json --baseline-md .planning/phases/08-data-driven-family-knowledge-runtime/08-FAMILY-RUNTIME.md && python scripts/check_phase9_targeted_recovery.py --parity-manifest .planning/phases/07-failure-baseline-parity-slice/07-tier3-parity-manifest.json --phase8-md .planning/phases/08-data-driven-family-knowledge-runtime/08-FAMILY-RUNTIME.md --phase9-md .planning/phases/09-targeted-tier3-recovery-accuracy/09-TARGETED-RECOVERY.md --module-rules tools/apdr/data/recovery/module_rules.json --compatibility-rules tools/apdr/data/recovery/compatibility_rules.json && python scripts/check_phase10_benchmark_closeout.py --parity-manifest .planning/phases/07-failure-baseline-parity-slice/07-tier3-parity-manifest.json --baseline-summary runs/20260327-150339-apdr/summary.json --rerun-json .planning/phases/10-benchmark-verification-accuracy-closeout/10-targeted-rerun.json --case-delta-json .planning/phases/10-benchmark-verification-accuracy-closeout/10-case-delta.json --benchmark-md .planning/phases/10-benchmark-verification-accuracy-closeout/10-BENCHMARK-VERIFICATION.md --watchlist-md .planning/phases/10-benchmark-verification-accuracy-closeout/10-WATCHLIST-APPENDIX.md --guards-md .planning/phases/10-benchmark-verification-accuracy-closeout/10-PRESERVATION-GUARDS.md --gaps-md .planning/phases/10-benchmark-verification-accuracy-closeout/10-UNRECOVERED-GAPS.md` |
| **Estimated runtime** | ~8-20 minutes once the manifest-driven rerun and checker exist |

---

## Sampling Rate

- **After every task commit:** Run the task-specific dry-run, structural check, or checker command listed below
- **After every plan wave:** Run the full suite command
- **Before `$gsd-verify-work`:** The Phase 10 rerun, the Phase 8 checker, the Phase 9 checker, and the Phase 10 checker must all be green
- **Max feedback latency:** keep task-level checks under 5 minutes by preferring `--dry-run`, `py_compile`, and structural checks before the full rerun

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|-----------|-------------------|-------------|--------|
| 10-01-01 | 01 | 1 | REC-05, EVD-01 | manifest structure | `rg -n 'canonical_case_ids|tier1_watchlist_case_ids|passed_case_ids|host_runtime_case_ids|local_helper_case_ids|unsolvable_case_ids|015e2ce27cecdea63564|00a4835bf36513ca58a3|0b677b13fca6cd0905ca' .planning/phases/10-benchmark-verification-accuracy-closeout/10-targeted-rerun-manifest.json` | no | pending |
| 10-01-02 | 01 | 1 | REC-05, EVD-01 | rerun wrapper dry-run | `python scripts/run_phase10_targeted_benchmark.py --manifest-json .planning/phases/10-benchmark-verification-accuracy-closeout/10-targeted-rerun-manifest.json --baseline-summary runs/20260327-150339-apdr/summary.json --pllm-csv pllm_results/csv/summary-all-runs.csv --output-json .planning/phases/10-benchmark-verification-accuracy-closeout/10-targeted-rerun.json --case-delta-json .planning/phases/10-benchmark-verification-accuracy-closeout/10-case-delta.json --output-md .planning/phases/10-benchmark-verification-accuracy-closeout/10-TARGETED-RERUN.md --context-log .planning/phases/10-benchmark-verification-accuracy-closeout/10-targeted-benchmark-context.log --dry-run` | no | pending |
| 10-02-01 | 02 | 2 | EVD-01 | benchmark verification note | `rg -n '## Commands|## Artifact Links|## Canonical Slice Delta|## Preservation Guards|## Requirement Verdicts|REC-05|EVD-01|EVD-02' .planning/phases/10-benchmark-verification-accuracy-closeout/10-BENCHMARK-VERIFICATION.md` | no | pending |
| 10-02-02 | 02 | 2 | REC-05, EVD-02 | checker plus split artifacts | `python scripts/check_phase10_benchmark_closeout.py --parity-manifest .planning/phases/07-failure-baseline-parity-slice/07-tier3-parity-manifest.json --baseline-summary runs/20260327-150339-apdr/summary.json --rerun-json .planning/phases/10-benchmark-verification-accuracy-closeout/10-targeted-rerun.json --case-delta-json .planning/phases/10-benchmark-verification-accuracy-closeout/10-case-delta.json --benchmark-md .planning/phases/10-benchmark-verification-accuracy-closeout/10-BENCHMARK-VERIFICATION.md --watchlist-md .planning/phases/10-benchmark-verification-accuracy-closeout/10-WATCHLIST-APPENDIX.md --guards-md .planning/phases/10-benchmark-verification-accuracy-closeout/10-PRESERVATION-GUARDS.md --gaps-md .planning/phases/10-benchmark-verification-accuracy-closeout/10-UNRECOVERED-GAPS.md` | no | pending |
| 10-03-01 | 03 | 3 | REC-05 | carry-forward regression suite | `cargo test --manifest-path tools/apdr/Cargo.toml --test test_resolver phase9_targeted_module_ -- --nocapture && cargo test --manifest-path tools/apdr/Cargo.toml --test test_resolver phase9_targeted_compatibility_ -- --nocapture && cargo test --manifest-path tools/apdr/Cargo.toml --test test_resolver phase7_family_ -- --nocapture && cargo test --manifest-path tools/apdr/Cargo.toml --test test_resolver data_driven_family_ -- --nocapture && python scripts/check_phase8_family_runtime.py --family-manifest .planning/phases/07-failure-baseline-parity-slice/07-family-snapshot-manifest.json --families-json tools/apdr/data/family_knowledge/touched_families.json --recovery-json tools/apdr/data/family_knowledge/touched_recovery_rules.json --baseline-md .planning/phases/08-data-driven-family-knowledge-runtime/08-FAMILY-RUNTIME.md && python scripts/check_phase9_targeted_recovery.py --parity-manifest .planning/phases/07-failure-baseline-parity-slice/07-tier3-parity-manifest.json --phase8-md .planning/phases/08-data-driven-family-knowledge-runtime/08-FAMILY-RUNTIME.md --phase9-md .planning/phases/09-targeted-tier3-recovery-accuracy/09-TARGETED-RECOVERY.md --module-rules tools/apdr/data/recovery/module_rules.json --compatibility-rules tools/apdr/data/recovery/compatibility_rules.json` | yes | pending |
| 10-03-02 | 03 | 3 | EVD-02 | milestone closeout note | `rg -n '## Milestone Outcome|## Benchmark Evidence|## Carry-Forward Verification|## Remaining Gaps|## Final Signoff|10-BENCHMARK-VERIFICATION.md|10-UNRECOVERED-GAPS.md' .planning/phases/10-benchmark-verification-accuracy-closeout/10-MILESTONE-CLOSEOUT.md` | no | pending |

---

## Wave 0 Requirements

- Existing infrastructure covers the phase.
- Phase 10 adds new Python scripts and generated artifacts, but it does not need a new test framework or new watch-mode tooling.

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| The main report keeps the `70`-case canonical slice separate from the `17`-case watchlist | EVD-01 | A checker can verify headings and counts, but a reviewer still needs to confirm the prose does not blur the contract boundary | Read `10-BENCHMARK-VERIFICATION.md` and `10-WATCHLIST-APPENDIX.md`, then confirm the benchmark note treats the watchlist as separate companion evidence rather than part of the main success-rate math |
| Follow-on notes for remaining unrecovered cases are actually useful for Phase 11 or backlog planning | EVD-02 | Automation can confirm that every unrecovered case has a note, but not whether the note is actionable | Read `10-UNRECOVERED-GAPS.md`, then confirm each case note points to a concrete next step, blocker, or future phase rather than a vague restatement of the bucket |

---

## Validation Sign-Off

- [x] All planned tasks have an automated verify step or explicit manual-only review instruction
- [x] Sampling continuity includes the locked Phase 7 parity manifest plus the carried-forward Phase 8 and Phase 9 checker surfaces
- [x] Existing infrastructure covers the phase
- [x] No watch-mode commands are required
- [x] Feedback latency remains bounded by dry-run or structural checks before the full rerun
- [x] `nyquist_compliant: true` is set in frontmatter

**Approval:** planned 2026-03-28
