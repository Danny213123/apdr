---
phase: 12
slug: live-benchmark-proof-and-requirement-reconciliation
status: planned
nyquist_compliant: true
wave_0_complete: true
created: 2026-03-28
---

# Phase 12 - Validation Strategy

> Validation contract for explicit live-proof preflight, live-or-blocked benchmark closeout, and milestone requirement reconciliation.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | Python proof scripts and checkers, structural `rg` checks, and the carried-forward Phase 10 benchmark checker plus Phase 8 and Phase 9 bounded verification surfaces |
| **Config file** | `tools/apdr/Cargo.toml` |
| **Quick run command** | `python scripts/run_phase10_targeted_benchmark.py --manifest-json .planning/phases/10-benchmark-verification-accuracy-closeout/10-targeted-rerun-manifest.json --baseline-summary runs/20260327-150339-apdr/summary.json --pllm-csv pllm_results/csv/summary-all-runs.csv --output-json .planning/phases/10-benchmark-verification-accuracy-closeout/10-targeted-rerun.json --case-delta-json .planning/phases/10-benchmark-verification-accuracy-closeout/10-case-delta.json --output-md .planning/phases/10-benchmark-verification-accuracy-closeout/10-TARGETED-RERUN.md --context-log .planning/phases/10-benchmark-verification-accuracy-closeout/10-targeted-benchmark-context.log --status-json .planning/phases/12-live-benchmark-proof-and-requirement-reconciliation/12-live-proof-status.json --probe-only --require-live --apdr-command <apdr-executable>` |
| **Full suite command** | Branch-specific: if live-ready, run the same command without `--probe-only` and regenerate the Phase 10 artifacts from measured output; otherwise rerun the probe-only command and validate the blocker reconciliation path with the Phase 10 and Phase 12 checkers |
| **Estimated runtime** | ~5-20 minutes depending on whether the terminal path is a probe-only blocker reconciliation or a live targeted rerun |

---

## Sampling Rate

- **After every task commit:** Run the task-specific probe, structural check, or deterministic checker listed below
- **After every plan wave:** Run the Phase 10 checker plus the new Phase 12 checker; add the live rerun command if the probe says live-ready
- **Before `$gsd-verify-work`:** The repo must show one valid terminal state, and `check_phase10_benchmark_closeout.py` plus `check_phase12_live_proof.py` must both be green
- **Max feedback latency:** keep task-level checks under 5 minutes by using `py_compile`, `--probe-only`, and structural checks before any full live rerun

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|-----------|-------------------|-------------|--------|
| 12-01-01 | 01 | 1 | REC-02, REC-03, REC-04 | rerun wrapper hardening | `python -m py_compile scripts/run_phase10_targeted_benchmark.py` | yes | pending |
| 12-01-02 | 01 | 1 | REC-02, REC-03, REC-04 | live-proof probe artifact | `python scripts/run_phase10_targeted_benchmark.py --manifest-json .planning/phases/10-benchmark-verification-accuracy-closeout/10-targeted-rerun-manifest.json --baseline-summary runs/20260327-150339-apdr/summary.json --pllm-csv pllm_results/csv/summary-all-runs.csv --output-json .planning/phases/10-benchmark-verification-accuracy-closeout/10-targeted-rerun.json --case-delta-json .planning/phases/10-benchmark-verification-accuracy-closeout/10-case-delta.json --output-md .planning/phases/10-benchmark-verification-accuracy-closeout/10-TARGETED-RERUN.md --context-log .planning/phases/10-benchmark-verification-accuracy-closeout/10-targeted-benchmark-context.log --status-json .planning/phases/12-live-benchmark-proof-and-requirement-reconciliation/12-live-proof-status.json --probe-only --require-live --apdr-command <apdr-executable>` | no | pending |
| 12-02-01 | 02 | 2 | REC-02, REC-03, REC-04 | live rerun or blocker terminal state | `rg -n '\"mode\": \"live\"|actual_mode|blocker_reason|live_ready|canonical_case_count|watchlist_case_count' .planning/phases/10-benchmark-verification-accuracy-closeout/10-targeted-rerun.json .planning/phases/12-live-benchmark-proof-and-requirement-reconciliation/12-live-proof-status.json` | no | pending |
| 12-02-02 | 02 | 2 | REC-02, REC-03, REC-04 | benchmark and requirement reconciliation docs | `rg -n '## Requirement Verdicts|REC-02|REC-03|REC-04|canonical 70-case|watchlist|hard blocker|measured outcome' .planning/phases/10-benchmark-verification-accuracy-closeout/10-BENCHMARK-VERIFICATION.md .planning/phases/10-benchmark-verification-accuracy-closeout/10-MILESTONE-CLOSEOUT.md .planning/phases/12-live-benchmark-proof-and-requirement-reconciliation/12-LIVE-PROOF.md .planning/phases/12-live-benchmark-proof-and-requirement-reconciliation/12-REQUIREMENT-RECONCILIATION.md` | no | pending |
| 12-03-01 | 03 | 3 | REC-02, REC-03, REC-04 | Phase 12 deterministic checker | `python scripts/check_phase12_live_proof.py --status-json .planning/phases/12-live-benchmark-proof-and-requirement-reconciliation/12-live-proof-status.json --benchmark-md .planning/phases/10-benchmark-verification-accuracy-closeout/10-BENCHMARK-VERIFICATION.md --closeout-md .planning/phases/10-benchmark-verification-accuracy-closeout/10-MILESTONE-CLOSEOUT.md --proof-md .planning/phases/12-live-benchmark-proof-and-requirement-reconciliation/12-LIVE-PROOF.md --reconciliation-md .planning/phases/12-live-benchmark-proof-and-requirement-reconciliation/12-REQUIREMENT-RECONCILIATION.md --requirements-md .planning/REQUIREMENTS.md --audit-md .planning/v2.1-MILESTONE-AUDIT.md` | no | pending |
| 12-03-02 | 03 | 3 | REC-02, REC-03, REC-04 | carried-forward benchmark closeout checker | `python scripts/check_phase10_benchmark_closeout.py --parity-manifest .planning/phases/07-failure-baseline-parity-slice/07-tier3-parity-manifest.json --baseline-summary runs/20260327-150339-apdr/summary.json --rerun-json .planning/phases/10-benchmark-verification-accuracy-closeout/10-targeted-rerun.json --case-delta-json .planning/phases/10-benchmark-verification-accuracy-closeout/10-case-delta.json --benchmark-md .planning/phases/10-benchmark-verification-accuracy-closeout/10-BENCHMARK-VERIFICATION.md --watchlist-md .planning/phases/10-benchmark-verification-accuracy-closeout/10-WATCHLIST-APPENDIX.md --guards-md .planning/phases/10-benchmark-verification-accuracy-closeout/10-PRESERVATION-GUARDS.md --gaps-md .planning/phases/10-benchmark-verification-accuracy-closeout/10-UNRECOVERED-GAPS.md` | yes | pending |
| 12-03-03 | 03 | 3 | REC-02, REC-03, REC-04 | milestone truth refresh | `rg -n 'REC-02|REC-03|REC-04|Phase 12|live proof|requirement reconciliation|no longer broken' .planning/REQUIREMENTS.md .planning/PROJECT.md .planning/STATE.md .planning/v2.1-MILESTONE-AUDIT.md` | yes | pending |

---

## Wave 0 Requirements

- Existing infrastructure covers the phase.
- Phase 12 adds one new deterministic checker and one new machine-readable proof-status artifact, but it reuses the Phase 10 rerun surface and benchmark checker.

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| The live-proof note clearly distinguishes a real live rerun from a blocker-backed reconciliation | REC-02, REC-03, REC-04 | Automation can verify fields and headings, but a reviewer still needs to confirm the prose does not blur the terminal state | Read `12-LIVE-PROOF.md`, then confirm it states either a measured live rerun or a specific blocker, not a vague "partial" outcome |
| Requirement reconciliation is honest about the measured outcome | REC-02, REC-03, REC-04 | A checker can confirm the requirement IDs appear, but not whether the revised wording actually stops overclaiming recovery improvement | Read `12-REQUIREMENT-RECONCILIATION.md`, `REQUIREMENTS.md`, and `10-MILESTONE-CLOSEOUT.md`, then confirm the repo promise matches the benchmark evidence |

---

## Validation Sign-Off

- [x] All planned tasks have an automated verify step or explicit manual-only review instruction
- [x] Sampling continuity keeps the locked Phase 10 benchmark checker in the final loop
- [x] Existing infrastructure covers the phase
- [x] No watch-mode commands are required
- [x] Feedback latency remains bounded by probe-only and structural checks before any live rerun
- [x] `nyquist_compliant: true` is set in frontmatter

**Approval:** planned 2026-03-28
