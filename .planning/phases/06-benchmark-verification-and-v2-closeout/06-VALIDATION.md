---
phase: 06
slug: benchmark-verification-and-v2-closeout
status: planned
nyquist_compliant: true
wave_0_complete: true
created: 2026-03-27
---

# Phase 6 - Validation Strategy

> Validation contract for final benchmark proof, broader corpus evidence, and v2 milestone closeout.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | benchmark capture scripts, `cargo fmt --check`, `cargo test`, `cargo clippy`, and structural `rg` checks |
| **Config file** | `tools/apdr/Cargo.toml` |
| **Quick run command** | `python -m py_compile scripts/measure_apdr_baseline.py scripts/profile_apdr_memory.py scripts/check_apdr_regression.py && cargo test --manifest-path tools/apdr/Cargo.toml --test test_resolver resolver_ -- --nocapture && cargo test --manifest-path tools/apdr/Cargo.toml validation_pipeline_ -- --nocapture` |
| **Full suite command** | `cargo fmt --manifest-path tools/apdr/Cargo.toml --all --check && cargo test --manifest-path tools/apdr/Cargo.toml -- --nocapture && cargo clippy --manifest-path tools/apdr/Cargo.toml --all-targets -- -D warnings` |
| **Estimated runtime** | ~6-12 minutes for the Rust verification loop, plus bounded benchmark capture time |

---

## Sampling Rate

- **After every task commit:** Run the task-specific benchmark, grep, or Rust verification command listed below
- **After every plan wave:** Run the full suite command
- **Before `$gsd-verify-work`:** Full suite must be green and the benchmark-verification docs must exist
- **Max feedback latency:** keep quick structural checks and targeted Rust tests under 3 minutes; allow the benchmark slice captures to run longer because they are core phase evidence

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|-----------|-------------------|-------------|--------|
| 06-01-01 | 01 | 1 | BENCH-01, BENCH-02, BENCH-04 | artifact generation | `python scripts/measure_apdr_baseline.py --fixtures-root tools/apdr/tests/fixtures --limit 3 --validation-backend env --output-json .planning/phases/06-benchmark-verification-and-v2-closeout/06-continuity-candidate.json --output-md .planning/phases/06-benchmark-verification-and-v2-closeout/06-CONTINUITY-CANDIDATE.md` | yes | pending |
| 06-01-02 | 01 | 1 | BENCH-03 | memory capture | `python scripts/profile_apdr_memory.py --snippet tools/apdr/tests/fixtures/sample_snippet.py --validation-backend env --output-json .planning/phases/06-benchmark-verification-and-v2-closeout/06-memory-profile.json` | yes | pending |
| 06-01-03 | 01 | 1 | BENCH-01, BENCH-02, BENCH-04 | regression gate + doc grep | `python scripts/check_apdr_regression.py --baseline .planning/phases/01-baseline-and-guardrails/01-baseline.json --candidate .planning/phases/06-benchmark-verification-and-v2-closeout/06-continuity-candidate.json && rg -n 'Continuity candidate capture|Pass-rate delta|Total duration delta|Validation duration delta' .planning/phases/06-benchmark-verification-and-v2-closeout/06-CONTINUITY-DELTA.md` | yes | pending |
| 06-02-01 | 02 | 2 | BENCH-01, BENCH-02, BENCH-04 | dataset benchmark capture | `python scripts/measure_apdr_baseline.py --dataset-root hard-gists --limit 25 --validation-backend env --output-json .planning/phases/06-benchmark-verification-and-v2-closeout/06-hard-gists-slice.json --output-md .planning/phases/06-benchmark-verification-and-v2-closeout/06-HARD-GISTS-SLICE.md` | yes | pending |
| 06-02-02 | 02 | 2 | BENCH-01, BENCH-02, BENCH-03, BENCH-04 | benchmark-verification doc grep | `rg -n '## Continuity Gate|## Hard-Gists Slice|## Memory Comparison|## Host Variance|## Requirement Verdicts|BENCH-01|BENCH-04' .planning/phases/06-benchmark-verification-and-v2-closeout/06-BENCHMARK-VERIFICATION.md` | yes | pending |
| 06-03-01 | 03 | 3 | BENCH-05 | final Rust verification gate | `cargo fmt --manifest-path tools/apdr/Cargo.toml --all --check && cargo test --manifest-path tools/apdr/Cargo.toml --test test_resolver resolver_ -- --nocapture && cargo test --manifest-path tools/apdr/Cargo.toml validation_pipeline_ -- --nocapture && cargo test --manifest-path tools/apdr/Cargo.toml -- --nocapture && cargo clippy --manifest-path tools/apdr/Cargo.toml --all-targets -- -D warnings` | yes | pending |
| 06-03-02 | 03 | 3 | BENCH-05 | closeout doc grep | `rg -n '## Milestone Outcome|## Benchmark Evidence|## Review Readiness|## Remaining Variance and Risk|## Final Signoff' .planning/phases/06-benchmark-verification-and-v2-closeout/06-MILESTONE-CLOSEOUT.md` | yes | pending |

---

## Wave 0 Requirements

- Existing infrastructure covers all phase requirements.
- No new benchmark framework should be added; Phase 6 should reuse `measure_apdr_baseline.py`, `profile_apdr_memory.py`, and `check_apdr_regression.py`.
- The hard-gists slice must be kept bounded and reproducible rather than turning into a full-corpus benchmark campaign.

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| Benchmark-verification package keeps continuity, hard-gists, memory, and host variance as separate evidence streams | BENCH-01, BENCH-02, BENCH-03, BENCH-04 | Grep can prove the sections exist, but a reviewer must still judge whether the claims stay properly separated | Read `06-BENCHMARK-VERIFICATION.md` and confirm the continuity gate, hard-gists slice, memory comparison, and host-variance sections do not collapse into one blended benchmark claim |
| Milestone closeout package is honest about blockers or unrelated dirty-worktree noise | BENCH-05 | Automated commands can fail, but a reviewer must judge whether the closeout narrative correctly attributes failures or blockers | If the final Rust verification gate fails, confirm `06-MILESTONE-CLOSEOUT.md` names the real blocker and does not silently treat unrelated local edits as Phase 6 work |
| Hard-gists slice remains a bounded milestone-evidence slice rather than a hidden full-corpus benchmark | BENCH-01, BENCH-04 | A command can prove the limit used, but a reviewer still needs to verify the narrative matches that scope | Read `06-HARD-GISTS-SLICE.md` and confirm it states the exact sample limit and selection rule, and that `06-BENCHMARK-VERIFICATION.md` refers to it as a slice, not the whole corpus |

---

## Validation Sign-Off

- [x] All planned tasks have an automated verify step or explicit manual-only review instruction
- [x] Sampling continuity includes both benchmark artifact generation and the existing Rust verification gate
- [x] Existing infrastructure covers the phase
- [x] No watch-mode commands are required
- [x] Feedback latency remains bounded outside the intentional benchmark captures
- [x] `nyquist_compliant: true` is set in frontmatter

**Approval:** approved 2026-03-27
