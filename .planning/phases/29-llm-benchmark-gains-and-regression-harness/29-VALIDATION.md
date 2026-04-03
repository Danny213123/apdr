---
phase: 29
slug: llm-benchmark-gains-and-regression-harness
status: ready
nyquist_compliant: true
wave_0_complete: true
created: 2026-04-03
---

# Phase 29 - Validation Strategy

> Per-phase validation contract for feedback sampling during execution.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | Python fixed-slice extraction and replay harness, deterministic delta checker, artifact grep checks, and light regression gates against prior proof seams |
| **Config file** | `.planning/phases/29-llm-benchmark-gains-and-regression-harness/29-benchmark-slice.json`, paired baseline/candidate fixture summaries, paired sample artifacts, `scripts/run_phase29_llm_benchmark.py`, and `scripts/check_phase29_benchmark_delta.py` |
| **Quick run command** | `/bin/zsh -lc "python3 scripts/run_phase29_llm_benchmark.py --slice-json .planning/phases/29-llm-benchmark-gains-and-regression-harness/29-benchmark-slice.json --summary-json .planning/phases/29-llm-benchmark-gains-and-regression-harness/29-llm-baseline-fixture-summary.json --output-json /tmp/phase29-llm-baseline.json --mode llm --variant baseline --probe-only && python3 scripts/run_phase29_llm_benchmark.py --slice-json .planning/phases/29-llm-benchmark-gains-and-regression-harness/29-benchmark-slice.json --summary-json .planning/phases/29-llm-benchmark-gains-and-regression-harness/29-llm-candidate-fixture-summary.json --output-json /tmp/phase29-llm-candidate.json --mode llm --variant candidate --probe-only && python3 scripts/run_phase29_llm_benchmark.py --slice-json .planning/phases/29-llm-benchmark-gains-and-regression-harness/29-benchmark-slice.json --summary-json .planning/phases/29-llm-benchmark-gains-and-regression-harness/29-llm-only-baseline-fixture-summary.json --output-json /tmp/phase29-llm-only-baseline.json --mode llm-only --variant baseline --probe-only && python3 scripts/run_phase29_llm_benchmark.py --slice-json .planning/phases/29-llm-benchmark-gains-and-regression-harness/29-benchmark-slice.json --summary-json .planning/phases/29-llm-benchmark-gains-and-regression-harness/29-llm-only-candidate-fixture-summary.json --output-json /tmp/phase29-llm-only-candidate.json --mode llm-only --variant candidate --probe-only"` |
| **Full suite command** | `/bin/zsh -lc "python3 scripts/run_phase29_llm_benchmark.py --slice-json .planning/phases/29-llm-benchmark-gains-and-regression-harness/29-benchmark-slice.json --summary-json .planning/phases/29-llm-benchmark-gains-and-regression-harness/29-llm-baseline-fixture-summary.json --output-json /tmp/phase29-llm-baseline.json --mode llm --variant baseline --probe-only && python3 scripts/run_phase29_llm_benchmark.py --slice-json .planning/phases/29-llm-benchmark-gains-and-regression-harness/29-benchmark-slice.json --summary-json .planning/phases/29-llm-benchmark-gains-and-regression-harness/29-llm-candidate-fixture-summary.json --output-json /tmp/phase29-llm-candidate.json --mode llm --variant candidate --probe-only && python3 scripts/run_phase29_llm_benchmark.py --slice-json .planning/phases/29-llm-benchmark-gains-and-regression-harness/29-benchmark-slice.json --summary-json .planning/phases/29-llm-benchmark-gains-and-regression-harness/29-llm-only-baseline-fixture-summary.json --output-json /tmp/phase29-llm-only-baseline.json --mode llm-only --variant baseline --probe-only && python3 scripts/run_phase29_llm_benchmark.py --slice-json .planning/phases/29-llm-benchmark-gains-and-regression-harness/29-benchmark-slice.json --summary-json .planning/phases/29-llm-benchmark-gains-and-regression-harness/29-llm-only-candidate-fixture-summary.json --output-json /tmp/phase29-llm-only-candidate.json --mode llm-only --variant candidate --probe-only && python3 scripts/check_phase29_benchmark_delta.py --baseline-artifact /tmp/phase29-llm-baseline.json --candidate-artifact /tmp/phase29-llm-candidate.json --status-json /tmp/phase29-llm-status.json --mode llm --probe-only && python3 scripts/check_phase29_benchmark_delta.py --baseline-artifact /tmp/phase29-llm-only-baseline.json --candidate-artifact /tmp/phase29-llm-only-candidate.json --status-json /tmp/phase29-llm-only-status.json --mode llm-only --probe-only && python3 scripts/check_phase29_benchmark_delta.py --baseline-artifact .planning/phases/29-llm-benchmark-gains-and-regression-harness/29-llm-baseline-sample.json --candidate-artifact .planning/phases/29-llm-benchmark-gains-and-regression-harness/29-llm-candidate-sample.json --status-json /tmp/phase29-llm-proof-status.json --mode llm --probe-only && python3 scripts/check_phase29_benchmark_delta.py --baseline-artifact .planning/phases/29-llm-benchmark-gains-and-regression-harness/29-llm-only-baseline-sample.json --candidate-artifact .planning/phases/29-llm-benchmark-gains-and-regression-harness/29-llm-only-candidate-sample.json --status-json /tmp/phase29-llm-only-proof-status.json --mode llm-only --probe-only"` |
| **Estimated runtime** | ~30 seconds |

---

## Sampling Rate

- **After every task commit:** Run the task's specific verify command
- **After every plan wave:** Run the quick run command
- **Before Phase 29 verification:** Run the full suite command and inspect the reviewer-facing delta/proof docs
- **Max feedback latency:** 30 seconds for deterministic probe checks

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|-----------|-------------------|-------------|--------|
| 29-01-01 | 01 | 1 | BEN-01 | probe/extraction | `python3 scripts/run_phase29_llm_benchmark.py --slice-json .planning/phases/29-llm-benchmark-gains-and-regression-harness/29-benchmark-slice.json --summary-json .planning/phases/29-llm-benchmark-gains-and-regression-harness/29-llm-baseline-fixture-summary.json --output-json /tmp/phase29-llm-baseline.json --mode llm --variant baseline --probe-only` | ✅ | ⬜ pending |
| 29-01-02 | 01 | 1 | BEN-01 | probe/extraction | `python3 scripts/run_phase29_llm_benchmark.py --slice-json .planning/phases/29-llm-benchmark-gains-and-regression-harness/29-benchmark-slice.json --summary-json .planning/phases/29-llm-benchmark-gains-and-regression-harness/29-llm-only-baseline-fixture-summary.json --output-json /tmp/phase29-llm-only-baseline.json --mode llm-only --variant baseline --probe-only` | ✅ | ⬜ pending |
| 29-02-01 | 02 | 2 | BEN-01, BEN-02 | proof-contract | `python3 scripts/check_phase29_benchmark_delta.py --baseline-artifact /tmp/phase29-llm-baseline.json --candidate-artifact /tmp/phase29-llm-candidate.json --status-json /tmp/phase29-llm-status.json --mode llm --probe-only` | ✅ | ⬜ pending |
| 29-02-02 | 02 | 2 | BEN-01, BEN-02 | proof-contract | `python3 scripts/check_phase29_benchmark_delta.py --baseline-artifact /tmp/phase29-llm-only-baseline.json --candidate-artifact /tmp/phase29-llm-only-candidate.json --status-json /tmp/phase29-llm-only-status.json --mode llm-only --probe-only` | ✅ | ⬜ pending |
| 29-02-03 | 02 | 2 | BEN-02 | proof-doc | `python3 scripts/check_phase29_benchmark_delta.py --baseline-artifact .planning/phases/29-llm-benchmark-gains-and-regression-harness/29-llm-baseline-sample.json --candidate-artifact .planning/phases/29-llm-benchmark-gains-and-regression-harness/29-llm-candidate-sample.json --status-json /tmp/phase29-llm-proof-status.json --mode llm --probe-only && python3 scripts/check_phase29_benchmark_delta.py --baseline-artifact .planning/phases/29-llm-benchmark-gains-and-regression-harness/29-llm-only-baseline-sample.json --candidate-artifact .planning/phases/29-llm-benchmark-gains-and-regression-harness/29-llm-only-candidate-sample.json --status-json /tmp/phase29-llm-only-proof-status.json --mode llm-only --probe-only` | ✅ | ⬜ pending |
| 29-03-01 | 03 | 3 | BEN-01, BEN-02 | runbook/proof | `rg -n '20260402-003618-apdr|20260402-184821-apdr|Phase 30|llm-no-output|docker-infrastructure-failure' .planning/phases/29-llm-benchmark-gains-and-regression-harness/29-BENCHMARK-RUNBOOK.md .planning/phases/29-llm-benchmark-gains-and-regression-harness/29-BENCHMARK-PROOF.md .planning/phases/29-llm-benchmark-gains-and-regression-harness/29-BENCHMARK-DELTA.md` | ✅ | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

- Phase 26 authored-intake truth remains the source of truth for what the LLM planned before validation; Phase 29 may compare it, but must not redefine it.
- Phase 27 Docker artifact truth remains the source of truth for authored-versus-executed Docker behavior.
- Phase 28 additive failure truth remains the source of truth for distinguishing `llm-no-output`, provider/tooling, Docker infrastructure, and dependency/runtime failures.

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| The Phase 29 delta document is reviewer-readable and keeps `llm` and `llm-only` separate | BEN-01, BEN-02 | Requires human review of the generated summary docs | Open `29-BENCHMARK-DELTA.md` and confirm it reports separate baseline-versus-candidate sections for `llm` and `llm-only`, rather than one blended score. |
| The runbook anchors the baseline to the intended April 2 runs | BEN-01 | Requires checking artifact narrative, not just machine fields | Open `29-BENCHMARK-RUNBOOK.md` and confirm it explicitly names the April 2 baseline run anchors and explains why they represent the pre-v2.5 before-state. |
| The proof pack does not overstate fixed-slice evidence as a full-corpus verdict | BEN-02 | Requires human judgment about evidence boundaries | Open `29-BENCHMARK-PROOF.md` and confirm it frames Phase 29 as a regression harness and handoff to Phase 30 rather than the final ship recommendation. |

---

## Validation Sign-Off

- [x] All tasks have `<automated>` verify or Wave 0 dependencies
- [x] Sampling continuity: no 3 consecutive tasks without automated verify
- [x] Wave 0 covers all MISSING references
- [x] No watch-mode flags
- [x] Feedback latency < 45s for deterministic probe checks
- [x] `nyquist_compliant: true` set in frontmatter

**Approval:** ready
