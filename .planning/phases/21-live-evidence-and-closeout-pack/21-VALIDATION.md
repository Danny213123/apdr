---
phase: 21
slug: live-evidence-and-closeout-pack
status: draft
nyquist_compliant: false
wave_0_complete: true
created: 2026-04-01
---

# Phase 21 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | Python proof scripts plus fixed-slice live replay |
| **Config file** | `scripts/run_phase20_recovery_benchmark.py`, `scripts/check_phase20_recovery_delta.py`, and `scripts/check_phase21_live_evidence.py` |
| **Quick run command** | `/bin/zsh -lc "python3 scripts/run_phase20_recovery_benchmark.py --slice-json .planning/phases/20-dominant-bucket-recovery-gains/20-dominant-bucket-slice.json --summary-json runs/20260330-020943-apdr/summary.json --output-json /tmp/phase21-baseline-check.json --mode baseline --validation-backend llm --model-name qwen3.5:9b --base-url http://localhost:11434 --probe-only && python3 scripts/check_phase20_recovery_delta.py --slice-json .planning/phases/20-dominant-bucket-recovery-gains/20-dominant-bucket-slice.json --baseline-json .planning/phases/20-dominant-bucket-recovery-gains/20-baseline-sample.json --candidate-json .planning/phases/20-dominant-bucket-recovery-gains/20-candidate-sample.json --status-json /tmp/phase21-delta-check.json --probe-only"` |
| **Full suite command** | `/bin/zsh -lc "python3 scripts/run_phase20_recovery_benchmark.py --slice-json .planning/phases/20-dominant-bucket-recovery-gains/20-dominant-bucket-slice.json --summary-json runs/20260330-020943-apdr/summary.json --output-json .planning/phases/21-live-evidence-and-closeout-pack/21-baseline-live.json --mode baseline --validation-backend llm --model-name qwen3.5:9b --base-url http://localhost:11434 --probe-only && python3 scripts/run_phase20_recovery_benchmark.py --slice-json .planning/phases/20-dominant-bucket-recovery-gains/20-dominant-bucket-slice.json --dataset-root hard-gists --output-json .planning/phases/21-live-evidence-and-closeout-pack/21-candidate-live.json --mode candidate --validation-backend llm --model-name qwen3.5:9b --base-url http://localhost:11434 --execute-live && python3 scripts/check_phase20_recovery_delta.py --slice-json .planning/phases/20-dominant-bucket-recovery-gains/20-dominant-bucket-slice.json --baseline-json .planning/phases/21-live-evidence-and-closeout-pack/21-baseline-live.json --candidate-json .planning/phases/21-live-evidence-and-closeout-pack/21-candidate-live.json --status-json /tmp/phase21-delta-status.json && python3 scripts/check_phase21_live_evidence.py --baseline-json .planning/phases/21-live-evidence-and-closeout-pack/21-baseline-live.json --candidate-json .planning/phases/21-live-evidence-and-closeout-pack/21-candidate-live.json --case-index .planning/phases/21-live-evidence-and-closeout-pack/21-case-index.json --evidence-md .planning/phases/21-live-evidence-and-closeout-pack/21-LIVE-EVIDENCE.md --cases-md .planning/phases/21-live-evidence-and-closeout-pack/21-REPRESENTATIVE-CASES.md --closeout-md .planning/phases/21-live-evidence-and-closeout-pack/21-MILESTONE-CLOSEOUT.md --status-json .planning/phases/21-live-evidence-and-closeout-pack/21-live-evidence-status.json"` |
| **Estimated runtime** | ~2700 seconds |

---

## Sampling Rate

- **After every task commit:** Run `/bin/zsh -lc "python3 scripts/run_phase20_recovery_benchmark.py --slice-json .planning/phases/20-dominant-bucket-recovery-gains/20-dominant-bucket-slice.json --summary-json runs/20260330-020943-apdr/summary.json --output-json /tmp/phase21-baseline-check.json --mode baseline --validation-backend llm --model-name qwen3.5:9b --base-url http://localhost:11434 --probe-only || python3 scripts/check_phase20_recovery_delta.py --slice-json .planning/phases/20-dominant-bucket-recovery-gains/20-dominant-bucket-slice.json --baseline-json .planning/phases/20-dominant-bucket-recovery-gains/20-baseline-sample.json --candidate-json .planning/phases/20-dominant-bucket-recovery-gains/20-candidate-sample.json --status-json /tmp/phase21-delta-check.json --probe-only"` |
- **After every plan wave:** Run the strongest automated checker available for that wave. Wave 1 should at minimum prove the runner can still extract the March 30 baseline contract; Wave 2 should run the Phase 21 evidence checker in probe mode; Wave 3 should run the full live suite.
- **Before `$gsd-verify-work`:** Full suite must be green, including the live candidate replay on the locked slice.
- **Max feedback latency:** 2700 seconds

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|-----------|-------------------|-------------|--------|
| 21-01-01 | 01 | 1 | EVD-08 | script-contract | `python3 scripts/run_phase20_recovery_benchmark.py --slice-json .planning/phases/20-dominant-bucket-recovery-gains/20-dominant-bucket-slice.json --summary-json runs/20260330-020943-apdr/summary.json --output-json /tmp/phase21-baseline-check.json --mode baseline --validation-backend llm --model-name qwen3.5:9b --base-url http://localhost:11434 --probe-only` | ✅ | ⬜ pending |
| 21-01-02 | 01 | 1 | EVD-08 | live-replay | `python3 scripts/run_phase20_recovery_benchmark.py --slice-json .planning/phases/20-dominant-bucket-recovery-gains/20-dominant-bucket-slice.json --dataset-root hard-gists --output-json .planning/phases/21-live-evidence-and-closeout-pack/21-candidate-live.json --mode candidate --validation-backend llm --model-name qwen3.5:9b --base-url http://localhost:11434 --execute-live` | ✅ | ⬜ pending |
| 21-02-01 | 02 | 2 | EVD-08 | artifact-contract | `python3 scripts/check_phase20_recovery_delta.py --slice-json .planning/phases/20-dominant-bucket-recovery-gains/20-dominant-bucket-slice.json --baseline-json .planning/phases/21-live-evidence-and-closeout-pack/21-baseline-live.json --candidate-json .planning/phases/21-live-evidence-and-closeout-pack/21-candidate-live.json --status-json /tmp/phase21-delta-status.json && rg -n 'recovered-delta|backend-path-truth|failure-family-truth|fallback-truth|fallback_outcome|resultOrigin' .planning/phases/21-live-evidence-and-closeout-pack/21-case-index.json` | ✅ | ⬜ pending |
| 21-02-02 | 02 | 2 | EVD-08 | artifact-contract | `python3 scripts/check_phase20_recovery_delta.py --slice-json .planning/phases/20-dominant-bucket-recovery-gains/20-dominant-bucket-slice.json --baseline-json .planning/phases/21-live-evidence-and-closeout-pack/21-baseline-live.json --candidate-json .planning/phases/21-live-evidence-and-closeout-pack/21-candidate-live.json --status-json /tmp/phase21-delta-status.json && rg -n '## Before/After Bucket Counts|March 30, 2026 baseline|v2.3 candidate|## Representative Cases|## Remaining Limits' .planning/phases/21-live-evidence-and-closeout-pack/21-LIVE-EVIDENCE.md .planning/phases/21-live-evidence-and-closeout-pack/21-REPRESENTATIVE-CASES.md` | ✅ | ⬜ pending |
| 21-03-01 | 03 | 3 | EVD-08 | proof-contract | `python3 scripts/check_phase20_recovery_delta.py --slice-json .planning/phases/20-dominant-bucket-recovery-gains/20-dominant-bucket-slice.json --baseline-json .planning/phases/21-live-evidence-and-closeout-pack/21-baseline-live.json --candidate-json .planning/phases/21-live-evidence-and-closeout-pack/21-candidate-live.json --status-json /tmp/phase21-delta-status.json` | ✅ | ⬜ pending |
| 21-03-02 | 03 | 3 | EVD-08 | closeout-contract | `python3 scripts/check_phase21_live_evidence.py --baseline-json .planning/phases/21-live-evidence-and-closeout-pack/21-baseline-live.json --candidate-json .planning/phases/21-live-evidence-and-closeout-pack/21-candidate-live.json --case-index .planning/phases/21-live-evidence-and-closeout-pack/21-case-index.json --evidence-md .planning/phases/21-live-evidence-and-closeout-pack/21-LIVE-EVIDENCE.md --cases-md .planning/phases/21-live-evidence-and-closeout-pack/21-REPRESENTATIVE-CASES.md --closeout-md .planning/phases/21-live-evidence-and-closeout-pack/21-MILESTONE-CLOSEOUT.md --status-json .planning/phases/21-live-evidence-and-closeout-pack/21-live-evidence-status.json` | ✅ | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

- Existing infrastructure covers the fixed slice, baseline source summary, and deterministic delta contract.

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| The candidate artifact is genuinely post-Phase-20 evidence rather than a reused older run | EVD-08 | Requires reading the recorded source run path and comparing it to the Phase 20 completion date | Open `21-LIVE-RUNBOOK.md` and confirm the candidate `source_run` was produced after 2026-04-01 and is not `runs/phase20-probe-candidate` or any run that predates the Phase 20 commits. |
| Representative cases really show the shipped truth surfaces from Phases 17-19 as well as the Phase 20 gains | EVD-08 | A reviewer must inspect real artifact paths and field values, not only count summaries | Open `21-case-index.json` and `21-REPRESENTATIVE-CASES.md`, then inspect the linked artifact directories. Confirm the selected cases expose `fallback_outcome`, `validation_path`, `failure_family`, and `resultOrigin` where those fields are part of the documented claim. |
| The closeout note and requirement truth describe the same terminal state | EVD-08 | Requires human judgment that the narrative is honest and does not overclaim beyond the machine-readable status | Read `21-MILESTONE-CLOSEOUT.md`, `REQUIREMENTS.md`, `ROADMAP.md`, and `STATE.md` together and confirm they agree on whether `EVD-08` is complete or still blocked. |

---

## Validation Sign-Off

- [ ] All tasks have `<automated>` verify or Wave 0 dependencies
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify
- [ ] Wave 0 covers all MISSING references
- [ ] No watch-mode flags
- [ ] Feedback latency < 2700s
- [ ] `nyquist_compliant: true` set in frontmatter

**Approval:** pending
