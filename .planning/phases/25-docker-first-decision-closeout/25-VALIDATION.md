---
phase: 25
slug: docker-first-decision-closeout
status: ready
nyquist_compliant: true
wave_0_complete: true
created: 2026-04-02
---

# Phase 25 - Validation Strategy

> Per-phase validation contract for feedback sampling during execution.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | JSON + Markdown closeout inputs, deterministic Python checker, and artifact grep checks |
| **Config file** | `.planning/phases/25-docker-first-decision-closeout/25-DECISION-INPUTS.json`, `.planning/phases/25-docker-first-decision-closeout/25-EVIDENCE-MATRIX.md`, `.planning/phases/25-docker-first-decision-closeout/25-MILESTONE-VERDICT.md`, `.planning/phases/25-docker-first-decision-closeout/25-CLOSEOUT-PROOF.md`, `.planning/phases/25-docker-first-decision-closeout/25-MILESTONE-READY.md`, and `scripts/check_phase25_decision_closeout.py` |
| **Quick run command** | `/bin/zsh -lc "python3 scripts/check_phase25_decision_closeout.py --inputs-json .planning/phases/25-docker-first-decision-closeout/25-DECISION-INPUTS.json --verdict-md .planning/phases/25-docker-first-decision-closeout/25-MILESTONE-VERDICT.md --proof-md .planning/phases/25-docker-first-decision-closeout/25-CLOSEOUT-PROOF.md --status-json /tmp/phase25-status.json --probe-only"` |
| **Full suite command** | `/bin/zsh -lc "python3 scripts/check_phase25_decision_closeout.py --inputs-json .planning/phases/25-docker-first-decision-closeout/25-DECISION-INPUTS.json --verdict-md .planning/phases/25-docker-first-decision-closeout/25-MILESTONE-VERDICT.md --proof-md .planning/phases/25-docker-first-decision-closeout/25-CLOSEOUT-PROOF.md --status-json .planning/phases/25-docker-first-decision-closeout/25-decision-proof-status.json --probe-only && rg -n 'pass_delta|docker_startup_duration_seconds|fixed_slice_only|phase23_human_uat|verdict:|replace|optional|reject' .planning/phases/25-docker-first-decision-closeout/25-DECISION-INPUTS.json .planning/phases/25-docker-first-decision-closeout/25-EVIDENCE-MATRIX.md .planning/phases/25-docker-first-decision-closeout/25-MILESTONE-VERDICT.md .planning/phases/25-docker-first-decision-closeout/25-CLOSEOUT-PROOF.md .planning/phases/25-docker-first-decision-closeout/25-MILESTONE-READY.md && git diff --check"` |
| **Estimated runtime** | ~5 seconds |

---

## Sampling Rate

- **After every task commit:** Run the task's specific verify command
- **After every plan wave:** Run the quick run command
- **Before Phase 25 verification:** Run the full suite command
- **Max feedback latency:** 5 seconds for deterministic closeout checks

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|-----------|-------------------|-------------|--------|
| 25-01-01 | 01 | 1 | EVD-10 | evidence-input | `rg -n 'fixed_slice_only|phase23_human_uat|pass_delta|docker_startup_duration_seconds|allowed_verdicts' .planning/phases/25-docker-first-decision-closeout/25-DECISION-INPUTS.json` | ✅ | ⬜ pending |
| 25-01-02 | 01 | 1 | EVD-10 | evidence-matrix | `rg -n 'replace|optional|reject|supporting evidence|blocking evidence|Phase 23' .planning/phases/25-docker-first-decision-closeout/25-EVIDENCE-MATRIX.md` | ✅ | ⬜ pending |
| 25-02-01 | 02 | 2 | EVD-10 | verdict-doc | `rg -n '^verdict:|pass delta|docker_startup_duration_seconds|fixed-slice|Phase 23' .planning/phases/25-docker-first-decision-closeout/25-MILESTONE-VERDICT.md` | ✅ | ⬜ pending |
| 25-02-02 | 02 | 2 | EVD-10 | decision-checker | `python3 scripts/check_phase25_decision_closeout.py --inputs-json .planning/phases/25-docker-first-decision-closeout/25-DECISION-INPUTS.json --verdict-md .planning/phases/25-docker-first-decision-closeout/25-MILESTONE-VERDICT.md --status-json /tmp/phase25-status.json --probe-only` | ✅ | ⬜ pending |
| 25-03-01 | 03 | 3 | EVD-10 | closeout-proof | `python3 scripts/check_phase25_decision_closeout.py --inputs-json .planning/phases/25-docker-first-decision-closeout/25-DECISION-INPUTS.json --verdict-md .planning/phases/25-docker-first-decision-closeout/25-MILESTONE-VERDICT.md --proof-md .planning/phases/25-docker-first-decision-closeout/25-CLOSEOUT-PROOF.md --status-json .planning/phases/25-docker-first-decision-closeout/25-decision-proof-status.json --probe-only` | ✅ | ⬜ pending |
| 25-03-02 | 03 | 3 | EVD-10 | closeout-ready | `rg -n 'gsd-complete-milestone|fixed-slice|Phase 23|verdict|next step' .planning/phases/25-docker-first-decision-closeout/25-CLOSEOUT-PROOF.md .planning/phases/25-docker-first-decision-closeout/25-MILESTONE-READY.md` | ✅ | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

- Phase 22 policy guarantees and Phase 24 comparison proof must already exist and remain the upstream decision inputs.
- Phase 23 human-UAT debt must stay readable from `.planning/phases/23-policy-truth-and-failure-semantics/23-HUMAN-UAT.md`.

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| The final recommendation reads as honest reviewer guidance rather than milestone marketing | EVD-10 | Requires human judgment about tone and evidence discipline | Open `25-MILESTONE-VERDICT.md` and `25-CLOSEOUT-PROOF.md` and confirm they clearly state the fixed-slice scope, the current tradeoffs, and why the chosen verdict is appropriate. |
| Any remaining Phase 23 browser-UAT debt stays visible at closeout | EVD-10 | Requires a human check across multiple closeout docs | Confirm the Phase 25 closeout artifacts either record completed Phase 23 UAT or explicitly carry the pending debt into the recommendation and readiness note. |
| Milestone archival is only proposed when the verdict and readiness note agree | EVD-10 | Requires human signoff on the final handoff | Read `25-MILESTONE-READY.md` and confirm it points to `$gsd-complete-milestone` only after the verdict, proof, and outstanding debt state agree. |

---

## Validation Sign-Off

- [x] All tasks have `<automated>` verify or Wave 0 dependencies
- [x] Sampling continuity: no 3 consecutive tasks without automated verify
- [x] Wave 0 covers all MISSING references
- [x] No watch-mode flags
- [x] Feedback latency < 30s for deterministic probe checks
- [x] `nyquist_compliant: true` set in frontmatter

**Approval:** ready
