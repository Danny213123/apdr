---
phase: 24
slug: env-first-vs-docker-first-comparison-harness
status: ready
nyquist_compliant: true
wave_0_complete: true
created: 2026-04-02
---

# Phase 24 - Validation Strategy

> Per-phase validation contract for feedback sampling during execution.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | Python CLI harness + deterministic comparison checker + artifact grep checks |
| **Config file** | `.planning/phases/24-env-first-vs-docker-first-comparison-harness/24-comparison-slice.json`, fixture summary JSONs, sample artifact JSONs, `scripts/run_phase24_policy_comparison.py`, and `scripts/check_phase24_policy_comparison.py` |
| **Quick run command** | `/bin/zsh -lc "python3 scripts/run_phase24_policy_comparison.py --slice-json .planning/phases/24-env-first-vs-docker-first-comparison-harness/24-comparison-slice.json --summary-json .planning/phases/24-env-first-vs-docker-first-comparison-harness/24-env-first-fixture-summary.json --output-json /tmp/phase24-env-artifact.json --mode env-first --llm-validation-policy env-first --probe-only && python3 scripts/run_phase24_policy_comparison.py --slice-json .planning/phases/24-env-first-vs-docker-first-comparison-harness/24-comparison-slice.json --summary-json .planning/phases/24-env-first-vs-docker-first-comparison-harness/24-docker-first-fixture-summary.json --output-json /tmp/phase24-docker-artifact.json --mode docker-first --llm-validation-policy docker-first --probe-only && python3 scripts/check_phase24_policy_comparison.py --env-artifact /tmp/phase24-env-artifact.json --docker-artifact /tmp/phase24-docker-artifact.json --status-json /tmp/phase24-comparison-status.json --probe-only"` |
| **Full suite command** | `/bin/zsh -lc "python3 scripts/run_phase24_policy_comparison.py --slice-json .planning/phases/24-env-first-vs-docker-first-comparison-harness/24-comparison-slice.json --summary-json .planning/phases/24-env-first-vs-docker-first-comparison-harness/24-env-first-fixture-summary.json --output-json /tmp/phase24-env-artifact.json --mode env-first --llm-validation-policy env-first --probe-only && python3 scripts/run_phase24_policy_comparison.py --slice-json .planning/phases/24-env-first-vs-docker-first-comparison-harness/24-comparison-slice.json --summary-json .planning/phases/24-env-first-vs-docker-first-comparison-harness/24-docker-first-fixture-summary.json --output-json /tmp/phase24-docker-artifact.json --mode docker-first --llm-validation-policy docker-first --probe-only && python3 scripts/check_phase24_policy_comparison.py --env-artifact /tmp/phase24-env-artifact.json --docker-artifact /tmp/phase24-docker-artifact.json --status-json /tmp/phase24-comparison-status.json --probe-only && python3 scripts/check_phase24_policy_comparison.py --env-artifact .planning/phases/24-env-first-vs-docker-first-comparison-harness/24-env-first-sample.json --docker-artifact .planning/phases/24-env-first-vs-docker-first-comparison-harness/24-docker-first-sample.json --status-json .planning/phases/24-env-first-vs-docker-first-comparison-harness/24-comparison-proof-status.json --probe-only"` |
| **Estimated runtime** | ~25 seconds |

---

## Sampling Rate

- **After every task commit:** Run the quick run command
- **After every plan wave:** Run the full suite command
- **Before Phase 24 verification:** Run the full suite command plus any live paired replay called for in the runbook
- **Max feedback latency:** 25 seconds for deterministic probe checks

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|-----------|-------------------|-------------|--------|
| 24-01-01 | 01 | 1 | CMP-01 | probe/extraction | `python3 scripts/run_phase24_policy_comparison.py --slice-json .planning/phases/24-env-first-vs-docker-first-comparison-harness/24-comparison-slice.json --summary-json .planning/phases/24-env-first-vs-docker-first-comparison-harness/24-env-first-fixture-summary.json --output-json /tmp/phase24-env-artifact.json --mode env-first --llm-validation-policy env-first --probe-only` | ✅ | ⬜ pending |
| 24-01-02 | 01 | 1 | CMP-01 | probe/extraction | `python3 scripts/run_phase24_policy_comparison.py --slice-json .planning/phases/24-env-first-vs-docker-first-comparison-harness/24-comparison-slice.json --summary-json .planning/phases/24-env-first-vs-docker-first-comparison-harness/24-docker-first-fixture-summary.json --output-json /tmp/phase24-docker-artifact.json --mode docker-first --llm-validation-policy docker-first --probe-only` | ✅ | ⬜ pending |
| 24-02-01 | 02 | 2 | CMP-01, CMP-02 | proof-contract | `python3 scripts/check_phase24_policy_comparison.py --env-artifact /tmp/phase24-env-artifact.json --docker-artifact /tmp/phase24-docker-artifact.json --status-json /tmp/phase24-comparison-status.json --probe-only` | ✅ | ⬜ pending |
| 24-02-02 | 02 | 2 | CMP-02 | proof-doc | `python3 scripts/check_phase24_policy_comparison.py --env-artifact .planning/phases/24-env-first-vs-docker-first-comparison-harness/24-env-first-sample.json --docker-artifact .planning/phases/24-env-first-vs-docker-first-comparison-harness/24-docker-first-sample.json --status-json .planning/phases/24-env-first-vs-docker-first-comparison-harness/24-comparison-proof-status.json --probe-only` | ✅ | ⬜ pending |
| 24-03-01 | 03 | 3 | CMP-01, CMP-02 | runbook/proof | `python3 scripts/check_phase24_policy_comparison.py --env-artifact .planning/phases/24-env-first-vs-docker-first-comparison-harness/24-env-first-sample.json --docker-artifact .planning/phases/24-env-first-vs-docker-first-comparison-harness/24-docker-first-sample.json --status-json .planning/phases/24-env-first-vs-docker-first-comparison-harness/24-comparison-proof-status.json --probe-only` | ✅ | ⬜ pending |
| 24-03-02 | 03 | 3 | CMP-01, CMP-02 | grep/proof | `rg -n 'llm_validation_policy|validation_backend|pass_delta|docker_startup_duration_seconds|Phase 25' scripts/run_phase24_policy_comparison.py scripts/check_phase24_policy_comparison.py .planning/phases/24-env-first-vs-docker-first-comparison-harness/24-COMPARISON-RUNBOOK.md .planning/phases/24-env-first-vs-docker-first-comparison-harness/24-COMPARISON-PROOF.md` | ✅ | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

- Existing benchmark replay infrastructure already supports `replay_manifest`, `validation_backend`, and `llm_validation_policy`.
- Existing Phase 22 and Phase 23 artifacts already define the policy-truth keys that the comparison harness must preserve.

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| The same fixed slice can be replayed twice with only `llm_validation_policy` changed | CMP-01 | Requires a supported local host with APDR, model runtime, dataset, and Docker availability | Run the Phase 24 runbook on a supported host, launch one env-first and one docker-first replay for the same slice, and confirm both artifacts report the same slice id, case set, model, base URL, and `validation_backend=llm`. |
| The comparison delta is reviewer-readable and not mistaken for a final verdict | CMP-02 | Requires human review of the generated docs | Open `24-COMPARISON-DELTA.md`, `24-COMPARISON-RUNBOOK.md`, and `24-COMPARISON-PROOF.md` and confirm they describe the harness contract, pass/bucket/timing deltas, and the handoff to Phase 25 without overstating fixed-slice evidence. |
| Phase 23 policy-truth debt remains visible while Phase 24 proceeds | CMP-01, CMP-02 | Requires human judgment about milestone evidence boundaries | Confirm the Phase 24 proof docs mention the open Phase 23 human-verification debt and do not imply every upstream proof surface is already fully signed off. |

---

## Validation Sign-Off

- [x] All tasks have `<automated>` verify or Wave 0 dependencies
- [x] Sampling continuity: no 3 consecutive tasks without automated verify
- [x] Wave 0 covers all MISSING references
- [x] No watch-mode flags
- [x] Feedback latency < 30s for deterministic probe checks
- [x] `nyquist_compliant: true` set in frontmatter

**Approval:** ready
