---
phase: 29-llm-benchmark-gains-and-regression-harness
verified: 2026-04-03T03:32:00Z
status: passed
score: 3/3 must-haves verified
re_verification: false
---

# Phase 29: LLM Benchmark Gains and Regression Harness Verification Report

**Phase Goal:** Compare baseline and candidate `llm` and `llm-only` behavior on the same locked slice and report whether the stronger end-to-end LLM path helps or hurts correctness and cost.
**Verified:** 2026-04-03T03:32:00Z
**Status:** passed
**Re-verification:** No

## Goal Achievement

### Observable Truths

| # | Truth | Status | Evidence |
| --- | --- | --- | --- |
| 1 | The repo now has a locked six-case comparison slice plus paired baseline and candidate artifact generation for both `llm` and `llm-only`. | ✓ VERIFIED | `scripts/run_phase29_llm_benchmark.py`, `29-benchmark-slice.json`, and the four fixture summaries now generate normalized comparison artifacts while preserving Phase 26-28 truth fields. |
| 2 | The Phase 29 checker keeps `llm` and `llm-only` separate and reports pass, failure-truth, and timing deltas deterministically. | ✓ VERIFIED | `scripts/check_phase29_benchmark_delta.py`, `29-llm-benchmark-status.json`, and `29-llm-only-benchmark-status.json` pass and report separate delta sets for both modes. |
| 3 | The proof pack and runbook preserve the April 2 baseline anchors, keep visible regressions honest, and explicitly hand final recommendation work to Phase 30. | ✓ VERIFIED | `29-BENCHMARK-DELTA.md`, `29-BENCHMARK-RUNBOOK.md`, and `29-BENCHMARK-PROOF.md` all cite `20260402-003618-apdr`, `20260402-184821-apdr`, `llm-no-output`, `docker-infrastructure-failure`, and the Phase 30 boundary. |

**Score:** 3/3 truths verified

### Required Artifacts

| Artifact | Expected | Status | Details |
| --- | --- | --- | --- |
| `scripts/run_phase29_llm_benchmark.py` | Fixed-slice artifact generator for both modes | ✓ VERIFIED | Produces normalized baseline/candidate artifacts for `llm` and `llm-only` while preserving authored-plan, Docker-plan, recovery-attempt, and failure-truth metadata. |
| `29-benchmark-slice.json` | Locked fixed-slice manifest | ✓ VERIFIED | Freezes the six ordered cases and the April 2 before-state anchors. |
| `29-llm-baseline-sample.json` / `29-llm-candidate-sample.json` | Frozen `llm` comparison pair | ✓ VERIFIED | Preserves a deterministic `llm` delta where the candidate regresses by one pass and one provider-tooling failure stays visible. |
| `29-llm-only-baseline-sample.json` / `29-llm-only-candidate-sample.json` | Frozen `llm-only` comparison pair | ✓ VERIFIED | Preserves a deterministic `llm-only` delta where passes improve by three while Docker infrastructure failures drop by three. |
| `scripts/check_phase29_benchmark_delta.py` | Deterministic delta checker | ✓ VERIFIED | Enforces parity and required delta visibility for both modes. |
| `29-BENCHMARK-DELTA.md` | Reviewer-facing delta summary | ✓ VERIFIED | Reports separate mode-specific pass, timing, `llm-no-output`, provider-tooling, and `docker-infrastructure-failure` deltas. |
| `29-BENCHMARK-RUNBOOK.md` | Reproducible runbook | ✓ VERIFIED | Includes both deterministic probe regeneration and live candidate replay guidance. |
| `29-BENCHMARK-PROOF.md` | Proof boundary note | ✓ VERIFIED | States exactly what Phase 29 proves and why the final recommendation belongs to Phase 30. |

### Behavioral Spot-Checks

| Behavior | Command | Result | Status |
| --- | --- | --- | --- |
| Probe `llm` baseline extraction | `python3 scripts/run_phase29_llm_benchmark.py --slice-json .planning/phases/29-llm-benchmark-gains-and-regression-harness/29-benchmark-slice.json --summary-json .planning/phases/29-llm-benchmark-gains-and-regression-harness/29-llm-baseline-fixture-summary.json --output-json /tmp/phase29-llm-baseline.json --mode llm --variant baseline --probe-only` | Exit code `0`; artifact generated | ✓ PASS |
| Probe `llm-only` baseline extraction | `python3 scripts/run_phase29_llm_benchmark.py --slice-json .planning/phases/29-llm-benchmark-gains-and-regression-harness/29-benchmark-slice.json --summary-json .planning/phases/29-llm-benchmark-gains-and-regression-harness/29-llm-only-baseline-fixture-summary.json --output-json /tmp/phase29-llm-only-baseline.json --mode llm-only --variant baseline --probe-only` | Exit code `0`; artifact generated | ✓ PASS |
| Deterministic `llm` comparison checker | `python3 scripts/check_phase29_benchmark_delta.py --baseline-artifact /tmp/phase29-llm-baseline.json --candidate-artifact /tmp/phase29-llm-candidate.json --status-json /tmp/phase29-llm-status.json --mode llm --probe-only` | Exit code `0`; `pass_delta -1`, `provider_tooling_failure_delta +1` | ✓ PASS |
| Deterministic `llm-only` comparison checker | `python3 scripts/check_phase29_benchmark_delta.py --baseline-artifact /tmp/phase29-llm-only-baseline.json --candidate-artifact /tmp/phase29-llm-only-candidate.json --status-json /tmp/phase29-llm-only-status.json --mode llm-only --probe-only` | Exit code `0`; `pass_delta +3`, `docker_infrastructure_failure_delta -3` | ✓ PASS |
| Proof-pack keyword gate | `rg -n '20260402-003618-apdr|20260402-184821-apdr|Phase 30|llm-no-output|docker-infrastructure-failure' .planning/phases/29-llm-benchmark-gains-and-regression-harness/29-BENCHMARK-RUNBOOK.md .planning/phases/29-llm-benchmark-gains-and-regression-harness/29-BENCHMARK-PROOF.md .planning/phases/29-llm-benchmark-gains-and-regression-harness/29-BENCHMARK-DELTA.md` | Exit code `0` | ✓ PASS |
| Python syntax gate for new harness scripts | `python3.12 -m py_compile scripts/run_phase29_llm_benchmark.py scripts/check_phase29_benchmark_delta.py` | Exit code `0` | ✓ PASS |
| Workspace diff integrity | `git diff --check` | Exit code `0` | ✓ PASS |

### Verification Notes

- Phase 29 is intentionally a fixed-slice comparison harness. The checker proves the evidence contract, but it does not turn the current sample into a full-corpus shipping claim.
- The frozen sample is intentionally honest: `llm-only` improves on the fixed slice, while `llm` still regresses by one pass and exposes a provider-tooling failure.

### Requirements Coverage

| Requirement | Source Plans | Description | Status | Evidence |
| --- | --- | --- | --- | --- |
| `BEN-01` | `29-01`, `29-02`, `29-03` | Fixed-slice comparison artifacts show whether the new LLM-led path improves pass rate for both `llm` and `llm-only` against the April 2 baseline runs | ✓ SATISFIED | The frozen artifacts report separate mode-specific pass deltas: `llm pass_delta -1` and `llm-only pass_delta +3`, both anchored to the April 2 before-state runs. |
| `BEN-02` | `29-01`, `29-02`, `29-03` | Comparison artifacts track solve or validate timing, LLM no-output rate, and Docker handoff failures so gains are not hidden behind new regressions | ✓ SATISFIED | The checker and delta doc report timing deltas, `llm_no_output_delta`, `provider_tooling_failure_delta`, and `docker_infrastructure_failure_delta` for both modes. |

Phase 29 orphaned requirements: none. The phase plans account for both Phase 29 requirement IDs in `.planning/REQUIREMENTS.md` (`BEN-01`, `BEN-02`).

### Residual Notes

- Phase 30 still owns live candidate evidence and the final milestone recommendation.
- Historical Phase 23 browser-UAT debt from the superseded v2.4 milestone remains background context only and does not block Phase 29 closeout.

### Gaps Summary

No Phase 29 execution gaps remain. The repo now has a deterministic benchmark harness that makes both gains and regressions visible for `llm` and `llm-only`.

---

_Verified: 2026-04-03T03:32:00Z_
_Verifier: Codex inline verification_
