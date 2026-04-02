---
phase: 24-env-first-vs-docker-first-comparison-harness
verified: 2026-04-02T17:08:10Z
status: passed
score: 3/3 must-haves verified
re_verification: false
---

# Phase 24: Env-First vs Docker-First Comparison Harness Verification Report

**Phase Goal:** The repo can compare docker-first and env-first `llm` behavior on the same slice and report whether the first-hop change helps or hurts correctness and cost.
**Verified:** 2026-04-02T17:08:10Z
**Status:** passed
**Re-verification:** No

## Goal Achievement

### Observable Truths

| # | Truth | Status | Evidence |
| --- | --- | --- | --- |
| 1 | APDR can generate or extract env-first and docker-first artifacts for the same fixed slice with matching model and backend contracts. | ✓ VERIFIED | `scripts/run_phase24_policy_comparison.py` normalizes both policies onto the same slice, keeps `validation_backend=llm`, and the frozen env-first and docker-first sample artifacts share the same `slice_id`, case ordering, model, and base URL. |
| 2 | Comparison outputs report pass delta, dominant-bucket delta, and timing delta between the two policies. | ✓ VERIFIED | `scripts/check_phase24_policy_comparison.py` computes pass, failure, skip, dominant-bucket, and timing deltas; the frozen proof status records `pass_delta=2`, `module-not-found=-1`, `environment-build-failed=-1`, and a non-zero Docker startup timing delta. |
| 3 | The comparison contract is deterministic and fails if the paired-policy evidence drifts from the locked slice or omits required metrics. | ✓ VERIFIED | The checker enforces identical slice/model/backend contracts, per-row parity rules, and non-zero comparison outputs, while the runbook and proof note freeze how probe-only extraction and paired live replay must be executed. |

**Score:** 3/3 truths verified

### Required Artifacts

| Artifact | Expected | Status | Details |
| --- | --- | --- | --- |
| `scripts/run_phase24_policy_comparison.py` | Extract or replay paired env-first and docker-first artifacts on the same fixed slice | ✓ VERIFIED | Supports probe-only fixture extraction and live paired replay while preserving matching `llm` contracts. |
| `scripts/check_phase24_policy_comparison.py` | Deterministic env-first vs docker-first delta checker | ✓ VERIFIED | Enforces contract parity and emits pass, bucket, and timing deltas into a status JSON. |
| `24-comparison-slice.json` | Locked comparison slice | ✓ VERIFIED | Freezes the fixed slice used for both policies. |
| `24-env-first-fixture-summary.json` | Env-first probe fixture summary | ✓ VERIFIED | Supplies the fixed env-first source fixture for probe extraction. |
| `24-docker-first-fixture-summary.json` | Docker-first probe fixture summary | ✓ VERIFIED | Supplies the fixed docker-first source fixture for probe extraction. |
| `24-env-first-sample.json` | Frozen env-first comparison artifact | ✓ VERIFIED | Captures normalized env-first output for the locked slice. |
| `24-docker-first-sample.json` | Frozen docker-first comparison artifact | ✓ VERIFIED | Captures normalized docker-first output for the locked slice. |
| `24-comparison-proof-status.json` | Frozen comparison status output | ✓ VERIFIED | Records the deterministic paired-policy delta contract. |
| `24-COMPARISON-DELTA.md` | Reviewer-facing comparison summary | ✓ VERIFIED | Summarizes the observed pass, bucket, and timing changes on the fixed slice. |
| `24-COMPARISON-RUNBOOK.md` | Repeatable operator runbook | ✓ VERIFIED | Documents probe-only extraction, paired live replay, and the contract-parity rules. |
| `24-COMPARISON-PROOF.md` | Honest proof-boundary note | ✓ VERIFIED | States that Phase 24 proves the harness and leaves the final keep/optional/reject decision to Phase 25. |

### Key Link Verification

| From | To | Via | Status | Details |
| --- | --- | --- | --- | --- |
| `24-comparison-slice.json` | `scripts/run_phase24_policy_comparison.py` | Fixed slice input for paired artifact generation | ✓ WIRED | The harness reads the locked slice and emits normalized outputs for each policy without changing the case set. |
| `24-env-first-fixture-summary.json` | `scripts/run_phase24_policy_comparison.py` | Probe-only env-first extraction | ✓ WIRED | The harness can materialize a deterministic env-first artifact directly from the frozen summary fixture. |
| `24-docker-first-fixture-summary.json` | `scripts/run_phase24_policy_comparison.py` | Probe-only docker-first extraction | ✓ WIRED | The harness can materialize a deterministic docker-first artifact directly from the frozen summary fixture. |
| `scripts/run_phase24_policy_comparison.py` | `scripts/check_phase24_policy_comparison.py` | Paired artifact normalization into comparison checker inputs | ✓ WIRED | The checker expects the harness output shape, including route, bucket, provenance, and timing fields. |
| `scripts/check_phase24_policy_comparison.py` | `24-comparison-proof-status.json` | Deterministic proof status generation | ✓ WIRED | The frozen proof status is produced by the checker and locks the current comparison delta contract. |
| `24-COMPARISON-RUNBOOK.md` | `24-COMPARISON-PROOF.md` | Human-readable replay and proof boundary | ✓ WIRED | The runbook explains how to reproduce the evidence, and the proof note explains what the evidence does and does not claim. |

### Data-Flow Trace

| Artifact | Data Variable | Source | Produces Real Data | Status |
| --- | --- | --- | --- | --- |
| `scripts/run_phase24_policy_comparison.py` | `artifact.run_contract.llm_validation_policy` | Fixed slice + fixture/live run config -> normalized artifact JSON | Yes | ✓ FLOWING |
| `scripts/run_phase24_policy_comparison.py` | `results[].validation_path`, `results[].failure_bucket`, `results[].docker_startup_duration_seconds` | Frozen summary rows or live replay outputs -> normalized paired artifact rows | Yes | ✓ FLOWING |
| `scripts/check_phase24_policy_comparison.py` | `comparison.pass_delta`, `comparison.dominant_bucket_deltas`, `comparison.timing_deltas` | Paired env-first and docker-first artifacts | Yes | ✓ FLOWING |
| `24-comparison-proof-status.json` | `passed`, `must_haves_verified`, `comparison` | Checker output persisted to proof artifact | Yes | ✓ FLOWING |

### Behavioral Spot-Checks

| Behavior | Command | Result | Status |
| --- | --- | --- | --- |
| Probe-only env-first artifact extraction | `python3 scripts/run_phase24_policy_comparison.py --slice-json .planning/phases/24-env-first-vs-docker-first-comparison-harness/24-comparison-slice.json --summary-json .planning/phases/24-env-first-vs-docker-first-comparison-harness/24-env-first-fixture-summary.json --output-json /tmp/phase24-env-artifact.json --mode env-first --llm-validation-policy env-first --probe-only` | Exit code `0`; normalized env-first artifact emitted | ✓ PASS |
| Probe-only docker-first artifact extraction | `python3 scripts/run_phase24_policy_comparison.py --slice-json .planning/phases/24-env-first-vs-docker-first-comparison-harness/24-comparison-slice.json --summary-json .planning/phases/24-env-first-vs-docker-first-comparison-harness/24-docker-first-fixture-summary.json --output-json /tmp/phase24-docker-artifact.json --mode docker-first --llm-validation-policy docker-first --probe-only` | Exit code `0`; normalized docker-first artifact emitted | ✓ PASS |
| Deterministic comparison checker on generated artifacts | `python3 scripts/check_phase24_policy_comparison.py --env-artifact /tmp/phase24-env-artifact.json --docker-artifact /tmp/phase24-docker-artifact.json --status-json /tmp/phase24-comparison-status.json --probe-only` | Exit code `0`; reports non-zero pass, bucket, and timing deltas | ✓ PASS |
| Deterministic comparison checker on frozen proof inputs | `python3 scripts/check_phase24_policy_comparison.py --env-artifact .planning/phases/24-env-first-vs-docker-first-comparison-harness/24-env-first-sample.json --docker-artifact .planning/phases/24-env-first-vs-docker-first-comparison-harness/24-docker-first-sample.json --status-json .planning/phases/24-env-first-vs-docker-first-comparison-harness/24-comparison-proof-status.json --probe-only` | Exit code `0`; frozen proof status reports `passed: true` | ✓ PASS |

### Cross-Phase Regression Gate

| Behavior | Command | Result | Status |
| --- | --- | --- | --- |
| Phase 23 policy-truth proof still passes | `python3 scripts/check_phase23_policy_truth.py --slice-json .planning/phases/23-policy-truth-and-failure-semantics/23-policy-truth-slice.json --status-json /tmp/phase23-policy-truth-status.json --probe-only` | Exit code `0` | ✓ PASS |
| Phase 22 docker-first policy proof still passes | `python3 scripts/check_phase22_docker_policy.py --slice-json .planning/phases/22-docker-first-policy-and-safe-degradation/22-docker-policy-slice.json --status-json /tmp/phase22-policy-status.json --probe-only` | Exit code `0` | ✓ PASS |
| Workspace diff integrity | `git diff --check` | Exit code `0` | ✓ PASS |

### Requirements Coverage

| Requirement | Source Plans | Description | Status | Evidence |
| --- | --- | --- | --- | --- |
| `CMP-01` | `24-01`, `24-02`, `24-03` | Repo can compare env-first versus docker-first `llm` validation on the same fixed benchmark slice with matching model and backend contracts | ✓ SATISFIED | The harness produces paired artifacts from the same fixed slice with the same model, base URL, execution mode, and `validation_backend=llm` contract. |
| `CMP-02` | `24-02`, `24-03` | Comparison artifacts report pass, dominant-bucket, and timing deltas so the first-hop policy can be judged on both correctness and cost | ✓ SATISFIED | The checker and frozen proof status persist pass, dominant-bucket, and timing deltas, and the comparison note surfaces them in reviewer-facing form. |

Phase 24 orphaned requirements: none. The phase plans account for all Phase 24 requirement IDs in `.planning/REQUIREMENTS.md` (`CMP-01`, `CMP-02`).

### Human Verification Required

None for Phase 24 harness closeout. The carried Phase 23 browser-UAT debt remains visible in downstream artifacts, but it does not block Phase 24 because the comparison harness, proof contract, and runbook are all automated and verifiable from frozen artifacts plus CLI probes.

### Gaps Summary

No Phase 24 execution gaps remain. The repo now has a deterministic paired-policy harness, frozen sample artifacts, and a reviewer-readable proof pack that stays honest about the remaining milestone boundary: Phase 24 proves the comparison machinery, while Phase 25 owns the final docker-first keep/optional/reject recommendation.

---

_Verified: 2026-04-02T17:08:10Z_
_Verifier: Codex inline verification_
