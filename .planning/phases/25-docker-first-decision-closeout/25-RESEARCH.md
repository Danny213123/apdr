# Phase 25: Docker-First Decision Closeout - Research

**Researched:** 2026-04-02
**Domain:** Turning the repaired docker-first policy, the fixed-slice comparison evidence, and the remaining Phase 23 browser-UAT debt into a reviewer-readable milestone verdict that stays honest about evidence boundaries
**Confidence:** High

## Summary

Phase 25 should not reopen routing behavior or measurement infrastructure. Phase 22 already locked the docker-first `llm` routing contract and its safe degradation semantics, Phase 23 already made requested policy and actual path inspectable, and Phase 24 already proved the repo can compare env-first versus docker-first on the same fixed slice. The remaining gap is a closeout-quality answer to the milestone question: should docker-first replace env-first, remain optional, or be rejected for `llm` mode?

The strongest available evidence is the Phase 24 fixed-slice comparison contract. Its machine-checked proof shows docker-first improving the locked slice from `1` pass to `3` passes, reducing `module-not-found` and `environment-build-failed`, and reducing total duration by `172.0` seconds while still paying a positive `docker_startup_duration_seconds` cost of `61.0`. That is enough to support a real recommendation, but not enough to support an unqualified "replace env-first everywhere" claim, because the evidence is still fixed-slice scoped and Phase 23 browser-UAT remains pending.

That means Phase 25 should be planned as a decision and evidence-synthesis phase, not as another benchmark phase. The phase needs one canonical decision-input artifact that freezes what is actually known, one deterministic checker that validates the verdict against those inputs, and one closeout proof pack that makes the final recommendation auditable and honest about what remains unverified. The planning default should therefore bias toward `optional` unless execution adds stronger live paired replay evidence and/or clears the Phase 23 human-UAT debt.

The clean shape is three waves. First, freeze the decision inputs and tradeoff matrix from Phase 22-24 plus the open Phase 23 UAT debt. Second, create the final verdict document and a deterministic closeout checker that can reject unsupported verdicts. Third, create the reviewer-facing closeout proof and milestone-readiness pack so Phase 25 execution can roll directly into milestone archival without drifting into marketing language.

## Phase Requirements

| ID | Requirement | Research Support |
|----|-------------|------------------|
| EVD-10 | Milestone closes with a reviewer-readable recommendation on whether docker-first should replace env-first, remain optional, or be rejected for `llm` mode | Phase 24 already supplies machine-checked correctness and timing deltas, and Phase 23 already identifies the remaining browser-UAT debt that the final recommendation must not hide. |

## Evidence That Should Drive Planning

### Phase 24 already supplies machine-checked decision inputs

`24-comparison-proof-status.json` already records the current comparison contract: `pass_delta=2`, `failure_delta=-2`, `module-not-found=-1`, `environment-build-failed=-1`, `version-not-found=0`, and `docker_startup_duration_seconds=61.0`. Phase 25 should freeze those facts into a dedicated decision-input artifact rather than repeatedly parsing the earlier proof pack by hand.

### The best current recommendation is evidence-positive but scope-limited

The current fixed-slice evidence favors docker-first on both correctness and total runtime for the locked slice, but it is still fixed-slice evidence. That means the research-backed default recommendation is not "replace" or "reject"; it is "remain optional unless stronger live paired replay and browser-UAT evidence arrive during execution." The plan should preserve that branch explicitly instead of assuming that a positive sample delta alone is enough for a default change.

### Phase 23 human verification debt is still part of the milestone truth

`23-HUMAN-UAT.md` still shows two pending browser checks for the `Validation truth` surfaces. Phase 25 should not pretend that debt vanished. The final verdict must either incorporate a completed UAT result or carry the debt explicitly as a reason the recommendation remains bounded.

### Phase 22 still defines the safety floor

Any final recommendation must remain consistent with the docker-first contract from Phase 22: explicit env-first control, safe degradation for missing or unusable Docker, exact bypass reasons, and Docker or bypass artifacts in each `llm` case. Phase 25 should cite those guarantees when explaining why docker-first is safe enough to evaluate or keep enabled.

### The milestone closeout needs a machine-checkable gate

Previous milestone phases used deterministic proof contracts to prevent closeout drift. Phase 25 should do the same for the recommendation itself: one checker should read a decision-input artifact plus the verdict document and fail if the verdict does not cite the relevant evidence, hides the fixed-slice boundary, or recommends `replace` while Phase 23 UAT is still pending and no stronger evidence has been recorded.

## Implementation Recommendations

### 1. Freeze one canonical Phase 25 decision-input artifact

Recommended files:

- `.planning/phases/25-docker-first-decision-closeout/25-DECISION-INPUTS.json`
- `.planning/phases/25-docker-first-decision-closeout/25-EVIDENCE-MATRIX.md`

Recommended responsibilities:

- normalize the Phase 22 policy guarantees, Phase 23 pending UAT state, and Phase 24 comparison deltas into one machine-readable JSON artifact
- record the current evidence boundary explicitly: fixed slice only, no full-corpus claim, pending browser-UAT count, and whether live paired replay evidence exists
- create a reviewer-facing matrix that maps each possible verdict (`replace`, `optional`, `reject`) to supporting evidence, risks, and blockers

### 2. Add a deterministic verdict checker and the milestone verdict artifact

Recommended files:

- `scripts/check_phase25_decision_closeout.py`
- `.planning/phases/25-docker-first-decision-closeout/25-MILESTONE-VERDICT.md`
- `.planning/phases/25-docker-first-decision-closeout/25-decision-proof-status.json`

Recommended responsibilities:

- require an explicit verdict of `replace`, `optional`, or `reject`
- require the verdict document to cite pass delta, bucket deltas, runtime tradeoffs, and the current evidence boundary
- reject unsupported verdicts; for example, if Phase 23 UAT is still pending and no stronger live paired replay evidence is recorded, the checker should reject an unqualified `replace`
- write a status JSON that later verification can reuse as the deterministic Phase 25 proof gate

### 3. Build the reviewer-facing closeout proof and milestone-readiness pack

Recommended files:

- `.planning/phases/25-docker-first-decision-closeout/25-CLOSEOUT-PROOF.md`
- `.planning/phases/25-docker-first-decision-closeout/25-MILESTONE-READY.md`

Recommended responsibilities:

- explain exactly what the final verdict proves and what it does not prove
- keep the fixed-slice scope and pending Phase 23 UAT debt visible if still unresolved
- provide the final handoff checklist for milestone archival so execution can move cleanly into `$gsd-complete-milestone`

## Validation Architecture

### Quick checks

- `python3 scripts/check_phase25_decision_closeout.py --inputs-json .planning/phases/25-docker-first-decision-closeout/25-DECISION-INPUTS.json --verdict-md .planning/phases/25-docker-first-decision-closeout/25-MILESTONE-VERDICT.md --proof-md .planning/phases/25-docker-first-decision-closeout/25-CLOSEOUT-PROOF.md --status-json /tmp/phase25-status.json --probe-only`
- `rg -n 'pass_delta|docker_startup_duration_seconds|fixed_slice_only|phase23_human_uat|verdict:' .planning/phases/25-docker-first-decision-closeout/25-DECISION-INPUTS.json .planning/phases/25-docker-first-decision-closeout/25-EVIDENCE-MATRIX.md .planning/phases/25-docker-first-decision-closeout/25-MILESTONE-VERDICT.md`

### Artifact checks

- `rg -n 'replace|optional|reject|fixed-slice|Phase 23|docker_startup_duration_seconds|pass delta' scripts/check_phase25_decision_closeout.py .planning/phases/25-docker-first-decision-closeout/25-MILESTONE-VERDICT.md .planning/phases/25-docker-first-decision-closeout/25-CLOSEOUT-PROOF.md`

### Phase-close checks

- confirm the decision-input artifact agrees with the frozen Phase 24 comparison proof and the open Phase 23 UAT file
- confirm the verdict checker can fail an unsupported verdict and pass the chosen verdict
- confirm the closeout proof and milestone-ready note make the next archival step unambiguous without overstating fixed-slice evidence

## Canonical Files For Planning

- `.planning/PROJECT.md`
- `.planning/ROADMAP.md`
- `.planning/REQUIREMENTS.md`
- `.planning/STATE.md`
- `.planning/phases/22-docker-first-policy-and-safe-degradation/22-DOCKER-POLICY-PROOF.md`
- `.planning/phases/22-docker-first-policy-and-safe-degradation/22-VERIFICATION.md`
- `.planning/phases/23-policy-truth-and-failure-semantics/23-VERIFICATION.md`
- `.planning/phases/23-policy-truth-and-failure-semantics/23-HUMAN-UAT.md`
- `.planning/phases/24-env-first-vs-docker-first-comparison-harness/24-COMPARISON-DELTA.md`
- `.planning/phases/24-env-first-vs-docker-first-comparison-harness/24-COMPARISON-PROOF.md`
- `.planning/phases/24-env-first-vs-docker-first-comparison-harness/24-COMPARISON-RUNBOOK.md`
- `.planning/phases/24-env-first-vs-docker-first-comparison-harness/24-comparison-proof-status.json`
- `.planning/phases/24-env-first-vs-docker-first-comparison-harness/24-VERIFICATION.md`

## Out of Scope For This Phase

- changing env-first or docker-first routing behavior again
- widening the fixed-slice comparison into a full-corpus claim by default
- redesigning benchmark UI surfaces
- replacing the current proof contracts with informal prose-only closeout
- archiving the milestone without an explicit recommendation artifact

## Source Base

No external browsing was required for Phase 25 planning. The source of truth is the repo's own Phase 22 policy contract, Phase 23 UAT debt, Phase 24 comparison proof, and the active v2.4 milestone documents already present in the workspace.

---
*Research created: 2026-04-02*
*Phase: 25-docker-first-decision-closeout*
