# Phase 16: Proof, Comparison, and Closeout - Research

**Researched:** 2026-03-29
**Domain:** Milestone closeout packaging for macOS replay gains, Windows non-regression, and Phase 15 LLM-quality evidence
**Confidence:** High

## Summary

Phase 16 does not need new exploratory product research. The repo already contains the important proof surfaces: Phase 14 owns the replay-slice manifests, the macOS and Windows proof-note templates, and the regression checker; Phase 15 owns the tier3 baseline-versus-candidate artifact contract, the agent-quality checker, and the small-model policy note. What is still missing is one bounded closeout layer that can aggregate those artifacts, keep sample-contract proof separate from live benchmark evidence, and update the milestone docs without overstating what shipped.

As of March 29, 2026, the repo still does not contain live `14-macos-before.json`, `14-macos-after.json`, `14-windows-before.json`, `14-windows-after.json`, `15-tier3-baseline.json`, or `15-tier3-candidate.json` artifacts. That means Phase 16 has to support two honest terminal states. The first is a live-evidence signoff when those artifacts exist and pass the carried-forward checkers. The second is a contract-complete but live-proof-pending closeout when only the bounded sample artifacts exist. The closeout package should make that distinction machine-readable and reviewer-readable.

The right Phase 16 shape is therefore three sequential plans. First, create a single closeout evidence contract and checker that knows which Phase 14 and Phase 15 artifacts are expected, which ones are sample or live, and which ones are still missing. Second, turn that contract into a split comparison pack for macOS performance, Windows non-regression, and LLM-quality deltas without duplicating all earlier proof notes. Third, rerun the carried-forward proof suite and write the milestone closeout note plus requirement reconciliation so `EVD-04` and `EVD-06` only flip complete when the evidence really exists.

## Phase Requirements

| ID | Requirement | Research Support |
|----|-------------|------------------|
| EVD-04 | Milestone closeout includes before-and-after macOS benchmark comparisons that make the claimed performance gain reviewer-readable on the reproducible replay slice | Phase 14 already defines the replay pair, thresholds, and proof-note contract, but Phase 16 still needs a closeout bundle that names evidence mode and links the comparison into milestone signoff. |
| EVD-06 | Milestone closeout includes an explicit Windows non-regression comparison for the benchmark-performance work performed in v2.2 | Phase 14 already defines the Windows guardrail pair and checker logic, but Phase 16 still needs to carry that result into the final closeout and requirement verdicts. |

## Evidence That Should Drive Planning

### Phase 14 already owns the performance-proof contract

`.planning/phases/14-macos-execution-path-optimization/14-MACOS-REPLAY.md` and `14-WINDOWS-GUARDRAIL.md` define the exact artifact pairs, thresholds, and comparison assumptions for macOS and Windows. `scripts/check_phase14_macos_replay.py` already validates those artifacts and the reviewer-facing notes. Phase 16 should wrap or reuse that contract instead of inventing a second comparison format.

### Phase 15 already owns the tier3 quality-proof contract

`.planning/phases/15-langchain-langgraph-tier3-intelligence-improvements/15-AGENT-QUALITY.md` and `15-QWEN-POLICY-MATRIX.md` define the baseline-versus-candidate artifact surface for small-model and agent-quality claims, while `scripts/check_phase15_agent_quality.py` already enforces that the candidate changes attributable policy fields and improves replay quality without hiding failures. Phase 16 should cite and bundle that evidence rather than re-deriving it.

### The open gap is aggregation and truth management

The current state file still lists live artifact capture as pending. That is the real planning gap. Without one machine-readable closeout status file and one final milestone note, the repo could accidentally blur "sample contract exists" with "live proof is complete." Phase 16 should make that impossible.

### Reviewer readability matters as much as checker accuracy

Earlier phases deliberately split machine-readable JSON artifacts from reviewer-facing Markdown notes. That pattern should continue. The closeout package should point to the dedicated Phase 14 and Phase 15 proof notes and add a small amount of final synthesis, not duplicate every table inline.

## Implementation Recommendations

### 1. Build one bounded closeout evidence checker first

Recommended files:

- `scripts/check_phase16_closeout.py`
- `.planning/phases/16-proof-comparison-and-closeout/16-closeout-evidence-status.json`
- `.planning/phases/16-proof-comparison-and-closeout/16-CLOSEOUT-EVIDENCE.md`

Recommended responsibilities:

- accept Phase 14 macOS and Windows artifact pairs plus the Phase 15 baseline/candidate pair
- classify each pair as `sample`, `live`, `missing`, or `mixed`
- reuse the carried-forward Phase 14 and Phase 15 checker assumptions instead of duplicating incompatible logic
- emit one machine-readable closeout status file and one reviewer-facing evidence inventory note

### 2. Keep the comparison pack split by claim type

Recommended files:

- `.planning/phases/16-proof-comparison-and-closeout/16-MACOS-COMPARISON.md`
- `.planning/phases/16-proof-comparison-and-closeout/16-WINDOWS-NONREGRESSION.md`
- `.planning/phases/16-proof-comparison-and-closeout/16-LLM-QUALITY-DELTA.md`

Recommended responsibilities:

- keep macOS performance, Windows non-regression, and LLM-quality evidence separately reviewable
- reference the Phase 14 and Phase 15 proof docs instead of restating all underlying data
- make evidence mode explicit in every note so a reviewer can tell whether the numbers come from live artifacts or bounded sample contracts

### 3. Make requirement truth depend on the actual evidence mode

Recommended files:

- `.planning/phases/16-proof-comparison-and-closeout/16-MILESTONE-CLOSEOUT.md`
- `.planning/REQUIREMENTS.md`
- `.planning/ROADMAP.md`
- `.planning/STATE.md`

Recommended responsibilities:

- rerun the Phase 14 and Phase 15 checkers before signoff
- update `EVD-04` and `EVD-06` only if the evidence mode is live and the comparisons pass
- if only sample artifacts exist, keep the requirement status honest and name the remaining live-proof blocker explicitly

## Validation Architecture

### Quick checks

- `python3 -m py_compile scripts/check_phase16_closeout.py`
- `rg -n 'sample|live|missing|mixed|phase14|phase15' .planning/phases/16-proof-comparison-and-closeout/16-CLOSEOUT-EVIDENCE.md`
- `rg -n '## macOS Performance|## Windows Guardrail|## LLM Quality|## Final Signoff' .planning/phases/16-proof-comparison-and-closeout/*.md`

### Artifact checks

- `python3 scripts/check_phase14_macos_replay.py --macos-before .planning/phases/14-macos-execution-path-optimization/14-macos-before-sample.json --macos-after .planning/phases/14-macos-execution-path-optimization/14-macos-after-sample.json --windows-before .planning/phases/14-macos-execution-path-optimization/14-windows-before-sample.json --windows-after .planning/phases/14-macos-execution-path-optimization/14-windows-after-sample.json --macos-md .planning/phases/14-macos-execution-path-optimization/14-MACOS-REPLAY.md --windows-md .planning/phases/14-macos-execution-path-optimization/14-WINDOWS-GUARDRAIL.md`
- `python3 scripts/check_phase15_agent_quality.py --baseline .planning/phases/15-langchain-langgraph-tier3-intelligence-improvements/15-tier3-baseline-sample.json --candidate .planning/phases/15-langchain-langgraph-tier3-intelligence-improvements/15-tier3-candidate-sample.json`
- `python3 scripts/check_phase16_closeout.py --phase14-macos-before .planning/phases/14-macos-execution-path-optimization/14-macos-before-sample.json --phase14-macos-after .planning/phases/14-macos-execution-path-optimization/14-macos-after-sample.json --phase14-windows-before .planning/phases/14-macos-execution-path-optimization/14-windows-before-sample.json --phase14-windows-after .planning/phases/14-macos-execution-path-optimization/14-windows-after-sample.json --phase15-baseline .planning/phases/15-langchain-langgraph-tier3-intelligence-improvements/15-tier3-baseline-sample.json --phase15-candidate .planning/phases/15-langchain-langgraph-tier3-intelligence-improvements/15-tier3-candidate-sample.json --status-json .planning/phases/16-proof-comparison-and-closeout/16-closeout-evidence-status.json`

## Canonical Files For Planning

- `.planning/PROJECT.md`
- `.planning/REQUIREMENTS.md`
- `.planning/ROADMAP.md`
- `.planning/STATE.md`
- `.planning/research/SUMMARY.md`
- `.planning/phases/14-macos-execution-path-optimization/14-MACOS-REPLAY.md`
- `.planning/phases/14-macos-execution-path-optimization/14-WINDOWS-GUARDRAIL.md`
- `.planning/phases/14-macos-execution-path-optimization/14-03-SUMMARY.md`
- `.planning/phases/15-langchain-langgraph-tier3-intelligence-improvements/15-AGENT-QUALITY.md`
- `.planning/phases/15-langchain-langgraph-tier3-intelligence-improvements/15-QWEN-POLICY-MATRIX.md`
- `.planning/phases/15-langchain-langgraph-tier3-intelligence-improvements/15-04-SUMMARY.md`
- `scripts/check_phase14_macos_replay.py`
- `scripts/check_phase15_agent_quality.py`

## Out of Scope For This Phase

- changing the Phase 14 or Phase 15 comparison logic instead of reusing their existing proof contracts
- adding new benchmark or agent features
- inventing fresh deterministic fix rules to improve LLM results at closeout time
- archiving the milestone before the closeout note and requirement truth are reconciled

## Source Base

No new external browsing was required for Phase 16 planning. The phase is a closeout and reconciliation phase, so the source of truth is the repo's existing Phase 14 and Phase 15 proof contracts plus the milestone requirements and state documents.

---
*Research created: 2026-03-29*
*Phase: 16-proof-comparison-and-closeout*
