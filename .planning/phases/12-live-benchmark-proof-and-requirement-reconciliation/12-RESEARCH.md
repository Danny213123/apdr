# Phase 12: Live Benchmark Proof & Requirement Reconciliation - Research

**Researched:** 2026-03-28
**Domain:** Converting the dry-run-only Phase 10 evidence flow into either live benchmark proof or an explicit blocker-backed requirement reconciliation
**Confidence:** Medium

## Summary

Phase 12 is the final `v2.1` milestone gate. The repo already has the locked Phase 7 comparison boundary, the Phase 8 family-runtime verification, the Phase 9 targeted-recovery policy surface, the Phase 10 split evidence package, and the Phase 11 state repair. What it does not have is trustworthy live proof for `REC-02`, `REC-03`, and `REC-04`. The current Phase 10 rerun wrapper can generate useful dry-run comparison artifacts, but it silently falls back to dry-run when no APDR command is provided and, even in live mode, it currently synthesizes rerun results from subprocess return codes instead of reading the emitted APDR output metadata and requirements.

Primary recommendation: split Phase 12 into three sequential plans. First, harden the rerun tooling so live proof is explicit, preflighted, and grounded in real APDR output metadata rather than a return-code approximation. Second, execute one of the two allowed terminal paths from the refreshed audit: either regenerate the Phase 10 artifacts from a live rerun, or record a hard blocker and reconcile the benchmark and requirement narrative to that measured outcome. Third, add a deterministic Phase 12 checker and refresh the milestone audit and state docs so the proof flow no longer remains ambiguous or broken.

## Phase Requirements

| ID | Requirement | Research Support |
|----|-------------|------------------|
| REC-02 | APDR reduces `module-not-found` outcomes on the targeted parity slice compared with the 2026-03-27 baseline | Phase 12 must either regenerate the canonical-slice evidence from a live rerun or explicitly narrow this claim to the measured outcome if live proof cannot run. |
| REC-03 | APDR reduces `version-not-found` and dependency-mapping failures on the targeted parity slice compared with the 2026-03-27 baseline | The current dry-run artifact cannot prove this; Phase 12 needs measured rerun buckets or a blocker-backed reconciliation note. |
| REC-04 | APDR improves the number of APDR-failed but `pllm`-passing cases it can recover on the targeted slice | The Phase 10 case-delta package already defines the comparison surface; Phase 12 must either regenerate it live or truthfully restate the observed non-improvement. |

## Evidence That Should Drive Planning

### Phase 12 starts from a dry-run-only proof surface

The repaired milestone state is explicit:

- `PROJECT.md` says Phase 12 is the only remaining gate because the rerun proof is still dry-run only
- `STATE.md` says the final work is to prove the recovery claims on a live targeted rerun or reconcile the docs to the measured result
- `v2.1-MILESTONE-AUDIT.md` says the proof flow is still broken and gives exactly two valid next paths

That means Phase 12 is not a new recovery-design phase. It is a proof-and-truthfulness phase.

### The current rerun wrapper has three proof defects

`scripts/run_phase10_targeted_benchmark.py` is useful, but not yet strong enough for milestone proof:

- if `--dry-run` is omitted and `--apdr-command` is missing, it silently flips to dry-run
- live execution only records subprocess return codes and log tails, not the emitted `output_data_*.yml` metadata or refreshed `requirements.txt`
- preservation guards are still derived from baseline data only, so the current live path does not yet re-measure the guard outcomes

Phase 12 wave 1 should fix those defects before any new benchmark claim is made.

### The repo already has the right metadata-reading behavior to reuse

`benchmark_ui/runner.py` already knows how to:

- detect newly written `output_data_*.yml` files
- read output metadata
- read updated `requirements.txt`
- apply the host-runtime-with-valid-requirements rule consistently

Phase 12 should mirror or reuse that behavior in the rerun wrapper instead of inventing a second interpretation of APDR outputs.

### The benchmark boundary stays locked

Nothing in Phase 12 should change:

- the canonical `70`-case Phase 7 slice
- the separate `17`-case watchlist
- the Phase 8 touched-family migration boundary
- the bounded Phase 9 recovery-policy surface

If the live proof path fails, Phase 12 must reconcile the milestone promise to the measured dry-run state. It must not change the benchmark contract to make the problem disappear.

### The blocker path is a first-class outcome, not an error case

The refreshed roadmap, audit, and Phase 11 handoff all allow two terminal states:

1. a live targeted rerun regenerates the Phase 10 artifacts with measured outputs, or
2. the repo records a hard blocker and narrows the requirements and closeout narrative to the measured outcome

That means Phase 12 needs explicit blocker artifacts, not just a failed command pasted into a summary.

### Requirement reconciliation must be evidence-grounded

If the live rerun cannot happen, the repo still needs to resolve the audit. The only honest way to do that is:

- state exactly why live proof could not run
- preserve the measured dry-run evidence already on disk
- update `REQUIREMENTS.md`, the Phase 10 benchmark notes, the milestone closeout note, and the audit so they no longer claim an unproven accuracy win

This is more than wording cleanup. It is a formal narrowing of the milestone promise to the measured result.

### Phase 12 needs one checker that accepts both valid end states

Phases 7 through 11 each closed with deterministic verification. Phase 12 should do the same with one checker that fails:

- silent dry-run fallback
- missing live-proof readiness status
- a live rerun that still leaves Phase 10 docs in dry-run language
- a blocker path that leaves `REC-02`, `REC-03`, or `REC-04` overclaimed
- an audit that still says the proof flow is broken

## Implementation Recommendations

### 1. Harden the rerun tooling before touching milestone claims

Recommended files:

- `scripts/run_phase10_targeted_benchmark.py`
- `.planning/phases/12-live-benchmark-proof-and-requirement-reconciliation/12-live-proof-status.json`
- `.planning/phases/12-live-benchmark-proof-and-requirement-reconciliation/12-LIVE-PROOF.md`

Recommended responsibilities:

- add explicit live-proof semantics such as `--require-live`, `--probe-only`, and `--status-json`
- remove the silent auto-dry-run fallback
- read real APDR output metadata and updated requirements from rerun artifacts
- write a machine-readable readiness or blocker status artifact plus a reviewer-facing proof note

### 2. Regenerate or reconcile the benchmark evidence package

Recommended files:

- `.planning/phases/10-benchmark-verification-accuracy-closeout/10-targeted-rerun.json`
- `.planning/phases/10-benchmark-verification-accuracy-closeout/10-case-delta.json`
- `.planning/phases/10-benchmark-verification-accuracy-closeout/10-TARGETED-RERUN.md`
- `.planning/phases/10-benchmark-verification-accuracy-closeout/10-BENCHMARK-VERIFICATION.md`
- `.planning/phases/10-benchmark-verification-accuracy-closeout/10-WATCHLIST-APPENDIX.md`
- `.planning/phases/10-benchmark-verification-accuracy-closeout/10-PRESERVATION-GUARDS.md`
- `.planning/phases/10-benchmark-verification-accuracy-closeout/10-UNRECOVERED-GAPS.md`
- `.planning/phases/10-benchmark-verification-accuracy-closeout/10-MILESTONE-CLOSEOUT.md`
- `.planning/phases/12-live-benchmark-proof-and-requirement-reconciliation/12-REQUIREMENT-RECONCILIATION.md`

Recommended responsibilities:

- if live-ready, rerun the locked case set and regenerate the Phase 10 evidence from measured outputs
- if blocked, preserve the dry-run artifacts but explicitly reconcile the benchmark and milestone notes to that measured limitation
- keep the `70`-case canonical contract and `17`-case watchlist split explicit in both branches

### 3. Close with a checker and refreshed milestone truth

Recommended files:

- `scripts/check_phase12_live_proof.py`
- `.planning/REQUIREMENTS.md`
- `.planning/PROJECT.md`
- `.planning/STATE.md`
- `.planning/v2.1-MILESTONE-AUDIT.md`

Recommended responsibilities:

- verify the repo has a valid terminal state: live proof or blocker reconciliation
- refresh requirement status or wording so `REC-02`, `REC-03`, and `REC-04` match the evidence
- update project and state docs so the milestone no longer presents an unresolved proof gap

## Validation Architecture

### Quick checks

- `python -m py_compile scripts/run_phase10_targeted_benchmark.py scripts/check_phase12_live_proof.py`
- `python scripts/run_phase10_targeted_benchmark.py --manifest-json .planning/phases/10-benchmark-verification-accuracy-closeout/10-targeted-rerun-manifest.json --baseline-summary runs/20260327-150339-apdr/summary.json --pllm-csv pllm_results/csv/summary-all-runs.csv --output-json .planning/phases/10-benchmark-verification-accuracy-closeout/10-targeted-rerun.json --case-delta-json .planning/phases/10-benchmark-verification-accuracy-closeout/10-case-delta.json --output-md .planning/phases/10-benchmark-verification-accuracy-closeout/10-TARGETED-RERUN.md --context-log .planning/phases/10-benchmark-verification-accuracy-closeout/10-targeted-benchmark-context.log --status-json .planning/phases/12-live-benchmark-proof-and-requirement-reconciliation/12-live-proof-status.json --probe-only --require-live --apdr-command <apdr-executable>`

### Artifact checks

- `rg -n 'requested_mode|actual_mode|live_ready|blocker_reason|canonical_case_count|watchlist_case_count' .planning/phases/12-live-benchmark-proof-and-requirement-reconciliation/12-live-proof-status.json`
- `rg -n '## Live Readiness|## Terminal State|## Command Contract|## Blocking Conditions' .planning/phases/12-live-benchmark-proof-and-requirement-reconciliation/12-LIVE-PROOF.md`
- `rg -n 'REC-02|REC-03|REC-04|## Evidence Verdicts|## Requirement Updates|## Remaining Debt' .planning/phases/12-live-benchmark-proof-and-requirement-reconciliation/12-REQUIREMENT-RECONCILIATION.md`

### Phase-close checks

- live-ready path: `python scripts/run_phase10_targeted_benchmark.py --manifest-json .planning/phases/10-benchmark-verification-accuracy-closeout/10-targeted-rerun-manifest.json --baseline-summary runs/20260327-150339-apdr/summary.json --pllm-csv pllm_results/csv/summary-all-runs.csv --output-json .planning/phases/10-benchmark-verification-accuracy-closeout/10-targeted-rerun.json --case-delta-json .planning/phases/10-benchmark-verification-accuracy-closeout/10-case-delta.json --output-md .planning/phases/10-benchmark-verification-accuracy-closeout/10-TARGETED-RERUN.md --context-log .planning/phases/10-benchmark-verification-accuracy-closeout/10-targeted-benchmark-context.log --status-json .planning/phases/12-live-benchmark-proof-and-requirement-reconciliation/12-live-proof-status.json --require-live --apdr-command <apdr-executable>`
- blocker path: rerun the probe-only command above, then verify the reconciled Phase 10 and Phase 12 docs against the blocker note
- `python scripts/check_phase10_benchmark_closeout.py --parity-manifest .planning/phases/07-failure-baseline-parity-slice/07-tier3-parity-manifest.json --baseline-summary runs/20260327-150339-apdr/summary.json --rerun-json .planning/phases/10-benchmark-verification-accuracy-closeout/10-targeted-rerun.json --case-delta-json .planning/phases/10-benchmark-verification-accuracy-closeout/10-case-delta.json --benchmark-md .planning/phases/10-benchmark-verification-accuracy-closeout/10-BENCHMARK-VERIFICATION.md --watchlist-md .planning/phases/10-benchmark-verification-accuracy-closeout/10-WATCHLIST-APPENDIX.md --guards-md .planning/phases/10-benchmark-verification-accuracy-closeout/10-PRESERVATION-GUARDS.md --gaps-md .planning/phases/10-benchmark-verification-accuracy-closeout/10-UNRECOVERED-GAPS.md`
- `python scripts/check_phase12_live_proof.py --status-json .planning/phases/12-live-benchmark-proof-and-requirement-reconciliation/12-live-proof-status.json --benchmark-md .planning/phases/10-benchmark-verification-accuracy-closeout/10-BENCHMARK-VERIFICATION.md --closeout-md .planning/phases/10-benchmark-verification-accuracy-closeout/10-MILESTONE-CLOSEOUT.md --proof-md .planning/phases/12-live-benchmark-proof-and-requirement-reconciliation/12-LIVE-PROOF.md --reconciliation-md .planning/phases/12-live-benchmark-proof-and-requirement-reconciliation/12-REQUIREMENT-RECONCILIATION.md --requirements-md .planning/REQUIREMENTS.md --audit-md .planning/v2.1-MILESTONE-AUDIT.md`

## Canonical Files For Planning

- `.planning/PROJECT.md`
- `.planning/REQUIREMENTS.md`
- `.planning/ROADMAP.md`
- `.planning/STATE.md`
- `.planning/v2.1-MILESTONE-AUDIT.md`
- `.planning/phases/10-benchmark-verification-accuracy-closeout/10-BENCHMARK-VERIFICATION.md`
- `.planning/phases/10-benchmark-verification-accuracy-closeout/10-MILESTONE-CLOSEOUT.md`
- `.planning/phases/10-benchmark-verification-accuracy-closeout/10-TARGETED-RERUN.md`
- `.planning/phases/10-benchmark-verification-accuracy-closeout/10-targeted-rerun.json`
- `.planning/phases/10-benchmark-verification-accuracy-closeout/10-case-delta.json`
- `.planning/phases/10-benchmark-verification-accuracy-closeout/10-targeted-rerun-manifest.json`
- `.planning/phases/11-verification-backfill-and-state-repair/11-STATE-REPAIR.md`
- `scripts/run_phase10_targeted_benchmark.py`
- `scripts/check_phase10_benchmark_closeout.py`
- `benchmark_ui/runner.py`
- `benchmark_ui/service.py`
- `runs/20260327-150339-apdr/summary.json`
- `runs/20260327-150339-apdr/benchmark-context.log`
- `pllm_results/csv/summary-all-runs.csv`

## Out of Scope For This Phase

- changing resolver behavior, recovery policies, or the touched-family runtime
- changing the canonical `70`-case slice, the `17`-case watchlist, or the preservation guard membership
- reopening the Phase 7 or Phase 8 boundaries to make the proof problem easier
- adding new benchmark UI features
- modifying unrelated local edits in `benchmark_ui/service.py`, `web/src/main.js`, `tools/apdr/src/lib.rs`, or `tools/apdr/llm_py/tests/test_llm_integration.py`

---
*Research created: 2026-03-28*
*Phase: 12-live-benchmark-proof-and-requirement-reconciliation*
