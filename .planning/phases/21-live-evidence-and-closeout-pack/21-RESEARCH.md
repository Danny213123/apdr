# Phase 21: Live Evidence and Closeout Pack - Research

**Researched:** 2026-04-01
**Domain:** Reviewer-readable live evidence and milestone closeout for the v2.3 fixed dominant-bucket slice
**Confidence:** High

## Summary

Phase 21 should be treated as the live-evidence and closeout phase, not as another recovery-logic phase. Phase 20 already locked the comparison contract for the nine-case dominant-bucket slice and proved, in deterministic probe mode, that the shipped v2.3 changes can deliver more passes with lower `module-not-found`, `version-not-found`, and `environment-build-failed` counts. What the repo still does not have is the thing `EVD-08` actually requires: a post-Phase-20 live candidate artifact on that same fixed slice, plus a reviewer-facing package that ties before/after counts to representative real case artifacts.

The gap is concrete and local. The current candidate artifact in `.planning/phases/20-dominant-bucket-recovery-gains/20-candidate-sample.json` is explicitly synthetic, with `source_run: runs/phase20-probe-candidate`, and `scripts/run_phase20_recovery_benchmark.py` still rejects any mode except `--probe-only`. The newest saved run on disk is `runs/20260331-000811-apdr`, which predates the Phase 20 code landing on 2026-04-01, so the repo does not yet contain a live post-Phase-20 candidate run that can honestly serve as v2.3 closeout evidence.

That means Phase 21 should do three things in order. First, extend the fixed-slice runner so it can produce like-for-like live baseline and candidate artifacts instead of sample placeholders. Second, package representative case evidence that shows the real shipped path improvements, including Phase 17 fallback truth, Phase 18 backend-path truth, Phase 19 classification and provenance truth, and the Phase 20 dominant-bucket recovery gains. Third, add one machine-checkable closeout gate and one reviewer-facing milestone note so `EVD-08` only flips complete when the live evidence really exists.

## Phase Requirements

| ID | Requirement | Research Support |
|----|-------------|------------------|
| EVD-08 | Milestone evidence shows before-and-after tier3 bucket counts and representative case-level artifacts for the recovery changes shipped in v2.3 | Phase 20 already supplies the fixed slice and deterministic delta contract, but the repo still lacks a post-Phase-20 live candidate artifact and a reviewer-readable case pack that ties the bucket deltas to real artifacts. |

## Evidence That Should Drive Planning

### 1. Phase 20 locked the comparison contract, but not the live closeout evidence

The Phase 20 proof package is strong enough to define the Phase 21 boundary:

- `.planning/phases/20-dominant-bucket-recovery-gains/20-dominant-bucket-slice.json` freezes the nine locked March 30, 2026 relative paths
- `.planning/phases/20-dominant-bucket-recovery-gains/20-baseline-sample.json` preserves the baseline contract on that slice
- `.planning/phases/20-dominant-bucket-recovery-gains/20-candidate-sample.json` demonstrates the intended candidate shape, but it is still sample-backed rather than live-backed
- `.planning/phases/20-dominant-bucket-recovery-gains/20-VERIFICATION.md` explicitly says a real replay artifact for the same fixed slice remains Phase 21 scope

Phase 21 should therefore reuse the Phase 20 slice and delta contract rather than widen the evidence surface or introduce a second comparison schema.

### 2. The current Phase 20 runner cannot yet generate the live candidate artifact Phase 21 needs

`scripts/run_phase20_recovery_benchmark.py` already knows how to extract the fixed slice from an existing benchmark summary and preserve `validation_path`, `validation_backend`, and `resultOrigin` in the artifact rows. That is the right base for Phase 21. The missing part is live execution:

- the script currently errors unless `--probe-only` is supplied
- it has no path for generating a live candidate artifact from the current APDR code on the locked slice
- it therefore cannot yet replace the synthetic candidate sample with a real `runs/...` candidate artifact

This is exactly the seam Phase 21 should close instead of creating a brand-new runner.

### 3. There is no post-Phase-20 live candidate run on disk yet

The latest saved run directory under `runs/` is `20260331-000811-apdr`. That timestamp is before the 2026-04-01 Phase 20 completion, so it cannot serve as evidence for the Phase 20 dominant-bucket changes. The locked March 30 baseline remains trustworthy for the before-state, but the after-state still has to be generated.

This matters because Phase 21 is the first point where the milestone can honestly claim "before" and "after" on the same slice:

- before = March 30, 2026 baseline extracted from `runs/20260330-020943-apdr`
- after = a new live candidate replay produced from the current Phase 20 code on the exact same nine-case slice with the same `validation_backend: llm` and `model_name: qwen3.5:9b`

Anything older or synthetic is useful proof support, but not the reviewer-readable live closeout evidence required by `EVD-08`.

### 4. The repo now has enough truthful per-case metadata to build a real representative case pack

Earlier phases already added the fields that make a reviewer-facing casebook worth building:

- Phase 17: `fallback_invoked`, `fallback_outcome`, and `fallback_reason`
- Phase 18: `validation_path` and `escalated_backend`
- Phase 19: `failure_family`, `resultOrigin`, and separated historical versus live rows
- Phase 20: a fixed dominant-bucket slice and known candidate behaviors for recoverable module, version, and build families

So Phase 21 should not stop at a count table. It should package representative cases that show:

- at least one recovered dominant-bucket case from the fixed Phase 20 slice
- at least one case that exercises the Phase 18 backend-path truth surface
- at least one case that preserves the Phase 19 failure-family and provenance truth on a non-pass outcome
- enough links into real artifact directories that a reviewer can inspect the raw reports without reconstructing the benchmark by hand

### 5. Milestone closeout needs a machine-readable verdict as well as a reviewer note

Phase 16 already established the right closeout pattern for this repo:

- machine-readable status JSON for the final evidence mode
- one reviewer-facing milestone closeout note
- explicit requirement reconciliation instead of implied success

Phase 21 should follow the same pattern for v2.3. `EVD-08` should only flip complete if:

- the baseline and candidate artifacts are both live-backed or, for the baseline, extracted from the locked March 30 live source
- the candidate artifact uses the same fixed slice, model, and backend as the baseline
- the delta contract still passes on the live artifacts
- the representative case pack resolves to real artifact paths and exposes the Phase 17-19 truth fields that explain the observed gains

## Implementation Recommendations

### 1. Extend the Phase 20 runner into a live-capable fixed-slice evidence harness

Recommended files:

- `scripts/run_phase20_recovery_benchmark.py`
- `.planning/phases/21-live-evidence-and-closeout-pack/21-baseline-live.json`
- `.planning/phases/21-live-evidence-and-closeout-pack/21-candidate-live.json`
- `.planning/phases/21-live-evidence-and-closeout-pack/21-LIVE-RUNBOOK.md`

Recommended responsibilities:

- preserve the existing Phase 20 extraction path for the March 30 baseline
- add a live execution mode that reruns the locked nine-case slice against the current APDR code
- require the same `slice_id`, `validation_backend: llm`, and `model_name: qwen3.5:9b` across baseline and candidate artifacts
- record the exact source run path, command line, and artifact locations in a small runbook

### 2. Build a reviewer-facing before/after note plus a representative case pack

Recommended files:

- `.planning/phases/21-live-evidence-and-closeout-pack/21-LIVE-EVIDENCE.md`
- `.planning/phases/21-live-evidence-and-closeout-pack/21-REPRESENTATIVE-CASES.md`
- `.planning/phases/21-live-evidence-and-closeout-pack/21-case-index.json`

Recommended responsibilities:

- publish clearly labeled March 30 baseline versus v2.3 candidate bucket counts for the locked slice
- select representative cases that demonstrate recovered-delta behavior and truthful remaining-limit behavior
- include the saved artifact paths and the specific truth fields that should be inspected for each case
- keep the note concise and reviewer-oriented instead of duplicating the full raw JSON inline

### 3. Add a final Phase 21 checker and a milestone closeout note

Recommended files:

- `scripts/check_phase21_live_evidence.py`
- `.planning/phases/21-live-evidence-and-closeout-pack/21-live-evidence-status.json`
- `.planning/phases/21-live-evidence-and-closeout-pack/21-MILESTONE-CLOSEOUT.md`
- `.planning/REQUIREMENTS.md`
- `.planning/ROADMAP.md`
- `.planning/STATE.md`

Recommended responsibilities:

- validate the live baseline/candidate delta contract and the representative case index together
- fail if the live evidence drifts from the locked slice, model, or backend
- fail if the case pack stops pointing at real artifacts or drops the required Phase 17-19 truth fields
- write one explicit closeout verdict for `EVD-08` and update requirement or state truth to match

## Validation Architecture

### Quick checks

- `python3 scripts/run_phase20_recovery_benchmark.py --slice-json .planning/phases/20-dominant-bucket-recovery-gains/20-dominant-bucket-slice.json --summary-json runs/20260330-020943-apdr/summary.json --output-json /tmp/phase21-baseline-check.json --mode baseline --validation-backend llm --model-name qwen3.5:9b --base-url http://localhost:11434 --probe-only`
- `python3 scripts/check_phase20_recovery_delta.py --slice-json .planning/phases/20-dominant-bucket-recovery-gains/20-dominant-bucket-slice.json --baseline-json .planning/phases/20-dominant-bucket-recovery-gains/20-baseline-sample.json --candidate-json .planning/phases/20-dominant-bucket-recovery-gains/20-candidate-sample.json --status-json /tmp/phase21-delta-check.json --probe-only`

### Artifact checks

- `rg -n 'probe-only|execute-live|validation_backend|model_name|source_run' scripts/run_phase20_recovery_benchmark.py`
- `rg -n 'Before/After|March 30|v2.3 candidate|Representative Cases|fallback_outcome|validation_path|failure_family|resultOrigin' .planning/phases/21-live-evidence-and-closeout-pack`

### Phase-close checks

- inspect the live candidate artifact and confirm it uses the same locked slice, `validation_backend`, and `model_name` as the March 30 baseline artifact
- inspect the representative case pack and confirm each case links to real saved artifact paths for both the before-state and after-state when applicable
- inspect the closeout checker output and confirm `EVD-08` is only marked complete when both the live delta contract and the representative case contract are green

## Canonical Files For Planning

- `.planning/PROJECT.md`
- `.planning/ROADMAP.md`
- `.planning/REQUIREMENTS.md`
- `.planning/STATE.md`
- `.planning/phases/17-llm-fallback-stability-and-outcome-tracing/17-FALLBACK-PROOF.md`
- `.planning/phases/18-backend-escalation-and-path-truth/18-BACKEND-PROOF.md`
- `.planning/phases/19-failure-classification-and-run-accounting-integrity/19-ACCOUNTING-PROOF.md`
- `.planning/phases/20-dominant-bucket-recovery-gains/20-RECOVERY-DELTA.md`
- `.planning/phases/20-dominant-bucket-recovery-gains/20-VERIFICATION.md`
- `.planning/phases/20-dominant-bucket-recovery-gains/20-dominant-bucket-slice.json`
- `.planning/phases/20-dominant-bucket-recovery-gains/20-baseline-sample.json`
- `.planning/phases/20-dominant-bucket-recovery-gains/20-candidate-sample.json`
- `scripts/run_phase20_recovery_benchmark.py`
- `scripts/check_phase20_recovery_delta.py`
- `runs/20260330-020943-apdr/summary.json`
- `runs/20260331-000811-apdr/summary.json`

## Out of Scope For This Phase

- expanding resolver logic beyond the bounded v2.3 changes already shipped in Phases 17-20
- widening the comparison beyond the locked nine-case dominant-bucket slice
- redesigning benchmark UI surfaces instead of using the truthful artifacts the repo already stores
- treating synthetic candidate samples as equivalent to live milestone evidence
- claiming v2.3 closeout readiness without a machine-checkable live candidate artifact

## Source Base

No external browsing was required for Phase 21 planning. The source of truth is the repo’s own roadmap and requirement files, the completed Phase 17-20 proof and verification artifacts, the saved benchmark runs in `runs/`, and the existing Phase 20 extraction and delta-check scripts in `scripts/`.

---
*Research created: 2026-04-01*
*Phase: 21-live-evidence-and-closeout-pack*
