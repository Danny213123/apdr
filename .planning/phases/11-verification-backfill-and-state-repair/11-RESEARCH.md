# Phase 11: Verification Backfill & State Repair - Research

**Researched:** 2026-03-28
**Domain:** Backfilling missing verification artifacts, repairing stale planning-state docs, and refreshing the milestone audit without reopening resolver behavior
**Confidence:** Medium

## Summary

Phase 11 should not change resolver behavior, benchmark policy, or the targeted recovery implementation. The required work is already on disk: Phase 8 has summaries, a runtime note, targeted Rust tests, and a deterministic checker, but it never produced `08-VERIFICATION.md`. Phase 7 was operationally accepted, but the repo still shows `07-VERIFICATION.md` as `human_needed` and `07-HUMAN-UAT.md` as `partial`. The milestone audit also caught stale planning-state prose in `STATE.md` and milestone-level closeout language that still claims `v2.1` is ready for completion even though the audit opened gap-closure Phases 11 and 12.

Primary recommendation: split Phase 11 into three sequential plans. First, backfill the missing Phase 8 verification artifact and the accepted manual-review outcome for Phase 7. Second, repair milestone-state documents so `PROJECT.md`, `STATE.md`, and the Phase 10 milestone closeout note match the audited repo state and the new gap-closure roadmap. Third, add a deterministic Phase 11 checker and refresh `v2.1-MILESTONE-AUDIT.md` so the family-knowledge and stale-state gaps disappear, leaving only the live benchmark proof gaps for Phase 12.

## Phase Requirements

| ID | Requirement | Research Support |
|----|-------------|------------------|
| FAM-01 | Maintainers can define touched family aliases, package mappings, and rejection hints in data files instead of hardcoded Rust tables | Phase 8 already shipped the curated data files and summaries; the missing piece is a repo-backed Phase 8 verification report tying that behavior to evidence. |
| FAM-02 | APDR loads and applies data-driven family knowledge for the touched families used in the milestone accuracy slice | `08-02-SUMMARY.md`, `08-03-SUMMARY.md`, `08-FAMILY-RUNTIME.md`, and the targeted tests already provide the evidence needed for a verification report. |
| FAM-03 | Invalid or conflicting family-knowledge data fails with actionable validation errors before it can silently change recovery behavior | `08-01-SUMMARY.md`, the loader tests, and `check_phase8_family_runtime.py` already cover this; Phase 11 needs to encode that coverage in `08-VERIFICATION.md` and the refreshed audit. |

## Evidence That Should Drive Planning

### Phase 8 is implemented and validated locally, but not verified at the repo level

The audit gap is not missing code. Phase 8 already has:

- `08-01-SUMMARY.md`, `08-02-SUMMARY.md`, and `08-03-SUMMARY.md`
- `08-FAMILY-RUNTIME.md`
- `scripts/check_phase8_family_runtime.py`
- targeted Rust regression commands:
  - `cargo test --manifest-path tools/apdr/Cargo.toml --test test_resolver data_driven_family_ -- --nocapture`
  - `cargo test --manifest-path tools/apdr/Cargo.toml --test test_resolver phase7_family_ -- --nocapture`

What is missing is `.planning/phases/08-data-driven-family-knowledge-runtime/08-VERIFICATION.md`. That single missing artifact makes `FAM-01`, `FAM-02`, and `FAM-03` orphaned in the milestone audit.

### Phase 7's human approval exists operationally, but the repo artifacts still say otherwise

Phase 7 was accepted in-session, but the repo still contains:

- `07-VERIFICATION.md` with `status: human_needed`
- `07-HUMAN-UAT.md` with `status: partial`, `passed: 0`, and `pending: 2`

Phase 11 should backfill the accepted result directly into those docs:

- the canonical-slice watchlist boundary review passed
- the touched-family selection-rationale review passed

That turns Phase 7 from "operationally accepted but repo-stale" into "repo-backed accepted".

### The planning-state debt is larger than ROADMAP alone

Gap planning already corrected the biggest roadmap mismatch by adding Phases 11 and 12 and marking Phase 9 complete. The remaining state repair is mainly in:

- `STATE.md`, which still describes the repo as if it stopped after Phase 8
- `PROJECT.md`, which still lists the family-runtime and recovery-improvement claims as fully validated even though `REQUIREMENTS.md` moved `FAM-01`, `FAM-02`, `FAM-03`, `REC-02`, `REC-03`, and `REC-04` back to pending
- `10-MILESTONE-CLOSEOUT.md`, whose `Final Signoff` still says `v2.1` is ready for completion even though the audit opened follow-up phases

Phase 11 should repair those files so the planning surface matches the audited truth:

- Phase 8 family requirements are pending repo-verification backfill until this phase closes
- recovery-improvement proof remains open and belongs to Phase 12
- the milestone is not ready to archive yet

### Phase 11 should refresh the milestone audit, not replace it

The Phase 11 success criteria do not require the audit to pass fully. They require the audit to stop failing on:

- missing verification artifacts
- stale planning-state prose

That means Phase 11 should refresh `.planning/v2.1-MILESTONE-AUDIT.md` so that:

- `FAM-01`, `FAM-02`, and `FAM-03` are no longer orphaned
- stale-state tech debt entries are removed
- the remaining blockers are the Phase 12 live benchmark proof and requirement-reconciliation gaps around `REC-02`, `REC-03`, and `REC-04`

### A deterministic checker fits the repo's existing closeout pattern

Phases 7 through 10 all closed with deterministic scripts and reviewer notes. Phase 11 should follow the same pattern with:

- `scripts/check_phase11_verification_backfill.py`
- a reviewer-facing repair note such as `11-STATE-REPAIR.md`

The checker should validate:

- `08-VERIFICATION.md` exists and covers `FAM-01`, `FAM-02`, and `FAM-03`
- `07-HUMAN-UAT.md` and `07-VERIFICATION.md` no longer show unresolved manual-review debt
- `PROJECT.md`, `STATE.md`, and `10-MILESTONE-CLOSEOUT.md` reflect the audited post-gap-planning state
- the refreshed milestone audit no longer includes the fixed Phase 7/8/state gaps

## Implementation Recommendations

### 1. Backfill verification artifacts first

Recommended files:

- `.planning/phases/08-data-driven-family-knowledge-runtime/08-VERIFICATION.md`
- `.planning/phases/07-failure-baseline-parity-slice/07-HUMAN-UAT.md`
- `.planning/phases/07-failure-baseline-parity-slice/07-VERIFICATION.md`

Recommended responsibilities:

- build `08-VERIFICATION.md` from existing Phase 8 summaries, `08-FAMILY-RUNTIME.md`, the Phase 8 checker, and the targeted Rust tests
- convert `07-HUMAN-UAT.md` from pending to passed using the accepted manual review outcome
- update `07-VERIFICATION.md` so it no longer advertises unresolved `human_needed` state

### 2. Repair milestone-state docs without reopening Phase 12 work

Recommended files:

- `.planning/PROJECT.md`
- `.planning/STATE.md`
- `.planning/phases/10-benchmark-verification-accuracy-closeout/10-MILESTONE-CLOSEOUT.md`

Recommended responsibilities:

- update `PROJECT.md` so the family-verification and recovery-improvement claims align with the reset requirement state
- update `STATE.md` so the current focus, progress, blockers, and quick-reference sections reflect the gap-closure milestone state
- update the Phase 10 milestone closeout note so it no longer says the milestone is ready to archive before Phases 11 and 12 finish

### 3. Close with a checker and refreshed audit

Recommended files:

- `scripts/check_phase11_verification_backfill.py`
- `.planning/phases/11-verification-backfill-and-state-repair/11-STATE-REPAIR.md`
- `.planning/v2.1-MILESTONE-AUDIT.md`

Recommended responsibilities:

- validate the repaired artifact set deterministically
- write one reviewer-facing Phase 11 note summarizing what was fixed and what remains for Phase 12
- refresh the milestone audit so only the Phase 12 proof gaps remain

## Validation Architecture

### Quick checks

- `rg -n 'status: passed|FAM-01|FAM-02|FAM-03' .planning/phases/08-data-driven-family-knowledge-runtime/08-VERIFICATION.md`
- `rg -n 'status: passed|passed: 2|pending: 0' .planning/phases/07-failure-baseline-parity-slice/07-HUMAN-UAT.md`
- `python -m py_compile scripts/check_phase11_verification_backfill.py`

### Artifact checks

- `python scripts/check_phase7_baseline.py --summary-json runs/20260327-150339-apdr/summary.json --pllm-csv pllm_results/csv/summary-all-runs.csv --parity-manifest .planning/phases/07-failure-baseline-parity-slice/07-tier3-parity-manifest.json --family-manifest .planning/phases/07-failure-baseline-parity-slice/07-family-snapshot-manifest.json --baseline-md .planning/phases/07-failure-baseline-parity-slice/07-BASELINE.md`
- `cargo test --manifest-path tools/apdr/Cargo.toml --test test_resolver data_driven_family_ -- --nocapture`
- `cargo test --manifest-path tools/apdr/Cargo.toml --test test_resolver phase7_family_ -- --nocapture`
- `python scripts/check_phase8_family_runtime.py --family-manifest .planning/phases/07-failure-baseline-parity-slice/07-family-snapshot-manifest.json --families-json tools/apdr/data/family_knowledge/touched_families.json --recovery-json tools/apdr/data/family_knowledge/touched_recovery_rules.json --baseline-md .planning/phases/08-data-driven-family-knowledge-runtime/08-FAMILY-RUNTIME.md`

### Phase-close checks

- `python scripts/check_phase11_verification_backfill.py --phase7-verification .planning/phases/07-failure-baseline-parity-slice/07-VERIFICATION.md --phase7-uat .planning/phases/07-failure-baseline-parity-slice/07-HUMAN-UAT.md --phase8-verification .planning/phases/08-data-driven-family-knowledge-runtime/08-VERIFICATION.md --project-md .planning/PROJECT.md --state-md .planning/STATE.md --closeout-md .planning/phases/10-benchmark-verification-accuracy-closeout/10-MILESTONE-CLOSEOUT.md --audit-md .planning/v2.1-MILESTONE-AUDIT.md --repair-md .planning/phases/11-verification-backfill-and-state-repair/11-STATE-REPAIR.md`

## Canonical Files For Planning

- `.planning/PROJECT.md`
- `.planning/ROADMAP.md`
- `.planning/REQUIREMENTS.md`
- `.planning/STATE.md`
- `.planning/v2.1-MILESTONE-AUDIT.md`
- `.planning/phases/07-failure-baseline-parity-slice/07-VERIFICATION.md`
- `.planning/phases/07-failure-baseline-parity-slice/07-HUMAN-UAT.md`
- `.planning/phases/07-failure-baseline-parity-slice/07-BASELINE.md`
- `.planning/phases/07-failure-baseline-parity-slice/07-TIER3-PARITY-MANIFEST.md`
- `.planning/phases/07-failure-baseline-parity-slice/07-FAMILY-SNAPSHOTS.md`
- `.planning/phases/08-data-driven-family-knowledge-runtime/08-01-SUMMARY.md`
- `.planning/phases/08-data-driven-family-knowledge-runtime/08-02-SUMMARY.md`
- `.planning/phases/08-data-driven-family-knowledge-runtime/08-03-SUMMARY.md`
- `.planning/phases/08-data-driven-family-knowledge-runtime/08-FAMILY-RUNTIME.md`
- `.planning/phases/08-data-driven-family-knowledge-runtime/08-VALIDATION.md`
- `.planning/phases/10-benchmark-verification-accuracy-closeout/10-MILESTONE-CLOSEOUT.md`
- `scripts/check_phase7_baseline.py`
- `scripts/check_phase8_family_runtime.py`

## Out of Scope For This Phase

- changing resolver behavior, targeted recovery rules, or curated family-runtime logic
- generating a live APDR rerun or reconciling the benchmark-proof gaps for `REC-02`, `REC-03`, and `REC-04`
- rewriting the Phase 10 case-delta artifacts or benchmark conclusions beyond correcting milestone readiness and state alignment
- modifying unrelated local edits in `benchmark_ui/service.py`, `web/src/main.js`, `tools/apdr/src/lib.rs`, or `tools/apdr/llm_py/tests/test_llm_integration.py`

---
*Research created: 2026-03-28*
*Phase: 11-verification-backfill-and-state-repair*
