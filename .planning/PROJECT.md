# APDR

## What This Is

APDR is a Python dependency resolution and validation tool built around a Rust core, with Python-based LLM assistance and a benchmark UI for evaluating real-world snippets. After the v2.0 modernization pass, the next milestone focuses on making family knowledge easier to evolve and recovering LLM-tier accuracy on real benchmark failures.

## Core Value

APDR must stay correct under benchmark pressure while the Rust core remains fast enough and clear enough to evolve without fighting the codebase.

## Current State

- v2.0 shipped with 6 completed phases and 17 completed plans.
- The Rust modernization milestone closed its benchmark and review gates, including BENCH-03 through direct APDR private-memory comparison and BENCH-05 through the inherited five-command Rust review loop.
- The latest local stopped benchmark run is `runs\20260327-150339-apdr`, which processed 1,257 cases with 285 failures and 297 skips; 228 of the failures are tier3.
- The matching `pllm` comparison data in `pllm_results\csv\summary-all-runs.csv` overlaps all 1,257 processed APDR cases and shows 87 APDR failures where `pllm` passed at least once; Phase 7 locked that surface into a canonical 70-case tier3 parity slice plus a 17-case watchlist outside the Phase 7 contract.
- Phase 7 also isolated a 17-case touched-family snapshot corpus under `tools/apdr/tests/phase7_family_fixtures` and a deterministic baseline check in `scripts/check_phase7_baseline.py` so Phase 8 can change family runtime behavior without moving the comparison boundary.
- Phase 8 moved the touched family runtime into curated repo data under `tools/apdr/data/family_knowledge`, wired the resolver to consume it, and locked the migrated behavior behind `phase7_family_` regression tests plus `scripts/check_phase8_family_runtime.py`. Phase 11 completed the repo-backed verification backfill so `FAM-01`, `FAM-02`, and `FAM-03` are now closed at the milestone level.
- Phase 9 added bounded targeted-recovery policies under `tools/apdr/data/recovery/` for module-provider, stop-reason, and compatibility clusters. The retry loop now consults these policies before generic fallbacks, and 11 `phase9_targeted_` regression tests lock the behavior, but live benchmark proof for `REC-02`, `REC-03`, and `REC-04` remains open.
- Phase 10 produced the v2.1 evidence package with machine-readable case deltas (`10-case-delta.json`), reviewer-facing benchmark verification, watchlist appendix, preservation guards, and unrecovered-gap reporting. The package is useful, but the on-disk rerun proof is still dry-run only.
- Phase 11 closed the verification/state audit debt by backfilling `08-VERIFICATION.md`, backfilling the approved Phase 7 manual review, repairing milestone-state docs, adding `check_phase11_verification_backfill.py`, and refreshing the audit so only the live-proof recovery gaps remain.
- Phase 12 is now the only remaining milestone gate. It owns live benchmark proof and requirement reconciliation for `REC-02`, `REC-03`, and `REC-04`.

## Current Milestone: v2.1 Data-Driven Family Knowledge & LLM Recovery Accuracy

**Goal:** Replace brittle hardcoded family-knowledge behavior with data-driven rules and improve APDR's LLM recovery accuracy on the stopped benchmark failures surfaced on 2026-03-27.

**Target features:**
- Move touched family-knowledge bundles, aliases, and mapping hints into validated data files instead of hardcoded Rust tables.
- Improve tier3 recovery behavior on the stopped-run failure buckets, especially `module-not-found`, `environment-build-failed`, and `version-not-found`.
- Produce benchmark artifacts that track targeted APDR recovery deltas against the stopped APDR run and the matching `pllm` parity slice in `pllm_results`.

## Requirements

### Validated

- [x] Multi-tier dependency resolution (cache -> heuristic -> LLM) is working and benchmarked
- [x] Validation backends (env + Docker fallback) exist and remain the ground truth for correctness
- [x] Benchmark UI, saved runs, and case inspection workflow exist from the previous milestone
- [x] LLM recovery instrumentation and benchmark reporting exist from the previous milestone
- [x] Rust hot paths reduce unnecessary allocation, cloning, and contention in benchmark-critical code - v2.0
- [x] Validation and caching flows reduce total benchmark runtime without weakening correctness - v2.0
- [x] The Rust codebase is reorganized into clearer modules with smaller reviewable units - v2.0
- [x] Rust docs and comments explain non-obvious behavior, invariants, and fallbacks - v2.0
- [x] Touched Rust modules meet stronger review gates for style, error handling, and maintainability - v2.0
- [x] Benchmark evidence makes APDR recovery deltas inspectable at the case level - validated in Phase 7: failure-baseline-parity-slice
- [x] Benchmark reruns package targeted APDR versus baseline versus `pllm` deltas without reopening the Phase 8 migration boundary - validated in Phase 10: benchmark-verification-accuracy-closeout
- [x] Data-driven family knowledge now has repo-backed milestone verification coverage for the touched runtime boundary - validated in Phase 11: verification-backfill-and-state-repair

### Active

- [ ] Close the live benchmark proof gap and reconcile `REC-02`, `REC-03`, and `REC-04` against measured rerun evidence - Phase 12

### Out of Scope

- New benchmark UI or product-surface features - this milestone is about resolution accuracy and maintainability, not interface expansion
- Replacing the Rust/Python architecture - the goal is to improve the existing system, not rewrite it
- Changing benchmark datasets or scoring rules - the stopped benchmark needs to stay comparable while accuracy work lands
- Full async or Tokio migration - still deferred until benchmark evidence says it is the right next bottleneck
- Dropping Windows or Docker support - compatibility remains a hard constraint
- Full LLM provider replacement - this milestone should improve the current recovery path before considering a provider swap

## Context

- The primary milestone targets are the family-knowledge path and tier3 recovery behavior in the existing APDR stack.
- The stopped local benchmark run at `runs\20260327-150339-apdr` used `qwen3.5:9b` with RAG enabled and ended in `stopped` state after processing 1,257 cases.
- In that run, the visible dominant failure buckets were `module-not-found` (86), `environment-build-failed` (62), and `version-not-found` (33), with 228 of 285 failures landing in tier3.
- The `pllm` comparison data lives in `pllm_results\csv\summary-all-runs.csv` rather than under `runs\`, and it exposes 87 APDR-failed cases where `pllm` passed at least once.
- Among those APDR-failed and `pllm`-passing cases, the largest visible APDR validation statuses are `environment-build-failed` (21), `module-not-found` (19), missing explicit failure bucket tagging (18), `dependency-conflict` (12), and `version-not-found` (11).
- v2.0 already improved performance, module boundaries, and review quality, so this milestone should concentrate on correctness and recovery behavior rather than reopening broad modernization work.

## Constraints

- **Tech stack**: Rust 2021 core plus existing Python and JS helpers - keep the current architecture intact
- **Compatibility**: Windows and Docker validation flows must continue to work - benchmark users depend on both
- **Correctness**: Accuracy work cannot weaken dependency resolution fidelity or validation behavior - correctness remains primary
- **Benchmark target**: The stopped benchmark run and hard-gists corpus must remain comparable while accuracy work lands
- **Scope discipline**: Focus on family knowledge and recovery accuracy - avoid expanding back into broad performance or UI work

## Key Decisions

| Decision | Rationale | Outcome |
|----------|-----------|---------|
| Start a fresh v2.0 roadmap and ignore remaining v1.0 roadmap items | The previous milestone was a Rust modernization effort, not a continuation of prior feature work | Good |
| Reset phase numbering to Phase 1 for v2.0 | The previous milestone needed clean sequencing and avoided mixing old phase intent with the modernization roadmap | Good |
| Skip optional external domain research for v2.0 | The local codebase map and benchmark evidence were the relevant inputs for the brownfield modernization work | Good |
| Measure before optimizing | Benchmark speed and memory work needed baselines and regression checks, not intuition | Good |
| Keep the Phase 5 five-command Rust review loop as the Phase 6 closeout contract | Final signoff needed to reuse the same reviewer gate instead of inventing a new one | Good |
| Close BENCH-03 with direct APDR private-memory comparison instead of wrapper-level RSS | The targeted Rust workflow needed a direct process-level signal when wrapper RSS stayed noisy on Windows | Good |
| Use `runs\20260327-150339-apdr` as the v2.1 accuracy baseline | The new milestone needs one concrete stopped-run reference before changing family knowledge and LLM recovery behavior | Good |
| Skip external research for v2.1 by default | This milestone is driven by a local benchmark failure surface and the existing codebase, not by a new external product domain | Pending |
| Lock the first v2.1 migration boundary to the Phase 7 canonical slice and touched-family fixtures | Phase 8 needs a stable baseline so data-driven family work can be measured instead of guessed | Good |
| Close Phase 8 with curated touched-family data, bounded Phase 7 fixture regressions, and `check_phase8_family_runtime.py` | Phase 9 recovery work needs a locked runtime boundary and deterministic checker before accuracy changes begin | Good |
| Open Phase 11 and Phase 12 after the v2.1 milestone audit | The repo had real shipped work, but the audit showed missing verification/state artifacts and no live proof for the recovery-improvement claims | Good |

## Shipped Milestone Snapshot

<details>
<summary>v2.0 Rust Codebase Modernization</summary>

**Goal:** Refactor the Rust codebase for better benchmark performance, memory efficiency, maintainability, and review quality.

**Target features:**
- Faster benchmark execution through hot-path and validation-pipeline optimization
- Lower memory churn through better ownership, fewer unnecessary clones, and smarter shared-state handling
- Clearer Rust module boundaries, stronger docs, and cleaner review surfaces

</details>

## Evolution

This document evolves at phase transitions and milestone boundaries.

**After each phase transition** (via `$gsd-transition`):
1. Requirements invalidated? -> Move to Out of Scope with reason
2. Requirements validated? -> Move to Validated with phase reference
3. New requirements emerged? -> Add to Active
4. Decisions to log? -> Add to Key Decisions
5. "What This Is" still accurate? -> Update if drifted

**After each milestone** (via `$gsd-complete-milestone`):
1. Full review of all sections
2. Core Value check - still the right priority?
3. Audit Out of Scope - reasons still valid?
4. Update Context with current state

---
*Last updated: 2026-03-28 after Phase 11 completion*
