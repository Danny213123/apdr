# APDR

## What This Is

APDR is a Python dependency resolution and validation tool built around a Rust core, with Python-based LLM assistance and a benchmark UI for evaluating real-world snippets. After the v2.2 measurement and agent-seam work exposed live tier3 reliability gaps, the next milestone focuses on making LLM fallback, backend escalation, and benchmark reporting trustworthy on real benchmark runs.

## Core Value

APDR must stay correct under benchmark pressure while the Rust core remains fast enough and clear enough to evolve without fighting the codebase.

## Current State

- v2.0 shipped with 6 completed phases and 17 completed plans.
- The Rust modernization milestone closed its benchmark and review gates, including BENCH-03 through direct APDR private-memory comparison and BENCH-05 through the inherited five-command Rust review loop.
- v2.1 delivered data-driven family knowledge migration, targeted recovery context, and verification backfill, but it was superseded before live-proof closeout.
- v2.2 completed phases 13-16 and left behind useful measurement contracts, replay tooling, agent-runtime seams, and sample-backed proof packaging, but it was blocked on live artifact capture and superseded on 2026-03-30 before milestone signoff.
- The latest resumed benchmark run is `runs/20260330-020943-apdr` (resumed from `runs/20260330-004502-apdr`), and its combined summary shows 396 tier3 cases with only 26 passes and 1 unsolvable skip.
- The dominant live tier3 failure buckets in that run are `module-not-found` (139), `environment-build-failed` (107), and `version-not-found` (58), which together account for most of the current failure surface.
- That March 30 baseline run logged 452 attempted LangGraph fallback invocations, all of which failed with `ValueError: 'confidence' is already being used as a state key`; Phase 17 removed that crash, so those artifacts are now frozen before-state evidence rather than current behavior.
- That same baseline's saved outputs showed env-only attempt metadata and empty `docker_image_id` / `build_image_id` values, which is why Phase 18 made targeted Docker recovery and routed-backend truth explicit deliverables instead of assumptions.
- Benchmark summary and case-report accounting still have trust gaps, including resumed-run aggregation confusion and some host-runtime skip rows marked as successes, so reporting correctness is now part of the milestone surface instead of a side concern.
- The user wants the next gains to come from real reliability on live benchmark runs, not from UI work or another broad round of deterministic patch tables.
- Phase 17 completed on 2026-03-31: the LangGraph fallback state contract no longer trips the duplicate `confidence` key path, tier3 artifacts now record terminal fallback outcome fields, and the repo has a fixed-slice proof checker for later live replay.
- Phase 18 completed on 2026-03-31: eligible `llm`-mode env failures now route through a deterministic Docker middle hop before final agent fallback, saved artifacts preserve `validation_path` plus `escalated_backend`, and benchmark Doctor/proof surfaces now describe the real backend path.
- Phase 19 completed on 2026-04-01: APDR artifacts now expose `failure_family` for environment-specific versus dependency-resolution outcomes, benchmark summaries keep host-runtime cases in the skip bucket, resumed history is separated from live rows, and the repo now carries a deterministic accounting-proof package anchored to the March 30 baseline.
- Phase 20 completed on 2026-04-01: dominant-bucket module and compatibility recovery rules now target the selected March 30 failure families directly, and the repo now ships a deterministic nine-case proof package showing a positive pass delta with lower `module-not-found`, `version-not-found`, and `environment-build-failed` counts on a like-for-like llm-mode slice.

## Current Milestone: v2.3 Tier3 Validation Recovery and Reliability

**Goal:** Improve real tier3 benchmark yield by making LLM fallback, backend escalation, and benchmark reporting trustworthy on live runs.

**Target features:**
- Fix LangGraph fallback stability so `llm` validation mode can recover after env failure instead of crashing.
- Add Docker-aware recovery and accurate backend accounting in live `llm` validation runs.
- Improve tier3 handling for the dominant `module-not-found`, `environment-build-failed`, and `version-not-found` buckets without turning the milestone into another broad rule-table expansion.
- Produce benchmark evidence that clearly shows before/after recovery deltas and trustworthy per-case status reporting.

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
- [x] Replay-slice benchmark capture and fast native macOS replay tooling exist for focused local iteration - validated in Phase 14: macOS-execution-path-optimization
- [x] Tier3 benchmark artifacts can record agent-mode and inference-policy metadata with an explicit runtime seam - validated in Phase 15: langchain-langgraph-tier3-intelligence-improvements
- [x] `--validation-backend llm` fallback no longer collapses non-pass agent outcomes into unlabeled env-only results, and saved artifacts expose `fallback_invoked`, `fallback_outcome`, and `fallback_reason` - validated in Phase 17: llm-fallback-stability-and-outcome-tracing
- [x] Eligible tier3 validation failures can escalate through Docker and record the actual backend path taken per attempt - validated in Phase 18: backend-escalation-and-path-truth
- [x] Resumed-run summaries and per-case artifacts now preserve trustworthy failure classification and live-versus-historical accounting - validated in Phase 19: failure-classification-and-run-accounting-integrity

### Active

- [ ] Publish reviewer-ready live before/after evidence and representative case artifacts for the fixed v2.3 dominant-bucket slice

### Out of Scope

- New benchmark UI or product-surface features - this milestone is about recovery reliability and reporting correctness, not interface expansion
- Replacing the Rust/Python architecture - the goal is to improve the existing system, not rewrite it
- Changing benchmark datasets or scoring rules - the benchmark needs to stay comparable while quality and performance work lands
- Full async or Tokio migration - still deferred until benchmark evidence says it is the right next bottleneck
- Dropping Windows or Docker support - compatibility remains a hard constraint
- Full LLM provider replacement - improve the current agent behavior before considering a provider swap
- Another broad deterministic recovery-table expansion as the primary strategy - the milestone should target fallback reliability and benchmarked recovery behavior instead
- Reopening v2.2 sample-proof packaging as the main objective - live recovery reliability is the active milestone target now

## Context

- The v2.3 milestone is grounded in fresh local benchmark evidence from `runs/20260330-020943-apdr` and `runs/20260330-004502-apdr`, not in a new external product domain.
- The combined live baseline is 26 passes out of 395 non-skipped tier3 cases, with the largest failure buckets at `module-not-found`, `environment-build-failed`, and `version-not-found`.
- The benchmark command line for the frozen March 30 baseline used `--validation-backend llm`, but its saved artifacts show env-only attempts; Phase 18 now adds targeted Docker middle-hop routing plus explicit `validation_path` and `escalated_backend` fields so future runs can prove the actual route taken.
- The March 30 baseline fallback path was blocked by the duplicate `confidence` state-key crash; Phase 17 removed that crash, so the next milestone work can focus on classification integrity and real recovery gains instead of route-seam breakage.
- Some tier3 misses are likely true dependency-recovery gaps, while others are framework or host-runtime issues, so the milestone needs better failure classification as well as better recovery.
- v2.2 already created useful run-contract, replay, and agent-seam infrastructure, so v2.3 should reuse that groundwork instead of reopening broad measurement or UI work.
- Benchmark case rows, runtime guidance, and proof artifacts now keep requested backend mode separate from actual routed backend path, which gives Phase 19 a trustworthy routing surface to build on.
- The user wants the next milestone to focus on live benchmark reliability and real recovery gains instead of more sample-proof packaging or wide deterministic patch growth.

## Constraints

- **Tech stack**: Rust 2021 core plus existing Python and JS helpers - keep the current architecture intact
- **Compatibility**: Windows and Docker validation flows must continue to work - benchmark users depend on both
- **Correctness**: Accuracy work cannot weaken dependency resolution fidelity or validation behavior - correctness remains primary
- **Benchmark target**: The benchmark corpus and saved-run evidence must remain comparable while live recovery and reporting fixes land
- **Evidence discipline**: Reporting changes must make run summaries more truthful, not just more polished
- **Scope discipline**: Focus on live fallback reliability, backend escalation, and benchmark trustworthiness - avoid slipping back into broad deterministic patching or UI work

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
| Start v2.2 before closing v2.1 | The next milestone should prioritize stronger agent intelligence and macOS benchmark performance instead of spending more time on the old deterministic recovery path | Pending |
| Start v2.3 before closing v2.2 live-proof signoff | The latest live run exposed urgent reliability gaps in fallback and backend routing that matter more than extending sample-proof packaging | Pending |
| Use `runs/20260330-020943-apdr` plus its resumed predecessor `runs/20260330-004502-apdr` as the v2.3 live baseline | The new milestone needs one concrete live failure surface before changing fallback and reporting behavior | Pending |
| Skip optional external research for v2.3 | This milestone is driven by fresh local benchmark evidence and the existing codebase, not a new external domain | Good |
| Prioritize fallback stability and backend truth before wider recovery intelligence changes | Agent reasoning improvements are hard to trust while the live fallback path crashes and backend reporting is misleading | Good |
| Keep `llm` routing env-first in Phase 17 and defer Docker escalation policy to Phase 18 | The immediate need was to restore fallback stability and truthful artifact output without widening the routing surface mid-phase | Good |
| Keep requested `validation_backend` semantics stable and surface actual route truth in dedicated backend-path fields | Benchmark readers and saved artifacts need to show what really happened without breaking prior configured-backend contracts | Good |
| Treat Docker as targeted-but-optional for APDR `llm` mode in runtime guidance while keeping pure Docker mode strict | `llm` mode still starts in env validation, but operators need clear warnings about the lost Docker middle hop when Docker is unavailable | Good |
| Preserve environment-specific validation truth and result provenance before chasing new recovery gains | Bucket-improvement work would be hard to trust while benchmark accounting still mixed host-runtime skips and historical resume rows into live conclusions | Good |

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
*Last updated: 2026-04-01 after completing Phase 20*
