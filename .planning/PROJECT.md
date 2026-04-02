# APDR

## What This Is

APDR is a Python dependency resolution and validation tool built around a Rust core, with Python-based LLM assistance and a benchmark UI for evaluating real-world snippets. The latest shipped milestone, v2.3, hardened live tier3 recovery by fixing the `llm` fallback seam, making backend routing and accounting truthful, and publishing reviewer-readable live evidence on a fixed benchmark slice. The current milestone, v2.5, shifts from the docker-first policy question to a stronger goal: make `llm` and `llm-only` own the full case lifecycle from snippet analysis through Docker validation and recovery.

## Core Value

APDR must stay correct under benchmark pressure while the Rust core remains fast enough and clear enough to evolve without fighting the codebase.

## Current State

- v2.3 shipped on 2026-04-01 after 5 phases, 15 plans, and 30 tasks.
- v2.4 was superseded unfinished on 2026-04-02 after Phases 21.1, 22, 24, and 25 completed; its policy-proof artifacts remain useful context, but the milestone was overtaken before archival because the user pivoted to end-to-end LLM execution quality.
- The latest docker-backed `llm-only` runs on 2026-04-02 exposed real regressions: empty LLM package plans, low pass counts, and Docker attempts that sometimes fail after a successful build because `docker create` cannot see the freshly built `apdr-validate:*` image tag.
- In `runs/20260402-184821-apdr`, case `hard-gists/005bbad123ef309a5bef/snippet.py` shows both active failure seams at once: the LLM returned no package mapping, and the Docker path then failed after a successful build with `Unable to find image 'apdr-validate:...' locally`.
- The user wants the next milestone to make the LLM responsible for the whole APDR path for `llm` and `llm-only`: extract modules, infer packages and system deps, author Docker validation inputs, drive recovery, and leave enough artifacts to explain every decision.
- The fixed live slice used for closeout moved from `0/9` baseline passes to `2/9` candidate passes, with `module-not-found`, `version-not-found`, and `environment-build-failed` each reduced by 3 on that slice.
- `llm` validation now preserves truthful `fallback_*`, `validation_path`, `escalated_backend`, `failure_family`, and `resultOrigin` surfaces through APDR artifacts and benchmark readers.
- The final live candidate evidence comes from `runs/20260401-173232-apdr`; one tail case, `hard-gists/1239373/snippet.py`, remains explicitly interrupted and visible in the evidence pack rather than being hidden.
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
- The remaining open question is no longer whether docker-first should be optional; it is whether APDR can make LLM-led start-to-finish validation materially more reliable than the current hybrid path on real benchmark runs.
- Phase 21.1 completed on 2026-04-01: the repo no longer tracks APDR `target-*` build trees, APDR now prefers external cache/build defaults, the repo ships a supported cleanup helper, and the proof package records `source_delta_bytes=-5551066900`, `cache_delta_bytes=-15041748090`, and `target_delta_bytes=-20451258961`.
- The post-cleanup local footprint now shows `tools` at about `2.9G`, `tools/apdr/.apdr-cache` at about `1.3G`, and no remaining repo-local `tools/apdr/target` directory.
- Phase 22 completed on 2026-04-02: APDR now ships docker-first `llm` as the standard policy with env-first control, real Docker usability gating, exact `docker cli unavailable` versus `docker daemon unavailable` bypass reasons, and a fixed five-case proof contract.
- Phase 24 completed on 2026-04-02: the repo now ships a paired-policy comparison harness, frozen env-first and docker-first sample artifacts, a deterministic delta checker, and a runbook/proof pack for matched-slice replay.
- The frozen Phase 24 comparison contract currently reports `pass_delta=2`, `module-not-found=-1`, `environment-build-failed=-1`, and `docker_startup_duration_seconds=+61.0` on the locked slice.
- Phase 25 completed on 2026-04-02: the repo now ships an explicit `optional` verdict, a deterministic closeout checker, a bounded proof note, and a milestone-ready handoff that keeps the remaining Phase 23 browser UAT visible as residual debt.
- The v2.4 proof pack still matters as historical context, but it no longer defines the active roadmap direction because the live April 2 regressions exposed a more urgent end-to-end execution problem.
- Phase 17 completed on 2026-03-31: the LangGraph fallback state contract no longer trips the duplicate `confidence` key path, tier3 artifacts now record terminal fallback outcome fields, and the repo has a fixed-slice proof checker for later live replay.
- Phase 18 completed on 2026-03-31: eligible `llm`-mode env failures now route through a deterministic Docker middle hop before final agent fallback, saved artifacts preserve `validation_path` plus `escalated_backend`, and benchmark Doctor/proof surfaces now describe the real backend path.
- Phase 19 completed on 2026-04-01: APDR artifacts now expose `failure_family` for environment-specific versus dependency-resolution outcomes, benchmark summaries keep host-runtime cases in the skip bucket, resumed history is separated from live rows, and the repo now carries a deterministic accounting-proof package anchored to the March 30 baseline.
- Phase 20 completed on 2026-04-01: dominant-bucket module and compatibility recovery rules now target the selected March 30 failure families directly, and the repo now ships a deterministic nine-case proof package showing a positive pass delta with lower `module-not-found`, `version-not-found`, and `environment-build-failed` counts on a like-for-like llm-mode slice.
- Phase 21 completed on 2026-04-01: the repo now carries a live fixed-slice evidence pack with before/after counts, representative case artifacts, a final evidence checker, and a milestone closeout note anchored to the April 1 resumed candidate run.

## Current Milestone: v2.5 LLM End-to-End Resolver and Validation

**Goal:** Make `llm` and `llm-only` responsible for the full APDR case lifecycle, from snippet understanding through Docker validation and recovery, so real benchmark pass rate improves without losing truthful artifacts.

**Target features:**
- Have the LLM extract snippet modules, runtime intent, and initial dependency or system-dependency candidates before validation starts.
- Let the LLM author Docker-oriented validation inputs per case, including build or runtime guidance and reproducible debug artifacts.
- Feed install, build, and runtime logs back into the LLM so recovery is start-to-finish instead of collapsing into blank requirements or generic `Unknown` failures.
- Make `llm` and `llm-only` materially better on fixed-slice and live-run evidence, not just more featureful on paper.
- Keep case reports honest about what the LLM planned, what Docker actually executed, and whether a failure came from model no-output, Docker infrastructure, or the snippet itself.

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
- [x] Dominant-bucket recovery rules now improve the fixed March 30 slice while preserving like-for-like backend and model contracts - validated in Phase 20: dominant-bucket-recovery-gains
- [x] Reviewer-ready live evidence now shows the fixed-slice before/after delta, representative case artifacts, and explicit interrupted-tail truth - validated in Phase 21: live-evidence-and-closeout-pack
- [x] Repository-distributed and local APDR footprint is materially reduced with source-cleanup, safer defaults, supported cleanup tooling, and a deterministic proof package - validated in Phase 21.1: repository-footprint-and-download-size-reduction
- [x] Docker-first `llm` policy now exists with env-first control, exact unusable-Docker fallback reasons, and a deterministic proof contract - validated in Phase 22: docker-first-policy-and-safe-degradation
- [x] The repo now ships a matched env-first versus docker-first comparison harness with frozen artifacts, delta reporting, and a deterministic proof checker - validated in Phase 24: env-first-vs-docker-first-comparison-harness
- [x] The milestone now has a reviewer-readable docker-first verdict, deterministic closeout checker, and bounded proof pack - validated in Phase 25: docker-first-decision-closeout

### Active

- [ ] `llm` and `llm-only` can use the LLM to extract snippet modules, package intent, system dependencies, and runtime context before validation starts.
- [ ] `llm` and `llm-only` can use the LLM to author Docker-oriented validation inputs and bounded recovery steps, with those artifacts preserved per case.
- [ ] Latest-run failures no longer collapse into blank requirements, misleading `SystemDependency`, or generic `Unknown` when the real issue is LLM no-output or Docker infrastructure.
- [ ] Fixed-slice and live-run evidence show whether end-to-end LLM execution improves pass rate and reliability for both `llm` and `llm-only`.

### Out of Scope

- New benchmark UI or product-surface features - this milestone is about recovery reliability and reporting correctness, not interface expansion
- Replacing the Rust/Python architecture - the goal is to improve the existing system, not rewrite it
- Changing benchmark datasets or scoring rules - the benchmark needs to stay comparable while quality and performance work lands
- Full async or Tokio migration - still deferred until benchmark evidence says it is the right next bottleneck
- Dropping Windows or Docker support - compatibility remains a hard constraint
- Full LLM provider replacement - improve the current end-to-end behavior before considering a provider swap
- Another broad deterministic recovery-table expansion as the primary strategy - the milestone should target LLM-led planning and recovery behavior instead
- Reopening v2.4 policy-verdict wording as the main objective - the active problem is end-to-end execution quality, not re-litigating the already-recorded policy proof

## Context

- The v2.3 milestone is grounded in fresh local benchmark evidence from `runs/20260330-020943-apdr` and `runs/20260330-004502-apdr`, not in a new external product domain.
- The combined live baseline is 26 passes out of 395 non-skipped tier3 cases, with the largest failure buckets at `module-not-found`, `environment-build-failed`, and `version-not-found`.
- The benchmark command line for the frozen March 30 baseline used `--validation-backend llm`, but its saved artifacts show env-only attempts; Phase 18 added targeted Docker routing plus explicit `validation_path` and `escalated_backend` fields so future runs can prove the actual route taken.
- The March 30 baseline fallback path was blocked by the duplicate `confidence` state-key crash; Phase 17 removed that crash, so the next milestone work can focus on classification integrity and real recovery gains instead of route-seam breakage.
- Some tier3 misses are likely true dependency-recovery gaps, while others are framework or host-runtime issues, so the milestone needs better failure classification as well as better recovery.
- v2.2 already created useful run-contract, replay, and agent-seam infrastructure, so v2.3 should reuse that groundwork instead of reopening broad measurement or UI work.
- Benchmark case rows, runtime guidance, and proof artifacts now keep requested backend mode separate from actual routed backend path, which gives Phase 19 a trustworthy routing surface to build on.
- The user wants the next milestone to focus on live benchmark reliability and real recovery gains instead of more sample-proof packaging or wide deterministic patch growth.
- The latest local priority is to make the LLM itself do more of the start-to-finish work: extract modules, plan dependencies, author Docker validation inputs, and own recovery loops for `llm` and `llm-only`.
- Current April 2 local runs show two benchmark-visible blockers that this milestone must address together: repeated `LLM package-resolution call returned no output` cases and Docker build-to-run handoff failures where `docker create` cannot see the just-built image tag.
- The shipped v2.3 closeout is intentionally fixed-slice scoped; future work must not overstate it as a full-corpus benchmark claim.
- Phase 21.1 finished the repo-footprint reduction pass first, so the remaining v2.4 work can evaluate docker-first routing from a materially smaller checkout and a supported cleanup baseline.

## Constraints

- **Tech stack**: Rust 2021 core plus existing Python and JS helpers - keep the current architecture intact
- **Compatibility**: Windows and Docker validation flows must continue to work - benchmark users depend on both
- **Correctness**: Accuracy work cannot weaken dependency resolution fidelity or validation behavior - correctness remains primary
- **Benchmark target**: The benchmark corpus and saved-run evidence must remain comparable while live recovery and reporting fixes land
- **Evidence discipline**: Reporting changes must make run summaries more truthful, not just more polished
- **Scope discipline**: Focus on LLM-led end-to-end execution quality and benchmark trustworthiness - avoid slipping back into broad deterministic patching or UI work

## Key Decisions

| Decision | Rationale | Outcome |
|----------|-----------|---------|
| Measure before optimizing or claiming wins | APDR milestones rely on benchmark evidence and proof contracts, not intuition | Good |
| Start v2.3 from the March 30 live baseline instead of the older v2.2 sample-proof package | The active problem was live fallback and routing reliability on real tier3 runs | Good |
| Keep `llm` routing env-first, then Docker, then `llm-agent` | That preserves existing semantics while still adding targeted recovery for eligible env failures | Good |
| Keep requested `validation_backend` semantics stable and surface actual route truth separately | Operators need to see what happened without breaking legacy configured-backend meaning | Good |
| Preserve environment-specific failure truth and live-versus-historical provenance before chasing bucket gains | Recovery deltas are only meaningful if accounting is trustworthy | Good |
| Close v2.3 only with live fixed-slice evidence and explicit scope limits | The shipped claim should stay honest and reviewer-auditable instead of drifting into full-corpus marketing | Good |
| Start v2.4 by explicitly testing the Docker-first `llm` question instead of assuming the Phase 18 env-first policy is final | The next valuable decision is whether the first validation hop is still paying for itself now that Docker routing and evidence surfaces are repaired | Pending |
| Insert a pre-22 footprint phase before continuing Docker-first work | Repo-distributed build artifacts and local APDR caches are large enough to distort day-to-day development cost | Good |
| Skip optional external research for v2.4 | This is a local runtime-policy and evidence question, not a new external product domain | Good |
| Build a fixed-slice paired comparison harness before publishing the docker-first verdict | The recommendation needs matched env-first versus docker-first evidence, not intuition or incomparable saved runs | Good |
| Keep the final v2.4 verdict at `optional` unless stronger live paired replay evidence or cleared Phase 23 browser UAT justifies a stronger claim | The current fixed-slice evidence is positive, but the remaining human-verification debt still bounds what can be claimed honestly | Good |
| Start v2.5 from the April 2 local regressions instead of archiving v2.4 first | The active pain moved from policy choice to real `llm` / `llm-only` execution failures on current runs | Pending |
| Treat the LLM as the primary case author for `llm` and `llm-only` | The user wants the model to extract modules, author Docker validation inputs, and drive recovery end-to-end | Pending |
| Skip optional external research for v2.5 | The milestone is driven by current repo behavior and fresh local benchmark evidence, not a new external domain | Good |

## Shipped Milestone Snapshot

<details>
<summary>v2.3 Tier3 Validation Recovery and Reliability</summary>

**Goal:** Improve real tier3 benchmark yield by making LLM fallback, backend escalation, and benchmark reporting trustworthy on live runs.

**Delivered:**
- Stable `llm` fallback outcomes with explicit fallback metadata.
- Targeted `env -> docker -> llm-agent` routing and truthful backend-path artifacts.
- Failure-family and resume-provenance fixes that keep reporting honest.
- Dominant-bucket recovery gains on a fixed live slice.
- A live evidence pack showing `0/9 -> 2/9` passes on the locked closeout slice.

</details>

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
*Last updated: 2026-04-02 after starting milestone v2.5*
