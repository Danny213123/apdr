# Phase 14: macOS Execution-Path Optimization - Research

**Researched:** 2026-03-29
**Domain:** Turning the Phase 13 measurement contract into a fast, reproducible macOS replay lane with explicit Windows guardrails
**Confidence:** High

## Summary

Phase 14 should not start by guessing at low-level optimizations. The repo now has the measurement surface needed to make macOS execution-path changes attributable: canonical run contracts in `benchmark_ui`, per-case stage timings in APDR artifacts, and a checker-backed report format that distinguishes `env-fast` from `docker-proof` and warm from cold cache. The next step is to lock the replay boundary, build a repeatable macOS replay runner around the native env path that APDR already optimizes well, and add explicit regression checks so the macOS speedups do not quietly shift cost onto Windows.

The strongest local path is already present in the codebase. `tools/apdr/src/docker/builder/env_backend.rs` restores validated envs from compressed archives and tries APFS copy-on-write clones from a hot sibling before falling back to extraction or cold builds. `tools/apdr/README.md` documents a bounded `validated-envs/` cache plus a `wheelhouse/` cache. That means Phase 14 should favor a native `env-fast` replay lane with deliberate cold and warm captures instead of treating Docker as the inner loop. The benchmark runner also already exposes exactly the right control points for a replay lane: `workers`, saved loadouts, resume, `run_intent`, `cache_state`, `build_profile`, and `--run-contract-json`.

The biggest remaining gap is replay determinism. Right now the benchmark boundary is mostly expressed through `snippet_limit`, dataset roots, and historical run state. That is not a locked slice. Phase 14 should promote exact case manifests to first-class inputs, use them for both the macOS replay lane and the representative Windows guardrail slice, and make the measurement and regression scripts consume those manifests directly. Once the slice boundary is stable, the phase can tune the macOS path without turning every comparison into an argument about case ordering or cache drift.

## Phase Requirements

| ID | Requirement | Research Support |
|----|-------------|------------------|
| MAC-03 | APDR provides a fast macOS replay mode built around native env validation and a locked benchmark slice for local iteration | Use explicit replay manifests plus a native `env-fast` lane that leans on the existing validated-env and wheelhouse caches rather than Docker. |
| MAC-04 | APDR demonstrates substantial before-and-after macOS benchmark gains on the locked replay slice without reducing correctness on preserved pass or skip cases | Add a dedicated replay runner plus regression checks that compare the same slice, stage timings, and preserved status counts before claiming gains. |
| WIN-01 | macOS-focused benchmark-performance changes do not regress the representative Windows comparison slice | Define a separate Windows guardrail manifest and extend the regression checker to validate seconds-per-case, total/stage regressions, and preserved pass/skip behavior against a Windows artifact pair. |

## Evidence That Should Drive Planning

### Phase 13 already created the right measurement contract

`benchmark_ui/run_contract.py`, `benchmark_ui/runner.py`, `tools/apdr/test_executor.py`, and `scripts/measure_apdr_baseline.py` now agree on the fields that identify a comparable run: model, backend, cache state, build profile, context window, inference policy, and architecture. Phase 14 should build on that instead of inventing a second artifact format for replay runs.

### A locked slice needs exact manifests, not `snippet_limit`

`benchmark_ui/runner.py` currently discovers snippets from the dataset archive and optionally truncates them with `snippet_limit`. That is useful for ad hoc sampling, but it is too fragile for a milestone replay boundary because archive contents or ordering can change. The replay slice and Windows guardrail slice should be explicit JSON manifests that list the exact relative paths in the intended order, plus slice identifiers and rationale fields for reviewer context.

### The benchmark runner already supports most of the replay control plane

`benchmark_ui/service.py` and `benchmark_ui/state.py` already normalize and persist `run_intent`, `cache_state`, `llm_context_window`, `inference_policy`, `build_profile`, `workers`, loadout names, and saved-run config. `benchmark_ui/runner.py` already persists `run_contract.json`, supports resume, and passes per-run metadata into APDR with `--run-contract-json`. Phase 14 should extend that same path with a replay-manifest input instead of bypassing the benchmark runner.

### APDR's native env backend already contains the macOS-friendly optimization surface

`tools/apdr/src/docker/builder/env_backend.rs` shows that successful env validations are cached under `validated-envs/`, restored from archive, and, on filesystems that support it, cloned via a hot copy-on-write path before a cold rebuild. `tools/apdr/README.md` documents retention controls for the validated-env cache and the pip wheelhouse. The inner loop should therefore optimize around warm native env reuse first, because the codebase already pays attention to that path.

### Measured replay runs should not pay hidden build or Docker overhead

`tools/apdr/test_executor.py` currently chooses the freshest APDR binary but can still fall back to `cargo run --quiet --` when a built binary is stale or missing. That is acceptable for compatibility, but it is the wrong default for a performance replay lane because it folds cargo build or startup cost into the measured run. Similarly, `benchmark_ui/state.py` can auto-start Docker Desktop, which is useful for proof runs but should not be part of a fast native replay intent. Phase 14 should make build-profile selection and native replay preflight explicit.

### The current regression checker is a useful base, but it is not enough yet

`scripts/check_apdr_regression.py` already compares total time, validation time, and selected stage timings plus pass-rate deltas. It does not yet reason about `llm_duration_ms`, `docker_startup_duration_ms`, seconds-per-case, manifest identity, execution-mode drift, cache-state drift, or preserved pass/skip counts. Phase 14 should extend this script instead of replacing it, then layer a phase-specific checker and reviewer notes on top.

### Existing committed artifacts already show Windows-shaped evidence

Older phase artifacts under `.planning/phases/01-*` and `.planning/phases/03-*` contain `D:\apdr\...` paths and Windows-specific Docker/build errors. That is enough evidence to justify a dedicated Windows comparison slice and an artifact-based guardrail flow, even if the actual Windows rerun happens on a separate host during execution. Phase 14 should formalize that comparison path instead of leaving Windows non-regression implicit.

## Implementation Recommendations

### 1. Lock the macOS replay and Windows guardrail boundaries first

Recommended files:

- `.planning/phases/14-macos-execution-path-optimization/14-macos-replay-slice.json`
- `.planning/phases/14-macos-execution-path-optimization/14-windows-guardrail-slice.json`
- `benchmark_ui/state.py`
- `benchmark_ui/service.py`
- `benchmark_ui/runner.py`
- `scripts/measure_apdr_baseline.py`

Recommended responsibilities:

- add explicit replay-manifest support to benchmark config, saved runs, and historical rendering
- create one locked macOS replay manifest and one representative Windows guardrail manifest
- make `scripts/measure_apdr_baseline.py` accept a manifest path so it can generate exact-slice captures

### 2. Add a dedicated macOS replay runner and native fast-lane policy

Recommended files:

- `scripts/run_phase14_replay.py`
- `benchmark_ui/runner.py`
- `benchmark_ui/state.py`
- `benchmark_ui/service.py`
- `tools/apdr/test_executor.py`

Recommended responsibilities:

- create one replay entrypoint that can run cold baseline, prewarm caches, and warm candidate captures on the locked slice
- use `env` validation and explicit build-profile selection by default for `macos-replay`
- make invalidating conditions visible before a run is treated as evidence: Rosetta, Docker backend, mixed cache state, stale or missing release binary, or incompatible worker policy

### 3. Extend the regression contract and close with a checker-backed proof pack

Recommended files:

- `scripts/check_apdr_regression.py`
- `scripts/check_phase14_macos_replay.py`
- `.planning/phases/14-macos-execution-path-optimization/14-MACOS-REPLAY.md`
- `.planning/phases/14-macos-execution-path-optimization/14-WINDOWS-GUARDRAIL.md`

Recommended responsibilities:

- compare artifact pairs on the same slice id and execution mode
- enforce pass/skip preservation and windows guardrail thresholds in addition to timing deltas
- produce reviewer-facing notes that link the actual macOS before/after and Windows comparison artifacts to the requirement verdicts

## Validation Architecture

### Quick checks

- `python3 -m py_compile scripts/measure_apdr_baseline.py scripts/check_apdr_regression.py`
- `python3 -m unittest benchmark_ui.test_runner_events benchmark_ui.test_service_tier_stats benchmark_ui.test_run_contract`

### Artifact checks

- `rg -n 'slice_id|cases|relative_path|reason' .planning/phases/14-macos-execution-path-optimization/14-macos-replay-slice.json .planning/phases/14-macos-execution-path-optimization/14-windows-guardrail-slice.json`
- `rg -n 'replay_manifest|slice_id|run_intent|build_profile' benchmark_ui/runner.py benchmark_ui/service.py benchmark_ui/state.py scripts/measure_apdr_baseline.py`
- `rg -n 'llm_duration_ms|docker_startup_duration_ms|seconds_per_case|preserved' scripts/check_apdr_regression.py`

### Phase-close checks

- `python3 scripts/run_phase14_replay.py --manifest-json .planning/phases/14-macos-execution-path-optimization/14-macos-replay-slice.json --dataset-root <dataset-root> --validation-backend env --baseline-json .planning/phases/14-macos-execution-path-optimization/14-macos-before.json --candidate-json .planning/phases/14-macos-execution-path-optimization/14-macos-after.json --output-md .planning/phases/14-macos-execution-path-optimization/14-MACOS-REPLAY.md`
- `python3 scripts/check_apdr_regression.py --baseline .planning/phases/14-macos-execution-path-optimization/14-macos-before.json --candidate .planning/phases/14-macos-execution-path-optimization/14-macos-after.json --max-total-regression-pct -20 --max-validation-regression-pct 0 --min-pass-rate-delta 0.0`
- `python3 scripts/check_phase14_macos_replay.py --macos-before .planning/phases/14-macos-execution-path-optimization/14-macos-before.json --macos-after .planning/phases/14-macos-execution-path-optimization/14-macos-after.json --windows-before .planning/phases/14-macos-execution-path-optimization/14-windows-before.json --windows-after .planning/phases/14-macos-execution-path-optimization/14-windows-after.json --macos-md .planning/phases/14-macos-execution-path-optimization/14-MACOS-REPLAY.md --windows-md .planning/phases/14-macos-execution-path-optimization/14-WINDOWS-GUARDRAIL.md`

## Canonical Files For Planning

- `.planning/PROJECT.md`
- `.planning/REQUIREMENTS.md`
- `.planning/ROADMAP.md`
- `.planning/STATE.md`
- `.planning/research/SUMMARY.md`
- `.planning/phases/13-measurement-and-run-contract-hardening/13-03-SUMMARY.md`
- `.planning/phases/13-measurement-and-run-contract-hardening/13-MEASUREMENT-CONTRACT.md`
- `benchmark_ui/state.py`
- `benchmark_ui/service.py`
- `benchmark_ui/runner.py`
- `benchmark_ui/run_contract.py`
- `scripts/measure_apdr_baseline.py`
- `scripts/check_apdr_regression.py`
- `tools/apdr/test_executor.py`
- `tools/apdr/src/docker/builder/env_backend.rs`
- `tools/apdr/README.md`

## Out of Scope For This Phase

- improving tier3 reasoning quality, tool use, retrieval, or context engineering beyond the benchmark metadata needed for replay
- adding new deterministic dependency-recovery rules to improve LLM accuracy
- turning Docker proof runs into the default local inner loop on macOS
- changing the milestone closeout format for Phase 16
- modifying unrelated local edits in `tools/apdr/llm_py/*`, `tools/apdr/Cargo.lock`, or `web/src/main.js`

---
*Research created: 2026-03-29*
*Phase: 14-macos-execution-path-optimization*
