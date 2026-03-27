# Phase 3: Validation Pipeline Throughput - Research

**Researched:** 2026-03-27
**Domain:** APDR env and Docker validation throughput, cache reuse, backend fallback cost, and benchmark telemetry
**Confidence:** Medium

## Summary

Phase 3 should stay tightly focused on the validation path in `tools/apdr/src/docker/builder.rs`, with only the smallest supporting changes needed elsewhere to make the validation pipeline cheaper and easier to measure. Phase 1 and Phase 2 already established the relevant evidence: validation dominates the bounded baseline, the only hard failure in the initial sample came from env-to-Docker escalation on Windows, and the large validation delta in the Phase 2 candidate was mostly cache-path noise rather than a pure performance win. The highest-value Phase 3 work is therefore not a broad module split yet; it is targeted orchestration cleanup so env and Docker attempts reuse more work, retry with less duplication, and emit artifacts that make cache hits versus real validation cost explicit.

Primary recommendation: split Phase 3 into three sequential plans. First, refactor the env-validation loop in `docker/builder.rs` so attempt staging, validated-env cache detection, and restore-or-build behavior live behind explicit helpers instead of long inline branches. Second, tighten backend-attempt plumbing and benchmark reporting so Docker-agent probing, backend escalation, cached-env reuse, and stage-level timings are exposed consistently for review. Third, close the phase with both a continuity candidate artifact and a forced-validation candidate artifact so the milestone keeps the original baseline comparison while also proving the real validation path is faster when cache shortcuts are removed.

## Phase Requirements

| ID | Requirement | Research Support |
|----|-------------|------------------|
| VAL-01 | Validation reuses caches, layers, or artifacts more effectively to reduce repeated build work | `validate_requirements_env()` already has validated-env archive and legacy-dir reuse paths, but the restore logic is intertwined with fresh-build logic and hard to improve safely while it stays inline. |
| VAL-02 | Validation fallback and retry paths avoid unnecessary duplicate environment creation | The env loop still duplicates attempt preparation, metadata writing, and restore/build branching before Docker escalation. The Docker path similarly mixes per-version retries with repeated orchestration setup. |
| VAL-03 | Python-version or backend attempts use a more efficient execution strategy than the current bottlenecks | `attempt_langgraph_agent()` rechecks Python importability of `docker_agent` on each invocation and `validate_requirements_docker()` still performs serial orchestration with repeated setup work that can be isolated and cached. |
| VAL-04 | Validation telemetry clearly separates solve time, env create time, install time, and smoke or runtime cost | The JSON artifacts already carry stage timings, but the Markdown summaries still emphasize only solve and total validation time per sample, which hides where wins or regressions actually came from. |
| VAL-05 | Validation changes preserve Windows and Docker compatibility | Phase 1 recorded a real Windows Docker permission failure, so Phase 3 has to keep that behavior observable and compatible instead of papering over it with Linux-only assumptions. |

## Evidence That Should Drive Planning

### Baseline and candidate signals

- Phase 1 baseline sample: `3` deterministic fixture snippets
- Aggregate solve duration: `553 ms`
- Aggregate validation duration: `40,237 ms`
- Aggregate install duration: `1,077 ms`
- Aggregate pass rate: `33.33%`
- Representative failure: `cfscrape_snippet.py` escalated from env validation into Docker and failed on this Windows host with `CreateFile C:\Users\danny\.docker\buildx\instances: Access is denied`

Phase 2 did improve the resolver path, but its bounded candidate recorded `0 ms` validation time because cached validation paths were reused. That is useful evidence about reuse behavior, but it also means Phase 3 must measure warm-path and forced-validation behavior separately if the milestone wants credible validation-throughput claims.

### Ranked hotspot evidence from Phase 1

- `tools/apdr/src/docker/builder.rs`: ranked `#1` in `01-HOTSPOT-AUDIT.md` because validation dominated the baseline and the slowest failure flowed through env-to-Docker escalation
- `tools/apdr/src/resolver/tier3_llm.rs`: ranked `#4` because fallback subprocess and IPC cost already appears on unresolved cases

### Concrete code-shape concerns

- `tools/apdr/src/docker/builder.rs:96-496` (`validate_requirements_env`) still builds work directories, cache-hit metadata, requirements files, smoke files, and `ValidationAttempt` state inline for every candidate Python version.
- `tools/apdr/src/docker/builder.rs:263-369` duplicates validated-env archive restore, legacy-dir copy, restore verification, fallback-to-build, and cache-marker touching inside one large conditional.
- `tools/apdr/src/docker/builder.rs:1152-1351` (`create_and_install_env`) tracks env creation and install timing, but the surrounding caller path still makes it hard to compare cold builds, restored envs, and retry branches consistently.
- `tools/apdr/src/docker/builder.rs:552-735` (`attempt_langgraph_agent` plus `parse_agent_result`) runs a fresh `python3 -c "import docker_agent"` probe and hand-parses JSON-like output even though the result format is already JSON.
- `scripts/measure_apdr_baseline.py` aggregates `env_create_duration_ms`, `install_duration_ms`, and `smoke_duration_ms`, but its per-sample Markdown table still only shows solve and total validation time. That makes review harder when validation wins come from only one stage.

## Implementation Recommendations

### 1. Extract env-attempt staging and cache-source detection

Phase 3 should make env validation more testable before it tries to make it faster. The cleanest path is to introduce small helper types in `docker/builder.rs` that own:

- per-attempt paths (`work_dir`, log paths, env dir, requirements-install file, smoke-test file)
- validated-env cache source detection (`archive`, `legacy dir`, or `none`)
- restore-or-build execution that updates `ValidationAttempt` and `ValidationSummary` in one place

That removes duplicated path setup and cache bookkeeping from `validate_requirements_env()` without changing the output layout or the current build key scheme.

### 2. Tighten backend-attempt plumbing instead of only adding more retries

The Docker and agent paths need cheaper orchestration before the milestone adds any more validation-heavy benchmark work. Recommended targets:

- cache the `docker_agent` import probe result so repeated backend attempts do not rerun `python3 -c "import docker_agent"` every time
- parse agent JSON output with `serde_json` rather than custom string scanning
- keep env-to-Docker summary merging behind one helper so attempt history remains stable and reviewable

This work still belongs to Phase 3 because it changes how expensive backend attempts are, not just how the code is organized.

### 3. Make the benchmark artifacts validation-aware

Phase 3 needs better measurement, not just faster code. Extend the benchmark tooling so reviewers can see:

- per-sample `env_create_duration_ms`, `install_duration_ms`, and `smoke_duration_ms`
- which backend actually ran for each sample
- whether a sample reused a validated env or had to build cold
- whether the candidate run used the warm path or `--force-validate`

That gives Phase 3 a way to separate true validation wins from cached-result reuse.

### 4. Close the phase with warm and forced candidate artifacts

The phase should end with two candidate captures:

- a continuity artifact that keeps the Phase 1 command shape for apples-to-apples milestone comparison
- a forced-validation artifact that uses the same sample rule but passes `--force-validate` so env creation, install, smoke, and fallback stages are exercised for real

The delta document should report both views and explicitly say which one supports continuity and which one supports real validation-throughput claims.

## Validation Architecture

### Quick checks

- `cargo test --manifest-path tools/apdr/Cargo.toml validation_pipeline_ -- --nocapture`
- `python -m py_compile scripts/measure_apdr_baseline.py scripts/check_apdr_regression.py`
- `cargo clippy --manifest-path tools/apdr/Cargo.toml --all-targets -- -D warnings`

### Candidate benchmark checks

- `python scripts/measure_apdr_baseline.py --fixtures-root tools/apdr/tests/fixtures --limit 3 --validation-backend env --output-json .planning/phases/03-validation-pipeline-throughput/03-validation-candidate.json --output-md .planning/phases/03-validation-pipeline-throughput/03-VALIDATION-CANDIDATE.md`
- `python scripts/measure_apdr_baseline.py --fixtures-root tools/apdr/tests/fixtures --limit 3 --validation-backend env --force-validate --output-json .planning/phases/03-validation-pipeline-throughput/03-validation-candidate-forced.json --output-md .planning/phases/03-validation-pipeline-throughput/03-VALIDATION-CANDIDATE-FORCED.md`
- `python scripts/check_apdr_regression.py --baseline .planning/phases/01-baseline-and-guardrails/01-baseline.json --candidate .planning/phases/03-validation-pipeline-throughput/03-validation-candidate.json`

### Artifact checks

- `03-validation-candidate.json` must contain `validation_duration_ms`, `env_create_duration_ms`, `install_duration_ms`, and `smoke_duration_ms`
- `03-VALIDATION-CANDIDATE.md` must show per-sample backend and stage breakdowns, not just solve and total validation time
- `03-VALIDATION-CANDIDATE-FORCED.md` must explicitly say it was recorded with `--force-validate`
- `03-VALIDATION-DELTA.md` must distinguish continuity comparison from forced-validation throughput evidence

## Canonical Files For Planning

- `.planning/PROJECT.md`
- `.planning/ROADMAP.md`
- `.planning/REQUIREMENTS.md`
- `.planning/STATE.md`
- `.planning/phases/01-baseline-and-guardrails/01-baseline.json`
- `.planning/phases/01-baseline-and-guardrails/01-HOTSPOT-AUDIT.md`
- `.planning/phases/02-resolver-memory-and-algorithm-efficiency/02-RESOLVER-DELTA.md`
- `.planning/codebase/CONCERNS.md`
- `tools/apdr/src/docker/builder.rs`
- `tools/apdr/src/cache/maintenance.rs`
- `tools/apdr/src/lib.rs`
- `scripts/measure_apdr_baseline.py`
- `scripts/check_apdr_regression.py`

## Out-of-Scope For This Phase

- large module extraction or file splits reserved for Phase 4
- documentation-heavy cleanup and broader panic-path review reserved for Phase 5
- replacing the benchmark sample rule or dataset inputs from Phase 1
- requiring a working Docker daemon on this exact Windows host as part of the phase definition
- a broad rewrite of `resolver/tier3_llm.rs` beyond the validation-adjacent agent plumbing needed for backend-attempt efficiency

---
*Research created: 2026-03-27*
*Phase: 03-validation-pipeline-throughput*
