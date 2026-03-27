---
phase: 03
slug: validation-pipeline-throughput
status: planned
nyquist_compliant: true
wave_0_complete: true
created: 2026-03-27
---

# Phase 3 - Validation Strategy

> Validation contract for env and Docker throughput work, backend-attempt cleanup, and validation-focused benchmark reporting.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | `cargo test`, `cargo clippy`, and the Phase 1 benchmark/regression scripts |
| **Config file** | `tools/apdr/Cargo.toml` |
| **Quick run command** | `cargo test --manifest-path tools/apdr/Cargo.toml validation_pipeline_ -- --nocapture && python -m py_compile scripts/measure_apdr_baseline.py scripts/check_apdr_regression.py` |
| **Full suite command** | `cargo test --manifest-path tools/apdr/Cargo.toml -- --nocapture && cargo clippy --manifest-path tools/apdr/Cargo.toml --all-targets -- -D warnings && python scripts/measure_apdr_baseline.py --help && python scripts/check_apdr_regression.py --help` |
| **Phase comparison command** | `python scripts/check_apdr_regression.py --baseline .planning/phases/01-baseline-and-guardrails/01-baseline.json --candidate .planning/phases/03-validation-pipeline-throughput/03-validation-candidate.json` |
| **Estimated runtime** | ~3-6 minutes for the Rust and script verification loop; longer only when capturing the warm and forced validation candidate artifacts |

---

## Sampling Rate

- **After every task commit:** Run the quick command plus the task-specific verify command from the active plan
- **After every plan wave:** Run the full suite command
- **Before `$gsd-verify-work`:** Generate the warm and forced candidate artifacts, then run the phase comparison command
- **Max feedback latency:** keep code-path feedback under 90 seconds; only the benchmark-candidate capture step may exceed that

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|-----------|-------------------|-------------|--------|
| 03-01-01 | 01 | 1 | VAL-01 | targeted Rust unit tests | `cargo test --manifest-path tools/apdr/Cargo.toml validation_pipeline_detects_ -- --nocapture` | yes | pending |
| 03-01-02 | 01 | 1 | VAL-02 | targeted Rust unit tests | `cargo test --manifest-path tools/apdr/Cargo.toml validation_pipeline_merges_env_history_before_docker_attempts -- --nocapture` | yes | pending |
| 03-01-03 | 01 | 1 | VAL-05 | artifact + lint verification | `rg -n "ValidatedEnvCacheSource|EnvAttemptPaths|prepare_env_validation_attempt|materialize_env_for_attempt|merge_backend_retry_history" tools/apdr/src/docker/builder.rs && cargo clippy --manifest-path tools/apdr/Cargo.toml --all-targets -- -D warnings` | yes | pending |
| 03-02-01 | 02 | 2 | VAL-03 | targeted Rust unit tests | `cargo test --manifest-path tools/apdr/Cargo.toml validation_pipeline_caches_docker_agent_probe -- --nocapture` | yes | pending |
| 03-02-02 | 02 | 2 | VAL-04 | script smoke + artifact contract | `python -m py_compile scripts/measure_apdr_baseline.py scripts/check_apdr_regression.py && python scripts/measure_apdr_baseline.py --help && python scripts/check_apdr_regression.py --help` | yes | pending |
| 03-02-03 | 02 | 2 | VAL-04 | artifact verification | `rg -n "Cache hit|Env create ms|Install ms|Smoke ms|Backend|--max-env-create-regression-pct|--max-install-regression-pct|--max-smoke-regression-pct" scripts/measure_apdr_baseline.py scripts/check_apdr_regression.py` | yes | pending |
| 03-03-01 | 03 | 3 | VAL-04 | warm candidate capture | `python scripts/measure_apdr_baseline.py --fixtures-root tools/apdr/tests/fixtures --limit 3 --validation-backend env --output-json .planning/phases/03-validation-pipeline-throughput/03-validation-candidate.json --output-md .planning/phases/03-validation-pipeline-throughput/03-VALIDATION-CANDIDATE.md` | no | pending |
| 03-03-02 | 03 | 3 | VAL-01 | forced validation candidate capture | `python scripts/measure_apdr_baseline.py --fixtures-root tools/apdr/tests/fixtures --limit 3 --validation-backend env --force-validate --output-json .planning/phases/03-validation-pipeline-throughput/03-validation-candidate-forced.json --output-md .planning/phases/03-validation-pipeline-throughput/03-VALIDATION-CANDIDATE-FORCED.md` | no | pending |
| 03-03-03 | 03 | 3 | VAL-04 | regression gate + delta note | `python scripts/check_apdr_regression.py --baseline .planning/phases/01-baseline-and-guardrails/01-baseline.json --candidate .planning/phases/03-validation-pipeline-throughput/03-validation-candidate.json` | no | pending |

---

## Wave 0 Requirements

- Existing Rust test infrastructure is already present from Phases 1 and 2.
- No new package install is required before execution.
- The same local Python interpreters used in earlier baseline work should remain available for continuity.
- Docker availability on the host should stay unchanged while capturing the candidate artifacts so the delta remains interpretable.

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| The delta note distinguishes warm-path cache reuse from forced-validation throughput evidence | VAL-01, VAL-04 | Numeric scripts can report timings, but they cannot decide whether a claimed win came from cache reuse or from cheaper env or Docker orchestration | Read `03-VALIDATION-DELTA.md`, confirm it cites both `03-validation-candidate.json` and `03-validation-candidate-forced.json`, and verify the narrative keeps continuity comparison separate from forced-validation performance claims |
| Windows and Docker compatibility notes remain visible after throughput refactors | VAL-05 | This host already recorded a Windows-specific Docker permission failure, and reviewers need to know whether that behavior changed, improved, or remained a host constraint | Read the candidate artifacts and delta note, and confirm any Docker-unavailable or permission-denied behavior is reported as compatibility context rather than omitted from the performance story |

---

## Validation Sign-Off

- [x] All planned tasks have an automated verify step or explicit artifact check
- [x] Sampling continuity still references the committed Phase 1 baseline
- [x] Existing infrastructure covers the phase
- [x] No watch-mode commands are required
- [x] Quick feedback latency stays bounded for code changes
- [x] `nyquist_compliant: true` is set in frontmatter

**Approval:** passed
