# Phase 1: Baseline & Guardrails - Research

**Researched:** 2026-03-26
**Domain:** APDR benchmark measurement, runtime telemetry, and Rust hotspot triage
**Confidence:** Medium

## Summary

Phase 1 should create a repeatable baseline and guardrail layer before any performance refactor lands. The repo already exposes most of the timing data needed for this phase through `ResolveResult::summary_lines()` in `tools/apdr/src/lib.rs`, through `tools/apdr/test_executor.py`, and through the benchmark summary flow in `benchmark_ui/runner.py`, so the shortest path is to reuse those interfaces rather than invent a second benchmark protocol.

Primary recommendation: add repo-level scripts under `scripts/` that run bounded APDR benchmark samples, capture machine-readable timing and pass-rate artifacts into `.planning/phases/01-baseline-and-guardrails/`, and then compare later runs against that baseline. Keep Phase 1 focused on measurement, hotspot ranking, and regression gates; do not spend the phase on resolver or validator optimizations yet.

## Phase Requirements

| ID | Requirement | Research Support |
|----|-------------|------------------|
| BASE-01 | Benchmark baseline captures end-to-end runtime, validation runtime, and pass rate before optimization work begins | Existing APDR summary fields already expose solve, validation, env create, install, smoke, and status metrics. |
| BASE-02 | Benchmark baseline captures memory-sensitive indicators for key Rust workflows | A wrapper script can sample peak process memory without modifying the benchmark protocol. |
| BASE-03 | The repo has a repeatable command set for fmt, clippy, targeted tests, and benchmark comparison | README and a regression checker can publish and enforce the exact commands. |
| BASE-04 | High-risk performance hotspots are ranked from measured evidence, not only code inspection | Baseline JSON plus memory profile JSON can be combined with static scans into a ranked hotspot audit. |
| BASE-05 | Each optimization phase defines a regression check before refactoring begins | A comparison script can block pass-rate drops or timing regressions beyond explicit thresholds. |

## Current Evidence

### Existing timing surfaces

- `tools/apdr/src/lib.rs:524` emits `SOLVE_DURATION_MS`, `VALIDATION_DURATION_MS`, `ENV_CREATE_DURATION_MS`, `INSTALL_DURATION_MS`, and `SMOKE_DURATION_MS` in `summary_lines()`.
- `tools/apdr/test_executor.py:79-129` already parses and persists those summary fields into `output_data_<python>.yml`.
- `benchmark_ui/runner.py:257-268` forces `--force-validate` for non-LLM APDR benchmark cases and writes per-case artifacts to the run directory.
- `benchmark_ui/runner.py:515-542` and `benchmark_ui/service.py:1399-1427` already aggregate per-case duration fields into summary state.

### Known hotspot signals

- `tools/apdr/src/resolver/mod.rs`: 4,674 lines, 192 `.clone()` calls
- `tools/apdr/src/docker/builder.rs`: 2,712 lines, 22 `.clone()` calls
- `tools/apdr/src/resolver/family_knowledge.rs`: 2,082 lines, 3 `.clone()` calls
- `tools/apdr/src/resolver/pypi_client.rs`: 1,302 lines, 8 `.clone()` calls
- `tools/apdr/src/resolver/tier3_llm.rs`: 1,139 lines, 29 `.clone()` calls
- `tools/apdr/src/resolver/pre_solve.rs`: 766 lines, 28 `.clone()` calls plus `Arc::try_unwrap(...).unwrap()` lock-unwrapping paths

### Fragile review surfaces

- `tools/apdr/src/resolver/pre_solve.rs` has multiple `unwrap()` calls around shared-state parallel solve teardown.
- `tools/apdr/src/resolver/tier3_llm.rs` uses `expect()` on subprocess pipes.
- `tools/apdr/src/docker/builder.rs` escalates backend behavior off `summary.attempts.last().unwrap()`, which makes fallback logic harder to reason about.

## Implementation Recommendations

### 1. Reuse the APDR summary contract

Do not parse human-readable logs for baseline metrics. Phase 1 tooling should consume the structured summary keys that already leave the Rust CLI through `summary_lines()` and then get preserved by `test_executor.py`.

Recommended outputs:

- `.planning/phases/01-baseline-and-guardrails/01-baseline.json`
- `.planning/phases/01-baseline-and-guardrails/01-memory-profile.json`
- `.planning/phases/01-baseline-and-guardrails/01-BASELINE.md`
- `.planning/phases/01-baseline-and-guardrails/01-HOTSPOT-AUDIT.md`

### 2. Use bounded deterministic samples

Phase 1 does not need a full 2.9K-case run to be useful. A deterministic subset is enough if the sample is recorded clearly.

Recommended sample sources:

- `tools/apdr/tests/fixtures/*.py` for fast smokeable resolver cases
- `hard-gists/**/snippet.py` with an explicit numeric limit for heavier validation cases

The harness should sort inputs deterministically and record the exact sample list into the artifact so later comparisons are apples-to-apples.

### 3. Keep memory capture outside the Rust core for now

Phase 1 should avoid invasive instrumentation unless the wrapper approach proves insufficient.

Recommended approach:

- Windows path: query the APDR child process with PowerShell `Get-Process` and capture `WorkingSet64` / `PeakWorkingSet64`
- Unix path: use Python `resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss`

This keeps Phase 1 focused on observability and avoids contaminating the Rust optimization work with temporary instrumentation code.

### 4. Make hotspot ranking evidence-backed

The hotspot audit should combine:

- dynamic timing from the baseline harness
- dynamic memory signal from the memory profiler
- static signals from line counts, `.clone()` density, and panic-prone or lock-heavy code paths

That audit should explicitly rank candidate files for Phase 2 and Phase 3 rather than producing a generic "code smells" list.

### 5. Publish a single guardrail command set

The modernization workflow should have one documented command set that later phases can run before and after changes:

- `cargo fmt --manifest-path tools/apdr/Cargo.toml --all --check`
- `cargo clippy --manifest-path tools/apdr/Cargo.toml --all-targets -- -D warnings`
- `cargo test --manifest-path tools/apdr/Cargo.toml`
- `python scripts/measure_apdr_baseline.py ...`
- `python scripts/profile_apdr_memory.py ...`
- `python scripts/check_apdr_regression.py --baseline ... --candidate ...`

## Validation Architecture

### Quick checks

- `python -m py_compile scripts/measure_apdr_baseline.py scripts/profile_apdr_memory.py scripts/check_apdr_regression.py`
- `python scripts/measure_apdr_baseline.py --help`
- `python scripts/profile_apdr_memory.py --help`
- `python scripts/check_apdr_regression.py --help`

### Contract tests

- Extend `tools/apdr/tests/test_cli.rs` so the summary keys consumed by the new baseline harness are protected by `cargo test --manifest-path tools/apdr/Cargo.toml --test test_cli`

### Artifact checks

- `01-baseline.json` should include pass-rate totals and the five timing families emitted by `summary_lines()`
- `01-memory-profile.json` should include a peak-memory field and the exact command that was profiled
- `01-HOTSPOT-AUDIT.md` should reference the concrete hotspot files and the evidence used to rank them

## Canonical Files For Planning

- `.planning/PROJECT.md`
- `.planning/ROADMAP.md`
- `.planning/REQUIREMENTS.md`
- `.planning/STATE.md`
- `.planning/codebase/CONCERNS.md`
- `.planning/codebase/TESTING.md`
- `tools/apdr/src/lib.rs`
- `tools/apdr/test_executor.py`
- `benchmark_ui/runner.py`
- `benchmark_ui/state.py`
- `tools/apdr/tests/test_cli.rs`
- `tools/apdr/README.md`

## Out-of-Scope For This Phase

- Resolver or validator algorithm changes intended to improve speed directly
- Large Rust module splits
- Async runtime migration
- LLM prompt or caching work

---
*Research created: 2026-03-26*
*Phase: 01-baseline-and-guardrails*
