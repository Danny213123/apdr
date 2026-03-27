# Phase 2: Resolver Memory & Algorithm Efficiency - Research

**Researched:** 2026-03-27
**Domain:** APDR resolver hot-path ownership, retry-loop efficiency, and metadata cache reuse
**Confidence:** Medium

## Summary

Phase 2 should stay tightly focused on the Rust solve path that every benchmark case traverses before env or Docker validation starts. Phase 1 already measured the baseline and ranked the first modernization targets: `tools/apdr/src/resolver/mod.rs`, `tools/apdr/src/resolver/pre_solve.rs`, and `tools/apdr/src/resolver/pypi_client.rs`. The highest-value Phase 2 work is therefore not a large module split yet; it is a targeted ownership and algorithm cleanup in those three files so APDR does less cloning, less repeated lookup work, and less lock-heavy coordination before Phase 3 attacks validation throughput.

Primary recommendation: split Phase 2 into three sequential plans. First, replace the `Arc<Mutex<...>>` result aggregation and panic-prone teardown in `pre_solve.rs`, while consolidating repeated metadata persistence logic in `pypi_client.rs`. Second, simplify `validate_with_retries()` and dependency-update helpers in `resolver/mod.rs` so recovery work does not keep re-normalizing package names and re-rendering the same requirements unless state actually changed. Third, close the phase with a bounded candidate benchmark and a written delta against the committed Phase 1 baseline.

## Phase Requirements

| ID | Requirement | Research Support |
|----|-------------|------------------|
| EFF-01 | Hot-path Rust code reduces unnecessary cloning and allocation in resolver and cache flows | `resolver/mod.rs` and `pre_solve.rs` both repeat clone-heavy state updates and string rendering in benchmark-critical paths. |
| EFF-02 | Shared-state contention in benchmark-critical paths is reduced with a better ownership or aggregation strategy | `pre_solve.rs` currently aggregates multi-version solver results through `Arc<Mutex<_>>` buckets and unwrap-based teardown. |
| EFF-03 | Repeated metadata lookups or recomputation in solve and validate paths are reduced or eliminated | `pypi_client.rs` rebuilds dependency-name vectors and knowledge-cache updates in several parallel branches, and `resolver/mod.rs` re-renders requirements repeatedly inside retries. |
| EFF-04 | Candidate-selection and retry logic use clearer, cheaper algorithms in the hottest Rust paths | `validate_with_retries()` currently mixes iteration bookkeeping, candidate selection, mutation, logging, and recovery policy in one large function. |
| EFF-05 | Performance-oriented refactors preserve deterministic behavior and benchmark correctness | Existing `test_resolver.rs` coverage plus the Phase 1 baseline/regression scripts are enough to protect correctness if Phase 2 reuses them explicitly. |

## Phase 1 Evidence That Should Drive Planning

### Baseline signals

- Phase 1 baseline sample: `3` deterministic fixture snippets
- Aggregate solve duration: `553 ms`
- Aggregate validation duration: `40,237 ms`
- Aggregate install duration: `1,077 ms`
- Aggregate pass rate: `33.33%`
- Representative peak RSS: `19,595,264` bytes on `tools/apdr/tests/fixtures/sample_snippet.py`

Even though validation dominates the sample wall time today, every benchmark case still pays the resolver and pre-solve cost. Phase 2 therefore targets the control-flow and allocation churn that repeats across all cases before the more expensive validation pipeline work in Phase 3.

### Ranked hotspot evidence from Phase 1

- `tools/apdr/src/resolver/mod.rs`: `4,674` lines and heavy retry/mutation logic in `validate_with_retries()`
- `tools/apdr/src/resolver/pre_solve.rs`: `766` lines, `28` `.clone()` calls, and `Arc::try_unwrap(...).unwrap()` result teardown
- `tools/apdr/src/resolver/pypi_client.rs`: `1,302` lines and repeated metadata persistence branches around `fetch_versions()`, `dependency_specs()`, and `bulk_prefetch_from_kgraph()`

### Concrete code-shape concerns

- `pre_solve.rs:170-247` collects solver results into `Arc<Mutex<BTreeMap<...>>>` and `Arc<Mutex<Vec<_>>>`, then unwraps them back into owned collections.
- `pypi_client.rs:53-145` and `pypi_client.rs:196-314` duplicate version/spec persistence work across cache, KGraph, smartPip, and PyPI branches.
- `resolver/mod.rs:899-1868` repeatedly renders requirements, normalizes package names, and copies note strings inside the retry loop.

## Implementation Recommendations

### 1. Replace shared result buckets in `pre_solve.rs`

The multi-version branch of `solve_dependency_graph()` should move from shared `Arc<Mutex<_>>` aggregation to per-thread owned results returned through scoped join handles. That keeps the single-version fast path intact, preserves candidate-order preference, and removes the lock contention plus panic-prone `Arc::try_unwrap(...).unwrap()` teardown currently used to recover the final results.

Recommended shape:

- keep `solver_candidate_versions()` and the single-version branch unchanged except for clone cleanup
- introduce a small result carrier such as `PythonSolveAttempt`
- join each scoped worker into a local vector of attempts
- classify successes, hard failures, and incomplete metadata failures from that owned vector after the join

### 2. Consolidate metadata persistence in `pypi_client.rs`

`fetch_versions()`, `dependency_specs()`, and `bulk_prefetch_from_kgraph()` all do variants of the same work:

- save versions or dependency specs into `CacheStore`
- derive dependency names from specs
- save dependency graph entries
- update the in-process knowledge cache

That repetition increases allocation churn and makes later changes risky. Phase 2 should add shared helper functions for version persistence and dependency-spec persistence so every metadata source path uses the same update logic and the same batching rules.

### 3. Make retry bookkeeping in `resolver/mod.rs` cheaper and easier to follow

`validate_with_retries()` should keep a dirty-state buffer for rendered requirements instead of recomputing the same string whenever nothing changed. The same pass should consolidate note propagation and dependency lookup logic so recovery paths stop repeating ad-hoc normalized scans for package updates.

Recommended targets:

- a retry-loop state helper for rendered requirements, seen requirement sets, and note propagation
- shared dependency lookup helpers for package-name and import-name updates
- keeping benchmark-context reads, iteration snapshots, and retry budgets deterministic

### 4. End Phase 2 with before/after evidence

Phase 2 should not wait until milestone closeout to prove the resolver cleanup was safe. Use the Phase 1 scripts to produce:

- `.planning/phases/02-resolver-memory-and-algorithm-efficiency/02-resolver-candidate.json`
- `.planning/phases/02-resolver-memory-and-algorithm-efficiency/02-RESOLVER-CANDIDATE.md`
- `.planning/phases/02-resolver-memory-and-algorithm-efficiency/02-RESOLVER-DELTA.md`

The candidate run should reuse the bounded fixture sample from Phase 1 so `check_apdr_regression.py` can compare like-for-like totals.

## Validation Architecture

### Quick checks

- `cargo test --manifest-path tools/apdr/Cargo.toml --test test_resolver pre_solver_ -- --nocapture`
- `cargo test --manifest-path tools/apdr/Cargo.toml --test test_resolver resolver_ -- --nocapture`
- `cargo clippy --manifest-path tools/apdr/Cargo.toml --all-targets -- -D warnings`

### Candidate benchmark checks

- `python scripts/measure_apdr_baseline.py --fixtures-root tools/apdr/tests/fixtures --limit 3 --validation-backend env --output-json .planning/phases/02-resolver-memory-and-algorithm-efficiency/02-resolver-candidate.json --output-md .planning/phases/02-resolver-memory-and-algorithm-efficiency/02-RESOLVER-CANDIDATE.md`
- `python scripts/check_apdr_regression.py --baseline .planning/phases/01-baseline-and-guardrails/01-baseline.json --candidate .planning/phases/02-resolver-memory-and-algorithm-efficiency/02-resolver-candidate.json`

### Artifact checks

- `02-resolver-candidate.json` must contain `solve_duration_ms`, `validation_duration_ms`, and `pass_rate`
- `02-RESOLVER-DELTA.md` must record the exact baseline and candidate commands plus the measured delta
- new or updated resolver tests must stay under `tools/apdr/tests/test_resolver.rs`

## Canonical Files For Planning

- `.planning/PROJECT.md`
- `.planning/ROADMAP.md`
- `.planning/REQUIREMENTS.md`
- `.planning/STATE.md`
- `.planning/phases/01-baseline-and-guardrails/01-HOTSPOT-AUDIT.md`
- `.planning/phases/01-baseline-and-guardrails/01-baseline.json`
- `.planning/phases/01-baseline-and-guardrails/01-memory-profile.json`
- `.planning/codebase/CONCERNS.md`
- `tools/apdr/src/resolver/mod.rs`
- `tools/apdr/src/resolver/pre_solve.rs`
- `tools/apdr/src/resolver/pypi_client.rs`
- `tools/apdr/tests/test_resolver.rs`
- `scripts/measure_apdr_baseline.py`
- `scripts/check_apdr_regression.py`

## Out-of-Scope For This Phase

- `tools/apdr/src/docker/builder.rs` throughput refactors reserved for Phase 3
- large module extraction or file splits reserved for Phase 4
- documentation-heavy review cleanup reserved for Phase 5
- async runtime migration or broader architecture rewrites

---
*Research created: 2026-03-27*
*Phase: 02-resolver-memory-and-algorithm-efficiency*
