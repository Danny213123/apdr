# Phase 6: Benchmark Verification & v2 Closeout - Research

**Researched:** 2026-03-27
**Domain:** Final benchmark proof, host-variance framing, and milestone closeout packaging for the v2 Rust modernization work
**Confidence:** Medium

## Summary

Phase 6 should prove the modernization work with the benchmark and review artifacts that already exist instead of inventing a new measurement framework. The repo already has the essential pieces: a committed Phase 1 baseline and memory profile, a reusable benchmark harness that supports both bounded fixture captures and dataset-root runs, a regression gate script, explicit Phase 2 and Phase 3 delta notes, and a completed Phase 5 reviewer package plus full-suite verification loop. The planning question is therefore not "how do we benchmark APDR for the first time?" but "how do we turn the existing evidence into a final, reviewable milestone verdict without blurring warm-path continuity, host variance, and broader corpus evidence?"

Primary recommendation: plan Phase 6 as three sequential plans. First, rerun the bounded three-snippet continuity benchmark with the exact Phase 1 contract and refresh the representative memory profile. Second, add a reproducible hard-gists slice and synthesize all benchmark evidence into a dedicated benchmark-verification package that keeps continuity, broader corpus evidence, and forced-validation host variance separate. Third, rerun the final Rust verification gate and write a milestone closeout or signoff artifact that points reviewers back to the benchmark-verification report plus the Phase 4 and Phase 5 review-ready surfaces.

## Phase Requirements

| ID | Requirement | Research Support |
|----|-------------|------------------|
| BENCH-01 | End-to-end benchmark runtime improves measurably versus the v2 baseline | Phase 1 baseline, Phase 2 continuity candidate, Phase 3 continuity candidate, and the regression gate already provide the comparison contract. |
| BENCH-02 | Validation-heavy cases complete faster than the v2 baseline | Phase 3 already separated warm continuity evidence from forced-validation host variance, which Phase 6 can reuse without overstating a Windows-specific result. |
| BENCH-03 | Memory churn or peak memory indicators improve on the targeted Rust workflows | Phase 1 already committed a representative `peak_rss_bytes` artifact and the memory wrapper can reproduce the same measurement shape. |
| BENCH-04 | Benchmark pass rate is maintained or improved after modernization work | The bounded baseline and candidate JSON artifacts already carry pass-rate fields, and the regression gate enforces the floor. |
| BENCH-05 | The final milestone package can survive a codebase review focused on performance, layout, documentation, and standards | Phase 4 and Phase 5 already produced structural summaries, a reviewer guide, and a passing full Rust verification loop that Phase 6 can re-run and cite directly. |

## Evidence That Should Drive Planning

### The benchmark harness already supports the required evidence shapes

`scripts/measure_apdr_baseline.py` already supports:

- `--fixtures-root` for the bounded continuity sample
- `--dataset-root` for broader corpora such as `hard-gists`
- `--limit` for a reproducible bounded slice
- `--force-validate` for explicit validation-path evidence
- machine-readable JSON plus companion Markdown output

That means Phase 6 should keep using the same harness rather than introducing a separate final-phase benchmark tool. The main planning work is choosing the exact bounded and dataset-root outputs and then turning them into reviewer-facing verdicts.

### The continuity comparison contract is already locked

Phase 1, Phase 2, and Phase 3 already established the stable bounded comparison contract:

- same fixture root: `tools/apdr/tests/fixtures`
- same deterministic lexicographic selection rule
- same sample limit: `3`
- same validation backend: `env`
- same baseline JSON contract consumed by `scripts/check_apdr_regression.py`

Phase 2 and Phase 3 also established two important interpretation rules that Phase 6 should preserve:

- bounded continuity evidence must reuse the committed Phase 1 sample rule exactly
- warm-path continuity evidence and forced-validation evidence must not be blended into one claim

### The forced-validation artifact is evidence, not the final gate

Phase 3 already captured the real validation-path evidence that matters most for host variance:

- the warm continuity artifact passes the regression gate because cache reuse removes validation work
- the forced-validation artifact exposes the still-open Windows Docker permission issue
- the delta note already frames that result as host variance rather than a pure APDR regression

That strongly supports the user decision to keep forced-validation host variance as explicit evidence instead of turning it into a Phase 6 completion blocker.

### Memory evidence is still representative and reproducible, but narrow

The memory wrapper in `scripts/profile_apdr_memory.py` can reproduce the Phase 1 artifact shape exactly:

- same snippet: `tools/apdr/tests/fixtures/sample_snippet.py`
- same backend: `env`
- same JSON fields, including `peak_rss_bytes`

This is enough for a like-for-like representative comparison, but it is still a single-snippet measure rather than a whole-corpus memory study. Phase 6 should therefore frame BENCH-03 as representative peak-RSS evidence, not as a full memory-model proof across all workloads.

### The review-readiness gate already exists

Phase 5 closed with all the review-facing surfaces that BENCH-05 needs:

- `.planning/phases/05-documentation-error-handling-and-review-readiness/05-REVIEWER-GUIDE.md`
- `.planning/phases/05-documentation-error-handling-and-review-readiness/05-VALIDATION.md`
- green `cargo fmt --check`, targeted tests, full Rust suite, and `cargo clippy -D warnings`

That means the final closeout phase should rerun the existing verification commands and point back to the reviewer guide rather than define a new review checklist framework.

### `hard-gists/` is in scope, but Phase 6 should preflight slice readability

Project docs and roadmap context still name hard-gists as the comparison corpus. The directory exists at repo root, but a direct shell enumeration in this planning session returned an access-denied error even though the path resolves as a directory. That is not yet proof that the corpus is unusable for execution, but it is a good reason to keep the hard-gists work behind a bounded preflight and an explicit sampling rule instead of assuming the full corpus is immediately readable.

The safest plan shape is therefore:

- choose a reproducible bounded slice, not the entire corpus
- verify dataset-root readability before claiming the slice command is ready
- record any dataset-access issue as evidence or blocker instead of silently skipping the broader corpus output

## Implementation Recommendations

### 1. Plan the phase as three sequential waves

Recommended decomposition:

1. **Bounded continuity and memory refresh**
   - rerun the exact three-snippet continuity sample
   - refresh the representative memory profile
   - produce a Phase 6 continuity delta note against Phase 1 baseline

2. **Broader benchmark-verification package**
   - run a reproducible hard-gists slice
   - synthesize continuity, hard-gists, memory, and host-variance evidence into one dedicated benchmark-verification artifact
   - keep continuity, broader corpus evidence, and forced-validation host variance in separate sections

3. **Milestone closeout and review gate**
   - rerun the full Rust verification commands
   - write a milestone closeout or signoff artifact that points to the benchmark-verification package and the Phase 5 reviewer guide
   - avoid touching unrelated dirty local files if they interfere with the verification gate

This matches the user's split-package decision and avoids mixing benchmark proof with milestone summary too early.

### 2. Keep the bounded continuity rerun identical to the Phase 1 contract

For Phase 6 continuity evidence, planning should keep the exact baseline contract:

- `--fixtures-root tools/apdr/tests/fixtures`
- `--limit 3`
- `--validation-backend env`
- output JSON plus Markdown in the Phase 6 directory
- run `scripts/check_apdr_regression.py` directly against `01-baseline.json`

This is the cleanest evidence for BENCH-01, BENCH-02, and BENCH-04 because it preserves the original regression gate.

### 3. Use a bounded hard-gists slice, not the whole corpus

The broader milestone evidence should be explicit and reproducible. Recommended slice:

- dataset root: `hard-gists`
- selection rule: first `25` `snippet.py` files in lexicographic order, as determined by `measure_apdr_baseline.py`
- backend: `env`
- no `--force-validate` for the slice capture itself

Why this shape:

- it is much broader than the three-snippet continuity gate
- it is still bounded enough to fit into a phase execution loop
- it keeps the forced-validation story separate, which matches the Phase 3 and Phase 6 decisions

### 4. Make the benchmark-verification package carry the BENCH-01 through BENCH-04 verdicts

The dedicated benchmark-verification artifact should contain, at minimum:

- a continuity gate section referencing the fresh Phase 6 bounded candidate plus regression-gate result
- a hard-gists slice section with the exact slice rule and resulting totals
- a memory comparison section against `01-memory-profile.json`
- a host-variance section that cites `03-validation-candidate-forced.json` and `03-VALIDATION-DELTA.md`
- a requirement-verdict table for `BENCH-01` through `BENCH-04`

That package becomes the benchmark-side half of the split closeout design.

### 5. Reuse the existing Phase 5 review gate for BENCH-05

The closeout plan should not redefine review quality. It should rerun:

- `cargo fmt --manifest-path tools/apdr/Cargo.toml --all --check`
- `cargo test --manifest-path tools/apdr/Cargo.toml --test test_resolver resolver_ -- --nocapture`
- `cargo test --manifest-path tools/apdr/Cargo.toml validation_pipeline_ -- --nocapture`
- `cargo test --manifest-path tools/apdr/Cargo.toml -- --nocapture`
- `cargo clippy --manifest-path tools/apdr/Cargo.toml --all-targets -- -D warnings`

Then the milestone closeout artifact can cite:

- benchmark evidence from the dedicated Phase 6 benchmark-verification package
- layout reviewability from Phase 4 summaries
- documentation and standards evidence from Phase 5 guide plus validation contract

### 6. Treat unrelated dirty files as blockers, not phase scope

Current state explicitly says unrelated local edits in:

- `tools/apdr/src/lib.rs`
- `tools/apdr/llm_py/tests/test_llm_integration.py`

must remain untouched. Phase 6 planning should therefore tell executors:

- rerun the Rust verification gate
- fix only Phase 6 or milestone-scope regressions if they are inside relevant benchmark or review artifacts
- if failures are rooted in those unrelated dirty files, record them as blockers rather than modifying them

## Validation Architecture

### Quick checks

- `python -m py_compile scripts/measure_apdr_baseline.py scripts/profile_apdr_memory.py scripts/check_apdr_regression.py`
- `cargo test --manifest-path tools/apdr/Cargo.toml --test test_resolver resolver_ -- --nocapture`
- `cargo test --manifest-path tools/apdr/Cargo.toml validation_pipeline_ -- --nocapture`

### Benchmark checks

- `python scripts/measure_apdr_baseline.py --fixtures-root tools/apdr/tests/fixtures --limit 3 --validation-backend env --output-json .planning/phases/06-benchmark-verification-and-v2-closeout/06-continuity-candidate.json --output-md .planning/phases/06-benchmark-verification-and-v2-closeout/06-CONTINUITY-CANDIDATE.md`
- `python scripts/profile_apdr_memory.py --snippet tools/apdr/tests/fixtures/sample_snippet.py --validation-backend env --output-json .planning/phases/06-benchmark-verification-and-v2-closeout/06-memory-profile.json`
- `python scripts/check_apdr_regression.py --baseline .planning/phases/01-baseline-and-guardrails/01-baseline.json --candidate .planning/phases/06-benchmark-verification-and-v2-closeout/06-continuity-candidate.json`
- `python scripts/measure_apdr_baseline.py --dataset-root hard-gists --limit 25 --validation-backend env --output-json .planning/phases/06-benchmark-verification-and-v2-closeout/06-hard-gists-slice.json --output-md .planning/phases/06-benchmark-verification-and-v2-closeout/06-HARD-GISTS-SLICE.md`

### Phase-close checks

- `cargo fmt --manifest-path tools/apdr/Cargo.toml --all --check`
- `cargo test --manifest-path tools/apdr/Cargo.toml -- --nocapture`
- `cargo clippy --manifest-path tools/apdr/Cargo.toml --all-targets -- -D warnings`
- confirm the benchmark-verification package keeps continuity, hard-gists, memory, and host variance in separate sections
- confirm the milestone closeout artifact references the benchmark-verification package plus the Phase 5 reviewer guide and validation contract

## Canonical Files For Planning

- `.planning/PROJECT.md`
- `.planning/ROADMAP.md`
- `.planning/REQUIREMENTS.md`
- `.planning/STATE.md`
- `.planning/codebase/CONVENTIONS.md`
- `.planning/codebase/TESTING.md`
- `.planning/phases/01-baseline-and-guardrails/01-BASELINE.md`
- `.planning/phases/01-baseline-and-guardrails/01-memory-profile.json`
- `.planning/phases/01-baseline-and-guardrails/01-VALIDATION.md`
- `.planning/phases/02-resolver-memory-and-algorithm-efficiency/02-03-SUMMARY.md`
- `.planning/phases/02-resolver-memory-and-algorithm-efficiency/02-RESOLVER-DELTA.md`
- `.planning/phases/03-validation-pipeline-throughput/03-03-SUMMARY.md`
- `.planning/phases/03-validation-pipeline-throughput/03-VALIDATION-DELTA.md`
- `.planning/phases/03-validation-pipeline-throughput/03-validation-candidate-forced.json`
- `.planning/phases/05-documentation-error-handling-and-review-readiness/05-CONTEXT.md`
- `.planning/phases/05-documentation-error-handling-and-review-readiness/05-REVIEWER-GUIDE.md`
- `.planning/phases/05-documentation-error-handling-and-review-readiness/05-VALIDATION.md`
- `scripts/measure_apdr_baseline.py`
- `scripts/profile_apdr_memory.py`
- `scripts/check_apdr_regression.py`
- `tools/apdr/README.md`

## Out-of-Scope For This Phase

- new resolver or validation optimizations beyond bounded verification or artifact-refresh work
- fixing Windows Docker permissions as a prerequisite for milestone completion
- expanding the hard-gists evidence from a bounded reproducible slice into a full-corpus benchmark campaign
- new reviewer frameworks or documentation systems beyond the existing Phase 5 guide and validation contract
- modifying unrelated local edits in `tools/apdr/src/lib.rs` or `tools/apdr/llm_py/tests/test_llm_integration.py`

---
*Research created: 2026-03-27*
*Phase: 06-benchmark-verification-and-v2-closeout*
