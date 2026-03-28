# Phase 6: Benchmark Verification & v2 Closeout - Context

**Gathered:** 2026-03-27
**Status:** Ready for planning

<domain>
## Phase Boundary

Validate that the v2 Rust modernization work delivered measurable benchmark and review-quality improvements by producing final benchmark evidence, documenting host-specific variance, and packaging a milestone closeout/signoff set. Phase 6 proves and summarizes the completed Phase 1 through Phase 5 work; it does not add new product capabilities or reopen earlier refactor scope except for bounded reruns and artifact refreshes needed for closeout.

</domain>

<decisions>
## Implementation Decisions

### Benchmark Evidence Scope
- **D-01:** Phase 6 should keep the bounded three-snippet sample as the official continuity and regression-gate benchmark because it is the committed comparison contract used in earlier phases.
- **D-02:** Phase 6 should add a hard-gists benchmark slice or report as milestone-level evidence so the closeout also reflects the stated comparison corpus in the active project scope.
- **D-03:** The bounded continuity evidence and the hard-gists evidence must be reported as distinct views of the milestone rather than blended into a single undifferentiated benchmark claim.

### Host Variance Policy
- **D-04:** The existing forced-validation evidence on this Windows host should remain part of the closeout package as explicit evidence about validation-path behavior.
- **D-05:** The known Windows Docker permission issue is not a Phase 6 completion blocker; it should be documented clearly as host variance and kept separate from APDR regression claims.

### Closeout Package Shape
- **D-06:** Phase 6 should produce a split package: one artifact set for benchmark verification evidence and a separate milestone closeout/signoff summary.
- **D-07:** The milestone signoff artifact should synthesize the benchmark, memory, review-readiness, and standards-compliance outcomes without replacing the underlying benchmark evidence artifacts.

### Final Verification Gate
- **D-08:** Phase 6 should use a hybrid verification gate that reruns fresh benchmark evidence for the bounded continuity sample plus the chosen hard-gists slice.
- **D-09:** The same Phase 6 gate should also rerun the existing Rust verification and reviewer-facing checks already established in earlier phases instead of relying only on artifact synthesis.

### the agent's Discretion
- The planner may choose the exact hard-gists slice size and sampling rule, as long as it is explicit, reproducible, and kept separate from the bounded continuity gate.
- The planner may choose the exact filenames and structure of the split Phase 6 deliverables, as long as benchmark evidence and milestone signoff remain distinct.
- The planner may decide whether the final memory comparison is refreshed through a new representative capture or synthesized from the committed baseline and later artifacts, provided BENCH-03 stays evidence-based.

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Milestone scope and state
- `.planning/PROJECT.md` - active milestone scope, including the hard-gists comparison-corpus constraint and v2 modernization goal.
- `.planning/REQUIREMENTS.md` - Phase 6 requirement IDs `BENCH-01` through `BENCH-05` and the current traceability map.
- `.planning/ROADMAP.md` - Phase 6 goal, success criteria, and milestone sequencing.
- `.planning/STATE.md` - current ready-to-plan state and the carry-forward notes that point Phase 6 at the committed benchmark artifacts.

### Baseline and benchmark evidence
- `.planning/phases/01-baseline-and-guardrails/01-BASELINE.md` - committed three-snippet baseline totals, sample rule, and baseline command contract.
- `.planning/phases/01-baseline-and-guardrails/01-memory-profile.json` - representative peak-RSS baseline artifact for BENCH-03 comparisons.
- `.planning/phases/01-baseline-and-guardrails/01-VALIDATION.md` - baseline validation contract and canonical guardrail-command loop.
- `.planning/phases/02-resolver-memory-and-algorithm-efficiency/02-RESOLVER-DELTA.md` - committed resolver hot-path delta against the baseline.
- `.planning/phases/03-validation-pipeline-throughput/03-VALIDATION-DELTA.md` - continuity-versus-forced validation evidence and the Windows Docker variance framing to preserve in Phase 6.

### Review-readiness and signoff inputs
- `.planning/phases/05-documentation-error-handling-and-review-readiness/05-CONTEXT.md` - locked reviewer-guide and panic-path decisions that the closeout package must preserve.
- `.planning/phases/05-documentation-error-handling-and-review-readiness/05-REVIEWER-GUIDE.md` - reviewer-facing ownership, fallback, and verification guidance for the five modernized Rust areas.
- `.planning/phases/05-documentation-error-handling-and-review-readiness/05-VALIDATION.md` - exact verification commands and validation cadence already adopted for review readiness.
- `.planning/codebase/CONVENTIONS.md` - Rust documentation, naming, and error-handling conventions relevant to BENCH-05 review claims.
- `.planning/codebase/TESTING.md` - existing Rust and Python test patterns relevant to the final verification gate.

### Tooling and dataset surfaces
- `scripts/measure_apdr_baseline.py` - benchmark harness that supports both fixture-root continuity captures and dataset-root runs such as hard-gists, with optional forced validation.
- `scripts/profile_apdr_memory.py` - representative peak-memory capture wrapper for APDR executions.
- `scripts/check_apdr_regression.py` - regression gate against the committed Phase 1 baseline artifact.
- `hard-gists/` - repo-local comparison corpus for the additional Phase 6 milestone evidence slice.

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `scripts/measure_apdr_baseline.py`: already emits machine-readable JSON plus reviewer-facing Markdown for both bounded fixture runs and broader dataset runs.
- `scripts/profile_apdr_memory.py`: captures representative peak RSS without adding new Rust instrumentation.
- `scripts/check_apdr_regression.py`: provides the existing continuity gate against `.planning/phases/01-baseline-and-guardrails/01-baseline.json`.
- `.planning/phases/03-validation-pipeline-throughput/03-VALIDATION-DELTA.md`: already separates warm-path continuity evidence from forced-validation evidence and documents the Windows Docker variance.
- `.planning/phases/05-documentation-error-handling-and-review-readiness/05-REVIEWER-GUIDE.md`: already packages the review-facing module and verification context that the milestone signoff should point back to.

### Established Patterns
- Benchmark claims should compare candidate artifacts against the committed Phase 1 baseline before claiming a win.
- Warm-path continuity evidence and forced-validation evidence should stay separate, with host-specific Docker variance reported explicitly.
- Benchmark artifacts should be JSON-first with companion Markdown summaries rather than one-off narrative-only notes.
- Final review guidance should reuse the existing verification commands and reviewer guide instead of creating a new framework.

### Integration Points
- Phase 6 benchmark outputs should be written under `.planning/phases/06-benchmark-verification-and-v2-closeout/` and tie back to the committed Phase 1 baseline artifacts.
- The bounded continuity rerun should flow through `scripts/measure_apdr_baseline.py` plus `scripts/check_apdr_regression.py`.
- The additional milestone evidence should use the same harness against `hard-gists/` so the reporting shape stays consistent with earlier benchmark artifacts.
- The milestone signoff artifact should synthesize the Phase 2 through Phase 5 deltas, summaries, and reviewer guide rather than duplicating their internal analysis.

</code_context>

<specifics>
## Specific Ideas

- Keep the bounded continuity benchmark, the hard-gists milestone slice, and the forced-validation host-variance evidence in clearly separate sections so reviewers do not mistake one for another.
- Treat Phase 6 as proof-and-closeout work, not as a new optimization phase.
- Reuse existing scripts and verification commands rather than adding a fresh benchmark framework for the final milestone package.

</specifics>

<deferred>
## Deferred Ideas

None - discussion stayed within phase scope.

</deferred>

---

*Phase: 06-benchmark-verification-and-v2-closeout*
*Context gathered: 2026-03-27*
