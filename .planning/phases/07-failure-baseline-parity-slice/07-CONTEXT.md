# Phase 7: Failure Baseline & Parity Slice - Context

**Gathered:** 2026-03-28
**Status:** Ready for planning

<domain>
## Phase Boundary

Turn the March 27, 2026 stopped APDR run and the matching `pllm` comparison CSV into a reproducible milestone baseline for the tier3 parity failures, and add benchmark-derived regression snapshots for the touched family-knowledge cases before Phase 8 moves that behavior into data files. This phase defines the accuracy contract and protects current behavior; it does not try to improve recovery yet.

</domain>

<decisions>
## Implementation Decisions

### Canonical Slice
- **D-01:** The canonical Phase 7 improvement slice is the `70` tier3 cases where APDR failed in `runs/20260327-150339-apdr` and `pllm_results/csv/summary-all-runs.csv` shows at least one pass.
- **D-02:** The `17` tier1 APDR-failed but `pllm`-passing cases are out of the Phase 7 milestone contract and should not define the baseline for Phase 8 through Phase 10.
- **D-03:** Later milestone delta claims should measure against this fixed `70`-case tier3 slice rather than the broader `87`-case overlap.

### Baseline Artifact Shape
- **D-04:** Phase 7 should create a script-generated JSON manifest as the source of truth for the canonical slice.
- **D-05:** Phase 7 should also produce a Markdown summary companion for reviewer-readable baseline context.
- **D-06:** The manifest must be reproducible from the stopped-run summary plus the `pllm` CSV with deterministic ordering and case-level labels, not just aggregate counts.

### Family Regression Snapshot Scope
- **D-07:** Phase 7 should add benchmark-derived regression snapshots now rather than relying only on the existing deterministic family fixtures.
- **D-08:** Benchmark-derived snapshot coverage should be limited to the touched family-knowledge cases expected to move into Phase 8 data files.
- **D-09:** Existing deterministic family fixtures and resolver tests remain part of the regression surface, but they are not sufficient by themselves for the migration boundary.

### Failure-Bucket Normalization
- **D-10:** Every canonical tier3 case must receive one explicit Phase 7 normalized milestone bucket for later reporting.
- **D-11:** Bucket normalization should use documented precedence derived from the stopped-run artifacts while preserving the raw APDR fields alongside the normalized value.
- **D-12:** Later delta reporting should group by the normalized Phase 7 bucket instead of trusting the raw stopped-run fields directly.

### the agent's Discretion
- The planner may choose the exact filenames for the generated JSON manifest and Markdown summary as long as both live under the Phase 7 directory and are easy to rerun.
- The planner may define the exact deterministic rule for identifying which canonical parity cases count as touched family-knowledge cases, as long as the rule is grounded in the existing family-knowledge runtime and is written into the artifact.
- The planner may define the exact normalized-bucket precedence order and representation for missing raw fields, as long as it is deterministic and the raw APDR values are preserved next to the normalized field.
- The planner may choose whether benchmark-derived snapshots land as structured fixture inputs, structured expected-output snapshots, or both, as long as touched family behavior is locked before Phase 8 changes runtime behavior.

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Milestone scope and carry-forward conventions
- `.planning/PROJECT.md` - active v2.1 scope, parity counts, and milestone constraints.
- `.planning/REQUIREMENTS.md` - Phase 7 requirement IDs `FAM-04` and `REC-01`.
- `.planning/ROADMAP.md` - Phase 7 goal, success criteria, and dependency chain into Phases 8 through 10.
- `.planning/STATE.md` - current v2.1 session state and immediate next-step context.
- `.planning/phases/06-benchmark-verification-and-v2-closeout/06-CONTEXT.md` - carries forward the benchmark-artifact convention that evidence should be JSON-first with a reviewer-readable Markdown companion.

### Raw parity inputs
- `runs/20260327-150339-apdr/summary.json` - stopped-run source of truth with per-case APDR metadata, tier labels, and raw validation fields.
- `runs/20260327-150339-apdr/benchmark-context.log` - raw benchmark trace log for ambiguous-case inspection when normalization needs more than summary fields.
- `pllm_results/csv/summary-all-runs.csv` - comparison source for determining which APDR-failed cases `pllm` passed and by how often.

### Family-knowledge runtime and regression surfaces
- `tools/apdr/src/resolver/family_knowledge/mod.rs` - current family-knowledge public facade and module split.
- `tools/apdr/src/resolver/family_knowledge/core.rs` - curated static family tables, explicit namespace mappings, and family-aware recovery logic that Phase 8 will start migrating.
- `tools/apdr/src/resolver/family_knowledge/detection.rs` - family detection helpers and namespace-allowance rules used to decide whether a parity case is family-related.
- `tools/apdr/src/resolver/family_knowledge/learned.rs` - existing learned-family JSON persistence path, useful for separating curated static knowledge from persisted learned knowledge.
- `tools/apdr/tests/test_resolver.rs` - current resolver regression coverage for family behavior.
- `tools/apdr/tests/fixtures/` - deterministic fixture corpus with existing family-heavy snippets such as `cfscrape`, legacy Flask, legacy PyMC3, legacy TensorFlow, johnny-cache, and OpenCV-style cases.

### Artifact patterns and adjacent data surfaces
- `scripts/measure_apdr_baseline.py` - established JSON-plus-Markdown artifact pattern for reproducible benchmark captures.
- `tools/apdr/data/seed/reference_aliases.tsv` - existing data-backed alias surface adjacent to the future family-knowledge migration.
- `tools/apdr/data/seed/pipreqs_mapping.tsv` - existing package-mapping data surface adjacent to Phase 8's data-driven family work.

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `runs/20260327-150339-apdr/summary.json` already contains the per-case fields Phase 7 needs: `artifact_dir`, `tier`, `succeeded`, `skipped`, `llm_calls`, `requirements`, `output_metadata`, and `log_tail`.
- `scripts/measure_apdr_baseline.py` already establishes the repo pattern of writing machine-readable JSON plus Markdown summaries under the current phase directory.
- `tools/apdr/tests/test_resolver.rs` and `tools/apdr/tests/fixtures/` already provide a deterministic regression harness for family-heavy scenarios.
- `tools/apdr/src/resolver/family_knowledge/core.rs`, `detection.rs`, and `learned.rs` already separate curated static rules, family detection, and learned JSON persistence, which makes it possible to define a touched-family boundary without reopening broad resolver architecture work.
- `tools/apdr/data/seed/reference_aliases.tsv` and `pipreqs_mapping.tsv` show that APDR already has adjacent data-driven mapping surfaces that Phase 8 can align with.

### Established Patterns
- Benchmark evidence in this repo should be reproducible, JSON-first, and paired with a short Markdown summary rather than existing only as one-off analysis notes.
- Reviewable regression protection should prefer deterministic fixture-and-test coverage, with any heavier benchmark-derived evidence clearly scoped and labeled.
- Family knowledge is already split between curated static Rust logic and persisted learned JSON; Phase 7 should define the migration boundary before Phase 8 changes that balance.
- Raw benchmark fields can be noisy or incomplete, so reviewer-facing artifacts should preserve raw values while adding a clearer derived label when later reporting depends on it.

### Integration Points
- New Phase 7 artifacts should live under `.planning/phases/07-failure-baseline-parity-slice/`.
- The canonical slice manifest should be generated from `runs/20260327-150339-apdr/summary.json` plus `pllm_results/csv/summary-all-runs.csv`.
- Touched-family snapshot work should connect back to `tools/apdr/tests/fixtures/` and `tools/apdr/tests/test_resolver.rs` or adjacent structured regression artifacts under the Phase 7 directory.
- The touched-family classifier should be grounded in the current family-knowledge runtime, especially `core.rs` and `detection.rs`, so Phase 8 can migrate only the behavior protected by the Phase 7 baseline.

</code_context>

<specifics>
## Specific Ideas

- The milestone contract should be explicit that the canonical slice is the `70` tier3 parity cases, not the full `87`-case APDR-failed overlap.
- The dominant canonical-tier buckets visible in local analysis are `environment-build-failed`, `module-not-found`, `dependency-conflict`, `version-not-found`, `syntax-error`, and a very small ambiguous remainder that normalization should resolve.
- Benchmark-derived snapshots should focus on cases that exercise the family-knowledge surfaces most likely to move out of curated Rust tables in Phase 8.

</specifics>

<deferred>
## Deferred Ideas

- The `17` tier1 APDR-failed but `pllm`-passing cases remain available as a future watchlist, but they are outside the Phase 7 milestone contract.

</deferred>

---

*Phase: 07-failure-baseline-parity-slice*
*Context gathered: 2026-03-28*
