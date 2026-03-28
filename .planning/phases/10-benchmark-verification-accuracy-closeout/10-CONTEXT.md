# Phase 10: Benchmark Verification & Accuracy Closeout - Context

**Gathered:** 2026-03-28
**Status:** Ready for planning

<domain>
## Phase Boundary

Phase 10 reruns the locked targeted benchmark slice, proves the case-level APDR versus baseline versus `pllm` delta, and packages the remaining unrecovered parity cases in a reviewer-readable closeout.

This phase measures the bounded recovery changes delivered by Phases 7 through 9. It does not reopen the canonical `70`-case slice, does not pull the `17`-case watchlist into the main contract, and does not relitigate the Phase 8 family-runtime migration boundary.

</domain>

<decisions>
## Implementation Decisions

### Rerun Scope
- **D-01:** The main benchmark report is the canonical `70`-case tier3 parity slice defined in Phase 7.
- **D-02:** The `17`-case watchlist/overlap set may appear only as a separate appendix or companion artifact. It must not be mixed into the main contract report.

### Evidence Package
- **D-03:** Phase 10 must produce separate artifacts for machine-readable case-level delta data, a reviewer-facing markdown summary, an unrecovered-case report, and a repeatable rerun/check note.
- **D-04:** Benchmark evidence and reviewer closeout notes stay distinct. Do not collapse the machine artifact and the human summary into one file.

### Preservation Gate
- **D-05:** Any regression in a previously passed targeted case is a hard blocker for Phase 10 closeout.
- **D-06:** Any drift in expected `host-runtime`, `unsolvable`, or `local-helper` skip behavior on the rerun is also a hard blocker.

### Remaining Gap Reporting
- **D-07:** Remaining unrecovered parity cases must be grouped by dominant failure bucket.
- **D-08:** The unrecovered-case writeup must include case IDs and short follow-on notes for each remaining gap, not just bucket totals.

### the agent's Discretion
- Artifact filenames, exact checker names, and report formatting are open as long as they preserve the separate-artifact contract above and keep the rerun path repeatable.

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Phase Contract And Locked Boundaries
- `.planning/PROJECT.md` - milestone intent, locked Phase 7 to Phase 9 decisions, and the requirement that Phase 10 package targeted deltas without reopening the migration boundary
- `.planning/REQUIREMENTS.md` - authoritative requirement text for `REC-05`, `EVD-01`, and `EVD-02`
- `.planning/ROADMAP.md` - Phase 10 goal, success criteria, and dependency on Phase 9
- `.planning/STATE.md` - current workflow position for Phase 10
- `.planning/phases/07-failure-baseline-parity-slice/07-BASELINE.md` - locked statement of the canonical `70`-case slice, normalized buckets, touched-family subset, and `17`-case watchlist boundary
- `.planning/phases/07-failure-baseline-parity-slice/07-TIER3-PARITY-MANIFEST.md` - reviewer-facing description of the Phase 7 parity slice contract
- `.planning/phases/07-failure-baseline-parity-slice/07-tier3-parity-manifest.json` - JSON source of truth for canonical case IDs, watchlist IDs, and bucket totals
- `.planning/phases/08-data-driven-family-knowledge-runtime/08-FAMILY-RUNTIME.md` - locked Phase 8 family-runtime boundary and checker contract
- `.planning/phases/09-targeted-tier3-recovery-accuracy/09-TARGETED-RECOVERY.md` - bounded Phase 9 recovery surface and explicit Phase 10 handoff measurements

### Locked Benchmark Inputs
- `runs/20260327-150339-apdr/summary.json` - locked APDR benchmark summary used to define the parity slice and compare rerun deltas
- `pllm_results/csv/summary-all-runs.csv` - locked `pllm` comparison corpus for overlap and case-level delta reporting

### Existing Deterministic Checks
- `scripts/check_phase7_baseline.py` - validates the Phase 7 parity manifest, family snapshot manifest, and baseline note against the locked benchmark inputs
- `scripts/check_phase8_family_runtime.py` - validates the bounded touched-family runtime contract that Phase 10 must preserve
- `scripts/check_phase9_targeted_recovery.py` - validates the Phase 9 targeted-recovery coverage and handoff contract that Phase 10 measures
- `scripts/measure_apdr_baseline.py` - existing bounded benchmark runner pattern for machine-readable JSON plus optional markdown output

### Targeted Recovery Sources Being Measured
- `tools/apdr/data/recovery/README.md` - scope rules for the bounded Phase 9 recovery policy data
- `tools/apdr/data/recovery/module_rules.json` - module-provider and stop-reason rules that Phase 10 must measure, not redefine
- `tools/apdr/data/recovery/compatibility_rules.json` - compatibility, companion, and Python-ceiling rules that Phase 10 must measure, not redefine
- `tools/apdr/data/family_knowledge/touched_families.json` - curated Phase 8 touched-family registry that must remain stable
- `tools/apdr/data/family_knowledge/touched_recovery_rules.json` - curated Phase 8 touched-family recovery rules that must remain stable

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `scripts/measure_apdr_baseline.py`: already implements a repeatable benchmark runner that writes machine-readable JSON and optional markdown summaries for a bounded snippet set
- `scripts/check_phase7_baseline.py`, `scripts/check_phase8_family_runtime.py`, and `scripts/check_phase9_targeted_recovery.py`: existing deterministic checker pattern for validating benchmark artifacts, reviewer notes, and locked boundaries
- `benchmark_ui/runner.py`: benchmark worker already creates `summary.json`, `benchmark-context.log`, and per-case artifact directories under `runs/<run-id>/`
- `benchmark_ui/service.py`: historical-run loading and run snapshot shaping already expose saved benchmark summaries to the UI and closeout flow

### Established Patterns
- Benchmark evidence in this repo is JSON-first with markdown companion notes instead of markdown-only reporting
- Locked parity and recovery scope is driven from manifest and curated data files rather than rediscovered ad hoc from a fresh run
- Phase closeout checkers live in `scripts/` and validate both structured artifacts and reviewer-facing markdown headings/content

### Integration Points
- Phase 10 should consume the locked benchmark inputs in `runs/20260327-150339-apdr/summary.json` and `pllm_results/csv/summary-all-runs.csv`
- Delta reporting should anchor to `.planning/phases/07-failure-baseline-parity-slice/07-tier3-parity-manifest.json` for canonical case membership and watchlist separation
- Preservation checks must keep passing against the Phase 8 and Phase 9 checker surfaces instead of inventing a new boundary source

</code_context>

<specifics>
## Specific Ideas

- The main reviewer narrative should talk about the canonical `70`-case slice as the contract and push the `17`-case watchlist into a clearly separated appendix or companion artifact.
- Remaining unrecovered cases should stay easy to triage: dominant bucket first, then case IDs plus short follow-on notes.
- The final rerun package should make it obvious how to repeat the comparison path later without reading implementation code first.

</specifics>

<deferred>
## Deferred Ideas

None - discussion stayed within the Phase 10 benchmark closeout boundary.

</deferred>

---

*Phase: 10-benchmark-verification-accuracy-closeout*
*Context gathered: 2026-03-28*
