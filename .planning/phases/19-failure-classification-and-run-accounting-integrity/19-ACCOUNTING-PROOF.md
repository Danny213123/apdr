# Phase 19 Accounting Proof

## Slice Contract

Phase 19 proof stays anchored to `.planning/phases/19-failure-classification-and-run-accounting-integrity/19-live-accounting-slice.json`, which freezes the March 30, 2026 live-derived review surface to these four snippets:

- `hard-gists/0115e0ce312f26ff59f4fbf4f5821ca2/snippet.py`
- `hard-gists/00135b0dfee0ae165ad2/snippet.py`
- `hard-gists/04ef258fa29e4e685287a30cf60462d0/snippet.py`
- `hard-gists/00e9638c0efad1adac878522cf172484/snippet.py`

The wrapper summary at `runs/20260330-020943-apdr/summary.json` resolves those fixed `relative_path` entries to resumed predecessor case artifacts under `runs/20260329-165524-apdr/cases/...`. The frozen slice records the observed March 30 validation status and reason for each case while also locking the reviewer-facing Phase 19 expectation for `expected_display_status` and `expected_failure_family`.

## Mixed Provenance Contract

`.planning/phases/19-failure-classification-and-run-accounting-integrity/19-mixed-provenance-fixture.json` is the bounded accounting contract for resumed-run truth. It contains an explicit `historical_results` block and a separate live `results` block so the checker can require two views at the same time:

- the combined operational view still reports all completed rows
- the live-only view excludes historical resume rows from current-run conclusions

The fixture is intentionally asymmetric: it has one historical skip plus two live rows, so the combined and live-only totals must differ. The machine-readable output in `19-accounting-proof-status.json` records both the classification checks and the mixed-provenance count checks.

## Probe Command

```text
python3 scripts/check_phase19_accounting.py --slice-json .planning/phases/19-failure-classification-and-run-accounting-integrity/19-live-accounting-slice.json --fixture-json .planning/phases/19-failure-classification-and-run-accounting-integrity/19-mixed-provenance-fixture.json --status-json .planning/phases/19-failure-classification-and-run-accounting-integrity/19-accounting-proof-status.json --probe-only
```

This is the deterministic review gate for Phase 19. It verifies the locked slice ordering, checks the frozen March 30 statuses and reasons against the source summary, and then runs the mixed-provenance fixture through the benchmark service snapshot logic to confirm that live-only counts stay clean.

## Before/After Review

Before the Phase 19 changes, host-runtime cases could be flattened into dependency-style conclusions downstream because benchmark readers upgraded some skipped cases into passes when `requirements.txt` existed and the wrapper exited zero. Resume provenance was also mixed into the current run because historical rows were seeded directly into the new summary `results` list, making later readers treat old rows as if they were live conclusions.

After the Phase 19 changes, reviewers should require all of these conditions:

- host-runtime and framework-runtime blockers still display as `SKIP`, not `PASS`
- true dependency misses still display as `FAIL`
- per-case artifacts surface `failure_family` so environment-specific cases stay distinct from dependency-resolution failures
- resumed historical rows remain available for operator context, but live-only counts exclude them from current-run conclusions

If the checker loses the fixed slice ordering, stops matching the March 30 source summary, or reports identical combined and live-only counts for the mixed-provenance fixture, the Phase 19 proof contract has not been met.
