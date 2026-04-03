# Phase 29-01 Summary

Phase 29-01 locked a six-case fixed slice and added a paired artifact harness for both `llm` and `llm-only`, anchored to the April 2 before-state runs instead of ad hoc sample rows.

Shipped changes:

- Added `29-benchmark-slice.json` with the locked six-case comparison slice and explicit baseline anchors `20260402-003618-apdr` for `llm` and `20260402-184821-apdr` for `llm-only`.
- Added paired frozen fixture summaries for both modes: `29-llm-baseline-fixture-summary.json`, `29-llm-candidate-fixture-summary.json`, `29-llm-only-baseline-fixture-summary.json`, and `29-llm-only-candidate-fixture-summary.json`.
- Added `scripts/run_phase29_llm_benchmark.py` to materialize normalized comparison artifacts while preserving Phase 26-28 truth surfaces such as authored-plan, Docker-plan, recovery-attempt, and failure-truth pointers.
- Generated frozen sample artifacts for both modes so later phases can compare pass, timing, and failure-truth deltas without depending on mutable live runs.

Verification:

- `python3 scripts/run_phase29_llm_benchmark.py --slice-json .planning/phases/29-llm-benchmark-gains-and-regression-harness/29-benchmark-slice.json --summary-json .planning/phases/29-llm-benchmark-gains-and-regression-harness/29-llm-baseline-fixture-summary.json --output-json /tmp/phase29-llm-baseline.json --mode llm --variant baseline --probe-only`
- `python3 scripts/run_phase29_llm_benchmark.py --slice-json .planning/phases/29-llm-benchmark-gains-and-regression-harness/29-benchmark-slice.json --summary-json .planning/phases/29-llm-benchmark-gains-and-regression-harness/29-llm-only-baseline-fixture-summary.json --output-json /tmp/phase29-llm-only-baseline.json --mode llm-only --variant baseline --probe-only`
- `python3.12 -m py_compile scripts/run_phase29_llm_benchmark.py`
