# Phase 28-03 Summary

Phase 28-03 froze the new recovery and failure-truth contract into deterministic samples, a checker, and a reviewer-readable proof note so Phase 29 can compare benchmark deltas against stable semantics instead of moving targets.

Shipped changes:

- Added `28-recovery-applied-sample.json` to freeze the bounded recovery-attempt contract.
- Added `28-failure-truth-sample.json` to freeze the final-failure truth contract.
- Added `scripts/check_phase28_recovery_truth.py` to validate both samples together.
- Added `28-RECOVERY-TRUTH-PROOF.md` to document exactly what Phase 28 proves and what stays deferred to Phase 29 benchmark comparison work.

Verification:

- `python3 scripts/check_phase28_recovery_truth.py --applied-json .planning/phases/28-llm-recovery-loop-and-failure-semantics/28-recovery-applied-sample.json --truth-json .planning/phases/28-llm-recovery-loop-and-failure-semantics/28-failure-truth-sample.json --status-json .planning/phases/28-llm-recovery-loop-and-failure-semantics/28-recovery-truth-status.json --probe-only`
- `git diff --check`
