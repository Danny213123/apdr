# 21-01 Summary

- Extended `scripts/run_phase20_recovery_benchmark.py` so the fixed-slice runner can do real live replays, resume a saved run, merge `historical_results` with live rows, and preserve interrupted cases explicitly when a live case never writes `resolution-report.txt`.
- Captured the frozen March 30, 2026 baseline as `.planning/phases/21-live-evidence-and-closeout-pack/21-baseline-live.json`.
- Captured the live candidate as `.planning/phases/21-live-evidence-and-closeout-pack/21-candidate-live.json` from the resumed run `runs/20260401-173232-apdr`.
- Wrote `.planning/phases/21-live-evidence-and-closeout-pack/21-LIVE-RUNBOOK.md` with baseline provenance, candidate provenance, exact commands, and the fixed-slice result contract.
