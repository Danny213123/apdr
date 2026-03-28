# Phase 12 Live Proof

Generated from the Phase 12 probe-only preflight on 2026-03-28.

## Live Readiness

- Requested mode: `live-proof`
- Actual mode: `probe-only`
- Terminal state: `ready-for-live-rerun`
- Live ready: `true`
- Locked canonical surface: 70 cases
- Watchlist surface: 17 cases
- Preservation guards: 11 cases

Phase 12 can proceed to the live targeted rerun path. This probe does not count as live proof by itself; it only confirms that the required manifest, baseline inputs, `pllm` comparison CSV, and local `apdr` executable are present and invocable.

## Command Contract

Phase 12 proof attempts must use the hardened wrapper in explicit live mode. The readiness probe was recorded with:

```text
python scripts/run_phase10_targeted_benchmark.py --manifest-json .planning/phases/10-benchmark-verification-accuracy-closeout/10-targeted-rerun-manifest.json --baseline-summary runs/20260327-150339-apdr/summary.json --pllm-csv pllm_results/csv/summary-all-runs.csv --status-json .planning/phases/12-live-benchmark-proof-and-requirement-reconciliation/12-live-proof-status.json --probe-only --require-live --apdr-command D:\apdr\tools\apdr\target\debug\apdr.exe
```

For the actual live rerun, keep `--require-live` and `--status-json`, remove `--probe-only`, and provide explicit rerun output targets. `--dry-run` remains available for analysis, but it does not satisfy the Phase 12 live-proof requirement.

Live rerun classification now depends on emitted `output_data_*.yml` metadata and refreshed `requirements.txt`, matching the benchmark runner behavior instead of relying only on subprocess return codes or log tails.

## Blocking Conditions

The refreshed milestone audit allows exactly two terminal states for this proof flow:

- Ready for a live targeted rerun
- Hard blocker

Current blocker state: none.

A hard blocker must be recorded if any of the following become false before the live rerun is claimed:

- The locked manifest, baseline summary, or `pllm` CSV is missing or unreadable.
- The explicit `apdr` command is missing or no longer invocable.
- The wrapper cannot write the machine-readable status artifact before reporting a terminal state.
- The live rerun request falls back to dry-run semantics instead of executing explicit live mode.
