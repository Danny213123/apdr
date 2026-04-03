# Phase 28-02 Summary

Phase 28-02 made final failure truth additive and explicit so benchmark cases no longer need to infer whether a miss came from model no-output, provider/tooling instability, Docker infrastructure, or a real dependency/runtime failure.

Shipped changes:

- `tools/apdr/src/lib.rs` now exports `recovery_outcome`, `failure_truth_class`, `failure_truth_detail`, and `recovery_attempts_path` through report and summary outputs.
- `tools/apdr/src/resolver/recovery_diagnostics.rs` now derives final failure truth from recovery attempts and Docker/runtime evidence while preserving the coarse `failure_family`.
- `benchmark_ui/runner.py` and `benchmark_ui/service.py` now surface `recoveryOutcome`, `failureTruthClass`, `failureTruthDetail`, and `recoveryAttemptsPath` for saved and live case inspection.
- `benchmark_ui/test_run_contract.py` and `benchmark_ui/test_runner_events.py` now pin the new benchmark truth keys in row-building and live-event flows.

Verification:

- `cargo test --manifest-path tools/apdr/Cargo.toml phase28_truth_ -- --nocapture`
- `python3 -m unittest benchmark_ui.test_run_contract benchmark_ui.test_runner_events`
