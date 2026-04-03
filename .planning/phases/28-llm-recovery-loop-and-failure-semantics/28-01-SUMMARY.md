# Phase 28-01 Summary

Phase 28-01 expanded the LLM recovery contract so recovery can see authored intake truth, authored Docker intent, and the latest executed Docker artifact pointers instead of only a flattened last-log string.

Shipped changes:

- `tools/apdr/llm_py/models.py` adds structured recovery artifact pointers plus `recovery_outcome`, `failure_class`, and `diagnostic_preview`.
- `tools/apdr/llm_py/prompts.py` now includes authored-plan, Docker-plan, intake-failure, and executed-artifact context in the recovery prompt.
- `tools/apdr/llm_py/actions/recovery.py` now returns structured abstain and provider-failure truth instead of collapsing every no-output path into a generic empty response.
- `tools/apdr/src/resolver/tier3_llm/core.rs` now threads the richer recovery request across the Rust/Python seam and preserves structured recovery metadata even when no fix is applied.
- `tools/apdr/src/resolver/retry_loop.rs` now persists bounded machine-readable recovery attempts to `recovery-attempts.json` and stops repeated provider/no-output failures after a small fixed number.

Verification:

- `cargo test --manifest-path tools/apdr/Cargo.toml phase28_recovery_ -- --nocapture`
- `python3.12 -m py_compile tools/apdr/llm_py/models.py tools/apdr/llm_py/prompts.py tools/apdr/llm_py/actions/recovery.py tools/apdr/llm_py/tests/test_recovery_mock.py`

Constraint note:

- The planned `pytest` command could not run in this environment because the available Python interpreter does not have `pytest` installed locally.
