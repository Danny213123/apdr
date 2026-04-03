# Phase 29-02 Summary

Phase 29-02 turned the fixed-slice artifacts into deterministic delta contracts so reviewers can see `llm` and `llm-only` separately instead of being forced into one blended pass-rate story.

Shipped changes:

- Added `scripts/check_phase29_benchmark_delta.py` to validate slice parity, run-contract parity, and required delta surfaces for both modes.
- Added `29-llm-benchmark-status.json` and `29-llm-only-benchmark-status.json` as frozen checker outputs for the Phase 29 proof package.
- Added `29-BENCHMARK-DELTA.md` with mode-specific pass, timing, `llm-no-output`, provider-tooling, and `docker-infrastructure-failure` deltas.
- Froze four comparison artifacts: `29-llm-baseline-sample.json`, `29-llm-candidate-sample.json`, `29-llm-only-baseline-sample.json`, and `29-llm-only-candidate-sample.json`.

Verification:

- `python3 scripts/check_phase29_benchmark_delta.py --baseline-artifact /tmp/phase29-llm-baseline.json --candidate-artifact /tmp/phase29-llm-candidate.json --status-json /tmp/phase29-llm-status.json --mode llm --probe-only`
- `python3 scripts/check_phase29_benchmark_delta.py --baseline-artifact /tmp/phase29-llm-only-baseline.json --candidate-artifact /tmp/phase29-llm-only-candidate.json --status-json /tmp/phase29-llm-only-status.json --mode llm-only --probe-only`
- `python3.12 -m py_compile scripts/check_phase29_benchmark_delta.py`
