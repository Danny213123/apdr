# Phase 29 Benchmark Runbook

This runbook explains how to regenerate the Phase 29 comparison artifacts. If these files drift or are deleted, rerun [`$gsd-execute-phase 29`](/Users/dannyguan/.codex/skills/gsd-execute-phase/SKILL.md) or follow the commands below directly.

The fixed before-state anchors are:

- `llm`: `20260402-003618-apdr`
- `llm-only`: `20260402-184821-apdr`

Those April 2 runs remain the before-state because they capture the pre-v2.5 behavior that still suffered from `llm-no-output` and `docker-infrastructure-failure` patterns.

## Probe-Only Extraction

Use the frozen fixture summaries when you want a deterministic proof check without launching APDR.

### 1. Generate the `llm` baseline artifact

```bash
python3 scripts/run_phase29_llm_benchmark.py \
  --slice-json .planning/phases/29-llm-benchmark-gains-and-regression-harness/29-benchmark-slice.json \
  --summary-json .planning/phases/29-llm-benchmark-gains-and-regression-harness/29-llm-baseline-fixture-summary.json \
  --output-json /tmp/phase29-llm-baseline.json \
  --mode llm \
  --variant baseline \
  --probe-only
```

### 2. Generate the `llm` candidate artifact

```bash
python3 scripts/run_phase29_llm_benchmark.py \
  --slice-json .planning/phases/29-llm-benchmark-gains-and-regression-harness/29-benchmark-slice.json \
  --summary-json .planning/phases/29-llm-benchmark-gains-and-regression-harness/29-llm-candidate-fixture-summary.json \
  --output-json /tmp/phase29-llm-candidate.json \
  --mode llm \
  --variant candidate \
  --probe-only
```

### 3. Compare the `llm` pair

```bash
python3 scripts/check_phase29_benchmark_delta.py \
  --baseline-artifact /tmp/phase29-llm-baseline.json \
  --candidate-artifact /tmp/phase29-llm-candidate.json \
  --status-json /tmp/phase29-llm-status.json \
  --mode llm \
  --probe-only
```

### 4. Generate the `llm-only` baseline artifact

```bash
python3 scripts/run_phase29_llm_benchmark.py \
  --slice-json .planning/phases/29-llm-benchmark-gains-and-regression-harness/29-benchmark-slice.json \
  --summary-json .planning/phases/29-llm-benchmark-gains-and-regression-harness/29-llm-only-baseline-fixture-summary.json \
  --output-json /tmp/phase29-llm-only-baseline.json \
  --mode llm-only \
  --variant baseline \
  --probe-only
```

### 5. Generate the `llm-only` candidate artifact

```bash
python3 scripts/run_phase29_llm_benchmark.py \
  --slice-json .planning/phases/29-llm-benchmark-gains-and-regression-harness/29-benchmark-slice.json \
  --summary-json .planning/phases/29-llm-benchmark-gains-and-regression-harness/29-llm-only-candidate-fixture-summary.json \
  --output-json /tmp/phase29-llm-only-candidate.json \
  --mode llm-only \
  --variant candidate \
  --probe-only
```

### 6. Compare the `llm-only` pair

```bash
python3 scripts/check_phase29_benchmark_delta.py \
  --baseline-artifact /tmp/phase29-llm-only-baseline.json \
  --candidate-artifact /tmp/phase29-llm-only-candidate.json \
  --status-json /tmp/phase29-llm-only-status.json \
  --mode llm-only \
  --probe-only
```

This probe-only path should pass without launching a live benchmark.

## Live Candidate Replay

Use this when you want current candidate evidence on a supported host. The baseline remains frozen to the April 2 anchors above; only the candidate side should be replayed here.

### Contract rules

Keep these values identical across both live candidate replays:

- the same `--slice-json`
- the same `model_name` and `base_url`
- the same `build_profile`
- the same `cache_state`

The intentional change between the two candidate replays is only `--mode` (`llm` versus `llm-only`).

### Replay the `llm` candidate

```bash
python3 scripts/run_phase29_llm_benchmark.py \
  --slice-json .planning/phases/29-llm-benchmark-gains-and-regression-harness/29-benchmark-slice.json \
  --output-json /tmp/phase29-live-llm-candidate.json \
  --mode llm \
  --variant candidate \
  --model-name qwen3.5:9b \
  --base-url http://localhost:11434 \
  --build-profile standard \
  --cache-state unknown \
  --execute-live
```

### Replay the `llm-only` candidate

```bash
python3 scripts/run_phase29_llm_benchmark.py \
  --slice-json .planning/phases/29-llm-benchmark-gains-and-regression-harness/29-benchmark-slice.json \
  --output-json /tmp/phase29-live-llm-only-candidate.json \
  --mode llm-only \
  --variant candidate \
  --model-name qwen3.5:9b \
  --base-url http://localhost:11434 \
  --build-profile standard \
  --cache-state unknown \
  --execute-live
```

### Compare live candidates against the frozen baselines

```bash
python3 scripts/check_phase29_benchmark_delta.py \
  --baseline-artifact .planning/phases/29-llm-benchmark-gains-and-regression-harness/29-llm-baseline-sample.json \
  --candidate-artifact /tmp/phase29-live-llm-candidate.json \
  --status-json /tmp/phase29-live-llm-status.json \
  --mode llm \
  --probe-only

python3 scripts/check_phase29_benchmark_delta.py \
  --baseline-artifact .planning/phases/29-llm-benchmark-gains-and-regression-harness/29-llm-only-baseline-sample.json \
  --candidate-artifact /tmp/phase29-live-llm-only-candidate.json \
  --status-json /tmp/phase29-live-llm-only-status.json \
  --mode llm-only \
  --probe-only
```

## Contract Parity Checks

The checker must confirm all of the following:

- same `slice_id`
- same ordered case set
- same model and base URL
- same build profile and cache state
- same top-level benchmark mode
- same `resultOrigin` coverage
- at least one meaningful correctness or regression delta

## Known Caveats

- The frozen proof package is deterministic and fixed-slice scoped; it is not the final live recommendation.
- The current fixed slice shows `llm-only` gains, but it still keeps `llm-no-output`, provider-tooling instability, and `docker-infrastructure-failure` visible when they occur.
- Phase 30 owns the live evidence pack, representative case selection, and the final ship recommendation.
